import sys
import re
import os
import pandas as pd
import json
import hashlib
import glob
from pathlib import Path
from PySide6.QtWidgets import (QApplication, QMainWindow, QTableView, QStyledItemDelegate, QComboBox, QButtonGroup, QStackedWidget,
                               QVBoxLayout, QHBoxLayout, QPushButton, QFrame, QHeaderView, QTextEdit, QListWidgetItem, QTableWidget, QTableWidgetItem,
                               QWidget, QLabel, QLineEdit, QFileDialog, QCheckBox, QDateEdit, QMessageBox, QStyleOptionViewItem, QStyle, QSpacerItem,
                               QMenu, QDialog, QProgressBar, QDialogButtonBox, QSplitter, QListWidget, QFormLayout, QScrollArea, QDockWidget,
                               QSizePolicy)
from PySide6.QtCore import QAbstractTableModel, Qt, QSortFilterProxyModel, QDate, QEvent, QTimer, QObject, Signal, QThread, QModelIndex, QProcess
from PySide6.QtGui import QKeySequence, QAction, QColor, QPalette
from datetime import datetime
import subprocess
import csv
from collections import defaultdict

# to do
# Notes manager:
# - implement reply-to stuff
# - get status filter checkboxes in place and have default to showing Active
# - get simple and advanced text filtering from main window into this one
# Rclone:
# - can't get Code shell to see rclone. works fine from regular powershell and compiled app
# - not doing copy on windows
# Playlists:
# - doesn't create timestamped submission
# - doesn't give user dialog for Custom
# end to do

class ClipboardHelper:
    @staticmethod
    def copy_table_selection(table_view):
        """Standardized multi-cell copy for any QTableView."""
        selection = table_view.selectionModel().selectedIndexes()
        if not selection:
            return

        # Sort by row, then by column
        selection.sort(key=lambda x: (x.row(), x.column()))

        copy_data = []
        current_row = -1
        row_contents = []

        for idx in selection:
            if idx.row() != current_row:
                if row_contents:
                    copy_data.append("\t".join(row_contents))
                row_contents = []
                current_row = idx.row()
            
            # Use DisplayRole to get what the user actually sees
            val = idx.data(Qt.DisplayRole)
            row_contents.append(str(val) if val is not None else "")

        if row_contents:
            copy_data.append("\t".join(row_contents))

        clipboard_text = "\n".join(copy_data)
        QApplication.clipboard().setText(clipboard_text)

class CSVImporter:
    """The 'Mundane' CSV implementation of our import contract."""
    def __init__(self, file_path):
        self.file_path = file_path

    def get_raw_df(self):
        try:
            return pd.read_csv(self.file_path, encoding='cp1252', dtype=str).fillna("")
        except Exception as e:
            print(f"Import Error: {e}")
            return None

class AdvancedImportMapperDialog(QDialog):
    def __init__(self, incoming_df, target_cols, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Surgical Data Importer")
        self.resize(1200, 800)
        
        self.incoming_df = incoming_df
        self.target_cols = target_cols
        self.column_mapping_widgets = []

        layout = QVBoxLayout(self)
        
        # --- HEADER SECTION ---
        header_info = QLabel("<b>Mapping Engine:</b> Match columns to Project Headers. Redundant mappings are blocked.")
        header_info.setStyleSheet("color: #888; margin-bottom: 5px;")
        layout.addWidget(header_info)

        # --- THE TABLE (The Driver) ---
        self.table = QTableView()
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableView.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(True)
        # Make the table expand with the window
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        
        self.model = PandasModel(self.incoming_df, self)
        self.table.setModel(self.model)

        # --- THE MAPPING BAR (The Passenger) ---
        self.mapping_bar = QWidget()
        self.mapping_bar.setStyleSheet("background-color: #222;")
        self.mapping_layout = QHBoxLayout(self.mapping_bar)
        self.mapping_layout.setContentsMargins(0, 5, 0, 5)
        self.mapping_layout.setSpacing(0)
        
        # Spacer for vertical header
        self.v_spacer = QSpacerItem(self.table.verticalHeader().width(), 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        self.mapping_layout.addSpacerItem(self.v_spacer)

        for i, col_name in enumerate(self.incoming_df.columns):
            combo = QComboBox()
            # We use a custom "Update" method to handle the exclusion logic
            combo.addItems(["-- Skip --"] + self.target_cols)
            combo.currentIndexChanged.connect(self.validate_unique_mappings)
            
            # Initial Smart Guess
            if col_name.upper() in [t.upper() for t in self.target_cols]:
                match = next(t for t in self.target_cols if t.upper() == col_name.upper())
                combo.setCurrentText(match)
            
            self.column_mapping_widgets.append(combo)
            self.mapping_layout.addWidget(combo)

        layout.addWidget(self.mapping_bar)
        layout.addWidget(self.table)

        # --- FOOTER ---
        footer = QHBoxLayout()
        footer.addWidget(QLabel("<b>Index/Match Column:</b>"))
        self.index_combo = QComboBox()
        self.index_combo.addItems(self.target_cols)
        if "SHOTNAME" in self.target_cols: self.index_combo.setCurrentText("SHOTNAME")
        footer.addWidget(self.index_combo)
        footer.addStretch()

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        footer.addWidget(self.buttons)
        layout.addLayout(footer)

        # --- EVENT LINKING ---
        # Sync widths when columns are resized OR window is resized
        self.table.horizontalHeader().sectionResized.connect(self.sync_column_widths)
        # Force an initial sync after the window renders
        QTimer.singleShot(50, self.sync_column_widths)
        
        # Run initial validation
        self.validate_unique_mappings()

    def resizeEvent(self, event):
        """Ensures the mapping bar stays in sync when the user stretches the window."""
        super().resizeEvent(event)
        self.sync_column_widths()

    def sync_column_widths(self):
        """Strictly aligns dropdowns to the table's header geometry."""
        # Update the leading spacer to match current vertical header width
        v_width = self.table.verticalHeader().width()
        self.v_spacer.changeSize(v_width, 20, QSizePolicy.Fixed, QSizePolicy.Minimum)
        
        for i, combo in enumerate(self.column_mapping_widgets):
            w = self.table.columnWidth(i)
            combo.setFixedWidth(w)
        self.mapping_layout.invalidate()

    def validate_unique_mappings(self):
        """Prevents the user from selecting the same target header twice."""
        # 1. See what is currently selected across all boxes
        selected_targets = [
            c.currentText() for c in self.column_mapping_widgets 
            if c.currentText() != "-- Skip --"
        ]
        
        # 2. Check for duplicates to disable the OK button
        has_duplicates = len(selected_targets) != len(set(selected_targets))
        self.buttons.button(QDialogButtonBox.Ok).setEnabled(not has_duplicates)
        
        # 3. Visual feedback: Turn duplicated combos red
        for combo in self.column_mapping_widgets:
            val = combo.currentText()
            if val != "-- Skip --" and selected_targets.count(val) > 1:
                combo.setStyleSheet("background-color: #662222; color: white;")
                combo.setToolTip(f"Duplicate mapping: '{val}' is assigned multiple times!")
            else:
                combo.setStyleSheet("background-color: #333; color: #eee;")
                combo.setToolTip("")

    def get_map_config(self):
        final_map = {}
        for i, combo in enumerate(self.column_mapping_widgets):
            target = combo.currentText()
            if target != "-- Skip --":
                final_map[target] = self.incoming_df.columns[i]
        return final_map, self.index_combo.currentText()

class AddTempRootDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Add Temporary Data Root")
        self.setMinimumWidth(400)
        
        layout = QVBoxLayout(self)
        
        # Name Input
        layout.addWidget(QLabel("Root Name (e.g., usb_drive, client_drop):"))
        self.edit_name = QLineEdit()
        layout.addWidget(self.edit_name)
        
        # Path Input
        layout.addWidget(QLabel("Directory Path:"))
        path_layout = QHBoxLayout()
        self.edit_path = QLineEdit()
        self.btn_browse = QPushButton("Browse...")
        self.btn_browse.clicked.connect(self.browse_path)
        
        path_layout.addWidget(self.edit_path)
        path_layout.addWidget(self.btn_browse)
        layout.addLayout(path_layout)
        
        # Buttons
        btn_layout = QHBoxLayout()
        self.btn_ok = QPushButton("Add Temp Root")
        self.btn_ok.setStyleSheet("background-color: #2563eb; color: white; font-weight: bold;")
        self.btn_ok.clicked.connect(self.validate_and_accept)
        
        self.btn_cancel = QPushButton("Cancel")
        self.btn_cancel.clicked.connect(self.reject)
        
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_cancel)
        btn_layout.addWidget(self.btn_ok)
        layout.addLayout(btn_layout)

    def browse_path(self):
        directory = QFileDialog.getExistingDirectory(self, "Select Directory")
        if directory:
            self.edit_path.setText(directory)

    def validate_and_accept(self):
        name = self.edit_name.text().strip()
        path = self.edit_path.text().strip()
        
        if not name or not path:
            QMessageBox.warning(self, "Missing Info", "Both Name and Path are required.")
            return
            
        import re
        if not re.match(r"^[a-zA-Z0-9_]+$", name):
            QMessageBox.warning(self, "Invalid Name", "Name can only contain letters, numbers, and underscores (no spaces).")
            return
            
        import os
        if not os.path.exists(path):
            QMessageBox.warning(self, "Invalid Path", "The specified directory does not exist.")
            return
            
        self.accept()

    def get_data(self):
        return self.edit_name.text().strip(), self.edit_path.text().strip()
    
class GlobalPointerDelegate(QStyledItemDelegate):
    def __init__(self, engine, csv_path, parent=None):
        super().__init__(parent)
        self.engine = engine
        self.csv_path = csv_path
        
        # Figure out the relative filename so the engine knows what to look for
        try:
            self.rel_filename = os.path.relpath(self.csv_path, self.engine.root).replace('\\', '/')
        except ValueError:
            self.rel_filename = os.path.basename(self.csv_path)

    def displayText(self, value, locale):
        """Intercepts {LOCALDOTDIR} and asks the engine for the resolved value."""
        text = super().displayText(value, locale)
        
        if str(text).strip().upper() == "{LOCALDOTDIR}":
            # We need the 'Key' for this row to ask the engine for the value.
            # We assume the Key is always in column 0 (which is standard for all your config files)
            model = self.parent().model()
            # Because displayText doesn't give us the index, this is a visual-only trick. 
            # It's safer to just do a direct global lookup!
            
            # Since displayText doesn't know its row, we just do a generic "Inheriting" label
            # UNLESS we can safely peek at the model (which requires paint override). 
            # For pure displayText, we return a standard string.
            return "{LOCALDOTDIR}  ⮎ (Global Default)"
            
        return text

    def paint(self, painter, option, index):
        """Surgically overrides paint to show the ACTUAL resolved value."""
        text = str(index.data(Qt.DisplayRole)).strip().upper()
        
        if text == "{LOCALDOTDIR}":
            # 1. Grab the Key from column 0 of the same row
            model = index.model()
            key = str(model.index(index.row(), 0).data(Qt.DisplayRole))
            
            # 2. Ask the Engine to resolve it
            resolved_val = self.engine._resolve_pointer(key, text, self.rel_filename)
            
            # 3. Format the display string
            display_text = f"{{LOCALDOTDIR}}  ⮎  {resolved_val}"
            
            # 4. Paint it with a slightly dimmed color so they know it's a pointer
            painter.save()
            # Draw selection background if needed
            if option.state & QStyle.State_Selected:
                painter.fillRect(option.rect, option.palette.highlight())
                painter.setPen(option.palette.highlightedText().color())
            else:
                painter.setPen(QColor(130, 130, 130)) # Dim grey text
                
            # Draw the text
            rect = option.rect.adjusted(3, 0, -3, 0) # Add a tiny bit of padding
            painter.drawText(rect, Qt.AlignVCenter | Qt.AlignLeft, display_text)
            painter.restore()
            return
            
        super().paint(painter, option, index)

class GenericCSVEditor(QDialog):
    def __init__(self, csv_path, title="CSV Editor", parent=None, allow_add_column=False, dropdown_cols=None, engine=None, allow_file_browse=False):
        super().__init__(parent)
        self.csv_path = os.path.normpath(csv_path)
        self.allow_add_column = allow_add_column
        self.dropdown_cols = dropdown_cols or {}
        self.engine = engine
        self.allow_file_browse = allow_file_browse
        
        self.setWindowTitle(f"{title} - {os.path.basename(self.csv_path)}")
        self.resize(1000, 500)
        
        layout = QVBoxLayout(self)
        
        # 1. Load Data
        try:
            self.df = pd.read_csv(self.csv_path, encoding='cp1252', dtype=str).fillna("")
        except Exception as e:
            self.df = pd.DataFrame()
            layout.addWidget(QLabel(f"Error loading CSV: {e}"))
            return

        # 2. Setup Table & Model
        self.table = QTableView()
        self.table.setAlternatingRowColors(True)
        self.model = PandasModel(self.df, self, read_only=False) 
        self.table.setModel(self.model)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.horizontalHeader().setStretchLastSection(True) 
        self.table.resizeColumnsToContents()
        
        # --- SURGICAL INJECTION: Apply the Global Preview Delegate ---
        if self.engine:
            self.preview_delegate = GlobalPointerDelegate(self.engine, self.csv_path, self.table)
            for i in range(self.model.columnCount()):
                self.table.setItemDelegateForColumn(i, self.preview_delegate)
                
        # 3. Apply the dropdown delegates (This will overwrite the preview delegate on specific columns, which is fine!)
        self._apply_dropdown_delegates()
        
        layout.addWidget(self.table)
        
        # 3. Row Controls (Add/Delete)
        row_ctrl = QHBoxLayout()
        btn_add = QPushButton("+ Add Row")
        btn_add.clicked.connect(self.add_row)
        # --- NEW: Optional Column Button ---
        if allow_add_column:
            btn_add_col = QPushButton("+ Add Column")
            btn_add_col.clicked.connect(self.add_column)
            row_ctrl.addWidget(btn_add_col)
        # ------------------------------------
        btn_label = "- Delete Selected (Row/Col)" if allow_add_column else "- Delete Selected Rows"
        btn_del = QPushButton(btn_label)
        btn_del.setStyleSheet("background-color: #882e2e; color: white;")
        btn_del.clicked.connect(self.delete_selected_structure)
        row_ctrl.addWidget(btn_add)
        row_ctrl.addWidget(btn_del)
        if self.allow_file_browse:
            btn_browse = QPushButton("📁 Choose File for Selected Cell")
            btn_browse.setStyleSheet("background-color: #2e5a88; color: white; font-weight: bold;")
            btn_browse.clicked.connect(self.browse_for_cell)
            row_ctrl.addWidget(btn_browse)
            
        row_ctrl.addStretch()
        layout.addLayout(row_ctrl)

        # 4. Save/Cancel Buttons
        self.buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.save_and_close)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def browse_for_cell(self):
        """Opens a file dialog and injects the path into the currently selected cell."""
        index = self.table.selectionModel().currentIndex()
        if not index.isValid():
            QMessageBox.information(self, "Select Cell", "Please click a cell in the 'Value' column first.")
            return
            
        # Give them a file picker (All Files so they can grab .exe, .app, shell scripts, etc.)
        from PySide6.QtWidgets import QFileDialog
        file_path, _ = QFileDialog.getOpenFileName(self, "Choose Executable or File", "", "All Files (*)")
        
        if file_path:
            # Force standard forward slashes for pipeline safety
            file_path = file_path.replace("\\", "/")
            # Inject directly into the model!
            self.model.setData(index, file_path, Qt.EditRole)

    def _apply_dropdown_delegates(self):
        """Surgically applies the StatusDelegate to requested columns."""
        if not self.dropdown_cols: return
        
        for col_name, options in self.dropdown_cols.items():
            if col_name in self.df.columns:
                col_idx = self.df.columns.get_loc(col_name)
                # Reusing your existing StatusDelegate!
                self.table.setItemDelegateForColumn(col_idx, StatusDelegate(options, self.table))

    def add_row(self):
        self.model.beginResetModel()
        # Create new row with empty strings
        new_row = {col: "" for col in self.df.columns}
        new_df = pd.DataFrame([new_row]).astype(str) # Force new row to string
        
        # Concatenate and IMMEDIATELY force the whole thing back to string 
        # to prevent any column from flipping to float/int
        self.df = pd.concat([self.df, new_df], ignore_index=True).astype(str)
        
        self.model._data = self.df
        self.model.endResetModel()

    def add_column(self):
        from PySide6.QtWidgets import QInputDialog
        col_name, ok = QInputDialog.getText(self, "New Column", "Enter Column Name (e.g. 'aws', 'home_pc'):")
        
        if ok and col_name:
            col_name = col_name.strip()
            if col_name in self.df.columns:
                QMessageBox.warning(self, "Duplicate", "Column already exists!")
                return

            self.model.beginResetModel()
            # Add the column to the DataFrame with empty strings
            self.df[col_name] = ""
            # Update the model reference
            self.model._data = self.df
            self.model.endResetModel()
            self.table.resizeColumnsToContents()
            
    def delete_selected_structure(self):
        selection_model = self.table.selectionModel()
        
        # 1. Grab FULL rows
        selected_rows = selection_model.selectedRows()
        
        # 2. Grab FULL columns ONLY if allowed
        selected_cols = []
        if getattr(self, 'allow_add_column', False): # Guard check
            for c in range(self.model.columnCount()):
                # Use PySide6.QtCore.QModelIndex if you didn't add it to imports yet
                if selection_model.isColumnSelected(c, QModelIndex()):
                    selected_cols.append(c)

        # 3. THE WALL: If nothing structural is selected (or cols were blocked)
        if not selected_rows and not selected_cols:
            msg = "Nothing to delete!\n\nPlease click the Row Numbers (left) to select rows."
            if getattr(self, 'allow_add_column', False):
                msg += " or Column Headers (top) to select columns."
            
            QMessageBox.information(self, "Selection Required", msg)
            return

        # 4. Column Deletion (Only enters if selected_cols was populated)
        if selected_cols:
            # ... (Existing Protected Headers check) ...
            col_names = [self.df.columns[c] for c in selected_cols]
            if QMessageBox.question(self, "Confirm", f"Delete: {', '.join(col_names)}?") == QMessageBox.Yes:
                self.model.beginResetModel()
                self.df.drop(columns=col_names, inplace=True)
                self.model._data = self.df
                self.model.endResetModel()
                return

        # 5. Row Deletion
        if selected_rows:
            if QMessageBox.question(self, "Confirm Delete", 
                f"Delete {len(selected_rows)} selected row(s)?", 
                QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
                
                indices = sorted([idx.row() for idx in selected_rows], reverse=True)
                self.model.beginResetModel()
                self.df.drop(self.df.index[indices], inplace=True)
                self.df.reset_index(drop=True, inplace=True)
                self.model._data = self.df
                self.model.endResetModel()

    def save_and_close(self):
        try:
            self.df.to_csv(self.csv_path, index=False, encoding='cp1252')
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Save Error", f"Could not save CSV:\n{e}")

    def reject(self):
        """Intercept Cancel to drop the model before hiding the UI."""
        if hasattr(self, 'table') and self.table:
            self.table.setModel(None)
        super().reject()

    def closeEvent(self, event):
        """Ensure C++ table drops Python model before destruction."""
        if hasattr(self, 'table') and self.table:
            self.table.setModel(None)
        # No preview_table in this class, so we can remove that elif
        super().closeEvent(event)

class PaddingNomBuilder:
    @staticmethod
    def build(raw_pad, style="printf"):
        """
        The Single Source of Truth for VFX sequence padding formatting.
        Kills NaNs, fixes single digits, and translates to app-specific nomenclature.
        """
        # 1. The Sanity Check (Kill NaN/Ghosts)
        padding_val = str(raw_pad).strip() if pd.notna(raw_pad) and str(raw_pad).strip() else ""
        
        # 2. The Strict Formatter (e.g., '4' -> '04')
        if padding_val.isdigit() and len(padding_val) == 1:
            padding_val = f"0{padding_val}"

        # 3. The Output Switchboard
        if style == "printf":
            # Nuke/Maya/Standard C-style: %04d
            return f"%{padding_val}d" if padding_val else "%d"
            
        elif style == "hash":
            # Nuke alt/RV style: ####
            if not padding_val: return "#"
            # Strip the '0' prefix for the multiplier to avoid octal math weirdness, default to 1
            return "#" * int(padding_val.lstrip('0') or 1)
            
        elif style == "houdini":
            # Houdini style: $F4
            if not padding_val: return "$F"
            return f"$F{padding_val.lstrip('0')}"
            
        else:
            # Failure-centric: Unrecognized style returns the raw parsed value
            return padding_val
       
class TextInjectionEngine:
    @staticmethod
    def extract_variables(filepath):
        """Finds any contiguous block of 4+ uppercase letters."""
        if not os.path.exists(filepath): 
            return set()
            
        with open(filepath, 'r') as f:
            content = f.read()
        
        # Simple, contiguous uppercase lookup
        matches = set(re.findall(r'[A-Z]{4,}', content))
        return matches

    @staticmethod
    def inject(source_path, target_path, mapping_dict):
        """Performs replacement, skipping variables set to IGNORE."""
        with open(source_path, 'r') as f:
            content = f.read()

        # Sort by length descending to prevent partial matches (e.g., 'SHOT' in 'SHOTNAME')
        for var in sorted(mapping_dict.keys(), key=len, reverse=True):
            val = mapping_dict[var]
            
            # If the UI set this to IGNORE, we do nothing to this string
            if val == "<<IGNORE>>":
                continue
                
            content = content.replace(var, str(val))

        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, 'w') as f:
            f.write(content)
        return True

class TemplateVariableResolver:
    def __init__(self, engine, mapping_df, app_window=None):
        self.engine = engine
        self.mapping_df = mapping_df # Now takes a DF directly
        self.app = app_window

    def get_resolved_map(self, row_dict, action_config=None):
        """THE ONE SOURCE OF TRUTH: Resolves a single row of data."""
        if action_config is None: action_config = {}
        final_map = {}
        
        for _, row in self.mapping_df.iterrows():
            var = row['Variable']
            src = row['Source_Type']
            key = str(row['Lookup_Key']).strip()
            val = "UNRESOLVED"

            if src == "IGNORE":
                val = "<<IGNORE>>"
            elif src == "HEADER":
                val = row_dict.get(key, "MISSING_COL")
            elif src == "CONFIG":
                val = self.engine.settings.get(key, "MISSING_SETTING")
            elif src == "CONSTANT":
                val = key
            elif src == "SCAN":
                shot = row_dict.get('SHOTNAME')
                if self.app and hasattr(self.app, 'resolve_scan_path'):
                    val = self.app.resolve_scan_path(shot) or "SCAN_NOT_FOUND"
                else:
                    val = "RESOLVER_NOT_FOUND"
            
            # --- THE GRAVITY INJECTION: EXPLICIT NUKE_WRITE ---
            elif src == "NUKE_WRITE":
                r_path = action_config.get('nuke_comp_render_path', "MISSING_COMP_PATH")
                r_file = action_config.get('nuke_comp_render_filename', "MISSING_COMP_FILE")
                
                template = f"{r_path}/{r_file}".replace("//", "/")
                
                # --- SURGICAL PADDING: Catch ALL padding variables ---
                for k, v in self.engine.settings.items():
                    if k.startswith('padding_') and f"{{{k}}}" in template:
                        padding_nomenclature = PaddingNomBuilder.build(v, style="printf")
                        template = template.replace(f"{{{k}}}", padding_nomenclature)
                
                # Resolve the remaining placeholders ({SHOTNAME}, {data_root}, etc.)
                ctx = {**self.engine.settings, **row_dict}
                for k in sorted(ctx.keys(), key=len, reverse=True):
                    placeholder = f"{{{k}}}"
                    if placeholder in template:
                        template = template.replace(placeholder, str(ctx[k]))
                val = template

            elif src == "NAMING":
                template = self.engine.naming_templates.get(key, "")
                ctx = {**self.engine.settings, **row_dict}
                for k in sorted(ctx.keys(), key=len, reverse=True):
                    placeholder = f"{{{k}}}"
                    if placeholder in template:
                        template = template.replace(placeholder, str(ctx[k]))
                val = template

            if isinstance(val, str) and src != "IGNORE":
                val = val.replace("\\", "/")
            final_map[var] = val
            
        return final_map

class TemplateMappingWidget(QWidget):
    # SURGICAL FIX: Change 'source_dfs' to 'manager_df' in the signature
    def __init__(self, engine, template_id, template_path, map_csv_path, manager_df, active_shotname=None, parent=None):
        super().__init__(parent)
        self.engine = engine
        self.template_path = template_path
        self.map_csv_path = map_csv_path
        
        # --- SURGICAL FIX: The data is already unified. Just copy it. ---
        self.full_context_df = manager_df.copy()
        
        # Find the index for our preview shot in the DF
        self.active_row_idx = 0
        if active_shotname:
            matches = self.full_context_df.index[self.full_context_df['SHOTNAME'] == active_shotname].tolist()
            if matches:
                self.active_row_idx = matches[0]
        
        self.setWindowTitle(f"Template Mapping: {template_id}")
        self.resize(1200, 600)
        
        layout = QVBoxLayout(self)

        # 1. DATA PREP: Sync variables before loading
        self.sync_variables_on_disk()
        
        # 2. LOAD DF
        self.df_map = pd.read_csv(self.map_csv_path, dtype=str).fillna("")
        
        # 3. ADD LIVE PREVIEW
        self.df_map['Live_Preview'] = self.df_map.apply(self.mock_resolve, axis=1)

        # 4. TABLE SETUP
        self.table = QTableView()
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableView.AllEditTriggers)
        
        self.model = PandasModel(self.df_map, self, read_only=False)
        self.model.flags = self.mapping_table_flags 
        self.table.setModel(self.model)

        self.model.dataChanged.connect(self.on_cell_edited)

        # 5. APPLY DELEGATE TO ALL COLUMNS
        # SURGICAL FIX: Removed generic options, added strict NUKE_WRITE
        self.options = ["HEADER", "CONFIG", "SCAN", "NAMING", "CONSTANT", "IGNORE", "NUKE_WRITE"]
        
        self.row_delegate = CSVEditorDelegate(self.options, self.table)
        
        for i in range(self.model.columnCount()):
            self.table.setItemDelegateForColumn(i, self.row_delegate)

        layout.addWidget(QLabel(f"<b>Template:</b> {template_path}"))
        layout.addWidget(self.table)

        self.table.resizeColumnsToContents()

    def keyPressEvent(self, event):
        """Surgically intercepts Copy before the Editor can steal the focus."""
        
        # 1. Catch the Copy Command (Cmd+C / Ctrl+C)
        if event.matches(QKeySequence.Copy):
            # 2. FORCE-CLOSE any active editor in the table 
            # This prevents the "last cell is currently being edited" ghosting.
            if self.table.indexWidget(self.table.currentIndex()):
                self.table.commitData(self.table.indexWidget(self.table.currentIndex()))
            
            # 3. Perform the centralized copy
            ClipboardHelper.copy_table_selection(self.table)
            
            # 4. Optional: Feedback via status bar
            curr = self
            while curr:
                if hasattr(curr, 'statusBar') and curr.statusBar():
                    curr.statusBar().showMessage("Copied selection to clipboard", 2000)
                    break
                curr = curr.parent()
            
            # IMPORTANT: Accept the event so it doesn't trigger the default Qt edit behavior
            event.accept()
            return 

        # Let all other keys (Enter, Arrows, etc.) behave normally
        super().keyPressEvent(event)

    def mapping_table_flags(self, index):
        if not index.isValid(): return Qt.NoItemFlags
        col_name = self.df_map.columns[index.column()]
        
        # 1. Hard Locks
        if col_name in ['Variable', 'Live_Preview']:
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable
            
        # 2. Lookup_Key is ALWAYS editable (Failure-centric: User can type notes if they want)
        # It will just be Greyed Out visually if Source is SCAN to show it's redundant.
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable

    def mock_resolve(self, row_map):
        """Wrapper that uses the REAL TemplateVariableResolver with polite failure handling."""
        
        # 1. POLITE FAILURE: Check if we actually have any shots loaded in the Asset Manager
        if self.full_context_df.empty:
            return "Preview not available: No shot data loaded."

        app_window = None
        curr = self
        while curr:
            if hasattr(curr, 'resolve_scan_path'):
                app_window = curr
                break
            curr = curr.parent()

        # Try the LIVE UI first, fallback to disk
        action_config = {}
        if hasattr(self.parent(), 'get_live_config'):
            action_config = self.parent().get_live_config()
        else:
            config_path = os.path.join(os.path.dirname(self.map_csv_path), "config.csv")
            if os.path.exists(config_path):
                df_cfg = pd.read_csv(config_path).fillna("")
                action_config = dict(zip(df_cfg['Key'], df_cfg['Value']))

        resolver = TemplateVariableResolver(self.engine, pd.DataFrame([row_map]), app_window=app_window)
        
        try:
            # 2. POLITE FAILURE: Ensure the active row index is actually valid
            if self.active_row_idx >= len(self.full_context_df):
                return "Preview not available: Selected shot index out of range."
                
            current_data = self.full_context_df.iloc[self.active_row_idx].to_dict()
            resolved_dict = resolver.get_resolved_map(current_data, action_config=action_config)
            
            val = resolved_dict.get(row_map['Variable'], "ERR")
            
            # 3. POLITE FAILURE: Translate the Engine's raw missing codes into human phrases
            lookup_key = row_map.get('Lookup_Key', 'Unknown')
            if val == "MISSING_COL":
                return f"Preview not available: Missing column '{lookup_key}' in Shot data."
            elif val == "MISSING_SETTING":
                return f"Preview not available: Missing '{lookup_key}' in Project Settings."
            elif val in ["SCAN_NOT_FOUND", "RESOLVER_NOT_FOUND"]:
                return f"Preview not available: Could not locate scan path for this shot."
            elif val == "MISSING_COMP_PATH":
                return "Preview not available: 'Output_Template_path' missing in Nuke Config."
            elif val == "MISSING_COMP_FILE":
                return "Preview not available: 'Output_Template_file' missing in Nuke Config."
                
            return "[ SKIPPED ]" if val == "<<IGNORE>>" else val
            
        except Exception as e:
            # Catch-all for any truly unexpected errors (like unparsed math or bad file formats)
            return f"Preview not available: {str(e)}"
    
    def refresh_previews(self):
        """Forces the Live Preview column to recalculate and repaint."""
        # 1. Recalculate the entire Live_Preview column
        self.df_map['Live_Preview'] = self.df_map.apply(self.mock_resolve, axis=1)
        
        # 2. Tell the model exactly which column changed so it redraws
        if 'Live_Preview' in self.df_map.columns:
            preview_col_idx = self.df_map.columns.get_loc('Live_Preview')
            top_left = self.model.index(0, preview_col_idx)
            bottom_right = self.model.index(self.model.rowCount() - 1, preview_col_idx)
            self.model.dataChanged.emit(top_left, bottom_right, [Qt.DisplayRole])
            self.table.viewport().update()

    def execute_template(self, template_data, manager_df, parent_window=None):
        """Iterates through selected shots and generates Nuke scripts."""
        selected_rows = manager_df[manager_df['Select'] == True]
        if selected_rows.empty:
            return

        mapping_df = pd.read_csv(template_data['Mapping_CSV'], dtype=str).fillna("")
        resolver = TemplateVariableResolver(self.engine, mapping_df, app_window=self.app)
        
        source_nk = PathSwapper.translate(template_data['Source_Path'])
        output_tpl = template_data['Output_Template']
        
        for _, row in selected_rows.iterrows():
            row_dict = row.to_dict()
            mapping_dict = resolver.get_resolved_map(row_dict)
            
            # Resolve Output Path
            output_path = output_tpl
            ctx = {**self.engine.settings, **row_dict}
            for k in sorted(ctx.keys(), key=len, reverse=True):
                placeholder = f"{{{k}}}"
                if placeholder in output_path:
                    output_path = output_path.replace(placeholder, str(ctx[k]))
            
            target_path = PathSwapper.translate(output_path)
            TextInjectionEngine.inject(source_nk, target_path, mapping_dict)

    def on_cell_edited(self, top_left, bottom_right):
        """Triggered whenever a cell is finished being edited."""
        row = top_left.row()
        col = top_left.column()
        
        # We don't want an infinite loop! 
        # Only recalculate if we edited something OTHER than the preview column itself.
        preview_col_idx = self.df_map.columns.get_loc('Live_Preview')
        
        if col != preview_col_idx:
            # 1. Grab the updated row data from the DataFrame
            row_data = self.df_map.iloc[row]
            
            # 2. Run the mock_resolve again for this specific row
            updated_preview = self.mock_resolve(row_data)
            
            # 3. Update the DataFrame silently (without triggering another signal yet)
            self.df_map.iat[row, preview_col_idx] = updated_preview
            
            # 4. Tell the model that the PREVIEW cell has changed so it repaints
            preview_index = self.model.index(row, preview_col_idx)
            self.model.dataChanged.emit(preview_index, preview_index, [Qt.DisplayRole])
            
            # 5. Force the viewport update for the IGNORE grey-out logic
            self.table.viewport().update()

    def sync_variables_on_disk(self):
        """Dumb extraction: 4+ uppercase letters. Now fails gracefully if file is missing."""
        content = ""
        found_vars = set()
        
        # --- SURGICAL FIX: Wrap file I/O in a check ---
        if os.path.exists(self.template_path):
            try:
                with open(self.template_path, 'r') as f:
                    content = f.read()
                found_vars = set(re.findall(r'[A-Z]{4,}', content))
            except Exception as e:
                print(f"Error reading template file: {e}")
        else:
            # If the file doesn't exist, we just proceed with found_vars as an empty set.
            # This allows the UI to open so the user can fix the path.
            print(f"Template path not found: {self.template_path}")

        if os.path.exists(self.map_csv_path):
            df = pd.read_csv(self.map_csv_path, dtype=str).fillna("")
        else:
            # Ensure we have the correct columns even if starting fresh
            df = pd.DataFrame(columns=['Variable', 'Source_Type', 'Lookup_Key'])

        existing = df['Variable'].tolist()
        new_entries = []
        for v in found_vars:
            if v not in existing:
                # Default new variables to HEADER type
                new_entries.append({'Variable': v, 'Source_Type': 'HEADER', 'Lookup_Key': v})
        
        if new_entries:
            # Use pd.concat for modern pandas compatibility
            df = pd.concat([df, pd.DataFrame(new_entries)], ignore_index=True)
            # Only save if we actually found new things, or if the file didn't exist
            df.to_csv(self.map_csv_path, index=False)
        elif not os.path.exists(self.map_csv_path):
            # Create the empty mapping file if it doesn't exist yet
            df.to_csv(self.map_csv_path, index=False)

    def save_mapping(self):
        """Called by the parent NukeSetupDialog to write the CSV."""
        if 'Live_Preview' in self.df_map.columns:
            final_df = self.df_map.drop(columns=['Live_Preview'])
        else:
            final_df = self.df_map
            
        final_df.to_csv(self.map_csv_path, index=False)

class ExecutionPreviewDialog(QDialog):
    def __init__(self, template_path, output_paths, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Execution Preview")
        self.resize(800, 500)
        
        layout = QVBoxLayout(self)
        
        # 1. Template Information
        layout.addWidget(QLabel("<b>Source Template:</b>"))
        lbl_tpl = QLabel(template_path)
        lbl_tpl.setStyleSheet("color: #2e885a; font-family: 'Courier New', 'Menlo', monospace;")
        layout.addWidget(lbl_tpl)
        
        layout.addSpacing(10)
        
        # 2. Output Information
        layout.addWidget(QLabel(f"<b>The following {len(output_paths)} files will be generated:</b>"))
        
        self.list_widget = QListWidget()
        self.list_widget.setAlternatingRowColors(True)
        self.list_widget.setStyleSheet("QListWidget { font-family: 'Courier New', 'Menlo', monospace; font-size: 11px; }")
        for p in output_paths:
            self.list_widget.addItem(p)
        layout.addWidget(self.list_widget)
        
        # 3. Warning Text
        lbl_warn = QLabel("<i>Note: Any missing directories in these paths will be created automatically. Existing files will be overwritten.</i>")
        lbl_warn.setStyleSheet("color: #e6a822;") # Warning orange
        layout.addWidget(lbl_warn)
        
        # 4. Action Buttons
        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.btn_exec = self.buttons.button(QDialogButtonBox.Ok)
        self.btn_exec.setText("EXECUTE")
        self.btn_exec.setStyleSheet("background-color: #f37321; color: white; font-weight: bold; padding: 5px 20px;")
        
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

class NukeEngine:
    def __init__(self, core_engine, app_window=None):
        self.engine = core_engine
        self.app = app_window
        self.nuke_root = os.path.join(self.engine.root, "Project_Actions", "Nuke")
        self.master_csv = os.path.join(self.nuke_root, "nuke_templates.csv")

    def get_template_registry(self):
        if not os.path.exists(self.master_csv): return []
        df_index = pd.read_csv(self.master_csv, dtype=str).fillna("")
        registry = []

        for _, row in df_index.iterrows():
            t_id = row['Template_ID']
            config_path = os.path.join(self.nuke_root, t_id, "config.csv")
            
            if os.path.exists(config_path):
                df_cfg = pd.read_csv(config_path).fillna("")
                cfg_dict = dict(zip(df_cfg['Key'], df_cfg['Value']))
                
                registry.append({
                    'Template_ID': t_id,
                    'Source_Path': cfg_dict.get('Source_NK', ''),
                    'Mapping_CSV': os.path.join(self.nuke_root, t_id, "mapping.csv"),
                    'Config_Dict': cfg_dict # We store this to pass to the executor easily
                })
        return registry
    
    def setup_action(self, template_id, source_path, manager_df, parent_window=None):
        dlg = NukeSetupDialog(self.engine, template_id, self.nuke_root, manager_df, parent=parent_window)
        dlg.exec()

    def execute_template(self, template_data, manager_df, parent_window=None):
        """The Heavy Lifter: Merges paths, resolves variables, and writes .nk files."""
        # To this safe fallback:
        selected_rows = manager_df[manager_df['Select'] == True]
        if selected_rows.empty:
            if self.app and hasattr(self.app, 'statusBar'):
                self.app.statusBar().showMessage("No shots selected.", 3000)
            return

        # 1. Setup Data
        action_cfg = template_data['Config_Dict']
        source_nk = PathSwapper.translate(action_cfg.get('Source_NK', ''))
        out_path_raw = action_cfg.get('Output_Template_path', '')
        out_file_raw = action_cfg.get('Output_Template_file', '')
        
        mapping_df = pd.read_csv(template_data['Mapping_CSV'], dtype=str).fillna("")
        resolver = TemplateVariableResolver(self.engine, mapping_df, app_window=self.app)
        
        # 2. Pre-flight check: Build output paths
        plan = []
        for _, row in selected_rows.iterrows():
            row_dict = row.to_dict()
            ctx = {**self.engine.settings, **row_dict}
            
            # Resolve directory and filename placeholders
            dir_path = out_path_raw
            file_name = out_file_raw
            for k in sorted(ctx.keys(), key=len, reverse=True):
                ph = f"{{{k}}}"
                if ph in dir_path: dir_path = dir_path.replace(ph, str(ctx[k]))
                if ph in file_name: file_name = file_name.replace(ph, str(ctx[k]))
            
            # Combine into final absolute path
            full_target = PathSwapper.translate(os.path.join(dir_path, file_name))
            plan.append((row_dict, full_target))

        # 3. Show Preview Dialog
        preview = ExecutionPreviewDialog(source_nk, [p[1] for p in plan], parent=parent_window)
        if preview.exec() != QDialog.Accepted:
            return

        # 4. EXECUTION LOOP
        success_count = 0
        for row_dict, target_path in plan:
            try:
                # Pass the action_cfg dictionary so ACTION_CONFIG and CONFIG_CONCAT work!
                mapping_dict = resolver.get_resolved_map(row_dict, action_config=action_cfg)
                
                if TextInjectionEngine.inject(source_nk, target_path, mapping_dict):
                    success_count += 1
            except Exception as e:
                print(f"FAILED TO GENERATE {target_path}: {e}")

        if self.app and hasattr(self.app, 'statusBar'):
            self.app.statusBar().showMessage(f"Successfully generated {success_count} Nuke scripts.", 5000)

class NukeSetupDialog(QDialog):
    def __init__(self, engine, template_id, nuke_root, manager_df, parent=None):
        super().__init__(parent)
        # --- PREVIEW CONTEXT SETUP ---
        self.preview_ctx = {**engine.settings}
        # Grab the first selected shot to use as our "Preview Truth"
        selected = manager_df[manager_df['Select'] == True]
        if not selected.empty:
            self.preview_ctx.update(selected.iloc[0].to_dict())
        elif not manager_df.empty:
            self.preview_ctx.update(manager_df.iloc[0].to_dict())

        self.engine = engine
        self.template_id = template_id
        self.dir_path = os.path.join(nuke_root, template_id)
        self.config_path = os.path.join(self.dir_path, "config.csv")
        self.mapping_path = os.path.join(self.dir_path, "mapping.csv")
        
        self.setWindowTitle(f"Setup Nuke Action: {template_id}")
        self.resize(1200, 1000)
        
        layout = QVBoxLayout(self)

        # --- TOP: DYNAMIC CONFIG SECTION ---
        self.path_group = QFrame()
        self.path_group.setStyleSheet("QFrame { background-color: #2b2b2b; padding: 10px; border-radius: 4px; }")
        self.path_layout = QVBoxLayout(self.path_group)
        
        # This dict will hold our QLineEdit references: { "KeyName": QLineEdit_Object }
        self.config_widgets = {}
        self.refresh_config_ui()
        
        layout.addWidget(self.path_group)

        # --- MIDDLE: MAPPING TABLE ---
        self.mapping_table = TemplateMappingWidget(
            self.engine, template_id, self.get_config_val('Source_NK'), 
            self.mapping_path, manager_df, parent=self  # <--- SURGICAL FIX: changed source_dfs to manager_df
        )
        layout.addWidget(self.mapping_table)

        # --- BOTTOM: BUTTONS ---
        btns = QHBoxLayout()
        self.btn_save = QPushButton("Save")
        self.btn_save_exit = QPushButton("Save & Exit")
        self.btn_cancel = QPushButton("Cancel")
        
        self.btn_save.clicked.connect(self.save_all)
        self.btn_save_exit.clicked.connect(lambda: self.save_all(close=True))
        self.btn_cancel.clicked.connect(self.reject)
        
        btns.addStretch(); btns.addWidget(self.btn_save); btns.addWidget(self.btn_save_exit); btns.addWidget(self.btn_cancel)
        layout.addLayout(btns)

    def keyPressEvent(self, event):
        """Catches Enter to gracefully finish text editing without triggering buttons."""
        if event.key() in (Qt.Key_Return, Qt.Key_Enter):
            focused = self.focusWidget()
            
            # 1. If we are in a text box, 'Enter' just finishes the edit and drops focus
            if isinstance(focused, QLineEdit):
                focused.clearFocus()
                event.accept() # Swallow the key so the dialog doesn't close
                return
                
            # 2. If the user explicitly Tabbed to a button (like Save) and hit Enter, let it click
            elif isinstance(focused, QPushButton):
                super().keyPressEvent(event)
                return
                
            # 3. If focused on anything else (or nothing), swallow the Enter key safely
            event.accept()
            return

        # Let all other keys (Escape, typing, etc.) behave normally
        super().keyPressEvent(event)
        
    def refresh_config_ui(self):
        # --- THE TD CONTROL PANEL ---
        FILE_KEYS = ["Source_NK", "Overlay_TPL"]
        DIR_KEYS  = ["nope", "not_this", "proxy_that"]
        BUILD_PREVIEW = [
            "Output_Template_path", "Output_Template_file", 
            "nuke_comp_render_path", "nuke_comp_render_filename"
        ]
        
        # 1. Clear existing UI
        for i in reversed(range(self.path_layout.count())): 
            item = self.path_layout.itemAt(i)
            if item.widget(): item.widget().setParent(None)
            elif item.layout():
                while item.layout().count():
                    item.layout().itemAt(0).widget().setParent(None)

        self.config_widgets = {}
        self.preview_labels = {} # NEW: Keep track of preview labels

        # 2. Load and Build
        df = pd.read_csv(self.config_path).fillna("") if os.path.exists(self.config_path) else pd.DataFrame(columns=["Key", "Value"])

        for _, row in df.iterrows():
            key, val = row['Key'], row['Value']
            
            # --- MAIN ROW ---
            row_layout = QHBoxLayout()
            lbl = QLabel(f"{key}:")
            lbl.setFixedWidth(250)
            edit = QLineEdit(str(val))
            self.config_widgets[key] = edit
            
            row_layout.addWidget(lbl)

            if key in FILE_KEYS or key in DIR_KEYS:
                btn_browse = QPushButton("Choose...")
                btn_browse.setFixedWidth(80)
                is_dir = key in DIR_KEYS
                btn_browse.clicked.connect(lambda chk=False, e=edit, k=key, d=is_dir: self.browse_explicit(e, k, d))
                row_layout.addWidget(btn_browse)

            row_layout.addWidget(edit)
            self.path_layout.addLayout(row_layout)

            # --- PREVIEW ROW ---
            if key in BUILD_PREVIEW:
                preview_layout = QHBoxLayout()
                
                # Spacer label to push the preview text perfectly under the QLineEdit
                spacer_lbl = QLabel("")
                spacer_lbl.setFixedWidth(250)
                if key in FILE_KEYS or key in DIR_KEYS:
                    spacer_lbl.setFixedWidth(250 + 85) # Accommodate the 'Choose...' button width
                    
                lbl_preview = QLabel("Preview resolving...")
                lbl_preview.setStyleSheet("color: #777777; font-family: 'Courier New', 'Menlo', monospace; font-size: 11px;")
                
                self.preview_labels[key] = lbl_preview
                preview_layout.addWidget(spacer_lbl)
                preview_layout.addWidget(lbl_preview)
                self.path_layout.addLayout(preview_layout)

                # Connect the live update signal to a centralized handler
                edit.textChanged.connect(lambda text, k=key: self.on_config_edited(k, text))
                
                # Fire it once to set the initial preview state
                self.update_config_preview(key, str(val))

    def on_config_edited(self, key, text):
        """Handles updating both the local label and the mapping table."""
        # 1. Update the little preview label beneath the text box
        self.update_config_preview(key, text)
        
        # 2. Force the mapping table to recalculate its live preview column
        if hasattr(self, 'mapping_table'):
            self.mapping_table.refresh_previews()
            
    def get_config_val(self, key):
        """Helper for the mapping table to find the Source NK path."""
        if os.path.exists(self.config_path):
            df = pd.read_csv(self.config_path)
            match = df[df['Key'] == key]
            if not match.empty: return match.iloc[0]['Value']
        return ""

    def get_live_config(self):
        """Returns the current state of the UI text boxes as a dictionary."""
        return {key: edit.text() for key, edit in self.config_widgets.items()}
    
    def update_config_preview(self, key, text):
        """Resolves the string live as the user types, using the first selected shot."""
        if key not in self.preview_labels: return
        
        template = text
        
        # 1. Handle ALL Padding Exceptions
        for k, v in self.preview_ctx.items():
            if str(k).startswith('padding_') and f"{{{k}}}" in template:
                pad_nom = PaddingNomBuilder.build(v, style="printf")
                template = template.replace(f"{{{k}}}", pad_nom)
            
        # 2. Resolve Standard Placeholders
        for k in sorted(self.preview_ctx.keys(), key=len, reverse=True):
            placeholder = f"{{{k}}}"
            if placeholder in template:
                template = template.replace(placeholder, str(self.preview_ctx[k]))
                
        # 3. Update UI
        self.preview_labels[key].setText(f"↳ {template}")

    def browse_explicit(self, target_edit, key_name, is_dir):
        """No meatballs, just a binary path picker."""
        if is_dir:
            path = QFileDialog.getExistingDirectory(self, f"Select Folder for {key_name}")
        else:
            path, _ = QFileDialog.getOpenFileName(self, f"Select File for {key_name}", "", "All Files (*.*)")
            
        if path:
            target_edit.setText(path)

    def save_all(self, close=False):
        """Dumb save: Just takes what's in the widgets and puts it in the CSV."""
        save_data = []
        for key, edit in self.config_widgets.items():
            save_data.append([key, edit.text()])
            
        df = pd.DataFrame(save_data, columns=["Key", "Value"])
        df.to_csv(self.config_path, index=False)
        
        self.mapping_table.save_mapping()
        
        if close: self.accept()

class ProjectManagerDialog(QDialog):
    def __init__(self, engine, parent=None):
        super().__init__(parent)
        self.engine = engine
        self.parent_app = parent 
        
        # Instantiate the dedicated Engine
        self.nuke_engine = NukeEngine(self.engine, app_window=self.parent_app)
        
        self.project_name = getattr(parent, 'project_label', 'Generic Project')
        self.setWindowTitle(f"[{self.project_name}] - Project Manager")
        self.resize(1100, 700)
        
        # --- SURGICAL FIX: Keep the UI strictly to df_shots ---
        if hasattr(self.parent_app, 'df_shots'):
            self.df_manager = self.parent_app.df_shots.copy()
        else:
            self.df_manager = pd.DataFrame()
            
        if 'Select' not in self.df_manager.columns:
            self.df_manager.insert(0, 'Select', False)

        layout = QVBoxLayout(self)

        # --- 1. APP ACTIONS SECTION (TOP) ---
        actions_group = QFrame()
        actions_group.setFrameShape(QFrame.StyledPanel)
        actions_group.setStyleSheet("QFrame { background-color: #2b2b2b; border-radius: 5px; }")
        actions_main_layout = QVBoxLayout(actions_group)
        
        actions_main_layout.addWidget(QLabel("<b>App Actions</b>"))
        
        self.actions_layout = QHBoxLayout()
        # Dynamically build blocks from the Nuke Master Log
        self.build_nuke_action_blocks(self.actions_layout)
        
        # Stretch to keep blocks to the left
        self.actions_layout.addStretch()
        actions_main_layout.addLayout(self.actions_layout)
        
        layout.addWidget(actions_group)

        # --- 2. FILTERS ---
        filter_bar = QHBoxLayout()
        self.seq_filter = QComboBox()
        self.seq_filter.addItem("All")
        if not self.df_manager.empty:
            seqs = sorted([s for s in self.df_manager['SEQUENCE'].unique() if s])
            self.seq_filter.addItems(seqs)
        self.seq_filter.currentTextChanged.connect(self.apply_filters)
        
        btn_all = QPushButton("Select All")
        btn_none = QPushButton("Select None")
        btn_all.setFocusPolicy(Qt.NoFocus)
        btn_none.setFocusPolicy(Qt.NoFocus)
        btn_all.clicked.connect(lambda: self.toggle_selection(True))
        btn_none.clicked.connect(lambda: self.toggle_selection(False))

        filter_bar.addWidget(QLabel("Sequence:"))
        filter_bar.addWidget(self.seq_filter)
        filter_bar.addStretch()
        filter_bar.addWidget(btn_all); filter_bar.addWidget(btn_none)
        layout.addLayout(filter_bar)

        # --- 3. THE TABLE ---
        self.table = QTableView()
        self.model = SelectionModel(self.df_manager, self) 
        self.proxy = QSortFilterProxyModel()
        self.proxy.setSourceModel(self.model)
        self.table.setModel(self.proxy)

        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.setSelectionMode(QTableView.MultiSelection) 
        self.table.setEditTriggers(QTableView.NoEditTriggers)
        self.table.setSortingEnabled(True)
        self.table.setStyleSheet("QTableView::item:selected { background-color: #2e5a88; color: white; }")
        self.table.selectionModel().selectionChanged.connect(self.sync_checkboxes_to_selection)

        layout.addWidget(self.table)
        
        # Final UI Cleanup
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.resizeColumnsToContents()

    def get_unified_execution_data(self):
        """Safely merges UI selections with master paths without row explosion."""
        if hasattr(self.parent_app, 'df_master') and not self.df_manager.empty:
            # --- THE FIX: Strip master down to 1 row per shot before merging ---
            master = self.parent_app.df_master.drop_duplicates(subset=['SHOTNAME']).copy()
            
            unified_df = pd.merge(self.df_manager, master, on='SHOTNAME', how='left', suffixes=('', '_dup'))
            return unified_df.loc[:, ~unified_df.columns.str.endswith('_dup')]
            
        return self.df_manager.copy()
    
    def build_nuke_action_blocks(self, layout):
        """Creates a UI block for every template registered in the master log."""
        templates = self.nuke_engine.get_template_registry()
        
        # Grab the Master DF once for the whole builder
        # Note: Your AssetManager calls it 'df_master'
        master_df = self.parent_app.df_master if hasattr(self.parent_app, 'df_master') else None

        if not templates:
            layout.addWidget(QLabel("<i>No Nuke Templates registered in Project_Actions</i>"))
            return

        for t_data in templates:
            t_id = t_data['Template_ID']
            s_path = t_data['Source_Path']
            
            block = QVBoxLayout()
            block.setContentsMargins(5, 5, 5, 5)
            
            lbl = QLabel(f"<b>{t_id.replace('_', ' ').title()}</b>")
            lbl.setAlignment(Qt.AlignCenter)
            block.addWidget(lbl)
            
            btn_setup = QPushButton("⚙️ Setup")
            btn_setup.setFixedWidth(140)
            
            # THE FIX: Pass master_df through the lambda so it's available for the mapper
            # Remove mdf=master_df from the lambda
            btn_setup.clicked.connect(lambda chk=False, tid=t_id, sp=s_path: 
                                      self.action_nuke_config(tid, sp))
            
            btn_exec = QPushButton("🚀 Execute")
            btn_exec.setFixedWidth(140)
            btn_exec.setStyleSheet("background-color: #f37321; color: white; font-weight: bold;")
            btn_exec.clicked.connect(lambda chk=False, td=t_data: 
                                     self.action_nuke_execute(td))
            
            block.addWidget(btn_setup)
            block.addWidget(btn_exec)
            layout.addLayout(block)
            layout.addSpacing(15)

    def action_nuke_config(self, template_id, source_path):
        self.nuke_engine.setup_action(
            template_id, 
            source_path, 
            self.get_unified_execution_data(), # Pass the Big Kahuna here
            parent_window=self
        )

    def action_nuke_execute(self, template_data):
        self.nuke_engine.execute_template(
            template_data, 
            self.get_unified_execution_data(), # Pass the Big Kahuna here
            parent_window=self
        )

    def apply_filters(self, text):
        self._is_filtering = True 
        if text != "All":
            mask = self.df_manager['SEQUENCE'] != text
            self.df_manager.loc[mask, 'Select'] = False
        col_idx = self.df_manager.columns.get_loc("SEQUENCE")
        self.proxy.setFilterKeyColumn(col_idx)
        self.proxy.setFilterFixedString("" if text == "All" else text)
        self._is_filtering = False
        self.model.beginResetModel(); self.model.endResetModel()
        self.restore_selections_from_data()

    def restore_selections_from_data(self):
        if not hasattr(self, 'table'): return
        self.table.selectionModel().blockSignals(True)
        self.table.clearSelection()
        for row in range(self.proxy.rowCount()):
            source_idx = self.proxy.mapToSource(self.proxy.index(row, 0))
            if source_idx.isValid() and self.model._data.iat[source_idx.row(), 0] == True:
                self.table.selectRow(row)
        self.table.selectionModel().blockSignals(False)

    def sync_checkboxes_to_selection(self, selected, deselected):
        if getattr(self, '_is_filtering', False): return 
        for sel_range in selected:
            for index in sel_range.indexes():
                if index.column() == 0: self.model.setData(self.proxy.mapToSource(index), True, Qt.CheckStateRole)
        for desel_range in deselected:
            for index in desel_range.indexes():
                if index.column() == 0: self.model.setData(self.proxy.mapToSource(index), False, Qt.CheckStateRole)

    def toggle_selection(self, state):
        if state: self.table.selectAll()
        else: self.table.clearSelection()

class NotesEngine:
    def __init__(self, engine, asset_row_data=None):
        self.engine = engine
        self.asset_context = asset_row_data or {}
        
        # --- THE AGGRESSIVE FIX ---
        # 1. Normalize the root immediately
        clean_root = os.path.abspath(os.path.normpath(self.engine.root))
        self.config_path = os.path.join(clean_root, "Notes_Config.csv")
        
        # 2. Debug Print (Temporary - check your console!)
        # print(f"DEBUG: NotesEngine looking for config at: {self.config_path}")
        # print(f"DEBUG: File Exists? {os.path.exists(self.config_path)}")
        
        self.config_df = pd.DataFrame()
        self.load_config()

    def load_config(self):
        if os.path.exists(self.config_path):
            try:
                # Use engine-level loading logic to stay consistent
                self.config_df = pd.read_csv(self.config_path, encoding='cp1252', dtype=str).fillna("")
            except Exception as e:
                self.config_df = pd.DataFrame()
        else:
            self.config_df = pd.DataFrame()

    def is_valid(self):
        """Checks for the 5 pillars by looking at the correct columns."""
        if self.config_df.empty: 
            return False
        
        # 1. Check for the 'main' settings (Keys)
        main_settings = ['substitute_unresolved_vars', 'unresolved_vars_default_string']
        found_keys = self.config_df['Key'].tolist()
        if not all(k in found_keys for k in main_settings):
            return False
            
        # 2. Check for the routing pillars (Key_Types)
        # We need at least one entry for 'root', 'tree', and 'name'
        found_types = self.config_df['Key_Type'].tolist()
        required_types = ['root', 'tree', 'name']
        if not all(t in found_types for t in required_types):
            return False
            
        return True

    def trigger_sync_ui(self, parent_window):
        """Reuses your existing sync logic to heal the project."""
        from PySide6.QtWidgets import QMessageBox
        res = QMessageBox.question(parent_window, "Notes System Incomplete", 
            "The Notes configuration is missing or incomplete for this project.\n\n"
            "Would you like to sync the project config now?")
        if res == QMessageBox.Yes:
            self.engine.bootstrap_template(mode='sync')
            self.load_config()
            return self.is_valid()
        return False

    def get_value(self, key_type, key_name):
        """Helper to find the specific 'Value' for a type/name pair."""
        mask = (self.config_df['Key_Type'] == key_type) & (self.config_df['Key'] == key_name)
        result = self.config_df.loc[mask, 'Value']
        return result.iloc[0] if not result.empty else ""

    def construct_path(self, scope="default", task="general", user="default"):
        """
        Builds the path by pulling instructions from the 'scope' (e.g., 'client').
        If a specific scope isn't found for a type, it falls back to 'default'.
        """
        # 1. Gather Instructions based on Scope from our Config
        root_tpl = self.get_value('root', scope) or self.get_value('root', 'default')
        tree_tpl = self.get_value('tree', scope) or self.get_value('tree', 'default')
        name_tpl = self.get_value('name', scope) or self.get_value('name', 'default')

        # 2. Build the Raw Template String
        raw_string = f"{root_tpl}/{tree_tpl}/{name_tpl}.csv"

        # 3. Build the Context (Project Settings + Asset Metadata + Session Data)
        # Combine engine settings (data_root, etc) with the specific row metadata
        ctx = {**self.engine.settings, **self.asset_context}
        
        # Add the dynamic session variables
        ctx.update({
            'TASK': task,
            'USER': user,
            'TIMESTAMP': datetime.now().strftime("%Y%m%d_%H%M%S"),
            'substitute_unresolved': self.get_value('main', 'substitute_unresolved_vars'),
            'unresolved_str': self.get_value('main', 'unresolved_vars_default_string')
        })

        # 4. Perform the Template Substitution
        resolved_string = raw_string
        # We sort by length descending so that {SHOTNAME_v2} is replaced before {SHOTNAME}
        for key in sorted(ctx.keys(), key=len, reverse=True):
            placeholder = f"{{{key}}}"
            if placeholder in resolved_string:
                val = str(ctx[key])
                # Handle Missing/NaN Data
                if not val or val == "nan":
                    if ctx.get('substitute_unresolved') == "True":
                        val = ctx.get('unresolved_str', 'unresolved')
                resolved_string = resolved_string.replace(placeholder, val)

        # 5. The "Agnostic" Hand-off
        # Use our PathSwapper to make sure the final path is valid for this OS
        return PathSwapper.translate(resolved_string)

class NotesEntryDialog(QDialog):
    def __init__(self, parent=None, shot_names=None):
        super().__init__(parent)
        self.project_name = getattr(parent, 'project_label', 'Generic Project')
        self.setWindowTitle(f"[{self.project_name}] - Project & Shot Manager")
        self.resize(500, 350)
        layout = QVBoxLayout(self)

        # Context Label (Shows which shots are getting the note)
        shot_str = ", ".join(shot_names[:3])
        if len(shot_names) > 3: shot_str += f" (+{len(shot_names)-3} more)"
        layout.addWidget(QLabel(f"<b>Adding note to:</b> {shot_str}"))

        self.note_edit = QTextEdit()
        self.note_edit.setPlaceholderText("Type your note here...")
        layout.addWidget(self.note_edit)

        # Standard Buttons
        self.buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def get_note_text(self):
        return self.note_edit.toPlainText().strip()

class NotesManagerDialog(QDialog):
    def __init__(self, parent=None, engine=None, mode="ALL", context_data=None):
        super().__init__(parent)
        self.engine = engine
        self.mode = mode # "ALL" or "SELECTION"
        self.context_data = context_data
        self.setWindowTitle(f"Notes Manager - {mode} NOTES")
        self.resize(1100, 600)
        
        self.df_notes = pd.DataFrame()
        self.setup_ui()
        self.refresh_data()

    def setup_ui(self):
        layout = QVBoxLayout(self)

        # --- 1. THE FILTER BAR (Reusing your UI pattern) ---
        filter_layout = QHBoxLayout()
        self.edit_search = QLineEdit()
        self.edit_search.setPlaceholderText("Search notes content or metadata...")
        self.edit_search.textChanged.connect(self.update_filter)
        filter_layout.addWidget(QLabel("<b>Filter:</b>"))
        filter_layout.addWidget(self.edit_search)
        
        self.status_combo = QComboBox()
        self.status_combo.addItems(["All", "active", "closed", "in-progress"])
        self.status_combo.currentTextChanged.connect(self.update_filter)
        filter_layout.addWidget(QLabel("Status:"))
        filter_layout.addWidget(self.status_combo)
        
        layout.addLayout(filter_layout)

        # --- 1.5 WARNING BANNER ---
        self.lbl_warning = QLabel("")
        self.lbl_warning.setStyleSheet("color: #ffaa00; font-weight: bold; background-color: #332222; padding: 8px; border-radius: 4px;")
        self.lbl_warning.setVisible(False)
        layout.addWidget(self.lbl_warning)

        # --- 2. THE TABLE ---
        self.table = QTableView()
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.setSelectionMode(QTableView.ExtendedSelection)
        self.table.setSortingEnabled(True)
        
        self.model = PandasModel(self.df_notes, self, read_only=True)
        self.model.dataChanged.connect(self.on_status_changed)
        self.proxy = QSortFilterProxyModel() # You can swap for MultiFilterProxy for full feature parity
        self.proxy.setSourceModel(self.model)
        self.table.setModel(self.proxy)
        
        layout.addWidget(self.table)

        # --- 3. BULK ACTIONS ---
        btn_layout = QHBoxLayout()
        self.btn_close = QPushButton("Mark Selected CLOSED")
        self.btn_close.setStyleSheet("background-color: #2e885a; color: white; font-weight: bold;")
        self.btn_close.clicked.connect(self.bulk_close_notes)
        
        btn_layout.addWidget(self.btn_close)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

    def on_status_changed(self, top_left, bottom_right, roles):
        """Catches the dropdown edit and saves it to the manifest."""
        if not roles or Qt.EditRole in roles:
            col_idx = top_left.column()
            col_name = self.df_notes.columns[col_idx]
            
            if col_name == "Status":
                row_idx = top_left.row()
                
                # THE FIX: Read the hidden _NOTE_FILE column
                note_file = self.df_notes.iat[row_idx, self.df_notes.columns.get_loc('_NOTE_FILE')]
                note_dir = self.df_notes.iat[row_idx, self.df_notes.columns.get_loc('_DIR')]
                new_status = self.df_notes.iat[row_idx, col_idx]
                
                # Push the change back to the physical .notes_info.csv
                self.parent().update_notes_manifest(note_dir, note_file, status=new_status)

    def notes_table_flags(self, index):
        if not index.isValid(): return Qt.NoItemFlags
        col_name = self.df_notes.columns[index.column()]
        if col_name == "Status":
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def refresh_data(self):
        """Crawls the project for manifests and loads the notes."""
        all_data = []
        search_dirs = set() 
        
        path_errors = 0
        existing_dirs = 0 # NEW: Tracks folders that physically exist on disk
        
        if self.mode == "ALL":
            proj_root = PathSwapper.translate(self.engine.settings.get('data_root', ''))
            if not proj_root or proj_root == "." or not os.path.exists(proj_root):
                path_errors += 1
            else:
                existing_dirs += 1 # The root exists!
                for root, dirs, files in os.walk(proj_root):
                    if ".notes_info.csv" in files:
                        search_dirs.add(root)
        else:
            # SELECTION MODE
            notes_eng = NotesEngine(self.engine)
            if self.context_data:
                for item in self.context_data:
                    notes_eng.asset_context = item
                    note_path = notes_eng.construct_path()
                    
                    if note_path and note_path != "nan" and note_path != ".":
                        d = os.path.dirname(note_path)
                        search_dirs.add(d)
                        # Actually check the disk!
                        if os.path.exists(d):
                            existing_dirs += 1
                    else:
                        path_errors += 1

        # Process whatever directories we found
        for d in search_dirs:
            m_path = os.path.join(d, ".notes_info.csv")
            if not os.path.exists(m_path): 
                continue 
                
            df_m = pd.read_csv(m_path, dtype=str)
            for _, m_row in df_m.iterrows():
                note_file = os.path.join(d, m_row['Filename'])
                if os.path.exists(note_file):
                    df_n = pd.read_csv(note_file, dtype=str)
                    if not df_n.empty:
                        row_dict = df_n.iloc[0].to_dict()
                        row_dict['Status'] = m_row['Status']
                        row_dict['_DIR'] = d
                        row_dict['_NOTE_FILE'] = m_row['Filename'] 
                        all_data.append(row_dict)

        self.df_notes = pd.DataFrame(all_data)
        self.model._data = self.df_notes
        self.model.beginResetModel(); self.model.endResetModel()
        
        # --- REFINED FEEDBACK LOGIC ---
        if self.df_notes.empty:
            if path_errors > 0 and existing_dirs == 0:
                self.lbl_warning.setText("⚠️ Cannot access notes. The configured paths are invalid or unresolved.")
                self.lbl_warning.setVisible(True)
            elif existing_dirs == 0:
                self.lbl_warning.setText("⚠️ The target directories do not exist on disk. No notes can be found.")
                self.lbl_warning.setVisible(True)
            else:
                self.lbl_warning.setText("ℹ️ The directories were searched, but no notes exist yet.")
                self.lbl_warning.setVisible(True)
        else:
            self.lbl_warning.setVisible(False)
        
        # UI POLISH: Hide the "Working/Snapshot" columns
        visible = ["Timestamp", "User", "SEQUENCE", "SHOTNAME", "FILENAME", "Note", "Status", "Reply_To"]
        
        for i, col in enumerate(self.df_notes.columns):
            if col not in visible:
                self.table.setColumnHidden(i, True)
        
        # Apply Status Dropdown safely
        if "Status" in self.df_notes.columns:
            idx = self.df_notes.columns.get_loc("Status")
            self.table.setItemDelegateForColumn(idx, StatusDelegate(["active", "closed", "in-progress"], self.table))
            
        self.table.resizeColumnsToContents()

    def show_context_menu(self, pos):
        idx = self.table.indexAt(pos)
        if not idx.isValid(): return
        
        menu = QMenu(self)
        reply_act = menu.addAction("Reply to Note")
        
        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        if action == reply_act:
            self.action_reply(idx)

    def action_reply(self, proxy_idx):
        source_idx = self.proxy.mapToSource(proxy_idx)
        orig_note_file = self.df_notes.iat[source_idx.row(), self.df_notes.columns.get_loc("Filename")]
        
        # Launch the Add Note logic on the parent, but passing the original filename as Reply_To
        self.parent().action_add_note(reply_to_file=orig_note_file)
        self.refresh_data() # Reload to show the new reply

    def collect_notes(self, rows_data):
        """Iterates through selected shots, finds manifests, and loads the data."""
        all_notes = []
        notes_eng = NotesEngine(self.engine)
        
        for row in rows_data:
            notes_eng.asset_context = row
            # We resolve the directory by looking at the 'tree' part of the config
            note_path = notes_eng.construct_path()
            note_dir = os.path.dirname(note_path)
            manifest_path = os.path.join(note_dir, ".notes_info.csv")
            
            if os.path.exists(manifest_path):
                df_m = pd.read_csv(manifest_path, dtype=str)
                for _, m_row in df_m.iterrows():
                    # Load the actual Note CSV (the snapshot)
                    full_note_path = os.path.join(note_dir, m_row['Filename'])
                    if os.path.exists(full_note_path):
                        df_note = pd.read_csv(full_note_path, dtype=str)
                        # Merge the manifest status ('active/closed') into the snapshot data
                        note_dict = df_note.iloc[0].to_dict()
                        note_dict['Status'] = m_row['Status']
                        note_dict['_DIR'] = note_dir # Hidden ref for saving back
                        all_notes.append(note_dict)

        if all_notes:
            self.df_notes = pd.DataFrame(all_notes)
            self.model._data = self.df_notes
            self.model.beginResetModel()
            self.model.endResetModel()
            self.table.resizeColumnsToContents()
            
            # Apply Status Dropdown to the Status column
            # (Assuming you use your StatusDelegate from before)
            status_idx = self.df_notes.columns.get_loc('Status')
            self.table.setItemDelegateForColumn(status_idx, StatusDelegate(['active', 'closed', 'in-progress'], self.table))

    def update_filter(self):
        search = self.edit_search.text()
        self.proxy.setFilterFixedString(search)

    def bulk_close_notes(self):
        """Updates the physical manifests for all selected rows."""
        selection = self.table.selectionModel().selectedRows()
        if not selection: return
        
        for idx in selection:
            source_row = self.proxy.mapToSource(idx).row()
            # THE FIX: Read the hidden _NOTE_FILE column
            note_file = self.df_notes.iat[source_row, self.df_notes.columns.get_loc('_NOTE_FILE')]
            note_dir = self.df_notes.iat[source_row, self.df_notes.columns.get_loc('_DIR')]
            
            # Use the method we wrote earlier!
            self.parent().update_notes_manifest(note_dir, note_file, status="closed")
            
            # Update UI view
            self.df_notes.iat[source_row, self.df_notes.columns.get_loc('Status')] = "closed"
        
        self.model.dataChanged.emit(self.model.index(0,0), self.model.index(len(self.df_notes)-1, 0))

class SingleLineWrapEdit(QTextEdit):
    def __init__(self, text="", parent=None):
        super().__init__(text, parent)
        self.setTabChangesFocus(True)
        self.setAcceptRichText(False)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setLineWrapMode(QTextEdit.WidgetWidth)

    def keyPressEvent(self, event):
        # Intercept both Return and Enter (numeric keypad)
        if event.key() in (Qt.Key_Return, Qt.Key_Enter):
            # 1. Clear focus (this signals to the user the entry is 'set')
            self.clearFocus()
            # 2. Accept the event so it stops here and doesn't trigger the Dialog 'OK'
            event.accept()
            return
        super().keyPressEvent(event)

class ImportWorker(QThread):
    found = Signal(list) # Emits list of (ProjectName, ConfigPath)
    finished = Signal()

    def __init__(self, start_dir):
        super().__init__()
        self.start_dir = os.path.normpath(start_dir)

    def run(self):
        results = []
        # os.walk is generally faster than glob for deep, broad trees
        for root, dirs, _ in os.walk(self.start_dir):
            if "_pipe_config" in dirs:
                config_path = os.path.join(root, "_pipe_config")
                # project name is the parent of the config folder
                project_name = os.path.basename(root)
                results.append((project_name, config_path))
                # Prune the search: don't look INSIDE the config folder
                dirs.remove("_pipe_config") 
        
        self.found.emit(results)
        self.finished.emit()

def generate_uuid(local_path, filename):
    lp = str(local_path) if pd.notna(local_path) else ""
    fn = str(filename) if pd.notna(filename) else ""
    clean_local = lp.replace("\\", "/")
    path_str = f"{clean_local}/{fn}".replace("//", "/")
    return hashlib.md5(path_str.encode()).hexdigest()

class MultilineDelegate(QStyledItemDelegate):
    def createEditor(self, parent, option, index):
        editor = QTextEdit(parent)
        editor.setAcceptRichText(False)
        editor.setLineWrapMode(QTextEdit.WidgetWidth)
        
        # --- NEW WINDOWS FOCUS FIX ---
        # This forces the table to "commit" the data as soon as the user clicks away
        editor.installEventFilter(self) 
        # -----------------------------

        def adjust_height():
            table = parent.parent() 
            if isinstance(table, QTableView):
                doc_height = editor.document().size().height()
                new_height = max(option.rect.height(), doc_height + 10)
                table.setRowHeight(index.row(), new_height)

        editor.textChanged.connect(adjust_height)
        return editor

    def eventFilter(self, editor, event):
        if event.type() == QEvent.FocusOut:
            try:
                self.commitData.emit(editor)
                self.closeEditor.emit(editor)
            except RuntimeError:
                pass # The C++ object is already being destroyed, let it die in peace
        return super().eventFilter(editor, event)

    def setEditorData(self, editor, index):
        value = index.data(Qt.EditRole)
        editor.setPlainText(str(value))

    def setModelData(self, editor, model, index):
        model.setData(index, editor.toPlainText(), Qt.EditRole)

class SubmissionReviewDialog(QDialog):
    def __init__(self, df_to_review, parent=None, engine=None, target_path=None, read_only=False):
        super().__init__(parent)
        self.read_only = read_only
        self.engine = engine
        
        self.project_name = getattr(parent, 'project_label', 'Generic Project')
        self.target_path = target_path
        
        if not self.target_path:
            base_dir = self.engine.project_root
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.target_path = os.path.join(base_dir, ".autosaves", "sessions", f"session_{ts}.csv")

        self.resize(1800, 600)
        display_name = os.path.basename(self.target_path).replace(".csv", "")
        self.setWindowTitle(f"[{self.project_name}] - Review Submission: {display_name}")

        layout = QVBoxLayout(self)

        # --- TOP TOOLBAR ---
        top_layout = QHBoxLayout()
        self.btn_validate = QPushButton("Flag Range Mismatches")
        self.btn_validate.setCheckable(True)
        self.btn_validate.setAutoDefault(False)
        self.btn_validate.setFixedWidth(200)
        self.btn_validate.setStyleSheet("QPushButton:checked { background-color: #882e2e; color: white; font-weight: bold; }")
        
        # Sync initial state from parent AssetManager
        parent_toggle = getattr(self.parent(), 'btn_validate_ranges', None)
        if parent_toggle and parent_toggle.isChecked():
            self.btn_validate.setChecked(True)

        self.btn_validate.toggled.connect(self.toggle_validation)
        top_layout.addWidget(self.btn_validate)
        top_layout.addStretch()
        layout.addLayout(top_layout)

        self.review_df = df_to_review.copy()    

        # ... (Your existing Data Prep & Re-ordering Logic) ...
        if 'SUBNOTES' not in self.review_df.columns:
            self.review_df['SUBNOTES'] = ""
        else:
            self.review_df['SUBNOTES'] = self.review_df['SUBNOTES'].astype(object).fillna("")

        raw_types = str(self.parent().engine.settings.get('submission_types', 'WIP, Final Pending QC, Final QC Approved'))
        self.sub_types = [s.strip() for s in raw_types.split(',') if s.strip()]
        
        if 'SUBTYPE' not in self.review_df.columns:
            self.review_df['SUBTYPE'] = self.sub_types[0] if self.sub_types else ""
        else:
            self.review_df['SUBTYPE'] = self.review_df['SUBTYPE'].astype(object).fillna("")

        raw_review_headers = str(self.parent().engine.settings.get('submission_review_headers', 'LOCALPATH,ALTSHOTNAME,FILENAME,FIRST,LAST,SUBTYPE,SUBNOTES'))
        priority_cols = [c.strip() for c in raw_review_headers.split(',') if c.strip()]
        existing_priority = [c for c in priority_cols if c in self.review_df.columns]
        leftovers = [c for c in self.review_df.columns if c not in existing_priority]
        self.review_df = self.review_df[existing_priority + leftovers]

        # --- TABLE & MODEL SETUP ---
        self.table = QTableView()
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        self.table.setEditTriggers(QTableView.AllEditTriggers)
        
        # --- THE FIX: Instantiate model THEN set the flag ---
        self.model = PandasModel(self.review_df, self)
        self.model.validation_enabled = self.btn_validate.isChecked() # <--- SYNC HERE
        self.model.flags = self.review_dialog_flags 
        
        self.proxy = QSortFilterProxyModel()
        self.proxy.setSourceModel(self.model)
        self.table.setModel(self.proxy)

        # ... (Rest of your Table/Button/Layout logic) ...
        self.note_idx = self.review_df.columns.get_loc("SUBNOTES")
        self.table.setItemDelegateForColumn(self.note_idx, MultilineDelegate(self.table))
        self.type_idx = self.review_df.columns.get_loc("SUBTYPE")
        self.table.setItemDelegateForColumn(self.type_idx, StatusDelegate(self.sub_types, self.table))
        
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_row_menu)
        layout.addWidget(self.table)

        # --- COLUMN HIDING ---
        cols_to_hide = [c for c in self.review_df.columns if c not in priority_cols]
        mgr_parent = self.parent()
        if mgr_parent and hasattr(mgr_parent, 'engine'):
            is_dual = str(mgr_parent.engine.settings.get('dual_name', 'False')).lower() == 'true'
            if not is_dual and 'ALTSHOTNAME' not in cols_to_hide:
                cols_to_hide.append('ALTSHOTNAME')

        for col in set(cols_to_hide):
            if col in self.review_df.columns:
                idx = self.review_df.columns.get_loc(col)
                self.table.setColumnHidden(idx, True)

        btn_layout = QHBoxLayout()
        # --- SURGICAL INJECTION: STATS LABEL ---
        self.lbl_stats = QLabel("SELECTED: 0 | TOTAL: 0")
        self.lbl_stats.setStyleSheet("color: #888; font-family: 'Courier New'; font-weight: bold; margin-right: 15px;")
        btn_layout.addWidget(self.lbl_stats)
        # ---------------------------------------
        self.btn_just_save = QPushButton("Save")
        self.btn_just_save.clicked.connect(self.quick_save)
        self.btn_save_exit = QPushButton("Save and Exit")
        self.btn_save_exit.clicked.connect(self.save_for_later_action)
        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.button(QDialogButtonBox.Ok).setText("Submit")
        self.buttons.button(QDialogButtonBox.Cancel).setText("Exit")
        btn_layout.addWidget(self.btn_just_save); btn_layout.addWidget(self.btn_save_exit); btn_layout.addStretch(); btn_layout.addWidget(self.buttons)
        layout.addLayout(btn_layout)

        # --- CONNECT SIGNALS ---
        self.table.selectionModel().selectionChanged.connect(self.update_stats)
        # Initial call to set "TOTAL"
        self.update_stats()

        self.buttons.accepted.connect(self.accept); self.buttons.rejected.connect(self.reject)
        self.setup_ui_polish()

    def update_stats(self):
        """Updates the row and selection counts at the bottom of the dialog."""
        total = self.proxy.rowCount()
        selection = self.table.selectionModel().selectedIndexes()
        selected_count = len({idx.row() for idx in selection})
        
        stats_text = f"SELECTED: {selected_count} | TOTAL: {total}"
        self.lbl_stats.setText(stats_text)
        
        # Subtle polish: highlight the text if items are selected
        color = "#58cc71" if selected_count > 0 else "#888"
        self.lbl_stats.setStyleSheet(f"color: {color}; font-family: 'Courier New'; font-weight: bold; margin-right: 15px;")

    def toggle_validation(self, enabled):
        """Surgically toggles the red highlights in the table."""
        if hasattr(self, 'model'):
            self.model.validation_enabled = enabled
            self.model.layoutChanged.emit()
    
    def validate_row_range(self, row_data):
        """Bridge: Passes the validation request up to the AssetManager."""
        parent = self.parent()
        # Keep walking up the tree until we find the AssetManager
        while parent:
            if hasattr(parent, 'validate_row_range'):
                return parent.validate_row_range(row_data)
            parent = parent.parent()
        return True, [] # Fallback
    
    def keyPressEvent(self, event):
        """Surgically intercepts Copy before the Editor can steal the focus."""
        
        # 1. Catch the Copy Command (Cmd+C / Ctrl+C)
        if event.matches(QKeySequence.Copy):
            # 2. FORCE-CLOSE any active editor in the table 
            # This prevents the "last cell is currently being edited" ghosting.
            if self.table.indexWidget(self.table.currentIndex()):
                self.table.commitData(self.table.indexWidget(self.table.currentIndex()))
            
            # 3. Perform the centralized copy
            ClipboardHelper.copy_table_selection(self.table)
            
            # 4. Optional: Feedback via status bar
            curr = self
            while curr:
                if hasattr(curr, 'statusBar') and curr.statusBar():
                    curr.statusBar().showMessage("Copied selection to clipboard", 2000)
                    break
                curr = curr.parent()
            
            # IMPORTANT: Accept the event so it doesn't trigger the default Qt edit behavior
            event.accept()
            return 

        # Let all other keys (Enter, Arrows, etc.) behave normally
        super().keyPressEvent(event)

    def get_data(self):
        # IMPORTANT: Always return the source data, not the proxy view
        return self.review_df

    def quick_save(self):
        """Saves current state and pings the Session Manager to update its preview."""
        if hasattr(self.model, '_data'):
            self.review_df = self.model._data 

        session_dir = os.path.dirname(self.target_path)
        os.makedirs(session_dir, exist_ok=True)
            
        try:
            self.review_df.to_csv(self.target_path, index=False, encoding='cp1252')
            
            parent = self.parent()
            if parent:
                # Sync 'Pending' status in main window
                if hasattr(parent, 'df_master'):
                    sent_uuids = self.review_df['UUID'].tolist()
                    mask = parent.df_master['UUID'].isin(sent_uuids)
                    parent.df_master.loc[mask, 'SUBSTATUS'] = "Pending"
                    parent.run_autosave()
                
                # --- NEW: SYNC SESSION MANAGER PREVIEW ---
                if hasattr(parent, 'active_dialogs'):
                    manager = parent.active_dialogs.get("session_manager")
                    if manager:
                        # Only refresh if the manager is actually looking at THIS file
                        current_item = manager.list_widget.currentItem()
                        if current_item and current_item.text() == os.path.basename(self.target_path).replace(".csv", ""):
                            manager.refresh_preview()

                if parent.statusBar():
                    parent.statusBar().showMessage(f"Progress saved: {os.path.basename(self.target_path)}", 3000)
        except Exception as e:
            from PySide6.QtWidgets import QMessageBox
            QMessageBox.critical(self, "Save Error", f"Failed to save session:\n{e}")

    def get_target_path(self):
        return self.target_path

    def show_row_menu(self, pos):
        # 1. Get the item under the mouse
        idx = self.table.indexAt(pos)
        if not idx.isValid(): return

        # 2. Get ALL selected indexes (this is more robust than selectedRows)
        selection = self.table.selectionModel().selectedIndexes()
        
        # 3. Unique set of source rows (mapped through the proxy)
        # We use a set first to collapse multiple cell selections in one row,
        # then sort it in REVERSE so we don't shift indices during deletion.
        rows_to_drop = sorted(
            {self.proxy.mapToSource(i).row() for i in selection}, 
            reverse=True
        )

        menu = QMenu(self)
        
        # --- RV ACTIONS ---
        rv_act = menu.addAction("Play selection in RV")
        rv_scans_act = menu.addAction("Play selection + Hero Scans in RV")
        menu.addSeparator()

        # --- REMOVE ACTION ---
        label = f"Remove {len(rows_to_drop)} items" if len(rows_to_drop) > 1 else "Remove from List"
        remove_act = menu.addAction(label)

        menu.addSeparator()
        reveal_act = menu.addAction("Reveal in Finder/Explorer")
        
        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        
        if action == remove_act:
            if not rows_to_drop: return

            # 4. PERFORM THE DROP
            self.model.beginResetModel()
            try:
                # Drop from the underlying DataFrame
                self.review_df.drop(self.review_df.index[rows_to_drop], inplace=True)
                
                # Reset the index so the DataFrame remains a clean 0-N sequence
                self.review_df.reset_index(drop=True, inplace=True)
                
                # 5. COMMIT TO DISK
                # This ensures the floating Manager sees the change immediately
                self.quick_save() 
            except Exception as e:
                print(f"Remove Error: {e}")
            self.model.endResetModel()
            
            # Optional: Refresh the UI polish (column widths, etc.)
            self.setup_ui_polish()
        
        elif action == reveal_act:
            self.reveal_selected_in_os()

        elif action == rv_act:
            self.launch_rv_from_dialog()
            
        elif action == rv_scans_act:
            self.launch_rv_with_scans_from_dialog()

    def launch_rv_with_scans_from_dialog(self):
        """Surgically gathers paths using the same robust logic as the simple Play action."""
        selection = self.table.selectionModel().selectedIndexes()
        if not selection: return

        rows = sorted({self.proxy.mapToSource(idx).row() for idx in selection})
        parent = self.parent()
        
        final_paths = []
        target_shots = set()
        target_ext = "exr" # Default fallback

        for r in rows:
            path_found = False
            
            # --- 1. ROBUST ORIGINALS COLLECTION (Same as simple Play) ---
            # A. Try ABSPATH first
            if 'ABSPATH' in self.review_df.columns:
                folder = str(self.review_df.iat[r, self.review_df.columns.get_loc("ABSPATH")])
                filename = str(self.review_df.iat[r, self.review_df.columns.get_loc("FILENAME")])
                if folder and folder != "nan":
                    final_paths.append(os.path.normpath(os.path.join(folder, filename)))
                    path_found = True
            
            # B. Reconstruct Fallback
            if not path_found:
                lp = str(self.review_df.iat[r, self.review_df.columns.get_loc("LOCALPATH")])
                fn = str(self.review_df.iat[r, self.review_df.columns.get_loc("FILENAME")])
                final_paths.append(os.path.normpath(os.path.join(self.engine.project_root, lp, fn)))

            # --- 2. GATHER SHOT NAMES FOR SCANS ---
            shot = str(self.review_df.iat[r, self.review_df.columns.get_loc("SHOTNAME")])
            if shot and shot != "nan" and shot != "":
                target_shots.add(shot)
            
            # Grab extension for the scan resolver
            if r == rows[0] and 'FILETYPE' in self.review_df.columns:
                target_ext = str(self.review_df.iat[r, self.review_df.columns.get_loc("FILETYPE")])

        # --- 3. GATHER HERO SCANS (Using Parent Logic) ---
        if hasattr(parent, 'resolve_scan_path'):
            for shot in target_shots:
                scan_path = parent.resolve_scan_path(shot, target_ext=target_ext)
                if scan_path:
                    final_paths.append(os.path.normpath(scan_path))

        # --- 4. TRIGGER VIA PARENT GUARD ---
        if final_paths:
            if parent.confirm_missing_files(final_paths, "RV"):
                parent.trigger_app(final_paths, "RV")

    def reveal_selected_in_os(self):
        selection = self.table.selectionModel().selectedIndexes()
        if not selection: return
        
        # 1. Get the source row from the dialog's proxy
        row = self.proxy.mapToSource(selection[0]).row()
        parent = self.parent()
        
        folder = ""
        filename = str(self.review_df.iat[row, self.review_df.columns.get_loc("FILENAME")])
        
        # 2. PATH RECOVERY STRATEGY
        # A. Try local ABSPATH first
        if 'ABSPATH' in self.review_df.columns:
            folder = str(self.review_df.iat[row, self.review_df.columns.get_loc("ABSPATH")])
            
        # B. Fallback: Ask the Main App's master table via UUID
        if (not folder or folder == "nan") and 'UUID' in self.review_df.columns:
            uuid = self.review_df.iat[row, self.review_df.columns.get_loc("UUID")]
            master_match = parent.df_master[parent.df_master['UUID'] == uuid]
            if not master_match.empty:
                folder = str(master_match.iloc[0]['ABSPATH'])

        # C. Last Resort: Reconstruct from LOCALPATH + Main App's Catalog Root
        if not folder or folder == "nan":
            lp = str(self.review_df.iat[row, self.review_df.columns.get_loc("LOCALPATH")])
            root_dir = self.engine.project_root
            folder = os.path.join(root_dir, lp)

        # 3. Final Verification before triggering
        if folder and folder != "nan":
            # Pass to the parent's unified OS trigger
            parent.trigger_os_reveal(folder, filename)
        else:
            parent.statusBar().showMessage("Reveal Failed: Could not reconstruct absolute path.", 5000)

    def launch_rv_from_dialog(self):
        selection = self.table.selectionModel().selectedIndexes()
        if not selection: return

        # Get unique source rows
        rows = sorted({self.proxy.mapToSource(idx).row() for idx in selection})
        parent = self.parent()
        paths = []

        for r in rows:
            path_found = False
            
            # 1. Direct Check: Does the local review_df have ABSPATH?
            if 'ABSPATH' in self.review_df.columns:
                folder = str(self.review_df.iat[r, self.review_df.columns.get_loc("ABSPATH")])
                filename = str(self.review_df.iat[r, self.review_df.columns.get_loc("FILENAME")])
                if folder and folder != "nan":
                    paths.append(os.path.join(folder, filename))
                    path_found = True
            
            # 2. Recovery: If ABSPATH is missing, use the UUID to ask the Parent Table
            if not path_found and 'UUID' in self.review_df.columns:
                uuid = self.review_df.iat[r, self.review_df.columns.get_loc("UUID")]
                master_match = parent.df_master[parent.df_master['UUID'] == uuid]
                
                if not master_match.empty:
                    folder = str(master_match.iloc[0]['ABSPATH'])
                    filename = str(master_match.iloc[0]['FILENAME'])
                    paths.append(os.path.join(folder, filename))
                    path_found = True

            # 3. Final Fallback: Reconstruct from LOCALPATH + Parent Root
            if not path_found:
                lp = str(self.review_df.iat[r, self.review_df.columns.get_loc("LOCALPATH")])
                fn = str(self.review_df.iat[r, self.review_df.columns.get_loc("FILENAME")])
                root_dir = self.engine.project_root
                paths.append(os.path.join(root_dir, lp, fn))

        # --- SURGICAL INJECTION: THE ACCESSIBILITY GUARD ---
        if paths:
            # We call the confirmation helper on the parent AssetManager
            if hasattr(parent, 'confirm_missing_files'):
                if parent.confirm_missing_files(paths, "RV"):
                    self.trigger_parent_rv(paths)
            else:
                # Fallback if helper is somehow missing
                self.trigger_parent_rv(paths)

    def trigger_parent_rv(self, paths):
        # Simply hand the paths back to the main app's unified launcher
        self.parent().trigger_app(paths, "RV")

    def review_dialog_flags(self, index):
        # If read_only is True, never return ItemIsEditable
        if not self.read_only and index.column() in (self.note_idx, self.type_idx):
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def save_for_later_action(self):
        """Surgically commits active edits and saves before closing."""
        # 1. Force the table to finish any active typing/editing in the current cell
        if self.table.currentIndex().isValid():
            self.table.commitData(self.table.viewport())
            
        # 2. Run the actual save logic
        self.quick_save()
        
        # 3. Exit with custom code 2 (Save for Later)
        self.done(2)

    def setup_ui_polish(self):
        header = self.table.horizontalHeader()
        self.table.resizeColumnsToContents()
        
        # Look up where SUBNOTES is sitting in the current DataFrame
        if "SUBNOTES" in self.review_df.columns:
            visual_idx = self.review_df.columns.get_loc("SUBNOTES")
            self.table.setColumnWidth(visual_idx, 700)
        if "SUBTYPE" in self.review_df.columns:
            self.table.setColumnWidth(self.review_df.columns.get_loc("SUBTYPE"), 150)

    def start_autosave_fuse(self):
        pass

    def get_data(self):
        return self.review_df
    
    def refresh_from_disk(self):
        """Surgically reloads the CSV data into the floating view."""
        if not self.target_path or not os.path.exists(self.target_path):
            return

        # 1. Force-close any active editor to prevent data corruption
        if self.table.currentIndex().isValid():
            self.table.commitData(self.table.viewport())

        # 2. Reload the data from the CSV we just appended to
        try:
            # We use the same loading logic as __init__
            new_df = pd.read_csv(self.target_path, encoding='cp1252', dtype=str).fillna("")
            
            # 3. Update the Model
            self.model.beginResetModel()
            self.review_df = new_df
            self.model._data = self.review_df
            self.model.endResetModel()
            
            # 4. Maintain UI Polish (column widths/hiding)
            self.setup_ui_polish()
            
            # 5. Visual feedback (Optional)
            self.setWindowTitle(self.windowTitle().replace(" *Updated*", "") + " *Updated*")
            QTimer.singleShot(2000, lambda: self.setWindowTitle(self.windowTitle().replace(" *Updated*", "")))
            
        except Exception as e:
            print(f"Sync Error: Could not reload {self.target_path}. {e}")

    def accept(self):
        """Custom confirmation before finalizing the submission."""
        # 1. Force commit any active cell edit
        if self.table.currentIndex().isValid():
            self.table.commitData(self.table.viewport())
        
        # 2. Gather stats for the summary
        count = len(self.review_df)
        dest_name = f"submission_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # 3. Create the custom Confirmation Dialog
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Question)
        msg.setWindowTitle("Confirm Submission")
        msg.setText(f"You are about to submit <b>{count}</b> items.")
        msg.setInformativeText(f"Destination: <br/><code style='color: #77aa77;'>{dest_name}</code>")
        
        btn_submit_open = msg.addButton("Submit & Open Folder", QMessageBox.AcceptRole)
        btn_submit = msg.addButton("Submit Only", QMessageBox.AcceptRole)
        btn_cancel = msg.addButton("Cancel", QMessageBox.RejectRole)
        
        msg.setDefaultButton(btn_submit)
        msg.exec()
        
        if msg.clickedButton() == btn_cancel:
            return # Don't close the review dialog

        # Store the user's choice so the AssetManager can see it
        self.open_folder_requested = (msg.clickedButton() == btn_submit_open)
        
        # Save progress to disk and proceed
        self.quick_save()
        super().accept()
        
    def reject(self):
        """Surgical cleanup for floating windows."""
        if hasattr(self, 'table') and self.table:
            self.table.setModel(None)
        # Calling super().reject() triggers the 'finished' signal with code 0
        super().reject()

    def closeEvent(self, event):
        """Ensure C++ table drops Python model before destruction."""
        if hasattr(self, 'table') and self.table:
            self.table.setModel(None)
        super().closeEvent(event)

class PlaylistReviewEditor(QDialog):
    def __init__(self, df_to_review, parent=None, engine=None, target_path=None, read_only=False):
        super().__init__(parent)
        self.read_only = read_only
        self.engine = engine

        self.project_name = getattr(parent, 'project_label', 'Generic Project')
        self.target_path = target_path
        
        if not self.target_path:
            base_dir = self.engine.project_root
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.target_path = os.path.join(base_dir, ".autosaves", "playlists", f"playlist_{ts}.csv")

        self.resize(1800, 600)
        display_name = os.path.basename(self.target_path).replace(".csv", "")
        self.setWindowTitle(f"[{self.project_name}] - Edit Playlist: {display_name}")

        layout = QVBoxLayout(self)

        # --- TOP TOOLBAR ---
        top_layout = QHBoxLayout()
        self.btn_validate = QPushButton("Flag Range Mismatches")
        self.btn_validate.setCheckable(True)
        self.btn_validate.setAutoDefault(False)
        self.btn_validate.setFixedWidth(200)
        self.btn_validate.setStyleSheet("QPushButton:checked { background-color: #882e2e; color: white; font-weight: bold; }")
        
        # Sync initial state from parent AssetManager
        parent_toggle = getattr(self.parent(), 'btn_validate_ranges', None)
        if parent_toggle and parent_toggle.isChecked():
            self.btn_validate.setChecked(True)

        self.btn_validate.toggled.connect(self.toggle_validation)
        top_layout.addWidget(self.btn_validate)
        top_layout.addStretch()
        layout.addLayout(top_layout)

        self.review_df = df_to_review.copy()

        # ... (Data Prep & Column Ordering Logic) ...
        if 'SUBNOTES' not in self.review_df.columns:
            self.review_df['SUBNOTES'] = ""
        else:
            self.review_df['SUBNOTES'] = self.review_df['SUBNOTES'].astype(object).fillna("")

        fallback_headers = str(self.parent().engine.settings.get('submission_review_headers', 'LOCALPATH,ALTSHOTNAME,FILENAME,FIRST,LAST,SUBNOTES'))
        raw_review_headers = str(self.parent().engine.settings.get('playlist_review_headers', fallback_headers))
        priority_cols = [c.strip() for c in raw_review_headers.split(',') if c.strip()]
        existing_priority = [c for c in priority_cols if c in self.review_df.columns]
        leftovers = [c for c in self.review_df.columns if c not in existing_priority]
        self.review_df = self.review_df[existing_priority + leftovers]

        # --- TABLE & MODEL SETUP ---
        self.table = QTableView()
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        
        self.model = PandasModel(self.review_df, self)
        self.model.validation_enabled = self.btn_validate.isChecked() # <--- SYNC HERE
        self.model.flags = self.review_dialog_flags 
        
        self.proxy = QSortFilterProxyModel()
        self.proxy.setSourceModel(self.model)
        self.table.setModel(self.proxy)

        # ... (Rest of Buttons & UI Polish) ...
        if "SUBNOTES" in self.review_df.columns:
            self.note_idx = self.review_df.columns.get_loc("SUBNOTES")
            self.table.setItemDelegateForColumn(self.note_idx, MultilineDelegate(self.table))

        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_row_menu)
        layout.addWidget(self.table)

        cols_to_hide = [c for c in self.review_df.columns if c not in priority_cols]
        for col in set(cols_to_hide):
            if col in self.review_df.columns:
                self.table.setColumnHidden(self.review_df.columns.get_loc(col), True)

        btn_layout = QHBoxLayout()

        # --- SURGICAL INJECTION: STATS LABEL ---
        self.lbl_stats = QLabel("SELECTED: 0 | TOTAL: 0")
        self.lbl_stats.setStyleSheet("color: #888; font-family: 'Courier New'; font-weight: bold; margin-right: 15px;")
        btn_layout.addWidget(self.lbl_stats)
        # ---------------------------------------

        self.btn_save = QPushButton("Save Playlist")
        self.btn_create_sub = QPushButton("Create Submission from Playlist")
        self.btn_create_sub.setStyleSheet("background-color: #2e885a; color: white; font-weight: bold;")
        self.btn_create_sub.clicked.connect(self.create_submission_from_playlist)
        self.btn_play_all = QPushButton("Play All in RV"); self.btn_play_all.setStyleSheet("background-color: #2e5a88; color: white; font-weight: bold;"); self.btn_play_all.clicked.connect(self.play_all_rv)
        self.btn_close = QPushButton("Close"); self.btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_save); btn_layout.addWidget(self.btn_create_sub); btn_layout.addWidget(self.btn_play_all); btn_layout.addStretch(); btn_layout.addWidget(self.btn_close)
        layout.addLayout(btn_layout)
        
        # --- CONNECT SIGNALS ---
        self.table.selectionModel().selectionChanged.connect(self.update_stats)
        self.update_stats()
        self.setup_ui_polish()

    def update_stats(self):
        """Updates the row and selection counts at the bottom of the dialog."""
        total = self.proxy.rowCount()
        selection = self.table.selectionModel().selectedIndexes()
        selected_count = len({idx.row() for idx in selection})
        
        stats_text = f"SELECTED: {selected_count} | TOTAL: {total}"
        self.lbl_stats.setText(stats_text)
        
        color = "#58cc71" if selected_count > 0 else "#888"
        self.lbl_stats.setStyleSheet(f"color: {color}; font-family: 'Courier New'; font-weight: bold; margin-right: 15px;")
        
    def toggle_validation(self, enabled):
        if hasattr(self, 'model'):
            self.model.validation_enabled = enabled
            self.model.layoutChanged.emit()
    
    def validate_row_range(self, row_data):
        """Bridge: Passes the validation request up to the AssetManager."""
        parent = self.parent()
        # Keep walking up the tree until we find the AssetManager
        while parent:
            if hasattr(parent, 'validate_row_range'):
                return parent.validate_row_range(row_data)
            parent = parent.parent()
        return True, [] # Fallback
    
    def keyPressEvent(self, event):
        """Surgically intercepts Copy before the Editor can steal the focus."""
        
        # 1. Catch the Copy Command (Cmd+C / Ctrl+C)
        if event.matches(QKeySequence.Copy):
            # 2. FORCE-CLOSE any active editor in the table 
            # This prevents the "last cell is currently being edited" ghosting.
            if self.table.indexWidget(self.table.currentIndex()):
                self.table.commitData(self.table.indexWidget(self.table.currentIndex()))
            
            # 3. Perform the centralized copy
            ClipboardHelper.copy_table_selection(self.table)
            
            # 4. Optional: Feedback via status bar
            curr = self
            while curr:
                if hasattr(curr, 'statusBar') and curr.statusBar():
                    curr.statusBar().showMessage("Copied selection to clipboard", 2000)
                    break
                curr = curr.parent()
            
            # IMPORTANT: Accept the event so it doesn't trigger the default Qt edit behavior
            event.accept()
            return 

        # Let all other keys (Enter, Arrows, etc.) behave normally
        super().keyPressEvent(event)

    def create_submission_from_playlist(self):
        """Passes the playlist data to a background Submission dialog to handle formatting and saving."""
        self.quick_save()
        
        # 1. Figure out a smart default name based on the current playlist
        default_name = "custom_session"
        if self.target_path:
            default_name = os.path.basename(self.target_path).replace(".csv", "")
            if default_name.startswith("playlist_"):
                default_name = default_name.replace("playlist_", "session_", 1)
        
        # 2. Build a tiny custom dialog so we can have 3 buttons
        from PySide6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton, QMessageBox
        
        prompt = QDialog(self)
        prompt.setWindowTitle("Create Submission")
        prompt.resize(350, 100)
        
        layout = QVBoxLayout(prompt)
        layout.addWidget(QLabel("Enter a name for the new Submission Session:"))
        
        name_input = QLineEdit(default_name)
        layout.addWidget(name_input)
        
        btn_layout = QHBoxLayout()
        btn_custom = QPushButton("Use Custom Name")
        btn_default = QPushButton("Use Auto-Timestamp")
        btn_cancel = QPushButton("Cancel")
        
        btn_layout.addWidget(btn_custom)
        btn_layout.addWidget(btn_default)
        btn_layout.addWidget(btn_cancel)
        layout.addLayout(btn_layout)
        
        # Logic to track which button they pressed
        prompt.choice = None
        def set_custom(): prompt.choice = "CUSTOM"; prompt.accept()
        def set_default(): prompt.choice = "DEFAULT"; prompt.accept()
        
        btn_custom.clicked.connect(set_custom)
        btn_default.clicked.connect(set_default)
        btn_cancel.clicked.connect(prompt.reject)
        
        # 3. Show the dialog and wait
        if prompt.exec() != QDialog.Accepted:
            return 
            
        parent = self.parent()
        session_path = None # Starts as None (the trigger for auto-timestamp)
        
        # 4. If they chose custom, build the specific path
        if prompt.choice == "CUSTOM":
            new_name = name_input.text().strip()
            if not new_name: return # Bail if they left it blank but clicked Custom
            
            if not new_name.lower().endswith(".csv"):
                new_name += ".csv"
                
            base_dir = self.engine.project_root
            session_path = os.path.join(base_dir, ".autosaves", "sessions", new_name)
            
            if os.path.exists(session_path):
                res = QMessageBox.warning(self, "Overwrite Session?", 
                                          f"A session named '{new_name}' already exists.\nDo you want to overwrite it?",
                                          QMessageBox.Yes | QMessageBox.No)
                if res == QMessageBox.No: return

        # 5. Instantiate the headless dialog (session_path is either Custom or None)
        headless_sub = SubmissionReviewDialog(self.review_df, parent, target_path=session_path)
        headless_sub.quick_save()
        
        # 6. Success Feedback (We ask the headless dialog what name it ended up using!)
        final_name = os.path.basename(headless_sub.target_path)
        QMessageBox.information(self, "Submission Created", 
                              f"Successfully created: {final_name}\n\n"
                              "You can load it from the Session Manager when you are ready to submit.")
        
        headless_sub.deleteLater()

    def quick_save(self):
        """Saves current state and pings the Session Manager."""
        if not self.target_path: return
        
        # Ensure model data is synced (safety first)
        if hasattr(self.model, '_data'):
            self.review_df = self.model._data

        os.makedirs(os.path.dirname(self.target_path), exist_ok=True)
            
        try:
            self.review_df.to_csv(self.target_path, index=False, encoding='cp1252')
            
            parent = self.parent()
            if parent:
                # --- NEW: SYNC SESSION MANAGER PREVIEW ---
                if hasattr(parent, 'active_dialogs'):
                    manager = parent.active_dialogs.get("session_manager")
                    if manager and manager.current_mode == "PLAYLIST":
                        current_item = manager.list_widget.currentItem()
                        if current_item and current_item.text() == os.path.basename(self.target_path).replace(".csv", ""):
                            manager.refresh_preview()

                if parent.statusBar():
                    parent.statusBar().showMessage(f"Playlist saved: {os.path.basename(self.target_path)}", 3000)
        except Exception as e:
            from PySide6.QtWidgets import QMessageBox
            QMessageBox.critical(self, "Save Error", f"Failed to save playlist:\n{e}")

    def play_all_rv(self):
        """Grabs all valid rows and throws them to RV."""
        paths = []
        for r in range(len(self.review_df)):
            folder = str(self.review_df.iat[r, self.review_df.columns.get_loc("ABSPATH")])
            filename = str(self.review_df.iat[r, self.review_df.columns.get_loc("FILENAME")])
            if folder and folder != "nan":
                paths.append(os.path.join(folder, filename))
        
        # --- SURGICAL FIX: USE THE GUARD ---
        from PySide6.QtWidgets import QApplication
        main_win = next((w for w in QApplication.topLevelWidgets() if hasattr(w, 'confirm_missing_files')), None)

        if paths and main_win:
            if main_win.confirm_missing_files(paths, "RV"):
                main_win.trigger_app(paths, "RV")
        elif paths:
            self.parent().trigger_app(paths, "RV")

    def launch_rv_with_scans_from_dialog(self):
        """Surgically gathers paths using the same robust logic as the simple Play action."""
        selection = self.table.selectionModel().selectedIndexes()
        if not selection: return

        rows = sorted({self.proxy.mapToSource(idx).row() for idx in selection})
        parent = self.parent()
        
        final_paths = []
        target_shots = set()
        target_ext = "exr" # Default fallback

        for r in rows:
            path_found = False
            
            # --- 1. ROBUST ORIGINALS COLLECTION (Same as simple Play) ---
            # A. Try ABSPATH first
            if 'ABSPATH' in self.review_df.columns:
                folder = str(self.review_df.iat[r, self.review_df.columns.get_loc("ABSPATH")])
                filename = str(self.review_df.iat[r, self.review_df.columns.get_loc("FILENAME")])
                if folder and folder != "nan":
                    final_paths.append(os.path.normpath(os.path.join(folder, filename)))
                    path_found = True
            
            # B. Reconstruct Fallback
            if not path_found:
                lp = str(self.review_df.iat[r, self.review_df.columns.get_loc("LOCALPATH")])
                fn = str(self.review_df.iat[r, self.review_df.columns.get_loc("FILENAME")])
                final_paths.append(os.path.normpath(os.path.join(self.engine.project_root, lp, fn)))

            # --- 2. GATHER SHOT NAMES FOR SCANS ---
            shot = str(self.review_df.iat[r, self.review_df.columns.get_loc("SHOTNAME")])
            if shot and shot != "nan" and shot != "":
                target_shots.add(shot)
            
            # Grab extension for the scan resolver
            if r == rows[0] and 'FILETYPE' in self.review_df.columns:
                target_ext = str(self.review_df.iat[r, self.review_df.columns.get_loc("FILETYPE")])

        # --- 3. GATHER HERO SCANS (Using Parent Logic) ---
        if hasattr(parent, 'resolve_scan_path'):
            for shot in target_shots:
                scan_path = parent.resolve_scan_path(shot, target_ext=target_ext)
                if scan_path:
                    final_paths.append(os.path.normpath(scan_path))

        # --- 4. TRIGGER VIA PARENT GUARD ---
        if final_paths:
            if parent.confirm_missing_files(final_paths, "RV"):
                parent.trigger_app(final_paths, "RV")

    def reveal_selected_in_os(self):
        selection = self.table.selectionModel().selectedIndexes()
        if not selection: return
        
        # 1. Get the source row from the dialog's proxy
        row = self.proxy.mapToSource(selection[0]).row()
        parent = self.parent()
        
        folder = ""
        filename = str(self.review_df.iat[row, self.review_df.columns.get_loc("FILENAME")])
        
        # 2. PATH RECOVERY STRATEGY
        # A. Try local ABSPATH first
        if 'ABSPATH' in self.review_df.columns:
            folder = str(self.review_df.iat[row, self.review_df.columns.get_loc("ABSPATH")])
            
        # B. Fallback: Ask the Main App's master table via UUID
        if (not folder or folder == "nan") and 'UUID' in self.review_df.columns:
            uuid = self.review_df.iat[row, self.review_df.columns.get_loc("UUID")]
            master_match = parent.df_master[parent.df_master['UUID'] == uuid]
            if not master_match.empty:
                folder = str(master_match.iloc[0]['ABSPATH'])

        # C. Last Resort: Reconstruct from LOCALPATH + Main App's Catalog Root
        if not folder or folder == "nan":
            lp = str(self.review_df.iat[row, self.review_df.columns.get_loc("LOCALPATH")])
            root_dir = self.engine.project_root
            folder = os.path.join(root_dir, lp)

        # 3. Final Verification before triggering
        if folder and folder != "nan":
            # Pass to the parent's unified OS trigger
            parent.trigger_os_reveal(folder, filename)
        else:
            parent.statusBar().showMessage("Reveal Failed: Could not reconstruct absolute path.", 5000)

    def show_row_menu(self, pos):
        # 1. Get the item under the mouse
        idx = self.table.indexAt(pos)
        if not idx.isValid(): return

        # 2. Get ALL selected indexes (this is more robust than selectedRows)
        selection = self.table.selectionModel().selectedIndexes()
        
        # 3. Unique set of source rows (mapped through the proxy)
        # We use a set first to collapse multiple cell selections in one row,
        # then sort it in REVERSE so we don't shift indices during deletion.
        rows_to_drop = sorted(
            {self.proxy.mapToSource(i).row() for i in selection}, 
            reverse=True
        )

        menu = QMenu(self)
        
        # --- RV ACTIONS ---
        rv_act = menu.addAction("Play selection in RV")
        rv_scans_act = menu.addAction("Play selection + Hero Scans in RV")
        menu.addSeparator()

        # --- REMOVE ACTION ---
        label = f"Remove {len(rows_to_drop)} items" if len(rows_to_drop) > 1 else "Remove from List"
        remove_act = menu.addAction(label)

        menu.addSeparator()
        reveal_act = menu.addAction("Reveal in Finder/Explorer")
        
        action = menu.exec(self.table.viewport().mapToGlobal(pos))
        
        if action == remove_act:
            if not rows_to_drop: return

            # 4. PERFORM THE DROP
            self.model.beginResetModel()
            try:
                # Drop from the underlying DataFrame
                self.review_df.drop(self.review_df.index[rows_to_drop], inplace=True)
                
                # Reset the index so the DataFrame remains a clean 0-N sequence
                self.review_df.reset_index(drop=True, inplace=True)
                
                # 5. COMMIT TO DISK
                # This ensures the floating Manager sees the change immediately
                self.quick_save() 
            except Exception as e:
                print(f"Remove Error: {e}")
            self.model.endResetModel()
            
            # Optional: Refresh the UI polish (column widths, etc.)
            self.setup_ui_polish()
        
        elif action == reveal_act:
            self.reveal_selected_in_os()

        elif action == rv_act:
            self.launch_rv_from_dialog()
            
        elif action == rv_scans_act:
            self.launch_rv_with_scans_from_dialog()

    def launch_rv_from_dialog(self):
        selection = self.table.selectionModel().selectedIndexes()
        if not selection: return
        rows = sorted({self.proxy.mapToSource(idx).row() for idx in selection})
        paths = []
        for r in rows:
            folder = str(self.review_df.iat[r, self.review_df.columns.get_loc("ABSPATH")])
            filename = str(self.review_df.iat[r, self.review_df.columns.get_loc("FILENAME")])
            if folder and folder != "nan":
                paths.append(os.path.join(folder, filename))
        
        # --- ROBUST PARENT LOOKUP ---
        # We look for the AssetManager specifically to find the helper
        mgr = self.parent()
        while mgr and not hasattr(mgr, 'confirm_missing_files'):
            mgr = mgr.parent()

        if paths and mgr:
            # Now we are 100% sure we are calling the AssetManager's helper
            if mgr.confirm_missing_files(paths, "RV"):
                mgr.trigger_app(paths, "RV")
        elif paths:
            # Emergency fallback if for some reason the manager isn't found
            self.parent().trigger_app(paths, "RV")

    def review_dialog_flags(self, index):
        if not self.read_only and hasattr(self, 'note_idx') and index.column() == self.note_idx:
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def setup_ui_polish(self):
        self.table.resizeColumnsToContents()
        if "SUBNOTES" in self.review_df.columns:
            self.table.setColumnWidth(self.review_df.columns.get_loc("SUBNOTES"), 700)
    
    def refresh_from_disk(self):
        """Surgically reloads the CSV data into the floating view."""
        if not self.target_path or not os.path.exists(self.target_path):
            return

        # 1. Force-close any active editor to prevent data corruption
        if self.table.currentIndex().isValid():
            self.table.commitData(self.table.viewport())

        # 2. Reload the data from the CSV we just appended to
        try:
            # We use the same loading logic as __init__
            new_df = pd.read_csv(self.target_path, encoding='cp1252', dtype=str).fillna("")
            
            # 3. Update the Model
            self.model.beginResetModel()
            self.review_df = new_df
            self.model._data = self.review_df
            self.model.endResetModel()
            
            # 4. Maintain UI Polish (column widths/hiding)
            self.setup_ui_polish()
            
            # 5. Visual feedback (Optional)
            self.setWindowTitle(self.windowTitle().replace(" *Updated*", "") + " *Updated*")
            QTimer.singleShot(2000, lambda: self.setWindowTitle(self.windowTitle().replace(" *Updated*", "")))
            
        except Exception as e:
            print(f"Sync Error: Could not reload {self.target_path}. {e}")

    def reject(self):
        """Surgical cleanup to drop the model before the window is destroyed."""
        if hasattr(self, 'table') and self.table:
            self.table.setModel(None)
        super().reject()

    def closeEvent(self, event):
        if hasattr(self, 'table') and self.table: self.table.setModel(None)
        super().closeEvent(event)

class SessionManagerDialog(QDialog):
    def __init__(self, base_dir, parent=None):
        super().__init__(parent)
        # Pull project name from parent AssetManager
        self.project_name = getattr(parent, 'project_label', 'Generic Project')
        self.setWindowTitle(f"[{self.project_name}] - Session & Playlist Manager")
        self.resize(1200, 700)
        self.base_dir = base_dir
        self.session_dir = os.path.join(base_dir, ".autosaves/sessions")
        self.data_dir = os.path.join(base_dir, "submission_data")
        self.log_dir = os.path.join(base_dir, "submission_logs")
        self.playlist_dir = os.path.join(base_dir, ".autosaves/playlists") # <--- NEW

        layout = QVBoxLayout(self)
        
        # --- NEW: MODE TOGGLE ---
        mode_layout = QHBoxLayout()
        
        self.btn_view_wip = QPushButton("WIP Sessions")
        self.btn_view_wip.setCheckable(True)
        self.btn_view_wip.setAutoDefault(False) # Kills the visual "stuck" glitch
        
        self.btn_view_sent = QPushButton("Sent Submissions")
        self.btn_view_sent.setCheckable(True)
        self.btn_view_sent.setAutoDefault(False)
        
        self.btn_view_playlists = QPushButton("Playlists")
        self.btn_view_playlists.setCheckable(True)
        self.btn_view_playlists.setAutoDefault(False)
        
        # The Native Toggle Manager
        self.mode_group = QButtonGroup(self)
        self.mode_group.setExclusive(True)
        self.mode_group.addButton(self.btn_view_wip)
        self.mode_group.addButton(self.btn_view_sent)
        self.mode_group.addButton(self.btn_view_playlists)
        
        self.btn_view_wip.setChecked(True)
        
        self.btn_view_wip.clicked.connect(lambda: self.switch_mode("WIP"))
        self.btn_view_sent.clicked.connect(lambda: self.switch_mode("SENT"))
        self.btn_view_playlists.clicked.connect(lambda: self.switch_mode("PLAYLIST")) 
        
        mode_layout.addWidget(self.btn_view_wip)
        mode_layout.addWidget(self.btn_view_sent)
        mode_layout.addWidget(self.btn_view_playlists)
        mode_layout.addStretch()
        layout.addLayout(mode_layout)

        splitter = QSplitter(Qt.Horizontal)
        
        # LEFT: List section ---
        left_widget = QWidget(); left_layout = QVBoxLayout(left_widget)
        self.list_label = QLabel("Saved WIP Sessions:")
        self.list_widget = QListWidget()
        self.list_widget.currentRowChanged.connect(self.load_preview)
        left_layout.addWidget(self.list_label); left_layout.addWidget(self.list_widget)
        
        # --- NEW: Rename Button ---
        self.btn_rename = QPushButton("Rename Playlist")
        self.btn_rename.clicked.connect(self.rename_session)
        left_layout.addWidget(self.btn_rename)
        
        self.btn_del = QPushButton("Delete Selected")
        self.btn_del.setStyleSheet("background-color: #882e2e; color: white;")
        self.btn_del.clicked.connect(self.delete_session)
        left_layout.addWidget(self.btn_del)
        
        # RIGHT: Preview
        right_widget = QWidget(); right_layout = QVBoxLayout(right_widget)
        self.preview_table = QTableView()
        self.preview_table.setAlternatingRowColors(True)
        
        # --- NEW: Make this a variable so we can update the text ---
        self.preview_label = QLabel("Preview Contents: (None selected)")
        self.preview_label.setStyleSheet("font-weight: bold; color: #888;")
        
        right_layout.addWidget(self.preview_label)
        right_layout.addWidget(self.preview_table)
        
        splitter.addWidget(left_widget); splitter.addWidget(right_widget)
        splitter.setStretchFactor(1, 3)
        layout.addWidget(splitter)
        
        self.btn_load = QPushButton("Load Selected")
        self.btn_load.setMinimumHeight(40)
        self.btn_load.setStyleSheet("background-color: #2e885a; color: white; font-weight: bold;")
        self.btn_load.clicked.connect(self.action_load_trigger) # Custom trigger
        
        # Add the Load button to the bottom of the layout (above the Exit button)
        layout.addWidget(self.btn_load)

        # --- THE PERSISTENT EXIT BUTTON ---
        exit_layout = QHBoxLayout()
        exit_layout.addStretch()
        self.btn_exit = QPushButton("Exit Manager")
        self.btn_exit.setFixedWidth(150)
        self.btn_exit.clicked.connect(self.close) # Closes the window
        exit_layout.addWidget(self.btn_exit)
        exit_layout.addStretch()
        
        layout.addLayout(exit_layout)

        # --- INITIALIZATION & STABILITY ---
        self.current_mode = "WIP"
        
        # 1. Set the initial visibility (WIP mode doesn't use Rename)
        self.btn_rename.hide()
        
        # 2. Bind the double-click behavior
        self.list_widget.doubleClicked.connect(self.action_load_trigger)
        
        # 3. Perform the SINGLE initial disk scan
        self.refresh_list()
        
        # 4. THE 'OPEN EMPTY' FIX: Ensure no auto-selection
        self.list_widget.clearSelection()
        self.list_widget.setCurrentRow(-1)
        self.clear_preview()
        
        # 5. Steal focus to the Exit button to prevent Row 0 highlighting
        self.btn_exit.setFocus()

    def clear_preview(self):
        """Surgically wipes the preview table and resets the label."""
        if hasattr(self, 'preview_table') and self.preview_table:
            self.preview_table.setModel(None)
            
        if hasattr(self, 'preview_label') and self.preview_label:
            self.preview_label.setText("Preview Contents: (None selected)")

    def refresh_preview(self):
        """Surgically re-triggers the preview load for the currently selected item."""
        current_row = self.list_widget.currentRow()
        if current_row >= 0:
            # We call the existing logic, passing the current row index
            self.load_preview(current_row)

    def switch_mode(self, mode):
        self.current_mode = mode

        # Dynamic Title Update
        mode_names = {"WIP": "WIP Sessions", "SENT": "Sent Submissions", "PLAYLIST": "Playlists"}
        friendly_mode = mode_names.get(mode, "Manager")
        self.setWindowTitle(f"[{self.project_name}] - {friendly_mode}")

        if mode == "WIP":
            self.btn_view_wip.setChecked(True)
            self.list_label.setText("WIP Sessions:")
            self.btn_load.setText("Load & Submit")
            self.btn_load.setEnabled(True)
            self.btn_del.setVisible(True)
            self.btn_rename.setVisible(False) 
            
        elif mode == "SENT":
            self.btn_view_sent.setChecked(True)
            self.list_label.setText("Sent Submissions:")
            self.btn_load.setText("Read-Only (History)")
            self.btn_load.setEnabled(True)
            self.btn_del.setVisible(False)
            self.btn_rename.setVisible(False) 
            
        elif mode == "PLAYLIST":
            self.btn_view_playlists.setChecked(True)
            self.list_label.setText("Playlists:")
            self.btn_load.setText("Load Playlist")
            self.btn_load.setEnabled(True)
            self.btn_del.setVisible(True)
            self.btn_rename.setVisible(True) 
            
        self.refresh_list()

    def refresh_list(self):
        self.list_widget.blockSignals(True)
        self.list_widget.clear()
        
        target = self.session_dir if self.current_mode == "WIP" else self.data_dir
        if self.current_mode == "PLAYLIST": target = self.playlist_dir
        
        if os.path.exists(target):
            files = sorted(glob.glob(os.path.join(target, "*.csv")), reverse=True)
            self.list_widget.addItems([os.path.basename(f).replace(".csv", "") for f in files])
            
        # Ensure nothing is selected so the preview doesn't show ghost data
        self.list_widget.setCurrentRow(-1)
        self.list_widget.clearSelection()
        self.clear_preview()
        
        self.list_widget.blockSignals(False)

    def load_preview(self, row):
        # If nothing is selected, clear the pane and bail out
        if row < 0 or not self.list_widget.currentItem(): 
            self.clear_preview()
            return
            
        name = self.list_widget.currentItem().text()
        
        # Update our new label
        if hasattr(self, 'preview_label'):
            self.preview_label.setText(f"Preview Contents: {name}")

        self.preview_table.setModel(None)
        self.preview_table.setSortingEnabled(True) 
        
        folder = self.session_dir if self.current_mode == "WIP" else self.data_dir
        if self.current_mode == "PLAYLIST": folder = self.playlist_dir
        path = os.path.join(folder, f"{name}.csv")
        
        if not os.path.exists(path): return
        
        df = pd.read_csv(path, encoding='cp1252')

        # --- SURGICAL UUID HEALING ---
        # Recalculate UUIDs based on the deterministic standard (Local + Linux slashes)
        # This ensures the preview matches the main table regardless of where it was saved.
        if 'LOCALPATH' in df.columns and 'FILENAME' in df.columns:
            df['UUID'] = df.apply(lambda x: generate_uuid(x['LOCALPATH'], x['FILENAME']), axis=1)

        # --- THE SURGICAL MERGE FOR SENT MODE ---
        if self.current_mode == "SENT":
            log_id = name.replace("submission_data_", "send_")
            log_path = os.path.join(self.log_dir, f"{log_id}.csv")
            
            if os.path.exists(log_path):
                log_df = pd.read_csv(log_path, encoding='cp1252')
                # Ensure the log itself is healed to the current platform standard
                if 'LOCALPATH' in log_df.columns:
                    log_df['UUID'] = log_df.apply(lambda x: generate_uuid(x['LOCALPATH'], x['FILENAME']), axis=1)
                
                # Merge the log data (like SUBSENT) back onto the preview if needed
                # Matching primarily on the now-synchronized UUID
                df = df.merge(log_df[['UUID', 'SUBSENT']], on='UUID', how='left')

        # Use parent=self to satisfy the model, but keep flags restricted
        self.preview_model = PandasModel(df, self)
        
        # --- PROXY WRAPPER ---
        proxy = QSortFilterProxyModel()
        proxy.setSourceModel(self.preview_model)
        self.preview_table.setModel(proxy)
        
        self.preview_table.resizeColumnsToContents()
        
        # Hide technical/hidden columns while keeping Shot Names visible
        # We also hide ABSPATH here if it exists to keep the preview clean
        for col in ['UUID', 'FILE_ID', 'ABSPATH', 'HAS_SHOT']:
            if col in df.columns:
                idx = df.columns.get_loc(col)
                self.preview_table.setColumnHidden(idx, True)

    def action_load_trigger(self):
        """Tells the parent AssetManager to launch the editor without closing this window."""
        item = self.list_widget.currentItem()
        
        # Safety Guard: Ensure an item is actually highlighted
        if not item or self.list_widget.currentRow() < 0:
            return
        
        # We call a new specialized method on the parent
        if self.parent() and hasattr(self.parent(), 'launch_session_from_manager'):
            self.parent().launch_session_from_manager(item.text(), self.current_mode)

    def start_autosave_fuse(self): 
        pass # Stub to satisfy the model during preview

    def rename_session(self):
        """Allows renaming of playlists and syncs the parent's memory cache."""
        if self.current_mode != "PLAYLIST": return
        item = self.list_widget.currentItem()
        if not item: return

        old_name = item.text()
        from PySide6.QtWidgets import QInputDialog, QMessageBox
        new_name, ok = QInputDialog.getText(self, "Rename Playlist", "Enter new name:", text=old_name)
        
        if ok and new_name and new_name != old_name:
            old_path = os.path.join(self.playlist_dir, f"{old_name}.csv")
            new_path = os.path.join(self.playlist_dir, f"{new_name}.csv")
            
            if os.path.exists(new_path):
                QMessageBox.warning(self, "Error", "A playlist with that name already exists.")
                return
            
            if os.path.exists(old_path):
                os.rename(old_path, new_path)
                
                # Update parent's memory cache so the right-click menu stays accurate
                parent = self.parent()
                if parent and hasattr(parent, 'playlist_cache'):
                    if old_name in parent.playlist_cache:
                        parent.playlist_cache.remove(old_name)
                    if new_name not in parent.playlist_cache:
                        parent.playlist_cache.append(new_name)
                        parent.playlist_cache.sort()
                        
                self.refresh_list()
                
                # Re-select the renamed item automatically
                matching_items = self.list_widget.findItems(new_name, Qt.MatchExactly)
                if matching_items:
                    self.list_widget.setCurrentItem(matching_items[0])

    def delete_session(self):
        item = self.list_widget.currentItem()
        if not item: return
        target_dir = self.playlist_dir if self.current_mode == "PLAYLIST" else self.session_dir
        session_name = item.text()
        path = os.path.join(target_dir, f"{session_name}.csv")
        
        if os.path.exists(path): 
            os.remove(path)
            
            # Sync parent caches
            parent = self.parent()
            if parent:
                if self.current_mode == "PLAYLIST" and hasattr(parent, 'playlist_cache'):
                    if session_name in parent.playlist_cache:
                        parent.playlist_cache.remove(session_name)
                elif self.current_mode == "WIP" and hasattr(parent, 'session_cache'):
                    if session_name in parent.session_cache:
                        parent.session_cache.remove(session_name)

            self.preview_table.setModel(None)
            self.refresh_list()
    
    def reject(self):
        """Intercept Cancel to drop the model before hiding the UI."""
        if hasattr(self, 'preview_table') and self.preview_table:
            self.preview_table.setModel(None)
        super().reject()

    def closeEvent(self, event):
        """Ensure C++ table drops Python model before destruction."""
        if hasattr(self, 'preview_table') and self.preview_table:
            self.preview_table.setModel(None)
        super().closeEvent(event)

class StatusDelegate(QStyledItemDelegate):
    def __init__(self, options, parent=None):
        super().__init__(parent)
        self.options = options

    def createEditor(self, parent, option, index):
        editor = QComboBox(parent)
        # We DO NOT set the items here anymore; we do it in setEditorData
        # to ensure custom values like 'Pending' are included.
        return editor

    def setEditorData(self, editor, index):
        if isinstance(editor, QComboBox):
            # 1. Clean the value from the model
            value = index.data(Qt.EditRole)
            safe_val = str(value).strip() if value and str(value).lower() != "nan" else ""
            
            # 2. Build the current list of options
            current_options = list(self.options)
            
            # 3. THE INJECTION: If 'Pending' (or anything else) isn't in the list, add it!
            if safe_val and safe_val not in current_options:
                current_options.append(safe_val)
            
            editor.blockSignals(True)
            editor.clear()
            editor.addItems(current_options)
            editor.setCurrentText(safe_val)
            editor.blockSignals(False)
        else:
            super().setEditorData(editor, index)

    def setModelData(self, editor, model, index):
        if isinstance(editor, QComboBox):
            # Only commit the data if it's a valid string
            # (Prevents accidental emptying during drag-select glitches)
            new_val = editor.currentText()
            model.setData(index, new_val, Qt.EditRole)
        else:
            super().setModelData(editor, model, index)

class CSVEditorDelegate(QStyledItemDelegate):
    def __init__(self, options, parent=None):
        super().__init__(parent)
        self.options = options

    def paint(self, painter, option, index):
        # 1. Copy the default style
        style_option = QStyleOptionViewItem(option)
        self.initStyleOption(style_option, index)

        # 2. Check if the MODEL wants this cell dimmed (BackgroundRole)
        bg_brush = index.data(Qt.BackgroundRole)
        if bg_brush:
            painter.save()
            painter.fillRect(option.rect, bg_brush)
            # If there's a background, dim the text too
            dim_text = QColor(110, 110, 110)
            style_option.palette.setColor(QPalette.Text, dim_text)
            style_option.palette.setColor(QPalette.WindowText, dim_text)
            painter.restore()

        # 3. Standard Paint
        super().paint(painter, style_option, index)

    def createEditor(self, parent, option, index):
        # ONLY show the dropdown for column 1 here
        if index.column() == 1 and self.options:
            editor = QComboBox(parent)
            editor.addItems(self.options)
            return editor
        return super().createEditor(parent, option, index)

    def setEditorData(self, editor, index):
        if isinstance(editor, QComboBox):
            value = index.data(Qt.EditRole)
            editor.setCurrentText(str(value))
        else:
            super().setEditorData(editor, index)

    def setModelData(self, editor, model, index):
        if isinstance(editor, QComboBox):
            model.setData(index, editor.currentText(), Qt.EditRole)
        else:
            super().setModelData(editor, model, index)

    def displayText(self, value, locale):
        """Surgically hijacks the display to show an inheritance cue without altering data."""
        text = super().displayText(value, locale)
        if str(text).strip().upper() == "{LOCALDOTDIR}":
            return "{LOCALDOTDIR}  ⮎ (Inheriting from Global)"
        return text

class ProjectSettingsEditor(QDialog):
    def __init__(self, engine, parent=None):
        super().__init__(parent)
        self.engine = engine
        self.setWindowTitle("Edit Project Settings")
        self.resize(900, 500)
        
        layout = QVBoxLayout(self)
        form = QFormLayout()
        form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        
        self.inputs = {}
        
        # --- FIX 1: Removed 'data_root' from this list ---
        path_keys = ['shots_csv', 'scrape_blacklist']

        # --- NEW: Load Raw CSV so we don't accidentally bake over pointers! ---
        raw_csv_path = os.path.join(self.engine.root, "Project_Settings.csv")
        try:
            raw_df = pd.read_csv(raw_csv_path, dtype=str, encoding='cp1252').fillna("")
            raw_settings = dict(zip(raw_df['Key'], raw_df['Value']))
        except:
            raw_settings = {}

        for key, resolved_value in self.engine.settings.items():
            # Block memory-only engine variables
            if key in ['data_root_raw', 'data_root_list']:
                continue
                
            # --- ORIGINAL: Checkbox Setup ---
            if key == 'dual_name':
                cb = QCheckBox("Enable dual-name shot matching")
                # Use raw_settings to determine state, fallback to resolved
                raw_val = raw_settings.get(key, resolved_value)
                cb.setChecked(str(raw_val).strip().lower() == 'true')
                self.inputs[key] = cb
                form.addRow(QLabel(f"<b>{key}:</b>"), cb)
                continue

            # --- FIX 3: The Data_Root Tuple Interceptor ---
            if key == 'data_root':
                raw_val = str(self.engine.settings.get('data_root_raw', resolved_value))
                edit = QLineEdit(raw_val)
                edit.hide()
                self.inputs[key] = edit
                
                preview_lbl = QLabel()
                preview_lbl.setStyleSheet("color: #aaa; background-color: #2b2b2b; padding: 5px; border-radius: 3px;")
                self._update_data_root_preview(preview_lbl, raw_val)
                
                btn_edit = QPushButton("Edit Data Roots...")
                btn_edit.clicked.connect(lambda chk=False, e=edit, l=preview_lbl: self.open_data_roots_editor(e, l))
                
                h_layout = QHBoxLayout()
                h_layout.addWidget(preview_lbl, stretch=1)
                h_layout.addWidget(btn_edit)
                h_layout.addWidget(edit) 
                form.addRow(QLabel(f"<b>{key}:</b>"), h_layout)
                continue

            # --- SURGICAL INJECTION: Text Editor & Preview Setup ---
            raw_val = str(raw_settings.get(key, resolved_value))
            
            edit = SingleLineWrapEdit(raw_val)
            edit.setMinimumHeight(45)
            edit.setMaximumHeight(70)
            self.inputs[key] = edit
            
            # Wrap the editor in a mini layout so we can attach the preview underneath
            v_layout = QVBoxLayout()
            v_layout.setSpacing(2)
            v_layout.setContentsMargins(0, 0, 0, 0)
            v_layout.addWidget(edit)
            
            # The sleek preview label
            if raw_val.strip().upper() == "{LOCALDOTDIR}":
                lbl_preview = QLabel(f"<i>Inherited: {resolved_value}</i>")
                lbl_preview.setStyleSheet("color: #777; font-size: 11px;")
                v_layout.addWidget(lbl_preview)

            if key in path_keys:
                h_layout = QHBoxLayout()
                h_layout.addLayout(v_layout)
                btn_browse = QPushButton("Browse...")
                btn_browse.clicked.connect(lambda chk=False, e=edit, k=key: self.browse_path(e, k))
                h_layout.addWidget(btn_browse)
                form.addRow(QLabel(f"<b>{key}:</b>"), h_layout)
            else:
                form.addRow(QLabel(f"<b>{key}:</b>"), v_layout)
            
        scroll = QScrollArea()
        scroll_w = QWidget(); scroll_w.setLayout(form)
        scroll.setWidget(scroll_w); scroll.setWidgetResizable(True)
        layout.addWidget(scroll)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        self.buttons.button(QDialogButtonBox.Save).setAutoDefault(False)
        self.buttons.accepted.connect(self.save_and_close)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def browse_path(self, edit_widget, key):
        """Fixed browser: Appends for blacklist, avoids .csv for directories."""
        current_val = edit_widget.toPlainText().strip()
        
        # 1. Setup Defaults
        default_names = {
            'shots_csv': 'project_shots.csv'
        }
        
        # Determine the directory to open in
        if current_val and "," in current_val:
            # For blacklist, try the last path in the list
            last_path = current_val.split(",")[-1].strip()
            start_dir = last_path if os.path.exists(last_path) else self.engine.root
        elif current_val and os.path.isdir(current_val):
            # If it's already a directory, open exactly there
            start_dir = current_val
        elif current_val and os.path.exists(os.path.dirname(current_val)):
            # If it's a file, open its parent
            start_dir = os.path.dirname(current_val)
        else:
            start_dir = self.engine.root

        # 2. Open the correct Dialog type
        # --- SURGICAL FIX: Move catalogs into the directory browser group ---
        if key in ['scrape_blacklist', 'catalog_dir']:
            prompt = "Select Directory to Exclude" if key == 'scrape_blacklist' else f"Select Directory for {key}"
            res = QFileDialog.getExistingDirectory(self, prompt, start_dir)
            
            if res:
                res = os.path.normpath(res)
                if key == 'scrape_blacklist':
                    # --- THE APPEND LOGIC ---
                    if current_val:
                        new_val = f"{current_val.rstrip(',')}, {res}"
                    else:
                        new_val = res
                    edit_widget.setPlainText(new_val)
                else:
                    # Overwrite for catalog_dir
                    edit_widget.setPlainText(res)
        else:
            # 3. CSV File Browser (For shots_csv, etc)
            default_fn = default_names.get(key, "data.csv")
            initial_path = os.path.join(start_dir, default_fn)
            
            res, _ = QFileDialog.getOpenFileName(
                self, 
                f"Select {key} File", 
                initial_path, 
                "CSV Files (*.csv)"
            )
            
            if res:
                res = os.path.normpath(res)
                if not res.lower().endswith(".csv"):
                    res += ".csv"
                edit_widget.setPlainText(res)

    def _update_data_root_preview(self, label, raw_json):
        import json
        try:
            data = json.loads(raw_json)
            if not data:
                label.setText("<i>No roots defined</i>")
                return
            lines = []
            for item in data:
                if isinstance(item, list) and len(item) >= 2:
                    lines.append(f"<b>{item[0]}</b> - {item[1]}")
            label.setText("<br>".join(lines))
        except:
            label.setText(f"<b>default</b> - {raw_json}")

    def open_data_roots_editor(self, hidden_edit, preview_label):
        current_data = hidden_edit.text()
        dlg = DataRootsEditorDialog(current_data, self)
        
        if dlg.exec():
            new_json = dlg.get_serialized_data()
            hidden_edit.setText(new_json)
            self._update_data_root_preview(preview_label, new_json)

    def save_and_close(self):
        cleaned_data = []
        for key, widget in self.inputs.items():
            if isinstance(widget, QCheckBox):
                val = "True" if widget.isChecked() else "False"
            # --- FIX 5: Catch the QLineEdit used by data_root ---
            elif isinstance(widget, QLineEdit):
                val = widget.text().strip()
            else:
                val = widget.toPlainText().replace('\n', '').replace('\r', '').strip().rstrip(',')
            
            self.engine.settings[key] = val
            cleaned_data.append([key, val])
            
        csv_path = os.path.join(self.engine.root, "Project_Settings.csv")
        try:
            pd.DataFrame(cleaned_data, columns=['Key', 'Value']).to_csv(csv_path, index=False, encoding='cp1252')
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Save Error", f"Could not write to config:\n{e}")

class MultiFilterProxy(QSortFilterProxyModel):
    def __init__(self):
        super().__init__()
        self.allowed_extensions = set()
        self.shot_regex = ""
        self.no_shot_mode = False
        self.sequence_filter = "All"
        self.status_filter = "All"
        self.start_date = None
        self.end_date = None
        self.search_text = ""
        self.search_cols = set() 
        self.advanced_rules = [] 
        
        # --- NEW: Latest Filter State ---
        self.latest_only_mode = False
        self.latest_dates = {} # Dict to cache { 'SHOTNAME': max_datetime }

    def set_latest_only(self, state):
        self.latest_only_mode = state
        # Explicitly run the math BEFORE we tell the view to refresh
        if self.latest_only_mode:
            self._precalculate_latest()
        self.invalidate()

    def _precalculate_latest(self):
        """Finds the winner for every shot passing current filters."""
        self.latest_dates.clear()
        model = self.sourceModel()
        if not model: return

        idx_shot = self.get_col("SHOTNAME")
        idx_date = self.get_col("MODDATE")
        if idx_shot == -1 or idx_date == -1: return

        # IMPORTANT: We loop the rows and check ONLY standard filters
        for r in range(model.rowCount()):
            if self._passes_standard_filters(r, QModelIndex()):
                shot = str(model.index(r, idx_shot).data())
                date_str = str(model.index(r, idx_date).data())
                
                if shot and shot != "nan" and date_str and date_str != "nan":
                    try:
                        # Standard Nuke/Pipeline timestamp: 2024-05-20 14:02:01
                        row_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                        
                        if shot not in self.latest_dates or row_date > self.latest_dates[shot]:
                            self.latest_dates[shot] = row_date
                    except (ValueError, TypeError):
                        pass

    def invalidate(self):
        """Intercepts the redraw to pre-calculate cross-row data if needed."""
        if getattr(self, 'latest_only_mode', False):
            self._precalculate_latest()
        super().invalidate()

    def get_col(self, name):
        model = self.sourceModel()
        if not model: return -1
        for i in range(model.columnCount()):
            if model.headerData(i, Qt.Horizontal, Qt.DisplayRole) == name:
                return i
        return -1

    def set_date_range(self, start, end):
        self.start_date = start; self.end_date = end
        self.invalidate()

    def set_extension_filter(self, extensions):
        self.allowed_extensions = extensions
        self.invalidate()

    def set_shot_filter(self, pattern, no_shot=False):
        self.shot_regex = pattern; self.no_shot_mode = no_shot
        self.invalidate()

    def set_status_filter(self, status):
        self.status_filter = status
        self.invalidate()

    def set_sequence_filter(self, seq):
        self.sequence_filter = seq
        self.invalidate()

    def set_search_filter(self, text, cols):
        self.search_text = text.lower()
        self.search_cols = cols
        self.invalidate()

    def set_simple_search(self, text, cols):
        self.search_text = text.lower()
        self.search_cols = cols
        self.invalidate()

    def set_advanced_search(self, rules):
        self.advanced_rules = rules
        self.invalidate()

    def filterAcceptsRow(self, source_row, source_parent):
        # 1. First, does it even pass basic text/date/status filters?
        if not self._passes_standard_filters(source_row, source_parent):
            return False

        # 2. If 'Latest Only' is on, check if this row is the winner
        if self.latest_only_mode:
            model = self.sourceModel()
            shot = str(model.index(source_row, self.get_col("SHOTNAME"), source_parent).data())
            date_str = str(model.index(source_row, self.get_col("MODDATE"), source_parent).data())
            
            if shot in self.latest_dates:
                try:
                    row_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    # If this row is OLDER than the pre-calculated max for this shot, hide it
                    if row_date < self.latest_dates[shot]:
                        return False
                except:
                    return False
            else:
                # If for some reason the shot isn't in our winner list, hide it to be safe
                return False

        return True
    
    def _passes_standard_filters(self, source_row, source_parent):
        """Your exact original logic lives in here now."""
        model = self.sourceModel()
        
        def get_col_data(col_name):
            idx = self.get_col(col_name)
            if idx == -1: return ""
            return str(model.index(source_row, idx, source_parent).data()).lower()

        # 1. SIMPLE SEARCH
        if self.search_text and self.search_cols:
            match_found = any(self.search_text in get_col_data(col) for col in self.search_cols)
            if not match_found: return False

        # 2. ADVANCED RECURSIVE SEARCH
        if self.advanced_rules:
            if not self.evaluate_advanced_rules(self.advanced_rules, get_col_data):
                return False
        
        # 1. Extension Filter
        idx = self.get_col("FILETYPE")
        if idx != -1:
            ext = model.index(source_row, idx, source_parent).data()
            if ext and ext not in self.allowed_extensions: return False
            
        # 2. Date Filter
        idx = self.get_col("MODDATE")
        if idx != -1:
            mod_date_str = model.index(source_row, idx, source_parent).data()
            if mod_date_str and (self.start_date or self.end_date):
                try:
                    row_date = datetime.strptime(mod_date_str, "%Y-%m-%d %H:%M:%S")
                    if self.start_date and row_date < self.start_date: return False
                    if self.end_date and row_date > self.end_date: return False
                except (ValueError, TypeError): pass 
        
        # NEW: Sequence Filter
        if self.sequence_filter != "All":
            idx_seq = self.get_col("SEQUENCE")
            if idx_seq != -1:
                row_seq = model.index(source_row, idx_seq, source_parent).data()
                if row_seq != self.sequence_filter: return False

        # 3. SURGICAL SHOT FILTERING (Crude & Effective Pass)
        if self.no_shot_mode:
            idx_has = self.get_col("HAS_SHOT")
            has_shot = model.index(source_row, idx_has, source_parent).data()
            if str(has_shot) == "True": return False
            
        elif self.shot_regex:
            # Grab our pre-calculated shot columns
            idx_p = self.get_col("SHOTNAME")
            idx_w = self.get_col("ALTSHOTNAME")
            p_val = str(model.index(source_row, idx_p, source_parent).data())
            w_val = str(model.index(source_row, idx_w, source_parent).data())
            
            # THE MATCH: Does our dropdown selection (shot_regex) 
            # contain the row's assigned SHOTNAME or ALTSHOTNAME?
            is_match = False
            if p_val and p_val in self.shot_regex: is_match = True
            if not is_match and w_val and w_val in self.shot_regex: is_match = True
            
            if not is_match: return False

        # 4. Status Filter
        if self.status_filter != "All":
            idx_sub = self.get_col("SUBSTATUS")
            row_status = model.index(source_row, idx_sub, source_parent).data()
            if self.status_filter == "Any Status":
                if not row_status or row_status == "": return False
            elif row_status != self.status_filter: return False

        return True
    
    def evaluate_advanced_rules(self, rules, get_col_data):
        """Recursively parses flat, indented rules into grouped logic."""
        def eval_level(start_idx, current_level):
            result = None
            i = start_idx
            
            while i < len(rules):
                rule = rules[i]
                
                # If we hit a LOWER level, this subgroup is done. Pop back up.
                if rule['level'] < current_level:
                    return result, i 
                    
                # If we hit a HIGHER level, dive into the subgroup recursively.
                if rule['level'] > current_level:
                    sub_result, next_i = eval_level(i, rule['level'])
                    
                    sub_logic = rule['logic']
                    if result is None: 
                        result = sub_result if sub_logic != 'NOT' else not sub_result
                    else:
                        if sub_logic == 'AND': result = result and sub_result
                        elif sub_logic == 'OR': result = result or sub_result
                        elif sub_logic == 'NOT': result = result and not sub_result
                        
                    i = next_i
                    continue

                # Base Case: Evaluate this specific row
                match = False
                text = rule['text'].lower()
                if text:
                    match = any(text in get_col_data(col) for col in rule['cols'])
                else:
                    match = True # Empty text boxes are ignored/pass

                # Apply to the running result
                if result is None:
                    result = match if rule['logic'] != 'NOT' else not match
                else:
                    if rule['logic'] == 'AND': result = result and match
                    elif rule['logic'] == 'OR': result = result or match
                    elif rule['logic'] == 'NOT': result = result and not match
                    
                i += 1
                
            return result, i

        final_res, _ = eval_level(0, 0)
        return final_res if final_res is not None else True

class SearchRuleWidget(QFrame):
    def __init__(self, builder_parent, level=0):
        super().__init__()
        self.builder = builder_parent
        self.level = level
        
        self.setFrameShape(QFrame.StyledPanel)
        self.setStyleSheet("QFrame { background-color: #2b2b2b; border-radius: 4px; }")
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(10 + (level * 30), 4, 4, 4) # This controls the Indentation!
        
        # 1. Logic Gate
        self.logic_combo = QComboBox()
        self.logic_combo.addItems(["AND", "OR", "NOT"])
        self.logic_combo.setFixedWidth(60)
        self.logic_combo.currentTextChanged.connect(self.builder.trigger_update)
        layout.addWidget(self.logic_combo)

        # 2. The Checkboxes (Replicated per row as requested!)
        self.checkboxes = {}
        cols = ["FILENAME", "LOCALPATH", "SEQUENCE", "SHOTNAME", "ALTSHOTNAME", "SUBNOTES"]
        for col in cols:
            cb = QCheckBox(col[:4]) # Shorten names to keep UI clean (FILE, LOCA, SHOT, etc.)
            cb.setToolTip(col)
            cb.setChecked(True if col in ["FILENAME", "LOCALPATH"] else False)
            cb.stateChanged.connect(self.builder.trigger_update)
            self.checkboxes[col] = cb
            layout.addWidget(cb)
            
        # 3. Text Input
        self.text_input = QLineEdit()
        self.text_input.setPlaceholderText("Search...")
        self.text_input.setClearButtonEnabled(True)
        self.text_input.textChanged.connect(self.builder.trigger_update)
        layout.addWidget(self.text_input, 1) # Stretch

        # 4. Action Buttons
        self.btn_add = QPushButton("Add")
        self.btn_group = QPushButton("Add Grouped")
        self.btn_sub = QPushButton("Add to Group")
        self.btn_del = QPushButton("X")
        self.btn_del.setStyleSheet("color: #ff5555; font-weight: bold; width: 25px;")
        
        # Disable "Add to Group" if we are at level 0 (not in a group)
        if self.level == 0:
            self.btn_sub.setEnabled(False)

        for btn in [self.btn_add, self.btn_group, self.btn_sub, self.btn_del]:
            layout.addWidget(btn)

        # Connect actions to the parent builder
        self.btn_add.clicked.connect(lambda: self.builder.add_row(self, 0))
        self.btn_group.clicked.connect(lambda: self.builder.add_row(self, self.level + 1))
        self.btn_sub.clicked.connect(lambda: self.builder.add_row(self, self.level))
        self.btn_del.clicked.connect(lambda: self.builder.delete_row(self))

    def get_data(self):
        active_cols = [col for col, cb in self.checkboxes.items() if cb.isChecked()]
        return {
            'level': self.level,
            'logic': self.logic_combo.currentText(),
            'cols': active_cols,
            'text': self.text_input.text().strip()
        }

class AdvancedSearchBuilder(QWidget):
    def __init__(self, proxy_model, parent=None):
        super().__init__(parent)
        self.proxy = proxy_model
        
        # --- FIX 1: THE FLOATING TOOL WINDOW ---
        # Instead of a Dialog, we make it a Widget but force it to float!
        self.setWindowFlags(Qt.Window) 
        self.setWindowTitle("Advanced Search Rules")
        self.resize(1200, 300)
        # ---------------------------------------
        
        # --- FIX 2: THE DEBOUNCER (Cures the sluggishness) ---
        self.update_timer = QTimer(self)
        self.update_timer.setSingleShot(True)
        self.update_timer.setInterval(250) # Waits 250ms after you stop typing to search
        self.update_timer.timeout.connect(self._apply_update)
        # -----------------------------------------------------
        
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(15, 15, 15, 15)
        
        self.rows_layout = QVBoxLayout()
        self.main_layout.addLayout(self.rows_layout)
        
        self.btn_start = QPushButton("+ Add Advanced Rule")
        self.btn_start.setMinimumHeight(35)
        self.btn_start.clicked.connect(self.start_builder)
        self.main_layout.addWidget(self.btn_start)
        
        self.main_layout.addStretch()

    def start_builder(self):
        self.btn_start.hide()
        self.add_row(None, 0)

    def add_row(self, reference_widget, new_level):
        new_row = SearchRuleWidget(self, level=new_level)
        
        if reference_widget:
            idx = self.rows_layout.indexOf(reference_widget)
            self.rows_layout.insertWidget(idx + 1, new_row)
        else:
            self.rows_layout.addWidget(new_row)
            
        # Hide the logic combo on the very first row
        if self.rows_layout.count() == 1:
            new_row.logic_combo.hide()
            
        self.trigger_update()

    def delete_row(self, widget):
        idx = self.rows_layout.indexOf(widget)
        if idx == -1: return
        
        # Confirm destruction if it has children
        my_level = widget.level
        has_children = False
        if idx + 1 < self.rows_layout.count():
            next_widget = self.rows_layout.itemAt(idx + 1).widget()
            if next_widget.level > my_level:
                has_children = True
                
        if has_children:
            from PySide6.QtWidgets import QMessageBox
            res = QMessageBox.warning(self, "Delete Group?", 
                "Deleting this row will also delete all indented rules below it. Continue?",
                QMessageBox.Yes | QMessageBox.No)
            if res == QMessageBox.No: return
            
            # Nuke the children
            while idx + 1 < self.rows_layout.count():
                child = self.rows_layout.itemAt(idx + 1).widget()
                if child.level > my_level:
                    child.deleteLater()
                    self.rows_layout.removeWidget(child)
                else:
                    break
        
        widget.deleteLater()
        self.rows_layout.removeWidget(widget)
        
        # Reset if empty
        if self.rows_layout.count() == 0:
            self.btn_start.show()
            
        # Ensure the first row always has its logic combo hidden
        elif self.rows_layout.count() > 0:
            first = self.rows_layout.itemAt(0).widget()
            first.logic_combo.hide()
            first.level = 0
            first.layout().setContentsMargins(10, 4, 4, 4)
            
        self.trigger_update()

    def clear_all_rules(self):
        """Nukes all rows, shows the start button, and triggers an update."""
        while self.rows_layout.count():
            item = self.rows_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        
        self.btn_start.show()
        self.trigger_update()

    def trigger_update(self):
        """Restarts the timer every time a key is pressed or checkbox clicked."""
        self.update_timer.start()

    def _apply_update(self):
        """Applies logic and notifies parent to enable/disable clear button."""
        rules = []
        for i in range(self.rows_layout.count()):
            widget = self.rows_layout.itemAt(i).widget()
            rules.append(widget.get_data())
            
        self.proxy.set_advanced_search(rules)

        # Notify the AssetManager (parent) to update the Clear button state
        # Rules exist if the list isn't empty and the first row has text
        has_content = len(rules) > 0 and any(r['text'] for r in rules)
        if hasattr(self.parent(), 'refresh_advanced_button_state'):
            self.parent().refresh_advanced_button_state(has_content)

    def closeEvent(self, event):
        """Hides the window instead of destroying it, keeping filters active."""
        self.hide()
        event.ignore()

class PandasModel(QAbstractTableModel):
    def __init__(self, data, parent_window, read_only=False): 
        super().__init__(parent_window) 
        self._data = data
        self.window = parent_window
        self.read_only = read_only

    def rowCount(self, parent=None): return self._data.shape[0]

    def columnCount(self, parent=None): 
        return len(self._data.columns)

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self._data.columns[col]
        return None

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid(): return None
        row, col = index.row(), index.column()
        
        # --- THE BACKGROUND / DIMMING / VALIDATION LOGIC ---
        if role == Qt.BackgroundRole:
            col_name = self._data.columns[col]
            
            # 1. Check for Range Validation (Highest Priority Highlight)
            if getattr(self, 'validation_enabled', False) and col_name in ['FIRST', 'LAST']:
                row_data = self._data.iloc[row]
                # We ask the parent (AssetManager) for the verdict
                # Note: Parent must implement validate_row_range
                parent = self.parent()
                if hasattr(parent, 'validate_row_range'):
                    is_valid, error_cols = parent.validate_row_range(row_data)
                    if not is_valid and col_name in error_cols:
                        return QColor(130, 40, 40) # Muted Red Alert

            # 2. Existing DIMMING Logic
            if "Source_Type" in self._data.columns:
                src_val = self._data.iat[row, self._data.columns.get_loc("Source_Type")]
                if src_val == "IGNORE":
                    return QColor(50, 50, 50) # Dark Grey for the whole row
                
                # Contextual dimming for SCAN redundant keys
                if col_name == "Lookup_Key" and src_val == ["SCAN"]:
                    return QColor(45, 45, 45) # Slightly different grey
            return None

        # --- DATA EXTRACTION ---
        col_name = self._data.columns[col]
        val = self._data.iloc[row, col]

        # --- 1. RENDER CHECKBOX ---
        if col_name == "Select":
            if role == Qt.CheckStateRole:
                return Qt.Checked if val is True else Qt.Unchecked
            elif role in (Qt.DisplayRole, Qt.EditRole):
                return None 

        if role in (Qt.DisplayRole, Qt.EditRole):
            return str(val) if pd.notna(val) else ""
            
        return None

    def flags(self, index):
        if not index.isValid(): return Qt.NoItemFlags
        col_name = self._data.columns[index.column()]
        
        # --- THE FIX: If the model is NOT read-only, everything is editable! ---
        if not self.read_only:
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable

        # --- LOCK DOWN EDITING (Read-Only Mode) ---
        if col_name == "Select":
            # Only the Select column gets Checkable + Enabled
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable
            
        # For the main app's Sub Status/Notes, keep them editable even in read_only mode
        if self.read_only and col_name in ["SUBSTATUS", "SUBNOTES", "Status"]:
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable
            
        # Everything else in Project Manager / Asset Manager is Read-Only
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def setData(self, index, value, role=Qt.EditRole):
        if not index.isValid(): return False
        row = index.row()
        col = index.column()
        col_name = self._data.columns[col]

        # --- 3. SAVE CHECKBOX STATE ---
        if col_name == "Select" and role == Qt.CheckStateRole:
            # Update the underlying dataframe
            self._data.iloc[row, col] = (value == Qt.Checked)
            self.dataChanged.emit(index, index, [Qt.CheckStateRole])
            return True

        if role == Qt.EditRole:
            self._data.iloc[row, col] = value
            self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.EditRole])
            
            # Keep your existing main app autosave trigger
            if self.read_only and self.window and hasattr(self.window, "start_autosave_fuse"):
                self.window.start_autosave_fuse()
            return True
        return False

class DataRootsEditorDialog(QDialog):
    """Bespoke editor to manage the JSON list of [name, path] tuples for data_root."""
    def __init__(self, current_data_string="", parent=None):
        super().__init__(parent)
        self.setWindowTitle("Manage Data Roots")
        self.resize(600, 300)
        
        layout = QVBoxLayout(self)
        
        # 1. THE TABLE
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Root ID", "Path", ""])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Fixed)
        self.table.setColumnWidth(2, 80)
        layout.addWidget(self.table)
        
        # 2. ACTIONS
        btn_layout = QHBoxLayout()
        btn_add = QPushButton("+ Add New Root")
        btn_add.clicked.connect(self.add_empty_row)
        btn_layout.addWidget(btn_add)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        # 3. SAVE / CANCEL
        btns = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

        # 4. LOAD DATA
        self.load_data(current_data_string)

    def load_data(self, raw_string):
        """Expects: [["default", "/mnt/jobs"], ["usb", "/Volumes/usb"]]"""
        self.table.setRowCount(0)
        if not raw_string: return
        
        try:
            data = json.loads(raw_string)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, list) and len(item) >= 2:
                        self.add_row(str(item[0]), str(item[1]))
                    elif isinstance(item, dict): # Fallback if we accidentally saved dicts
                        self.add_row(str(item.get('name', '')), str(item.get('path', '')))
            else:
                # Absolute legacy fallback
                self.add_row("default", raw_string)
        except:
            # Absolute legacy fallback
            self.add_row("default", raw_string)

    def add_empty_row(self):
        """Finds the next available generic name and focuses the cell for the user."""
        existing_names = []
        for r in range(self.table.rowCount()):
            item = self.table.item(r, 0)
            if item:
                existing_names.append(item.text().strip())

        # 1. Generate a unique default name: root_1, root_2, etc.
        counter = 1
        new_name = f"root_{counter}"
        while new_name in existing_names:
            counter += 1
            new_name = f"root_{counter}"

        # 2. Add the row
        self.add_row(new_name, "")

        # 3. IMMEDIATELY focus and select the name cell for the user to type
        last_row = self.table.rowCount() - 1
        name_item = self.table.item(last_row, 0)
        self.table.setCurrentItem(name_item)
        self.table.editItem(name_item) 

    def add_row(self, name, path):
        row = self.table.rowCount()
        self.table.insertRow(row)
        
        # ROOT ID Cell
        name_item = QTableWidgetItem(name)
        # Subtle UI hint: make IDs look like variables
        name_item.setToolTip("This ID will be used as the variable name (e.g. {default})")
        self.table.setItem(row, 0, name_item)
        
        # PATH Cell
        self.table.setItem(row, 1, QTableWidgetItem(path))
        
        # ACTION Buttons
        widget = QWidget()
        h_layout = QHBoxLayout(widget)
        h_layout.setContentsMargins(2, 2, 2, 2)
        h_layout.setSpacing(4)
        
        btn_browse = QPushButton("...")
        # BUG FIX: Use a more robust way to find the row during callback
        btn_browse.clicked.connect(lambda: self.browse_path_for_widget(btn_browse))
        
        btn_del = QPushButton("X")
        btn_del.setStyleSheet("background-color: #882e2e; color: white; font-weight: bold;")
        btn_del.clicked.connect(lambda: self.delete_row_for_widget(btn_del))
        
        h_layout.addWidget(btn_browse)
        h_layout.addWidget(btn_del)
        self.table.setCellWidget(row, 2, widget)

    def browse_path_for_widget(self, widget):
        pos = widget.parent().mapTo(self.table, widget.pos())
        row = self.table.indexAt(pos).row()
        if row < 0: return
        
        current_path = self.table.item(row, 1).text()
        folder = QFileDialog.getExistingDirectory(self, "Select Root Directory", current_path)
        
        if folder:
            # --- THE MANDATORY SANITIZATION ---
            # We force forward slashes here so the JSON string 
            # is identical and valid on Mac, Windows, and Linux.
            clean_path = folder.replace('\\', '/')
            self.table.setItem(row, 1, QTableWidgetItem(clean_path))

    def delete_row_for_widget(self, widget):
        pos = widget.parent().mapTo(self.table, widget.pos())
        row = self.table.indexAt(pos).row()
        if row >= 0:
            self.table.removeRow(row)

    def browse_path(self, row):
        current_path = self.table.item(row, 1).text()
        folder = QFileDialog.getExistingDirectory(self, "Select Root Directory", current_path)
        if folder:
            self.table.setItem(row, 1, QTableWidgetItem(folder))

    def get_serialized_data(self):
        """Returns a strict JSON-compliant list of lists."""
        data = []
        for r in range(self.table.rowCount()):
            name = self.table.item(r, 0).text().strip()
            # Final safety check: ensure no rogue backslashes enter the JSON string
            path = self.table.item(r, 1).text().strip().replace('\\', '/')
            if name and path:
                data.append([name, path])
        
        # This now produces [["root_1", "F:/jobs/rage"]] 
        # instead of [["root_1", "F:\jobs\rage"]]
        return json.dumps(data)

class SelectionModel(PandasModel):
    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid(): return None
        row, col = index.row(), index.column()
        col_name = self._data.columns[col]
        val = self._data.iloc[row, col]

        if col_name == "Select":
            if role == Qt.CheckStateRole:
                # SAFEST CHECK: Handles NumPy bools, Python bools, and strings
                is_checked = str(val).lower() in ['true', '1', 't'] or val == True
                return Qt.Checked if is_checked else Qt.Unchecked
            if role in (Qt.DisplayRole, Qt.EditRole): return None
            
        return super().data(index, role)

    def flags(self, index):
        if not index.isValid(): return Qt.NoItemFlags
        # ONLY Enabled and Selectable. Do NOT use ItemIsUserCheckable.
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def setData(self, index, value, role=Qt.EditRole):
        if not index.isValid(): return False
        
        col_name = self._data.columns[index.column()]
        if col_name == "Select" and role in (Qt.EditRole, Qt.CheckStateRole):
            # Force primitive Python boolean to avoid Pandas type-conflicts
            new_val = bool(value) if isinstance(value, bool) else (value == Qt.Checked)
            
            self._data.iat[index.row(), index.column()] = new_val
            self.dataChanged.emit(index, index, [Qt.CheckStateRole])
            return True
        return False

class ConfigHub(QDialog):
    def __init__(self, engine, parent=None):
        super().__init__(parent)
        self.engine = engine
        self.setWindowTitle(f"Project Configuration: {self.engine.settings.get('ProjectName', 'Unknown')}")
        self.resize(1200, 750)

        # Main Layout: Horizontal Split
        self.main_layout = QHBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        # 1. NAVIGATION SIDEBAR
        self.nav_list = QListWidget()
        self.nav_list.setFixedWidth(220)
        self.nav_list.setStyleSheet("""
            QListWidget { 
                background-color: #2b2b2b; 
                border: none; 
                outline: none;
                font-size: 13px;
                color: #ccc;
            }
            QListWidget::item { 
                padding: 15px 20px; 
                border-bottom: 1px solid #333; 
            }
            QListWidget::item:selected { 
                background-color: #5a2e88; 
                color: white; 
                border-left: 4px solid #8e44ad;
            }
            QListWidget::item:hover:!selected {
                background-color: #3d3d3d;
            }
        """)

        # 2. CONTENT AREA (The Stack)
        self.stack = QStackedWidget()
        self.stack.setStyleSheet("background-color: #1e1e1e; border-left: 1px solid #333;")

        # 3. BUILD THE PAGES
        # We reuse your existing classes. We just need to handle their layouts.
        self.setup_pages()

        # Connect Sidebar to Stack
        self.nav_list.currentRowChanged.connect(self.stack.setCurrentIndex)
        self.nav_list.setCurrentRow(0)

        self.main_layout.addWidget(self.nav_list)
        self.main_layout.addWidget(self.stack)

    def setup_pages(self):
        """Recreates the logic used by the dropdown siblings for a seamless transition."""
        
        # Define the configurations exactly as they are in your AssetManager methods
        config_definitions = [
            {
                "label": "Project Settings",
                "class": ProjectSettingsEditor,
                "args": [self.engine]
            },
            {
                "label": "Shot Registry",
                "class": GenericCSVEditor,
                "args": [self.engine.settings.get('shots_csv')],
                "kwargs": {"title": "Shots", "allow_add_column": False}
            },
            {
                "label": "Path Substitutions",
                "class": GenericCSVEditor,
                "args": [self.engine.settings.get('path_subs_csv', "")],
                "kwargs": {"title": "Path Subs", "allow_add_column": True} # Matches dropdown behavior
            },
            {
                "label": "Nuke Setup",
                "class": NukeSetupDialog,
                # We need to provide the template_id and manager_df as your dialog expects
                "args": [self.engine, "Primary", os.path.join(self.engine.root, "Project_Actions", "Nuke"), self.parent().df_master]
            },
            {
                "label": "Notes Config",
                "class": GenericCSVEditor,
                "args": [self.engine.settings.get('notes_config_csv', "")],
                "kwargs": {
                    "title": "Notes", 
                    "dropdown_cols": {"Status": ["Active", "Resolved", "On Hold"]} # Recreates your dropdown logic
                }
            }
        ]

        for config in config_definitions:
            try:
                # Instantiate with *args and **kwargs
                cls = config["class"]
                args = config.get("args", [])
                kwargs = config.get("kwargs", {})
                
                editor = cls(*args, **kwargs, parent=self)
                editor.setWindowFlags(Qt.Widget)
                
                # Standard embedding logic
                container = QWidget()
                page_layout = QVBoxLayout(container)
                header = QLabel(f"<h2>{config['label']}</h2>")
                header.setStyleSheet("color: #888; padding: 10px;")
                
                page_layout.addWidget(header)
                page_layout.addWidget(editor)
                
                self.nav_list.addItem(config['label'])
                self.stack.addWidget(container)
                
            except Exception as e:
                print(f"Architect Error: Could not embed {config['label']} - {e}")

    def exec_(self):
        return super().exec_()

class DynamicKeyValueEditor(QDialog):
    PRESETS = {
        "project_settings": {
            "title": "Project Settings",
            "data_root_keys": ["data_root"],
            "dir_browse_keys": ["catalog_dir"],
            "file_browse_keys": ["shots_csv"],
            "checkbox_keys": ["dual_name"]
        },
        "app_config": {
            "title": "App Executable Settings",
            "file_browse_keys": ["bin"]
        },
        "notes_config": {
            "title": "Notes Configuration"
        },
        "user_prefs": {
            "title": "User Preferences",
            "dir_browse_keys": ["user_projects_default_dir"]
        }
    }

    def __init__(self, csv_path, title=None, engine=None, parent=None, preset=None):
        super().__init__(parent)
        self.csv_path = os.path.normpath(csv_path)
        self.engine = engine
        
        p = self.PRESETS.get(preset, {})
        self.title_val = title or p.get("title", "Config Editor")
        self.file_browse_keys = p.get("file_browse_keys", [])
        self.dir_browse_keys = p.get("dir_browse_keys", [])
        self.checkbox_keys = p.get("checkbox_keys", [])
        self.data_root_keys = p.get("data_root_keys", [])
        
        self.inputs = {}
        self.setWindowTitle(f"{self.title_val} - {os.path.basename(self.csv_path)}")
        self.resize(950, 600)
        
        # --- CLEAN STYLES ---
        self.setStyleSheet("""
            QDialog { background-color: #1e1e1e; color: #e0e0e0; }
            QLabel { color: #888; font-size: 11px; }
            QPushButton { 
                background-color: #333; color: #eee; border: 1px solid #444; 
                border-radius: 3px; padding: 4px 8px; min-width: 70px;
            }
            QLineEdit, QTextEdit { 
                background-color: #252525; color: #eee; border: 1px solid #333; border-radius: 3px;
            }
        """)
        
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(15, 15, 15, 15)
        
        # Header
        header = QLabel(f"PROJECT CONFIG: {os.path.basename(self.csv_path).upper()}")
        header.setStyleSheet("font-size: 14px; font-weight: bold; color: #555; margin-bottom: 10px;")
        self.main_layout.addWidget(header)
        
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setStyleSheet("background-color: transparent;")
        
        scroll_content = QWidget()
        # THE FIX: This layout holds the rows and pushes them UP
        self.rows_container = QVBoxLayout(scroll_content)
        self.rows_container.setSpacing(12) 
        self.rows_container.setContentsMargins(0, 0, 10, 0)
        self.rows_container.setAlignment(Qt.AlignTop) # Stick to the roof

        try:
            self.df = pd.read_csv(self.csv_path, encoding='cp1252', dtype=str).fillna("")
            rel_path = os.path.relpath(self.csv_path, self.engine.root).replace('\\', '/') if self.engine else ""
        except:
            return

        for _, row in self.df.iterrows():
            key = str(row['Key'])
            raw_val = str(row['Value'])
            resolved_val = self.engine._resolve_pointer(key, raw_val, rel_path) if self.engine else raw_val

            # ONE VERTICAL BLOCK PER SETTING
            setting_block = QVBoxLayout()
            setting_block.setSpacing(2)

            # --- THE ROW: [LABEL (fixed)] [BUTTON (fixed)] [INPUT (stretching)] ---
            row_layout = QHBoxLayout()
            row_layout.setSpacing(10)

            # 1. Label (Fixed Width for a clean spine)
            lbl = QLabel(f"<b>{key}</b>")
            lbl.setFixedWidth(160)
            lbl.setStyleSheet("color: #bbb; font-size: 12px;")
            row_layout.addWidget(lbl)

            # 2. Browse Button (Only if needed)
            if key in self.file_browse_keys or key in self.dir_browse_keys or key in self.data_root_keys:
                btn_text = "Roots..." if key in self.data_root_keys else "Choose..."
                btn = QPushButton(btn_text)
                if key in self.data_root_keys: pass # Connected later
                elif key in self.file_browse_keys: btn.clicked.connect(lambda chk=False, k=key: self.browse_file(k))
                else: btn.clicked.connect(lambda chk=False, k=key: self.browse_dir(k))
                row_layout.addWidget(btn)

            # 3. The Input / Checkbox (Stretch to fill)
            if key in self.checkbox_keys:
                cb = QCheckBox("Enabled")
                cb.setChecked(str(raw_val).strip().lower() in ['true', '1', 't', 'yes'])
                self.inputs[key] = cb
                row_layout.addWidget(cb)
                row_layout.addStretch(1)
            elif key in self.data_root_keys:
                edit = QLineEdit(raw_val); edit.hide(); self.inputs[key] = edit
                preview_lbl = QLabel()
                self._update_data_root_preview(preview_lbl, raw_val, resolved_val)
                row_layout.addWidget(preview_lbl, 1)
                # Map the button we just added
                btn_widget = row_layout.itemAt(1).widget()
                btn_widget.clicked.connect(lambda chk=False, e=edit, l=preview_lbl: self.open_data_roots_editor(e, l))
            else:
                try:
                    edit = SingleLineWrapEdit(raw_val)
                    edit.setMinimumHeight(40); edit.setMaximumHeight(60)
                except NameError:
                    edit = QLineEdit(raw_val)
                self.inputs[key] = edit
                row_layout.addWidget(edit, 1) # STRETCH

            setting_block.addLayout(row_layout)

            # 4. Inheritance Sub-label (Directly under the input)
            if raw_val.strip().upper() == "{LOCALDOTDIR}":
                ptr_lbl = QLabel(f"      ⮎ Inheriting: {resolved_val}")
                ptr_lbl.setStyleSheet("color: #666; font-style: italic; margin-left: 170px;")
                setting_block.addWidget(ptr_lbl)

            self.rows_container.addLayout(setting_block)

        # IMPORTANT: This stretch pushes every row above it to the top
        self.rows_container.addStretch(1) 

        scroll.setWidget(scroll_content)
        self.main_layout.addWidget(scroll)

        # Footer
        line = QFrame(); line.setFrameShape(QFrame.HLine); line.setStyleSheet("background-color: #333;")
        self.main_layout.addWidget(line)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.save_data)
        self.buttons.rejected.connect(self.reject)
        self.main_layout.addWidget(self.buttons)

    # (Keep internal methods like save_data, browse_file, _update_data_root_preview exactly as before)
    def _update_data_root_preview(self, label, raw_string, resolved_string):
        label.setStyleSheet("color: #aaa; background-color: #111; padding: 6px; border: 1px solid #222; border-radius: 2px;")
        if str(raw_string).strip().upper() == "{LOCALDOTDIR}":
            label.setText(f"⮎ Inheriting: {resolved_string}")
            return
        try:
            data = json.loads(raw_string)
            label.setText(" | ".join([f"{i[0]}: {i[1]}" for i in data]) if data else "Empty")
        except: label.setText(str(raw_string))

    def open_data_roots_editor(self, line_edit, preview_label):
        dlg = DataRootsEditorDialog(line_edit.text(), self)
        if dlg.exec():
            new_val = dlg.get_serialized_data()
            line_edit.setText(new_val)
            self._update_data_root_preview(preview_label, new_val, new_val)

    def browse_file(self, key):
        widget = self.inputs[key]
        path, _ = QFileDialog.getOpenFileName(self, "Select File", "", "All Files (*)")
        if path:
            p = path.replace('\\', '/')
            widget.setPlainText(p) if hasattr(widget, 'setPlainText') else widget.setText(p)

    def browse_dir(self, key):
        widget = self.inputs[key]
        path = QFileDialog.getExistingDirectory(self, "Select Directory")
        if path:
            p = path.replace('\\', '/')
            widget.setPlainText(p) if hasattr(widget, 'setPlainText') else widget.setText(p)

    def save_data(self):
        for i, row in self.df.iterrows():
            key = str(row['Key'])
            if key in self.inputs:
                w = self.inputs[key]
                val = str(w.isChecked()) if isinstance(w, QCheckBox) else (w.toPlainText() if hasattr(w, 'toPlainText') else w.text())
                self.df.at[i, 'Value'] = val.strip()
        try:
            self.df.to_csv(self.csv_path, index=False, encoding='cp1252')
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Save Error", f"Could not save CSV:\n{e}")

class PathSwapper:
    PATHSUBS = [] 
    HEADERS = [] # We store the CSV headers here now
    TARGET_COLUMN = None # The user can set this to "aws", "gdrive", etc.

    @classmethod
    def translate(cls, path):
        if not path or pd.isna(path): return ""
        
        # 1. Determine our "Local" Column Index
        local_idx = -1
        
        # Priority A: User-specified arbitrary column
        if cls.TARGET_COLUMN and cls.TARGET_COLUMN in cls.HEADERS:
            local_idx = cls.HEADERS.index(cls.TARGET_COLUMN)
        
        # Priority B: Standard OS Fallback
        if local_idx == -1:
            if sys.platform == "darwin" and "Mac_Root" in cls.HEADERS:
                local_idx = cls.HEADERS.index("Mac_Root")
            elif sys.platform == "win32" and "Win_Root" in cls.HEADERS:
                local_idx = cls.HEADERS.index("Win_Root")
            elif "Linux_Root" in cls.HEADERS:
                local_idx = cls.HEADERS.index("Linux_Root")

        if local_idx == -1: return os.path.normpath(path) # No valid roots found

        normalized_path = str(path).replace("\\", "/")
        
        # 2. Iterate through all rows in the CSV
        for row in cls.PATHSUBS:
            local_root = str(row[local_idx]) if pd.notna(row[local_idx]) else ""
            if not local_root: continue
            
            # 3. Check against EVERY OTHER column in this row
            # This is the "Agnostic" part: we don't care if the path 
            # started as an 'aws' path or a 'mac' path.
            for i, cell_val in enumerate(row):
                if i == local_idx: continue # Don't swap a path with itself
                
                remote_root = str(cell_val).replace("\\", "/") if pd.notna(cell_val) else ""
                if not remote_root: continue

                if normalized_path.lower().startswith(remote_root.lower()):
                    # Swap the alien root for our defined local_root
                    new_path = local_root + normalized_path[len(remote_root):]
                    return os.path.normpath(new_path)
        
        return os.path.normpath(path)
    
class ConfigEngine:
    def __init__(self, root_path, bootstrap=False, use_pointers=False, template_name="_pipe_config_default"):
        self.root = os.path.abspath(os.path.normpath(root_path))
        self.project_root = os.path.dirname(os.path.normpath(self.root))
        
        # --- THE TEMPLATE ROUTER ---
        manager_dir = os.path.normpath(os.path.join(os.path.expanduser("~"), ".simplepipemanager"))
        target_template_dir = os.path.join(manager_dir, "_pipe_config_templates", template_name)
        
        # The Fallback Safety Net
        if not os.path.exists(target_template_dir):
            target_template_dir = os.path.join(manager_dir, "_pipe_config_templates", "_pipe_config_default")
            
        self.local_dot_dir = os.path.join(manager_dir, "_pipe_config")
        self.apps_dir = os.path.join(self.root, "Media_Actions")
        
        if bootstrap or not os.path.exists(self.apps_dir):
            self.bootstrap_template(use_pointers=use_pointers)
            
        self.pathsubs = self._load_flat_csv("Path_Subs.csv")
        # Global Injection
        PathSwapper.PATHSUBS = self.pathsubs

        # Load and SWAP settings immediately
        raw = self._load_kv_csv("Project_Settings.csv")
        self.settings = {}
        for k, v in raw.items():
            if k == 'data_root':
                # CRITICAL: Bypass PathSwapper here so Windows normpath doesn't
                # flip forward slashes to backslashes and break the JSON parser.
                self.settings[k] = str(v)
            elif "/" in str(v) or "\\" in str(v):
                self.settings[k] = PathSwapper.translate(str(v))
            else:
                self.settings[k] = v

        # --- THE MULTI-ROOT INTERCEPTOR ---
        if 'data_root' in self.settings:
            raw_dr = str(self.settings['data_root']).strip()
            
            # 1. Store the untouched payload for future multi-root scrapers
            self.settings['data_root_raw'] = raw_dr 

            # 2. Try to extract the first tuple's path for primary pipeline write actions
            try:
                import json
                dr_list = json.loads(raw_dr)
                
                if isinstance(dr_list, list) and len(dr_list) > 0:
                    first_item = dr_list[0]
                    # We expect format: [["name", "/path"], ["name2", "/path2"]]
                    if isinstance(first_item, (list, tuple)) and len(first_item) >= 2:
                        # NOW we translate the cleanly extracted path!
                        self.settings['data_root'] = PathSwapper.translate(str(first_item[1]))
            except Exception:
                # It's a legacy flat string (e.g. "/Volumes/jobs"). 
                # Translate it now since we skipped it in the initial loop.
                self.settings['data_root'] = PathSwapper.translate(raw_dr)
                
        self.apps = self._discover_apps()
        # Load Naming Templates
        self.naming_templates = self._load_kv_csv("Naming_Templates.csv")

    def _resolve_pointer(self, key, value, filename):
        """Resolves outward pointers by querying the exact same schema at the target boundary."""
        if str(value).strip().upper() == "{LOCALDOTDIR}":
            target_file = os.path.join(self.local_dot_dir, filename)
            
            if os.path.exists(target_file):
                try:
                    df = pd.read_csv(target_file, encoding='cp1252', dtype=str).fillna("")
                    match = df[df['Key'] == key]
                    if not match.empty:
                        return str(match.iloc[0]['Value'])
                except Exception as e:
                    print(f"Pointer resolution error for {key} in {filename}: {e}")
            
            # Failure-centric: If the global file/key is missing, return empty string
            return "" 
            
        # Not a pointer, return the local value untouched
        return value
        
    def _load_kv_csv(self, filename):
        path = os.path.join(self.root, filename)
        if not os.path.exists(path): return {}
        try:
            # ADDED: dtype=str to keep padding in settings
            df = pd.read_csv(path, encoding='cp1252', dtype=str).fillna("")
            raw_dict = dict(zip(df['Key'], df['Value']))
            
            # --- THE MAGIC: Resolve the pointers before returning ---
            return {k: self._resolve_pointer(k, v, filename) for k, v in raw_dict.items()}
            
        except Exception as e:
            print(f"Error loading {filename}: {e}")
            return {}

    def _load_flat_csv(self, filename):
        path = os.path.join(self.root, filename)
        if not os.path.exists(path): return []
        try: 
            df = pd.read_csv(path, encoding='cp1252', dtype=str).fillna("")
            
            # --- CRITICAL: Capture the headers for the PathSwapper ---
            if filename == "Path_Subs.csv":
                PathSwapper.HEADERS = df.columns.tolist()
                
            return df.values.tolist()
        except: 
            return []

    def _discover_apps(self):
        """Discovers apps by folder name and loads the OS-specific CSV."""
        apps = {}
        if not os.path.exists(self.apps_dir): return {}
        
        # Determine our current OS label for the sub-folder search
        plat_folder = "win" if sys.platform == "win32" else "mac" if sys.platform == "darwin" else "linux"
        
        # Look at each directory in Media_Actions (e.g., /Nuke, /RV)
        for app_name in os.listdir(self.apps_dir):
            app_path = os.path.join(self.apps_dir, app_name)
            if os.path.isdir(app_path):
                # Look for the specific OS csv inside that folder
                os_csv = os.path.join(app_path, f"{plat_folder}.csv")
                if os.path.exists(os_csv):
                    # Load the Key/Value pairs for this specific OS
                    apps[app_name] = self._load_kv_csv_multi(os_csv)
        return apps

    def _load_kv_csv_multi(self, path):
        """Modified loader to allow multiple rows with the same Key (for 'env')."""
        data = defaultdict(list)
        
        # Determine the relative filename so the resolver knows where to look globally
        try:
            rel_filename = os.path.relpath(path, self.root).replace('\\', '/')
        except ValueError:
            rel_filename = os.path.basename(path) # Fallback if paths cross drives on Windows

        try:
            df = pd.read_csv(path, encoding='cp1252', dtype=str).fillna("")
            for _, row in df.iterrows():
                key = str(row['Key'])
                raw_val = str(row['Value'])
                
                # --- THE MAGIC: Resolve the pointer for each row ---
                resolved_val = self._resolve_pointer(key, raw_val, rel_filename)
                
                data[key].append(resolved_val)
            return dict(data)
        except Exception as e:
            print(f"Error loading {path}: {e}")
            return {}

    def get_catalog_path(self, catalog_name):
        """Resolves a catalog key to a physical CSV path in the data root."""
        # 1. Grab the catalog_dir (the parent of catalogs) from settings
        c_dir = PathSwapper.translate(str(self.settings.get('catalog_dir', '')))
        
        # 2. Point to the 'catalogs' subfolder within that data root
        full_path = os.path.join(c_dir, "catalogs", f"{catalog_name}.csv")
        return os.path.normpath(full_path)
    
    def bootstrap_template(self, target_dir=None, mode='create', use_pointers=False):
        if target_dir is None:
            target_dir = self.root

        # --- THE HELPER THAT DOES THE WORK ---
        def ptr(default_val, force_local=False):
            if use_pointers and not force_local:
                return "{LOCALDOTDIR}" # This physically writes the string to the CSV!
            return default_val

        def write(filename, rows):
            path = os.path.join(target_dir, filename)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            if mode == 'create' or not os.path.exists(path):
                self.write_csv(path, rows)
                return

            # --- MODE: SYNC (The Additive Logic) ---
            if mode == 'sync' and os.path.exists(path):
                try:
                    existing_df = pd.read_csv(path, dtype=str).fillna("")
                    incoming_df = pd.DataFrame(rows[1:], columns=rows[0]).astype(str)
                    key_col = rows[0][0]
                    existing_keys = set(existing_df[key_col].tolist())
                    missing_rows = incoming_df[~incoming_df[key_col].isin(existing_keys)]
                    
                    if not missing_rows.empty:
                        updated_df = pd.concat([existing_df, missing_rows], ignore_index=True)
                        updated_df.to_csv(path, index=False)
                except Exception as e:
                    print(f"Failed to sync {filename}: {e}")

        # Project Settings
        write("Project_Settings.csv", [
            ["Key", "Value"],
            # data_root is specific to every project, so we force it to remain local (empty)
            ["data_root", ptr("", force_local=True)],
            # catalog_dir physically lives in the local project, so force local
            ["catalog_dir", ptr(self.project_root.replace('\\', '/'), force_local=True)], 
            
            # --- THE UPDATE: Absolute path to the generated Shots_Template.csv ---
            ["shots_csv", ptr(os.path.join(target_dir, "Shots_Template.csv").replace('\\', '/'), force_local=True)],
            
            ["dual_name", ptr("False")],
            ["submission_types", ptr("WIP, Final Pending QC, Final QC Approved")],
            ["status_options", ptr("Ready for Review, Needs Update, Approved, RTS, Pending")],
            ["submission_csv_headers", ptr("LOCALPATH,SHOTNAME,FILENAME,FIRST,LAST,SUBNOTES,SUBTYPE")],
            ["submission_review_headers", ptr("LOCALPATH,SHOTNAME,FILENAME,FIRST,LAST,SUBTYPE,SUBNOTES")],
            ["playlist_review_headers", ptr("LOCALPATH,SHOTNAME,FILENAME,FIRST,LAST,SUBNOTES")],
            ["scrape_blacklist", ptr(".trash, .snapshot, _thumbs")],
            ["padding_default", ptr("4")],
            ["padding_scans", ptr("4")],
            ["padding_renders", ptr("4")]
        ])
        
        # --- NEW: Project Actions (Nuke Folder-Based Structure) ---
        nuke_root = "Project_Actions/Nuke"
        
        # 1. THE MASTER INDEX (Just IDs now - Flat Tabular, no ptr)
        write(f"{nuke_root}/nuke_templates.csv", [
            ["Template_ID"],
            ["read_write_setup"]
        ])

        # 2. THE SPECIFIC TEMPLATE SUBDIRECTORY
        rw_dir = f"{nuke_root}/read_write_setup"
        
        # 3. THE CONFIG CSV (Paths - Key/Value, gets ptr)
        write(f"{rw_dir}/config.csv", [
            ["Key", "Value"],
            ["Source_NK", ptr("")],
            ["Output_Template_path", ptr("{data_root}/{SEQUENCE}/{SHOTNAME}/nuke/{SHOTNAME}")],
            ["Output_Template_file", ptr("{SHOTNAME}_comp_v001.nk")],
            ["nuke_comp_render_path", ptr("{data_root}/comp/{ALTSHOTNAME}/{ALTSHOTNAME}_comp_v001")],
            ["nuke_comp_render_filename", ptr("{ALTSHOTNAME}_comp_v001.{padding_default}.exr")]
        ])

        # 4. THE MAPPING CSV (Variables - Flat Tabular, no ptr)
        write(f"{rw_dir}/mapping.csv", [
            ["Variable", "Source_Type", "Lookup_Key"],
            ["FIRSTFRAME", "HEADER", "FIRSTFRAME"],
            ["LASTFRAME", "HEADER", "LASTFRAME"]
        ])

        # Path Subs (Flat Tabular, no ptr)
        write("Path_Subs.csv", [
            ["Mac_Root", "Win_Root", "Linux_Root", "cloud"],
            ["/Volumes/jobs", "J:", "/mnt/jobs", "{aws_config}"]
        ])

        # Naming Templates (Key/Value, gets ptr)
        write("Naming_Templates.csv", [
            ["Key", "Value"],
            ["scan_name_template", ptr("SHOTNAME_plate_vHEROPLATE")]
        ])

        # NOTES CONFIG (Scoped) --- (Key/Type/Value, Value gets ptr)
        write("Notes_Config.csv", [
            ["Key", "Key_Type", "Value"],
            # Global Logic
            ["substitute_unresolved_vars", "main", ptr("True")],
            ["unresolved_vars_default_string", "main", ptr("default")],
            
            # Roots (The Starting Points)
            ["default", "root", ptr("{data_root}")],
            ["client", "root", ptr("{s3_PROJECT_CLIENTNAME_config}")],
            
            # Trees (The Directory Structures)
            ["default", "tree", ptr("notes/{SHOTNAME}")],
            ["sequence_shot_task_user", "tree", ptr("{SEQUENCE}/{SHOTNAME}/{TASK}/{USER}")],
            ["client", "tree", ptr("prod/client/.notes/{SEQUENCE}/{SHOTNAME}/{TASK}/{USER}")],
            
            # Names (The File Conventions)
            ["default", "name", ptr("note_{TIMESTAMP}")],
            ["client", "name", ptr("note_CLIENTNAME_{TIMESTAMP}")]
        ])

        # Shots Template (Flat Tabular, no ptr)
        write("Shots_Template.csv", [
            ["SEQUENCE", "PROCESS", "SHOTNAME", "ALTSHOTNAME", "FIRSTFRAME", "LASTFRAME", "HEROPLATE"],
            ["abc_001", "0", "job_001_abc_0010", "abc_001_0010", "1001", "1100", "001"]
        ])

        # Nuke Nested Configs (Key/Value, gets ptr)
        nuke_base = "Media_Actions/Nuke"
        write(f"{nuke_base}/win.csv", [
            ["Key", "Value"],
            ["bin", ptr("G:/Program Files/Nuke16.0v6/Nuke16.0.exe")],
            ["flags", ptr("--nukex, -m 18")]
            #["env", ptr("OCIO=Z:/config/aces_1.2/config.ocio")],
            #["env", ptr("NUKE_PATH=Z:/pipeline/nuke")]
        ])
        write(f"{nuke_base}/mac.csv", [
            ["Key", "Value"],
            ["bin", ptr("/Applications/Nuke16.0v6/Nuke16.0v6.app/Contents/MacOS/Nuke16.0")],
            ["flags", ptr("--nukex")]
            # ["env", ptr("OCIO=/Volumes/jobs/config/aces_1.2/config.ocio")]
        ])
        write(f"{nuke_base}/linux.csv", [
            ["Key", "Value"],
            ["bin", ptr("/opt/Nuke16.0v6/Nuke16.0")],
            ["flags", ptr("--nukex")]
        ])

        # RV Nested Configs (Key/Value, gets ptr)
        rv_base = "Media_Actions/RV"
        write(f"{rv_base}/win.csv", [
            ["Key", "Value"],
            ["bin", ptr("C:/Program Files/OpenRV-win/bin/rv.exe")],
            ["flags", ptr("")]
        ])
        write(f"{rv_base}/mac.csv", [
            ["Key", "Value"],
            ["bin", ptr("/Applications/RV.app/Contents/MacOS/RV")],
            ["flags", ptr("")],
            ["env", ptr("/Users/uel/Dasein/daseinVfxPipe/aces_1.2/config.ocio")]
        ])
        write(f"{rv_base}/linux.csv", [
            ["Key", "Value"],
            ["bin", ptr("/opt/RV/bin/rv")],
            ["flags", ptr("")],
            ["env", ptr("/Users/uel/Dasein/daseinVfxPipe/aces_1.2/config.ocio")]
        ])

    def write_csv(self, path, rows):
        """Standard helper to actually touch the disk."""
        import csv
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows)

class ProjectLauncherDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Pipeline Control Center")
        self.resize(600, 520) 
        self.active_windows = {} 
        
        # --- THE FIX: Back to one clean layout ---
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)

        # --- 0. BOOTSTRAP USER PREFS ---
        self.prefs_path = os.path.normpath(os.path.join(os.path.expanduser("~"), ".simplepipemanager", "user_prefs.csv"))
        if not os.path.exists(self.prefs_path):
            os.makedirs(os.path.dirname(self.prefs_path), exist_ok=True)
            default_root = os.path.join(os.path.expanduser("~"), ".simplepipemanager", "projects").replace('\\', '/')
            pd.DataFrame([["user_projects_default_dir", default_root]], columns=['Key', 'Value']).to_csv(self.prefs_path, index=False)

        # --- 1. ENSURE GLOBAL CONFIG EXISTS & SPIN UP ENGINE ---
        self.global_config_dir = os.path.normpath(os.path.join(os.path.expanduser("~"), ".simplepipemanager", "_pipe_config"))
        if not os.path.exists(self.global_config_dir):
            temp_eng = ConfigEngine(self.global_config_dir, bootstrap=False)
            temp_eng.bootstrap_template(mode='create', use_pointers=False)
            
        self.global_engine = ConfigEngine(self.global_config_dir, bootstrap=False)

        # --- NEW: TOP ROW WITH PREFERENCES BUTTON ---
        top_row = QHBoxLayout()
        top_row.addWidget(QLabel("<b>Registered Projects:</b>"))
        top_row.addStretch()
        
        btn_user_prefs = QPushButton("⚙ Preferences")
        btn_user_prefs.setFixedWidth(120)
        btn_user_prefs.setStyleSheet("background-color: #444; color: #eee; font-size: 11px; padding: 4px;")
        btn_user_prefs.clicked.connect(self.open_user_prefs)
        top_row.addWidget(btn_user_prefs)
        
        layout.addLayout(top_row)

        # --- ADDITIVE: Lifecycle Filter ---
        self.check_show_retired = QCheckBox("Show Retired Projects")
        self.check_show_retired.stateChanged.connect(self._load_projects)
        layout.addWidget(self.check_show_retired)
        self.list_widget = QListWidget()
        self.list_widget.doubleClicked.connect(self.launch)
        layout.addWidget(self.list_widget)
        
        # Row for Config/Folder management
        btn_layout = QHBoxLayout()
        btn_new = QPushButton("New Project...")
        btn_new.clicked.connect(self.create_new)
        
        self.btn_import = QPushButton("Import Projects...")
        self.btn_import.clicked.connect(self.action_import_search)
        
        btn_edit = QPushButton("Open Config Folder")
        btn_edit.clicked.connect(self.open_folder)
        
        self.btn_update_config = QPushButton("Update Project Config")
        self.btn_update_config.clicked.connect(self.action_update_config)
        self.btn_update_config.setStyleSheet("background-color: #3d4c5c; color: #add8e6;")
        
        # --- ADDITIVE: Lifecycle Management Button ---
        self.btn_manage = QPushButton("Manage Project...")
        self.btn_manage.setStyleSheet("background-color: #5c3d3d; color: #ffbaba;")
        self.btn_manage.clicked.connect(self.handle_lifecycle)
        
        # --- 2. BUILD THE GLOBAL SETTINGS DROPDOWN ---
        self.btn_edit_globals = QPushButton("Global Settings ▾")
        self.btn_edit_globals.setStyleSheet("background-color: #2e5a88; color: white; font-weight: bold;")
        
        global_menu = QMenu(self)
        global_menu.addAction("Edit Global Project Settings", self.open_global_settings)
        global_menu.addSeparator()
        
        # Nested App Configs (Nuke, RV, etc.)
        app_menu = global_menu.addMenu("Global App Executables...")
        for app_name in sorted(self.global_engine.apps.keys()):
            app_menu.addAction(f"Edit {app_name} Paths", lambda a=app_name: self.open_global_app_config(a))
            
        global_menu.addSeparator()
        global_menu.addAction("Edit Global Path Subs", self.open_global_pathsubs)
        global_menu.addSeparator()
        global_menu.addAction("Edit Global Naming Templates", self.open_global_templates)
        global_menu.addSeparator()
        global_menu.addAction("Edit Global Notes Config", self.open_global_notes_config)
        
        self.btn_edit_globals.setMenu(global_menu)
        
        # --- 3. ADD TO YOUR EXISTING ROW ---
        btn_layout.addWidget(btn_new)
        btn_layout.insertWidget(1, self.btn_import)
        btn_layout.addWidget(btn_edit)
        btn_layout.addWidget(self.btn_update_config)
        btn_layout.addWidget(self.btn_manage)
        btn_layout.addWidget(self.btn_edit_globals) # <--- ADDED HERE
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        # THE LAUNCHER CONTROLS
        launch_layout = QHBoxLayout()
        
        self.btn_launch = QPushButton("Launch Selected Project")
        self.btn_launch.setStyleSheet("""
            background-color: #2e885a; 
            color: white; 
            font-weight: bold; 
            height: 45px; 
            font-size: 14px;
        """)
        self.btn_launch.clicked.connect(self.launch)
        
        self.btn_exit = QPushButton("Exit App")
        self.btn_exit.setFixedWidth(100)
        self.btn_exit.setFixedHeight(45)
        self.btn_exit.clicked.connect(self.close)
        
        launch_layout.addWidget(self.btn_launch)
        launch_layout.addWidget(self.btn_exit)
        layout.addLayout(launch_layout)

        # Perform the initial load AFTER UI is wired up
        self._load_projects()

    def open_user_prefs(self):
        """Launches the dynamic editor for the user's local preferences."""
        dlg = DynamicKeyValueEditor(self.prefs_path, preset="user_prefs", parent=self)
        dlg.exec()

    def _load_projects(self):
        manager_dir = os.path.normpath(os.path.join(os.path.expanduser("~"), ".simplepipemanager"))
        self.ledger_path = os.path.join(manager_dir, "projectconfigroots.csv")
        os.makedirs(manager_dir, exist_ok=True)

        # --- SEED THE DEFAULT TEMPLATE ---
        default_template_dir = os.path.join(manager_dir, "_pipe_config_templates", "_pipe_config_default")
        if not os.path.exists(default_template_dir):
            print("Building Default Global Template...")
            temp_eng = ConfigEngine(default_template_dir, bootstrap=False)
            temp_eng.bootstrap_template(mode='create', use_pointers=False)

        # 2. Define the Paths
        old_ledger = os.path.normpath(os.path.join(os.path.expanduser("~"), ".pipeconfigroot.csv"))
        self.ledger_path = os.path.join(manager_dir, "projectconfigroots.csv")

        # 3. Surgical Migration Logic
        if os.path.exists(old_ledger) and not os.path.exists(self.ledger_path):
            try:
                import shutil
                shutil.move(old_ledger, self.ledger_path)
                print(f"Migrated project ledger to: {self.ledger_path}")
            except Exception as e:
                print(f"Migration failed: {e}")
        
        # Ensure Schema & Add LOCALPIPECONFIG if missing
        if not os.path.exists(self.ledger_path):
            pd.DataFrame(columns=["ProjectName", "ConfigPath", "STATUS", "LOCALPIPECONFIG"]).to_csv(self.ledger_path, index=False)
        
        self.df_projects = pd.read_csv(self.ledger_path, dtype=str).fillna("")
        
        # Retrofit legacy ledgers with the new column
        if "LOCALPIPECONFIG" not in self.df_projects.columns:
            self.df_projects["LOCALPIPECONFIG"] = "_pipe_config_default"
            self.df_projects.to_csv(self.ledger_path, index=False)
        
        if 'STATUS' not in self.df_projects.columns:
            self.df_projects['STATUS'] = 'Active'
            self.df_projects.to_csv(self.ledger_path, index=False)
        
        self.df_projects['ConfigPath'] = self.df_projects['ConfigPath'].apply(PathSwapper.translate)
        
        self.list_widget.clear()
        
        # Filtering logic
        show_retired = hasattr(self, 'check_show_retired') and self.check_show_retired.isChecked()
        
        # We store the 'true' dataframe index in the list item's data role to avoid 
        # index mismatching when filtered
        for idx, row in self.df_projects.iterrows():
            status = row.get('STATUS', 'Active')
            if not show_retired and status == 'Retired':
                continue
                
            display_text = f"{row['ProjectName']} ({row['ConfigPath']})"
            item = QListWidgetItem(display_text)
            item.setData(Qt.UserRole, idx) # Store real DF index
            
            if status == 'Retired':
                item.setForeground(QColor(120, 120, 120))
                item.setText(f"[RETIRED] {display_text}")
                
            self.list_widget.addItem(item)

    def open_global_settings(self):
        csv_path = os.path.join(self.global_engine.root, "Project_Settings.csv")
        dlg = DynamicKeyValueEditor(csv_path, engine=self.global_engine, preset="project_settings", parent=self)
        if dlg.exec():
            self.global_engine.__init__(self.global_engine.root)

    def open_global_templates(self):
        """Surgically edit the global naming templates CSV."""
        t_path = os.path.join(self.global_engine.root, "Naming_Templates.csv")
        
        # Safety check
        if not os.path.exists(t_path):
            os.makedirs(os.path.dirname(t_path), exist_ok=True)
            import pandas as pd
            df_blank = pd.DataFrame([["scan_name_template", "SHOTNAME_plate_vHEROPLATE"]], 
                                    columns=['Key', 'Value']).astype(str)
            df_blank.to_csv(t_path, index=False, encoding='cp1252')

        # Notice we can actually use the DynamicKeyValueEditor here too since it's Key/Value!
        # But to keep it identical to your snippet, here is the GenericCSVEditor:
        dlg = GenericCSVEditor(t_path, title="Global Naming Templates", parent=self, engine=self.global_engine)
        if dlg.exec():
            self.global_engine.__init__(self.global_engine.root)

    def open_global_shots_editor(self):
        """Surgical strike on the Global Shots Template."""
        # For globals, it's always just the template file sitting in the root!
        s_path = os.path.join(self.global_engine.root, "Shots_Template.csv")
        
        if not os.path.exists(s_path):
            from PySide6.QtWidgets import QMessageBox
            res = QMessageBox.question(self, "File Not Found", 
                                     f"The template does not exist at:\n{s_path}\n\nCreate a blank one?")
            if res == QMessageBox.Yes:
                os.makedirs(os.path.dirname(s_path), exist_ok=True)
                import pandas as pd
                cols = ['SEQUENCE', 'PROCESS', 'SHOTNAME', 'ALTSHOTNAME', 'FIRSTFRAME', 'LASTFRAME', 'HEROPLATE']
                pd.DataFrame(columns=cols).astype(str).to_csv(s_path, index=False, encoding='cp1252')
            else:
                return

        dlg = GenericCSVEditor(s_path, title="Global Shots Template", parent=self, engine=self.global_engine)
        
        is_dual = str(self.global_engine.settings.get('dual_name', 'False')).lower() == 'true'
        if not is_dual and "ALTSHOTNAME" in dlg.df.columns:
            alt_idx = dlg.df.columns.get_loc("ALTSHOTNAME")
            dlg.table.setColumnHidden(alt_idx, True)

        if dlg.exec():
            self.global_engine.__init__(self.global_engine.root)

    def open_global_pathsubs(self):
        pathsubs_path = os.path.join(self.global_engine.root, "Path_Subs.csv")
        dlg = GenericCSVEditor(pathsubs_path, title="Global Path Subs", parent=self, allow_add_column=True, engine=self.global_engine)
        if dlg.exec():
            self.global_engine.__init__(self.global_engine.root)

    def open_global_app_config(self, app_name):
        import sys
        plat = "win" if sys.platform == "win32" else "mac" if sys.platform == "darwin" else "linux"
        csv_path = os.path.join(self.global_engine.apps_dir, app_name, f"{plat}.csv")
        
        # Using the DynamicKeyValueEditor here so we get the '📁 Browse' button automatically via preset!
        dlg = DynamicKeyValueEditor(
            csv_path, 
            title=f"Global {app_name} Config", 
            parent=self, 
            engine=self.global_engine,
            preset="app_config"
        )
        if dlg.exec():
            self.global_engine.__init__(self.global_engine.root)

    def open_global_notes_config(self):
        """Surgically edit the Global Notes routing."""
        n_path = os.path.join(self.global_engine.root, "Notes_Config.csv")
        
        if not os.path.exists(n_path):
            from PySide6.QtWidgets import QMessageBox
            res = QMessageBox.question(self, "Config Missing", 
                "The Notes Config file is missing.\n\nRun a Global Sync to generate missing files?", 
                QMessageBox.Yes | QMessageBox.No)
            
            if res == QMessageBox.Yes:
                # Sync the global directory (use_pointers=False because it's the master!)
                self.global_engine.bootstrap_template(mode='sync', use_pointers=False)
                
            if not os.path.exists(n_path):
                return

        notes_dropdowns = {'Key_Type': ['main', 'root', 'tree', 'name']}
        dlg = GenericCSVEditor(n_path, title="Global Notes Config", parent=self, dropdown_cols=notes_dropdowns, engine=self.global_engine)
        
        if dlg.exec():
            self.global_engine.__init__(self.global_engine.root)

    def handle_lifecycle(self):
        """Tiered dialog for Retire vs Wipe Config vs Obliterate."""
        curr_item = self.list_widget.currentItem()
        if not curr_item: return
        
        df_idx = curr_item.data(Qt.UserRole)
        project_name = self.df_projects.at[df_idx, 'ProjectName']
        config_path = self.df_projects.at[df_idx, 'ConfigPath']
        status = self.df_projects.at[df_idx, 'STATUS']

        choice = QMessageBox(self)
        # --- THE FIX: Force the layout to follow our code order, not the OS ---
        choice.setOption(QMessageBox.DontUseNativeDialog, True) 
        
        choice.setWindowTitle("Project Lifecycle Management")
        choice.setText(f"Manage Project: {project_name}")
        
        # We add them in the EXACT order we want them to appear (Left to Right)
        label_toggle = "Revive (Active)" if status == "Retired" else "Retire (Hide)"
        btn_toggle = choice.addButton(label_toggle, QMessageBox.ActionRole)
        
        btn_erase_config = choice.addButton("Erase Config (Keep Data)", QMessageBox.ActionRole)
        
        btn_delete = choice.addButton("OBLITERATE EVERYTHING", QMessageBox.DestructiveRole)
        
        btn_cancel = choice.addButton("Cancel", QMessageBox.RejectRole)
        
        # Optional: Set the default focus so you don't accidentally obliterate on Enter
        choice.setDefaultButton(btn_cancel)

        choice.exec()

        if choice.clickedButton() == btn_toggle:
            new_status = "Active" if status == "Retired" else "Retired"
            self.df_projects.at[df_idx, 'STATUS'] = new_status
            self.df_projects.to_csv(self.ledger_path, index=False)
            self._load_projects()

        elif choice.clickedButton() == btn_erase_config:
            self.erase_project_config(df_idx, project_name, config_path)

        elif choice.clickedButton() == btn_delete:
            self.obliterate_project(df_idx, project_name, config_path)

    def erase_project_config(self, df_idx, name, path):
        """
        Surgically removes the Project Folder (parent of _pipe_config) 
        and the ledger entry, but leaves the 'data_root' data untouched.
        """
        # path = .../ProjectName/_pipe_config
        # project_folder = .../ProjectName/
        project_folder = os.path.dirname(path)

        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Question)
        msg.setWindowTitle("Erase Project Config & Folder")
        msg.setText(f"Resetting Project: {name}")
        msg.setInformativeText(
            f"This will DELETE the folder:\n{project_folder}\n\n"
            "This allows you to reuse the name 'test' immediately.\n"
            "IT WILL NOT TOUCH YOUR ACTUAL SEQUENCE DATA/ROOT."
        )
        
        btn_confirm = msg.addButton("Reset / Erase", QMessageBox.AcceptRole)
        msg.addButton("Cancel", QMessageBox.RejectRole)
        
        msg.exec()
        if msg.clickedButton() != btn_confirm:
            return

        try:
            import shutil
            # 1. KILL THE PROJECT FOLDER (The one containing _pipe_config)
            if os.path.exists(project_folder):
                shutil.rmtree(project_folder)
            
            # 2. REMOVE FROM LEDGER
            self.df_projects = self.df_projects.drop(df_idx)
            self.df_projects.to_csv(self.ledger_path, index=False)
            
            # 3. REFRESH UI
            self._load_projects()
            QMessageBox.information(self, "Success", f"Project '{name}' folder and config erased. You can now recreate it.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Reset failed:\n{e}")

    def obliterate_project(self, df_idx, name, path):
        """Persistent 'Are you sure?' dialog with Root Discovery and Parent Cleanup."""
        
        # path = .../ProjectName/_pipe_config
        # project_folder = .../ProjectName/
        project_folder = os.path.dirname(path)
        
        settings_path = os.path.join(path, "Project_Settings.csv")
        data_root = None
        
        if os.path.exists(settings_path):
            try:
                set_df = pd.read_csv(settings_path, dtype=str).fillna("")
                match = set_df[set_df['Key'] == 'data_root']
                if not match.empty:
                    data_root = PathSwapper.translate(match.iloc[0]['Value'])
            except Exception as e:
                print(f"Failed to peek at project root: {e}")

        # Target 1: The Data (The 10TB Render Root)
        target_to_delete = data_root if data_root else project_folder

        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Warning)
        msg.setWindowTitle("!!! PERMANENT DELETION !!!")
        msg.setText(f"CRITICAL: You are about to delete the entire project:\n\n{target_to_delete}")
        msg.setInformativeText("This will wipe all data defined in data_root. This is permanent.")
        
        btn_browse = msg.addButton("Open Folder to Verify", QMessageBox.ActionRole)
        btn_abort = msg.addButton("Abort / Cancel", QMessageBox.RejectRole)
        btn_confirm = msg.addButton("I am certain. Delete it.", QMessageBox.DestructiveRole)
        
        while True:
            msg.exec()
            clicked = msg.clickedButton()

            if clicked == btn_browse:
                try:
                    if sys.platform == "win32": os.startfile(target_to_delete)
                    else: subprocess.run(["open" if sys.platform == "darwin" else "xdg-open", target_to_delete])
                except: pass
                continue 

            if clicked == btn_confirm:
                final_check = QMessageBox.question(self, "FINAL CONFIRMATION", 
                    f"Are you 100% absolutely sure you want to wipe '{name}' and ALL its data?",
                    QMessageBox.Yes | QMessageBox.No)
                
                if final_check == QMessageBox.Yes:
                    try:
                        import shutil
                        # 1. KILL THE DATA (The Project Root / Server Data)
                        if os.path.exists(target_to_delete):
                            shutil.rmtree(target_to_delete)
                        
                        # 2. KILL THE LOCAL PROJECT FOLDER (The one containing _pipe_config)
                        # We do this even if target_to_delete was different, to clear the 'test' name.
                        if os.path.exists(project_folder):
                            shutil.rmtree(project_folder)
                        
                        # 3. REMOVE FROM LEDGER
                        self.df_projects = self.df_projects.drop(df_idx)
                        self.df_projects.to_csv(self.ledger_path, index=False)
                        self._load_projects()
                        
                        QMessageBox.information(self, "Success", "Project obliterated.")
                        break 
                    except Exception as e:
                        QMessageBox.critical(self, "Error", f"Obliteration failed:\n{e}")
                        break
            else:
                break

    def action_import_search(self):
        start_dir = QFileDialog.getExistingDirectory(self, "Select Root to Search for Configs", os.path.expanduser("~"))
        if not start_dir: return

        self.btn_import.setEnabled(False)
        self.btn_import.setText("Searching...")
        
        self.import_thread = ImportWorker(start_dir)
        self.import_thread.found.connect(self.process_imported_configs)
        self.import_thread.finished.connect(self.on_import_finished)
        self.import_thread.start()

    def process_imported_configs(self, config_list):
        """Merges new configs into the CSV, avoiding duplicates."""
        if not config_list:
            QMessageBox.information(self, "Import", "No '_pipe_config' folders found in that directory.")
            return

        # 1. Load current ledger
        df_ledger = pd.read_csv(self.ledger_path)
        
        # 2. Build new rows, ensuring we use absolute paths
        new_rows = []
        for name, path in config_list:
            abs_path = os.path.abspath(path)
            # Check if this path is already registered
            if abs_path not in df_ledger['ConfigPath'].values:
                new_rows.append({"ProjectName": name, "ConfigPath": abs_path})

        if new_rows:
            # 3. Concatenate and Save
            df_new = pd.DataFrame(new_rows)
            df_updated = pd.concat([df_ledger, df_new], ignore_index=True)
            df_updated.to_csv(self.ledger_path, index=False)
            
            # 4. Refresh the UI
            self._load_projects()
            QMessageBox.information(self, "Import Complete", f"Successfully registered {len(new_rows)} new projects.")
        else:
            QMessageBox.information(self, "Import", "All discovered projects are already registered.")

    def on_import_finished(self):
        self.btn_import.setEnabled(True)
        self.btn_import.setText("Import Projects...")

    def open_folder(self):
        idx = self.list_widget.currentRow()
        if idx < 0: return
        path = self.df_projects.iloc[idx]['ConfigPath']
        if sys.platform == "win32": os.startfile(path)
        else: subprocess.run(["open" if sys.platform == "darwin" else "xdg-open", path])

    def create_new(self):
        # 1. Fetch the preference
        current_default = os.path.expanduser("~") # Fallback
        try:
            df_p = pd.read_csv(self.prefs_path, dtype=str)
            current_default = df_p.loc[df_p['Key'] == 'user_projects_default_dir', 'Value'].values[0]
        except: pass

        # 2. Pass it into the dialog
        dlg = NewProjectDialog(self)
        dlg.edit_parent.setText(current_default)
        dlg.update_preview()
        
        if dlg.exec() == QDialog.Accepted:
            # ... keep your existing creation logic exactly as it is ...
            path = dlg.full_config_path
            name = dlg.project_name

            # --- KEEPING YOUR WORKING EXPLICIT 2-LINER ---
            engine = ConfigEngine(
                path, 
                bootstrap=False, 
                use_pointers=True, 
                template_name="_pipe_config_default"
            )
            engine.bootstrap_template(mode='create', use_pointers=True)

            # Register in the Home Ledger
            self.df_projects = pd.read_csv(self.ledger_path)
            
            new_row = {
                "ProjectName": name, 
                "ConfigPath": path, 
                "STATUS": "Active",
                "LOCALPIPECONFIG": "_pipe_config_default"
            }
            
            df = pd.DataFrame([new_row])
            df.to_csv(self.ledger_path, mode='a', header=False, index=False)
            
            self._load_projects()

    def action_update_config(self):
        idx = self.list_widget.currentRow()
        if idx < 0: return
        
        project_path = self.df_projects.iloc[idx]['ConfigPath']
        
        # We don't need to 'launch' the engine, just use it to sync
        # We create a dummy engine instance pointing to the path
        temp_engine = ConfigEngine(project_path, bootstrap=False) 
        
        # Run the sync microsurgery
        temp_engine.bootstrap_template(target_dir=project_path, mode='sync')
        
        QMessageBox.information(self, "Success", "Project config synchronized (Missing files added).")

    def launch(self):
        idx = self.list_widget.currentRow()
        if idx < 0: return
        
        project_name = self.df_projects.iloc[idx]['ProjectName']
        config_path = self.df_projects.iloc[idx]['ConfigPath']
        
        # --- SURGICAL FIX: Check if the window exists AND isn't wrapped in a dead C++ object ---
        if config_path in self.active_windows:
            win = self.active_windows[config_path]
            try:
                # This call will fail if the C++ object is deleted
                win.isVisible() 
                win.show()
                win.raise_()
                win.activateWindow()
                return
            except (RuntimeError, AttributeError):
                # The C++ object is dead; remove the ghost reference and proceed to launch fresh
                self.active_windows.pop(config_path, None)

        engine = ConfigEngine(config_path)
        PathSwapper.PATHSUBS = engine.pathsubs
        
        # m_path = str(engine.settings.get('catalog_csv', ''))
        s_path = str(engine.settings.get('shots_csv', ''))

        window = AssetManager(
            #master_path=m_path,
            shot_path=s_path,
            engine=engine,
            project_label=project_name
        )
        
        self.active_windows[config_path] = window
        window.setAttribute(Qt.WA_DeleteOnClose)
        
        # This cleanup signal is good, but sometimes the timing is off in PySide; 
        # the try/except above is the bulletproof backup.
        window.destroyed.connect(lambda c=config_path: self.active_windows.pop(c, None))
        
        window.show()

class NewProjectDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Create New Project Configuration")
        self.setMinimumWidth(550)
        layout = QVBoxLayout(self)

        # --- THE HARD-CODED NUDGE ---
        # 1. Define the "Sanctuary" root
        self.default_root = os.path.normpath(
            os.path.join(os.path.expanduser("~"), ".simplepipemanager", "projects")
        ).replace('\\', '/')
        
        # 2. Ensure it exists so the user doesn't have to manually create it
        os.makedirs(self.default_root, exist_ok=True)

        # Name Input
        layout.addWidget(QLabel("<b>Project Name:</b>"))
        self.edit_name = QLineEdit()
        self.edit_name.setPlaceholderText("e.g. Rage, Project_X")
        layout.addWidget(self.edit_name)

        # Parent Directory Input
        layout.addWidget(QLabel("<b>Parent Directory:</b> (Defaulting to the Studio 'Sanctuary')"))
        h_layout = QHBoxLayout()
        # Nudge the user by pre-filling the default_root
        self.edit_parent = QLineEdit(self.default_root)
        
        btn_browse = QPushButton("Browse...")
        btn_browse.setStyleSheet("padding: 4px 10px;")
        btn_browse.clicked.connect(self.browse_parent)
        
        h_layout.addWidget(self.edit_parent)
        h_layout.addWidget(btn_browse)
        layout.addLayout(h_layout)

        # Result Path Preview (Crucial for visual confirmation)
        self.label_preview = QLabel("The configuration will be built in:")
        self.label_preview.setStyleSheet("color: #666; font-size: 11px; margin-top: 10px;")
        layout.addWidget(self.label_preview)
        
        self.path_preview = QLabel("...")
        self.path_preview.setStyleSheet("color: #77aa77; font-family: monospace; font-size: 11px;")
        self.path_preview.setWordWrap(True)
        layout.addWidget(self.path_preview)

        # Wiring
        self.edit_name.textChanged.connect(self.update_preview)
        self.edit_parent.textChanged.connect(self.update_preview)

        # Divider
        line = QFrame(); line.setFrameShape(QFrame.HLine); line.setStyleSheet("color: #333;")
        layout.addWidget(line)

        # Buttons
        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self.validate_and_accept)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)
        
        self.update_preview()

    def browse_parent(self):
        # Open the browser at the current text (or default if empty)
        start_dir = self.edit_parent.text() if self.edit_parent.text() else self.default_root
        res = QFileDialog.getExistingDirectory(self, "Select Parent Directory", start_dir)
        if res: 
            self.edit_parent.setText(res.replace('\\', '/'))

    def update_preview(self):
        name = self.edit_name.text().strip()
        parent = self.edit_parent.text().strip()
        
        if not name:
            self.path_preview.setText("<i>Waiting for project name...</i>")
            return

        # Explicitly showing the folder structure we've standardized on
        full_path = os.path.join(parent, name, "_pipe_config").replace('\\', '/')
        self.path_preview.setText(full_path)

    def validate_and_accept(self):
        name = self.edit_name.text().strip()
        parent = self.edit_parent.text().strip()
        
        if not name or not parent:
            return

        target_dir = os.path.join(parent, name, "_pipe_config").replace('\\', '/')
        
        if os.path.exists(target_dir):
            from PySide6.QtWidgets import QMessageBox
            QMessageBox.critical(self, "Path Error", 
                f"A configuration already exists at:\n{target_dir}\nPlease choose a different name or location.")
            return

        self.full_config_path = target_dir
        self.project_name = name
        self.accept()

class CatalogProvider:
    def __init__(self, engine):
        self.engine = engine

    def get_raw_csv_df(self, active_roots=None):
        """Loads and appends catalogs based ONLY on the requested root IDs."""
        # 1. If the UI doesn't explicitly ask for anything, return empty!
        if not active_roots:
            return pd.DataFrame()

        combined_frames = []

        for catalog_key in active_roots:
            target_path = self.get_catalog_path(catalog_key)
            if os.path.exists(target_path):
                try:
                    df = pd.read_csv(target_path, encoding='cp1252', dtype=str).fillna("")
                    # 🌟 THE MAGIC TAG: We record where this data came from!
                    df['ROOT_ID'] = catalog_key 
                    combined_frames.append(df)
                except Exception as e:
                    print(f"CatalogProvider Error reading {catalog_key}: {e}")

        if combined_frames:
            # Append them all together into one giant DataFrame
            return pd.concat(combined_frames, ignore_index=True)
        
        return pd.DataFrame()
    
    def get_catalog_path(self, catalog_name):
        return os.path.normpath(os.path.join(self.engine.project_root, "catalogs", f"{catalog_name}.csv"))
    
class Scraper(QObject):
    progress = Signal(int)
    finished = Signal()

    def __init__(self, roots_list, output_dir, blacklist_str=""):
        super().__init__()
        # List of [name, path] tuples
        self.roots_list = roots_list 
        self.output_dir = PathSwapper.translate(output_dir)
        
        self.blacklist = [os.path.normpath(p.strip()) for p in blacklist_str.split(',') if p.strip()]
        self.ext_singles = {".nk", ".mov", ".mp4", ".ods", ".zip"}
        self.ext_sequences = {".exr", ".jpg", ".png", ".tiff", ".dpx"}

    def run(self):
        # 1. PRE-FLIGHT: Validate roots
        valid_roots = []
        total_dirs = 0
        
        for name, r_path in self.roots_list:
            # SURGICAL FIX: Do NOT translate the root path here. 
            # The root path in roots_list is already the local path for this OS.
            clean_root = os.path.normpath(r_path) 
            
            if os.path.exists(clean_root):
                valid_roots.append((name, clean_root))
                # Count directories for progress
                for _, dirs, _ in os.walk(clean_root):
                    total_dirs += len(dirs)
        
        if not valid_roots:
            print("Scraper: No valid roots found to scan.")
            self.finished.emit()
            return

        processed_dirs = 0
        clean_blacklist = [p.lower() for p in self.blacklist]

        # 2. THE MAIN LOOP
        for root_name, current_root_dir in valid_roots:
            potential_sequences = defaultdict(list)
            final_rows = []
            
            for root, dirs, files in os.walk(current_root_dir):
                # Blacklist pruning
                for d in list(dirs):
                    if d.lower() in clean_blacklist:
                        dirs.remove(d)
                
                processed_dirs += 1
                if total_dirs > 0:
                    self.progress.emit(int((processed_dirs / total_dirs) * 100))
                
                local_p = self.get_local_path(root, current_root_dir)
                
                for f in files:
                    ext = os.path.splitext(f)[1].lower()
                    full_path = os.path.join(root, f)
                    
                    if ext in self.ext_singles:
                        try:
                            stat = os.stat(full_path)
                            final_rows.append(self.format_row(root, local_p, f, ext, "-", "-", stat.st_ctime, stat.st_mtime))
                        except: continue
                    elif ext in self.ext_sequences:
                        match = re.search(r'^(.*?)([._])(\d+)(\.[^.]+)$', f)
                        if match:
                            prefix, separator, frame, extension = match.groups()
                            potential_sequences[(root, local_p, prefix, separator, extension)].append({
                                "frame": int(frame), "pad": len(frame), "path": full_path, "original_name": f
                            })
                        else:
                            stat = os.stat(full_path)
                            final_rows.append(self.format_row(root, local_p, f, ext, "-", "-", stat.st_ctime, stat.st_mtime))

            # --- PROCESS SEQUENCES ---
            for (folder, local_folder, prefix, separator, ext), items in potential_sequences.items():
                if len(items) > 1:
                    items.sort(key=lambda x: x["frame"])
                    all_stats = [os.stat(i["path"]) for i in items]
                    formatted_name = f"{prefix}{separator}%0{items[0]['pad']}d{ext}"
                    final_rows.append(self.format_row(folder, local_folder, formatted_name, ext, min(i["frame"] for i in items), max(i["frame"] for i in items), 
                                                    min(s.st_ctime for s in all_stats), max(s.st_mtime for s in all_stats)))
                else:
                    item = items[0]
                    stat = os.stat(item["path"])
                    final_rows.append(self.format_row(folder, local_folder, item["original_name"], ext, "-", "-", stat.st_ctime, stat.st_mtime))

            # --- 3. WRITE THE ISOLATED CSV ---
            output_csv = os.path.join(self.output_dir, f"{root_name}.csv")
            headers = ["ABSPATH", "LOCALPATH", "FILENAME", "FILETYPE", "FIRST", "LAST", "CREATION", "MODDATE"]
            
            with open(output_csv, "w", newline="", encoding='cp1252') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(final_rows)
                
        self.finished.emit()

    def get_local_path(self, full_root, base_dir):
        """Surgical removal of current base_dir from the ABSPATH."""
        rel = os.path.relpath(full_root, base_dir)
        return "" if rel == "." else rel

    def format_row(self, root, local_root, name, ext, first, last, ctime, mtime):
        return {
            "ABSPATH": root, 
            "LOCALPATH": local_root,
            "FILENAME": name, 
            "FILETYPE": ext.lstrip('.'),
            "FIRST": first, "LAST": last,
            "CREATION": datetime.fromtimestamp(ctime).strftime("%Y-%m-%d %H:%M:%S"),
            "MODDATE": datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
        }

class UpdateScrapeDialog(QDialog):
    def __init__(self, parent=None, engine=None, target_roots=None): 
        super().__init__(parent)
        self.engine = engine
        self.setWindowTitle("Update Data")
        self.setMinimumWidth(450)
        layout = QVBoxLayout(self)

        # 1. PARSE THE DATA ROOTS (OR USE TARGETS)
        self.roots_list = []
        
        if target_roots is not None:
            # If the Dock passed us specific roots, use them directly
            self.roots_list = target_roots
        else:
            # Otherwise, fall back to parsing the engine's raw string
            raw_dr = str(self.engine.settings.get('data_root_raw', ''))
            try:
                import json
                parsed_dr = json.loads(raw_dr)
                if isinstance(parsed_dr, list):
                    for item in parsed_dr:
                        if isinstance(item, list) and len(item) >= 2:
                            self.roots_list.append((str(item[0]), str(item[1])))
            except:
                # Legacy flat string fallback
                flat_root = str(self.engine.settings.get('data_root', ''))
                if flat_root:
                    self.roots_list.append(("default", flat_root))

        # 2. RESOLVE THE CATALOG DIRECTORY
        base_output_dir = self.engine.project_root
        self.output_dir = os.path.join(base_output_dir, "catalogs")

        # 3. BUILD THE UI PREVIEW
        layout.addWidget(QLabel("<b>Ready to update data from the following roots:</b>"))
        
        roots_text = ""
        for name, path in self.roots_list:
            roots_text += f"• <b>{name}</b>: {path}<br>"
        if not roots_text:
            roots_text = "<i>No data roots configured.</i>"
            
        lbl_roots = QLabel(roots_text)
        lbl_roots.setStyleSheet("padding-left: 10px; color: #aaa;")
        layout.addWidget(lbl_roots)
        
        layout.addWidget(QLabel(f"<br><b>To Catalog Directory:</b><br>{self.output_dir}<br>"))

        self.pbar = QProgressBar()
        self.pbar.setVisible(False)
        layout.addWidget(self.pbar)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.button(QDialogButtonBox.Ok).setText("Proceed")
        
        if not self.roots_list:
            self.buttons.button(QDialogButtonBox.Ok).setEnabled(False)
            
        self.buttons.accepted.connect(self.start_scrape)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)

    def start_scrape(self):
        # --- NEW: PRE-FLIGHT ACCESSIBILITY CHECK ---
        missing_roots = []
        for name, r_path in self.roots_list:
            print("R_PATH: ", r_path)
            if not os.path.exists(r_path):
                missing_roots.append(f"{name}: {r_path}")

        if missing_roots:
            from PySide6.QtWidgets import QMessageBox
            msg = "The following Data Roots are currently inaccessible:\n\n"
            msg += "\n".join(missing_roots)
            msg += "\n\nDo you want to skip these and scrape only reachable locations?"
            
            res = QMessageBox.warning(self, "Inaccessible Roots", msg, 
                                    QMessageBox.Yes | QMessageBox.No)
            if res == QMessageBox.No:
                return # Abort the whole thing

        # Proceed with valid roots only
        self.pbar.setVisible(True)
        self.buttons.setEnabled(False)
        
        blacklist_val = str(self.engine.settings.get('scrape_blacklist', ''))
        
        # Ensure we point to project_root/catalogs
        catalog_dir = os.path.join(self.engine.project_root, "catalogs")
        os.makedirs(catalog_dir, exist_ok=True)

        self.worker_thread = QThread()
        self.worker = Scraper(self.roots_list, catalog_dir, blacklist_str=blacklist_val)
        
        self.worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.worker.run)
        self.worker.progress.connect(self.pbar.setValue)
        self.worker.finished.connect(self.on_finished)
        self.worker_thread.start()

    def on_finished(self):
        self.worker_thread.quit()
        self.worker_thread.wait()
        self.accept()

    def closeEvent(self, event):
        # Fix the crash here by using the renamed variable
        if hasattr(self, 'worker_thread') and self.worker_thread.isRunning():
            from PySide6.QtWidgets import QMessageBox
            QMessageBox.warning(self, "Scrape in Progress", "Please wait for the scrape to finish.")
            event.ignore() 
        else:
            event.accept()

class DataSourcesDock(QDockWidget):
    request_reload = Signal(list) 

    def __init__(self, main_window, engine):
        super().__init__("Data Management", main_window)
        self.main_window = main_window
        self.engine = engine
        self.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)

        self.session_roots = [] 
        self.root_checkboxes = {} 

        self.setup_ui()
        
        # 1. Clean up any ghosts from previous crashes FIRST
        self.cleanup_orphaned_catalogs()
        
        # 2. Then build the UI from the permanent roots
        self.sync_from_engine()

    def setup_ui(self):
        self.container = QWidget()

        self.container.setMinimumWidth(250)

        self.layout = QVBoxLayout(self.container)

        # 1. HEADER
        lbl_head = QLabel("<b>ACTIVE DATA SOURCES</b>")
        lbl_head.setAlignment(Qt.AlignCenter)
        self.layout.addWidget(lbl_head)

        # --- SURGICAL INJECTION: BATCH SCRAPE BUTTON ---
        self.btn_scrape_selected = QPushButton("⚡ Scrape Selected")
        self.btn_scrape_selected.setStyleSheet("background-color: #b07030; color: white; font-weight: bold; padding: 5px;")
        self.btn_scrape_selected.setToolTip("Scrape all checked Data Roots above")
        self.btn_scrape_selected.clicked.connect(self.on_scrape_selected)
        self.layout.addWidget(self.btn_scrape_selected)
        # -----------------------------------------------

        # 2. SCROLLABLE LIST
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll_widget = QWidget()
        self.scroll_layout = QVBoxLayout(self.scroll_widget)
        self.scroll_layout.setAlignment(Qt.AlignTop)
        self.scroll.setWidget(self.scroll_widget)
        self.layout.addWidget(self.scroll)

        # 3. TEMP ACTIONS
        self.btn_add_temp = QPushButton("+ Add Temporary Root")
        self.btn_add_temp.clicked.connect(self.on_add_temp)
        self.layout.addWidget(self.btn_add_temp)

        # 4. DIVIDER
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setFrameShadow(QFrame.Sunken)
        self.layout.addWidget(line)

        # 5. MAIN COMMANDS
        self.btn_reload = QPushButton("🔄 UPDATE VIEW")
        self.btn_reload.setStyleSheet("background-color: #2563eb; color: white; font-weight: bold; padding: 8px;")
        self.btn_reload.clicked.connect(self.on_reload_clicked)
        self.layout.addWidget(self.btn_reload)

        self.btn_manage = QPushButton("⚙️ Manage Saved Roots...")
        self.btn_manage.clicked.connect(self.on_manage_roots)
        self.layout.addWidget(self.btn_manage)

        self.setWidget(self.container)

    def cleanup_orphaned_catalogs(self):
        """Sweeps the catalogs directory on startup to kill ghosts from crashed sessions."""
        catalog_dir = os.path.join(self.engine.project_root, "catalogs")
        if not os.path.exists(catalog_dir):
            return

        # 1. Parse the permanent names directly from the raw string
        raw_dr = str(self.engine.settings.get('data_root_raw', ''))
        permanent_names = []
        try:
            import json
            parsed = json.loads(raw_dr)
            if isinstance(parsed, list):
                # Grab just the names (item[0]) from the valid tuples
                permanent_names = [str(item[0]) for item in parsed if isinstance(item, list) and len(item) > 0]
        except:
            # Legacy fallback
            if str(self.engine.settings.get('data_root', '')):
                permanent_names = ["default"]

        # 2. Sweep the folder
        for filename in os.listdir(catalog_dir):
            if filename.endswith(".csv"):
                catalog_name = filename[:-4] # Strip .csv
                
                if catalog_name not in permanent_names:
                    target_file = os.path.join(catalog_dir, filename)
                    try:
                        os.remove(target_file)
                        print(f"Startup Cleanup: Vaporized orphaned catalog '{filename}'")
                    except Exception as e:
                        print(f"Startup Cleanup Failed on '{filename}': {e}")
                        
    def get_temp_root_names(self):
        """Returns a list of root IDs that were flagged as temporary this session."""
        return [name for name, path, is_temp in self.session_roots if is_temp]
    
    def on_scrape_selected(self):
        """Gathers all checked roots and sends them to the Scraper in one batch."""
        active_ids = self.get_active_root_ids()
        
        if not active_ids:
            self.main_window.statusBar().showMessage("No Data Roots checked to scrape.", 3000)
            return

        # Build the exact [(name, path)] structure the dialog expects
        target_batch = []
        for name, path, is_temp in self.session_roots:
            if name in active_ids:
                target_batch.append((name, path))

        # Launch the dialog with the batch!
        # Adjust import if needed based on your file structure:
        # from table_view_simple_projects import UpdateScrapeDialog
        
        dlg = UpdateScrapeDialog(parent=self.main_window, engine=self.engine, target_roots=target_batch)
        if dlg.exec():
            self.main_window.statusBar().showMessage(f"Successfully scraped {len(target_batch)} Data Roots.", 4000)
            
            # Optional: You can auto-trigger a view update here if you want the UI to instantly 
            # show the newly scraped files, or just let the user click "UPDATE VIEW" manually.
            # self.on_reload_clicked()

    def sync_from_engine(self):
        """Initial parse: Turns engine's raw string into our session list."""
        self.session_roots = []
        raw_dr = str(self.engine.settings.get('data_root_raw', ''))
        
        try:
            import json
            parsed = json.loads(raw_dr)
            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, list) and len(item) >= 2:
                        # (ID, Path, Is_Temp)
                        self.session_roots.append([str(item[0]), str(item[1]), False])
        except:
            # Fallback for legacy
            flat = str(self.engine.settings.get('data_root', ''))
            if flat: self.session_roots.append(["default", flat, False])

        self.refresh_ui_list()

    def refresh_ui_list(self):
        """Clears and rebuilds the checkbox rows based on session_roots."""
        
        # 1. REMEMBER WHAT WAS CHECKED BEFORE DESTROYING
        if not self.root_checkboxes:
            # First launch: default to just the first root
            active_ids = [self.session_roots[0][0]] if self.session_roots else []
        else:
            # Subsequent refreshes: remember user's choices
            active_ids = self.get_active_root_ids()

        # 2. SAFELY AND INSTANTLY CLEAR THE UI
        for i in reversed(range(self.scroll_layout.count())):
            item = self.scroll_layout.takeAt(i)
            widget = item.widget()
            if widget:
                widget.hide()         # INSTANTLY removes it from the screen!
                widget.deleteLater()  # Safely queues memory cleanup
                
        self.root_checkboxes.clear()

        # 3. REBUILD THE ROWS
        for i, (name, path, is_temp) in enumerate(self.session_roots):
            row = QWidget()
            row_lay = QHBoxLayout(row)
            row_lay.setContentsMargins(2, 2, 2, 2)

            chk = QCheckBox(name)
            
            # Restore previous state!
            chk.setChecked(name in active_ids) 
            chk.setToolTip(f"Path: {path}")
            
            if is_temp:
                chk.setStyleSheet("color: #fbbf24; font-style: italic;") 

            self.root_checkboxes[name] = chk

            btn_scrape = QPushButton("Scrape")
            btn_scrape.setFixedWidth(50)
            btn_scrape.clicked.connect(lambda checked=False, n=name, p=path: self.on_scrape_request(n, p))

            row_lay.addWidget(chk)
            row_lay.addStretch()
            row_lay.addWidget(btn_scrape)
            self.scroll_layout.addWidget(row)

    def get_active_root_ids(self):
        """Returns list of names currently checked."""
        return [name for name, chk in self.root_checkboxes.items() if chk.isChecked()]

    def on_reload_clicked(self):
        """Emits the list of IDs the Main Window should pass to the Provider."""
        active_ids = self.get_active_root_ids()
        self.request_reload.emit(active_ids)

    def on_scrape_request(self, name, path):
        # Package for your existing UpdateScrapeDialog
        target = [(name, path)]
        from table_view_simple_projects import UpdateScrapeDialog # Adjust import as needed
        dlg = UpdateScrapeDialog(parent=self.main_window, engine=self.engine, target_roots=target)
        dlg.exec()

    def on_add_temp(self):
        """Adds a temporary, session-only root and prompts for an initial scrape."""
        dlg = AddTempRootDialog(self)
        if dlg.exec():
            new_name, new_path = dlg.get_data()
            
            # Prevent duplicate names in the current session
            existing_names = [r[0] for r in self.session_roots]
            if new_name in existing_names:
                from PySide6.QtWidgets import QMessageBox
                QMessageBox.warning(self, "Duplicate Name", f"A root named '{new_name}' already exists in this session.")
                return

            # Add to our session list with the is_temp flag set to True
            self.session_roots.append([new_name, new_path, True])
            
            # Rebuild the Dock UI to show the new orange row
            self.refresh_ui_list()
            
            # Automatically check the new box
            if new_name in self.root_checkboxes:
                self.root_checkboxes[new_name].setChecked(True)
            
            # Offer to scrape it immediately (since a new root has no catalog yet)
            from PySide6.QtWidgets import QMessageBox
            res = QMessageBox.question(self, "Scrape Required", 
                                       f"Temporary root '{new_name}' added!\n\nWould you like to scrape this directory now to build its catalog?",
                                       QMessageBox.Yes | QMessageBox.No)
            
            if res == QMessageBox.Yes:
                self.on_scrape_request(new_name, new_path)

    def on_manage_roots(self):
        """Opens the editor, detects renames, syncs physical catalogs, and saves."""
        raw_dr = self.engine.settings.get('data_root_raw', '')
        
        # Capture the "Before" state (Ignore temp roots since they aren't in the JSON)
        old_roots = [(name, path) for name, path, is_temp in self.session_roots if not is_temp]
        
        # Adjust import based on your setup
        # from table_view_simple_projects import DataRootsEditorDialog
        dlg = DataRootsEditorDialog(current_data_string=raw_dr, parent=self)
        
        if dlg.exec():
            # --- START THE "SAVING" UI SPINNER ---
            from PySide6.QtWidgets import QApplication
            from PySide6.QtCore import Qt
            QApplication.setOverrideCursor(Qt.WaitCursor)
            
            try:
                new_json_string = dlg.get_serialized_data()
                
                # 1. Parse the "After" state
                import json, os
                new_roots = []
                try:
                    parsed = json.loads(new_json_string)
                    if isinstance(parsed, list):
                        new_roots = [(str(item[0]), str(item[1])) for item in parsed if len(item) >= 2]
                except Exception as e:
                    print(f"Parse error during rename check: {e}")

                # 2. THE RENAME DETECTIVE
                # If the path is identical but the name changed, rename the physical CSV
                catalog_dir = os.path.join(self.engine.project_root, "catalogs")
                for old_name, old_path in old_roots:
                    for new_name, new_path in new_roots:
                        if old_path == new_path and old_name != new_name:
                            old_csv = os.path.join(catalog_dir, f"{old_name}.csv")
                            new_csv = os.path.join(catalog_dir, f"{new_name}.csv")
                            
                            if os.path.exists(old_csv):
                                try:
                                    os.rename(old_csv, new_csv)
                                    print(f"Auto-renamed catalog: {old_name}.csv -> {new_name}.csv")
                                except Exception as e:
                                    print(f"Failed to rename catalog {old_name}: {e}")

                # 3. Standard Save & Refresh Logic
                self.engine.settings['data_root'] = new_json_string
                self.engine.settings['data_root_raw'] = new_json_string
                
                self.main_window.save_engine_settings()
                self.main_window.live_refresh_config()
                self.sync_from_engine()
                
                self.main_window.statusBar().showMessage("Data Roots updated and catalogs synced.", 4000)
                
            finally:
                # --- ALWAYS RESTORE THE CURSOR, even if it crashes ---
                QApplication.restoreOverrideCursor()

class RcloneCopyTask(QProcess):
    # Signals: (current_file_index, speed, current_filename)
    progress_update = Signal(int, str, str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setProcessChannelMode(QProcess.MergedChannels)
        self.files_finished_in_this_run = 0
        self.readyRead.connect(self.parse_output)

    def parse_output(self):
        raw = self.readAllStandardOutput().data().decode()
        
        # Look for rclone's "Transferred: 12 / 100" line
        count_match = re.search(r'Transferred:\s+(\d+)\s+/\s+(\d+)', raw)
        speed_match = re.search(r'([\d\.]+\s[KMGT]B/s)', raw)
        file_match = re.search(r'\*\s+(.*?):', raw)

        if count_match:
            self.files_finished_in_this_run = int(count_match.group(1))
            
        speed = speed_match.group(1) if speed_match else "Checking..."
        filename = file_match.group(1) if file_match else "Syncing..."

        # Emit the count so the AssetManager can add it to the Global Total
        self.progress_update.emit(self.files_finished_in_this_run, speed, filename)

    def run_copy(self, source_list, destination, source_base_root, force_overwrite=False):
        import tempfile, os

        # 1. THE CRLF FIX: newline='\n' stops Windows from corrupting the rclone path list
        self.list_file = tempfile.NamedTemporaryFile(
            delete=False, mode='w', suffix='.txt', encoding='utf-8', newline='\n'
        )
        
        for p in source_list:
            relative_p = os.path.relpath(p, source_base_root)
            # Force forward slashes for the internal paths
            relative_p = relative_p.replace('\\', '/') 
            self.list_file.write(relative_p + "\n")
        self.list_file.close()

        # 2. PATH SANITIZATION
        # Ensure rclone's command line parser doesn't choke on Windows backslashes
        safe_list = self.list_file.name.replace('\\', '/')
        safe_source = source_base_root.replace('\\', '/')
        safe_dest = destination.replace('\\', '/')

        # 3. CORE ARGUMENTS
        args = [
            "copy", safe_source, safe_dest, 
            "--files-from", safe_list,
            "--transfers", "16", 
            "--checkers", "16", 
            "--progress", 
            "--stats", "1s"
        ]

        # 4. SMART LOGIC
        if not force_overwrite:
            # We rely purely on size-only to beat the SMB timestamp drift. 
            # We removed --ignore-existing and --exclude to prevent rclone filter clashes.
            args.append("--size-only")
        else:
            args.append("--ignore-times")

        self.start("rclone", args)

class WarpHubDialog(QDialog):
    def __init__(self, selected_items, parent=None):
        super().__init__(parent)
        self.setWindowTitle("🚀 GDrive Warp Copy Hub")
        self.resize(900, 700)
        
        layout = QVBoxLayout(self)
        
        # --- GLOBAL ACTIONS (Top Bar) ---
        global_lay = QHBoxLayout()
        
        # 1. The Solo View Toggles
        global_lay.addWidget(QLabel("👁 <b>Solo View:</b>"))
        
        self.view_grp = QButtonGroup(self)
        self.view_grp.setExclusive(True) # Ensures only one can be clicked at a time
        
        btn_all = QPushButton("Show All"); btn_all.setCheckable(True); btn_all.setChecked(True)
        btn_anchors = QPushButton("Anchors"); btn_anchors.setCheckable(True)
        btn_dest = QPushButton("Destinations"); btn_dest.setCheckable(True)
        btn_opts = QPushButton("Options"); btn_opts.setCheckable(True)

        # Add them to the logic group
        self.view_grp.addButton(btn_all); self.view_grp.addButton(btn_anchors)
        self.view_grp.addButton(btn_dest); self.view_grp.addButton(btn_opts)

        # Style them like a sleek segmented control
        toggle_style = """
            QPushButton { background-color: #222; color: #888; border: 1px solid #444; padding: 5px 15px; border-radius: 4px; }
            QPushButton:checked { background-color: #58cc71; color: black; font-weight: bold; border: 1px solid #58cc71; }
            QPushButton:hover:!checked { background-color: #333; color: white; }
        """
        for btn in self.view_grp.buttons():
            btn.setStyleSheet(toggle_style)
            global_lay.addWidget(btn)

        # Wire up the logic
        btn_all.clicked.connect(lambda: self.apply_view_mode("all"))
        btn_anchors.clicked.connect(lambda: self.apply_view_mode("anchor"))
        btn_dest.clicked.connect(lambda: self.apply_view_mode("dest"))
        btn_opts.clicked.connect(lambda: self.apply_view_mode("opts"))

        global_lay.addStretch()

        # 2. Match Destinations Button (Pushed to the far right)
        btn_sync_dest = QPushButton("🎯 Match All Destinations to Top")
        btn_sync_dest.setStyleSheet("background-color: #444; color: white; font-weight: bold; padding: 5px 15px; border-radius: 4px;")
        btn_sync_dest.clicked.connect(self.sync_dests)
        global_lay.addWidget(btn_sync_dest)
        
        layout.addLayout(global_lay)

        # --- SCROLLABLE LIST ---
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.container = QWidget()
        self.cards_layout = QVBoxLayout(self.container)
        self.cards_layout.setAlignment(Qt.AlignTop)
        
        self.cards = []
        for i, path in enumerate(selected_items):
            card = WarpCard(path)
            self.cards.append(card)
            self.cards_layout.addWidget(card)
            
            # --- THE HUB MAGIC: Link only the first card ---
            if i == 0:
                card.enable_master_mode()
                card.shift_requested.connect(self.sync_master_shift)
                card.sync_force_requested.connect(self.sync_force_all)
                # --- NEW: Wire up the toggle ---
                card.sync_dir_toggled.connect(self.sync_master_realign)
            
        self.scroll.setWidget(self.container)
        layout.addWidget(self.scroll)

        # --- FOOTER ---
        btns = QHBoxLayout()
        self.btn_run = QPushButton("🚀 START BATCH WARP")
        self.btn_run.setStyleSheet("background-color: #2e885a; color: white; font-weight: bold; padding: 12px;")
        self.btn_run.clicked.connect(self.accept)
        
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(self.reject)
        
        btns.addStretch()
        btns.addWidget(btn_cancel)
        btns.addWidget(self.btn_run)
        layout.addLayout(btns)

    def apply_view_mode(self, mode):
        """Broadcasts the solo mode down to every card."""
        for card in self.cards:
            card.set_view_mode(mode)

    def sync_force_all(self, state):
        """Forces all below cards to match the master card's Force setting."""
        for card in self.cards[1:]:
            card.chk_force.setChecked(state)

    def sync_master_realign(self):
        """Fired when the user clicks the 'Align' toggle. Forces all cards to snap to the new logic."""
        master = self.cards[0]
        if master.is_decoupled(): return
        
        if getattr(master, 'sync_direction', 'right') == 'left':
            # Snap everyone to the exact same directory depth from the root
            for c in self.cards[1:]:
                c.apply_absolute_split(master.split_idx)
        else:
            # Snapping back to Right. Calculate how many steps from the end the master is, 
            # and apply that same right-offset to all the below cards.
            master_right_steps = len(master.path_parts) - master.split_idx
            for c in self.cards[1:]:
                c.apply_absolute_split(len(c.path_parts) - master_right_steps)

    def sync_master_shift(self, delta):
        """Receives a slider click from the Master card."""
        master = self.cards[0]
        if master.is_decoupled(): return
        
        if getattr(master, 'sync_direction', 'right') == 'left':
            # LEFT MODE: All cards perfectly mirror the master's exact root depth
            for card in self.cards[1:]:
                card.apply_absolute_split(master.split_idx)
        else:
            # RIGHT MODE (Default): Standard relative shifting
            for card in self.cards[1:]:
                card.apply_relative_shift(delta)

    def sync_dests(self):
        if not self.cards: return
        master_val = self.cards[0].edit_dest.text()
        for card in self.cards:
            card.edit_dest.setText(master_val)

    def get_mappings(self):
        return [
            {
                'path': c.full_path, 
                'anchor': c.edit_anchor.text(), 
                'dest': c.edit_dest.text(),
                'force': c.is_force_enabled() # <--- NEW FLAG
            } for c in self.cards
        ]
    
class WarpCard(QFrame):
    shift_requested = Signal(int)
    sync_force_requested = Signal(bool)
    sync_dir_toggled = Signal()

    def __init__(self, full_path, parent=None):
        super().__init__(parent)
        self.full_path = os.path.normpath(full_path)
        self.setFrameShape(QFrame.StyledPanel)
        self.setStyleSheet("WarpCard { background-color: #333; border-radius: 5px; margin: 2px; }")

        self.path_parts = list(Path(self.full_path).parts)
        self.split_idx = len(self.path_parts) - 1 

        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(0) # Tighten up for collapsing
        
        # 0. TITLE ROW (Always visible so they know what file it is)
        self.header_lay = QHBoxLayout()
        self.header_lay.setContentsMargins(0, 0, 0, 5)
        self.lbl_name = QLabel(f"📦 <b>{os.path.basename(full_path)}</b>")
        self.lbl_name.setStyleSheet("color: #61afef; font-size: 13px;") 
        self.header_lay.addWidget(self.lbl_name)
        self.header_lay.addStretch()
        main_layout.addLayout(self.header_lay)

        # --- 1. ANCHOR WRAPPER ---
        self.w_anchor = QWidget()
        anchor_v_lay = QVBoxLayout(self.w_anchor)
        anchor_v_lay.setContentsMargins(0, 0, 0, 5)
        
        self.anchor_ctrl_lay = QHBoxLayout()
        self.anchor_ctrl_lay.addStretch()
        self.anchor_ctrl_lay.addWidget(QLabel("Move Split:"))
        
        self.btn_left = QPushButton("◀")
        self.btn_left.setFixedWidth(28)
        self.btn_left.setStyleSheet("background-color: #444; color: white; border-radius: 3px;")
        self.btn_left.clicked.connect(self.shift_left)
        
        self.btn_right = QPushButton("▶")
        self.btn_right.setFixedWidth(28)
        self.btn_right.setStyleSheet("background-color: #444; color: white; border-radius: 3px;")
        self.btn_right.clicked.connect(self.shift_right)
        
        self.anchor_ctrl_lay.addWidget(self.btn_left)
        self.anchor_ctrl_lay.addWidget(self.btn_right)
        anchor_v_lay.addLayout(self.anchor_ctrl_lay)

        self.split_lay = QHBoxLayout()
        self.split_lay.setSpacing(2)
        
        self.edit_anchor = QLineEdit()
        self.edit_anchor.setReadOnly(True)
        self.edit_anchor.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.edit_anchor.setStyleSheet("QLineEdit { color: #61afef; background-color: #1e2a35; border: 1px dashed #3b749e; border-radius: 3px; padding: 2px; }")
        
        self.edit_relative = QLineEdit()
        self.edit_relative.setReadOnly(True)
        self.edit_relative.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.edit_relative.setStyleSheet("QLineEdit { color: #61afef; background-color: #1e2a35; border: 2px solid #3b949e; border-radius: 3px; padding: 2px; }")

        self.split_lay.addWidget(self.edit_anchor)
        self.split_lay.addWidget(self.edit_relative)
        anchor_v_lay.addLayout(self.split_lay)
        main_layout.addWidget(self.w_anchor)

        # --- 2. DESTINATION WRAPPER ---
        self.w_dest = QWidget()
        dest_lay = QHBoxLayout(self.w_dest)
        dest_lay.setContentsMargins(0, 0, 0, 5)
        dest_lay.addWidget(QLabel("Destination: "))
        self.edit_dest = QLineEdit()
        btn_browse_dest = QPushButton("Browse...")
        btn_browse_dest.clicked.connect(self.browse_dest)
        dest_lay.addWidget(self.edit_dest)
        dest_lay.addWidget(btn_browse_dest)
        main_layout.addWidget(self.w_dest)

        # --- 3. OPTIONS WRAPPER ---
        self.w_opts = QWidget()
        self.opts_lay = QHBoxLayout(self.w_opts)
        self.opts_lay.setContentsMargins(0, 0, 0, 0)
        self.chk_force = QCheckBox("Force Overwrite (Ignore Size-Match)")
        self.chk_force.setStyleSheet("color: #aaa; font-size: 11px;")
        self.opts_lay.addWidget(self.chk_force)
        self.opts_lay.addStretch()
        main_layout.addWidget(self.w_opts)

        self.update_split_display()

    def set_view_mode(self, mode):
        """Hides/Shows sections of the card based on the active Solo filter."""
        self.w_anchor.setVisible(mode in ("all", "anchor"))
        self.w_dest.setVisible(mode in ("all", "dest"))
        self.w_opts.setVisible(mode in ("all", "opts"))

    def enable_master_mode(self):
        """Called by the Hub only on the very first card."""
        self.chk_decouple = QCheckBox("Decouple Anchors")
        self.chk_decouple.setToolTip("Don't sync slider movements to below cards")
        self.chk_decouple.setStyleSheet("color: #e5c07b; font-weight: bold;")
        self.anchor_ctrl_lay.insertWidget(0, self.chk_decouple)
        
        # --- NEW: The Alignment Toggle ---
        self.sync_direction = "right"
        self.btn_sync_dir = QPushButton("⮂ Align: Right")
        self.btn_sync_dir.setToolTip("Toggle how below cards match (Relative from file vs Absolute from root)")
        self.btn_sync_dir.setStyleSheet("QPushButton { background-color: #444; color: #aaa; border-radius: 3px; padding: 2px 10px; }")
        self.btn_sync_dir.clicked.connect(self.toggle_sync_dir)
        self.anchor_ctrl_lay.insertWidget(1, self.btn_sync_dir)
        
        self.anchor_ctrl_lay.insertStretch(2) # Keeps buttons formatted nicely

        self.btn_apply_force = QPushButton("⮟ Apply to All")
        self.btn_apply_force.setStyleSheet("QPushButton { background-color: #2e5a88; color: white; border-radius: 3px; padding: 2px 10px; font-weight: bold; } QPushButton:hover { background-color: #3b76b3; }")
        self.btn_apply_force.clicked.connect(lambda: self.sync_force_requested.emit(self.chk_force.isChecked()))
        self.opts_lay.insertWidget(1, self.btn_apply_force)

    def toggle_sync_dir(self):
        """Swaps the index from end-based to root-based and updates the UI."""
        if self.sync_direction == "right":
            self.sync_direction = "left"
            self.btn_sync_dir.setText("⮂ Align: Left")
            self.btn_sync_dir.setStyleSheet("QPushButton { background-color: #2e885a; color: white; border-radius: 3px; padding: 2px 10px; font-weight: bold; }")
        else:
            self.sync_direction = "right"
            self.btn_sync_dir.setText("⮂ Align: Right")
            self.btn_sync_dir.setStyleSheet("QPushButton { background-color: #444; color: #aaa; border-radius: 3px; padding: 2px 10px; }")
        
        # Mathematically swap the position (e.g., 1 step from right becomes 1 step from left)
        self.split_idx = len(self.path_parts) - self.split_idx
        self.update_split_display()
        
        # Tell the Hub to realign everyone below us!
        self.sync_dir_toggled.emit()

    def apply_absolute_split(self, target_idx):
        """Forces the slider to an exact directory depth from the root."""
        self.split_idx = target_idx
        self.update_split_display()

    def is_decoupled(self):
        return hasattr(self, 'chk_decouple') and self.chk_decouple.isChecked()

    def apply_relative_shift(self, delta):
        """Called externally by the Hub to force a relative step."""
        self.split_idx += delta
        self.update_split_display()

    # --- SLIDER LOGIC ---
    def update_split_display(self):
        # Clamping logic
        if self.split_idx < 1: self.split_idx = 1
        if self.split_idx > len(self.path_parts) - 1: self.split_idx = len(self.path_parts) - 1
        
        left_path = str(Path(*self.path_parts[:self.split_idx]))
        right_path = os.path.join(*self.path_parts[self.split_idx:])
        
        self.edit_anchor.setText(left_path)
        self.edit_relative.setText(right_path)
        
        len_left = max(len(left_path), 1)
        len_right = max(len(right_path), 1)
        self.split_lay.setStretch(0, len_left)
        self.split_lay.setStretch(1, len_right)
        
        self.edit_anchor.setCursorPosition(len(left_path))
        self.edit_relative.setCursorPosition(0)
        
        self.btn_left.setEnabled(self.split_idx > 1)
        self.btn_right.setEnabled(self.split_idx < len(self.path_parts) - 1)

    def shift_left(self):
        self.split_idx -= 1
        self.update_split_display()
        self.shift_requested.emit(-1) # Broadcast the step!

    def shift_right(self):
        self.split_idx += 1
        self.update_split_display()
        self.shift_requested.emit(1)  # Broadcast the step!

    def is_force_enabled(self):
        return self.chk_force.isChecked()

    def browse_dest(self):
        path = QFileDialog.getExistingDirectory(self, "Select Destination", self.edit_dest.text())
        if path: self.edit_dest.setText(os.path.normpath(path))

class WarpProgressHUD(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedSize(450, 150)
        self.setWindowFlags(Qt.SubWindow) # Keeps it on top of the parent
        
        # Sleek dark HUD styling
        self.setStyleSheet("""
            WarpProgressHUD {
                background-color: rgba(30, 30, 30, 230);
                border: 2px solid #58cc71;
                border-radius: 10px;
            }
            QLabel { color: white; font-family: 'Segoe UI', sans-serif; }
        """)

        layout = QVBoxLayout(self)
        
        self.lbl_title = QLabel("🚀 <b>GDrive Warp Transfer</b>")
        self.lbl_title.setStyleSheet("font-size: 16px; color: #58cc71;")
        self.lbl_title.setAlignment(Qt.AlignCenter)
        
        self.bar = QProgressBar()
        self.bar.setStyleSheet("""
            QProgressBar { border: 1px solid #555; border-radius: 5px; text-align: center; height: 20px; }
            QProgressBar::chunk { background-color: #58cc71; }
        """)
        
        self.lbl_speed = QLabel("Calculating speed...")
        self.lbl_speed.setAlignment(Qt.AlignCenter)
        
        self.lbl_file = QLabel("Initializing...")
        self.lbl_file.setStyleSheet("color: #aaa; font-size: 11px;")
        self.lbl_file.setAlignment(Qt.AlignCenter)
        self.lbl_file.setWordWrap(True)

        layout.addWidget(self.lbl_title)
        layout.addWidget(self.bar)
        layout.addWidget(self.lbl_speed)
        layout.addWidget(self.lbl_file)

        self.lbl_batch = QLabel("Task 1 of 1")
        self.lbl_batch.setStyleSheet("color: #888; font-size: 10px;")
        self.lbl_batch.setAlignment(Qt.AlignRight)
        
        # Add it to your layout (perhaps at the very bottom or top-right)
        self.layout().addWidget(self.lbl_batch)

        self.btn_done = QPushButton("Close")
        self.btn_done.hide()
        self.btn_done.clicked.connect(self.hide)
        self.btn_done.setStyleSheet("background-color: #2e885a; color: white; font-weight: bold;")
        self.layout().addWidget(self.btn_done)

    def show_summary(self, total_files, selected_item_count, dest_list):
        """Transforms the HUD into an honest completion report."""
        self.bar.hide()
        self.lbl_speed.hide()
        self.lbl_title.setText("✅ <b>Warp Complete</b>")
        self.lbl_title.setStyleSheet("font-size: 16px; color: #58cc71;")
        
        unique_dests = list(set(dest_list))
        if not unique_dests:
            dest_str = ""
        elif len(unique_dests) == 1:
            dest_str = f"To: {unique_dests[0]}"
        else:
            dest_str = f"Across {len(unique_dests)} distinct destinations"
        
        # Now it tells you exactly what you selected vs what it actually moved
        summary = (f"Processed <b>{selected_item_count}</b> selected items<br>"
                   f"(<i>{total_files} individual files on disk</i>)<br><br>"
                   f"<span style='color: #888;'>{dest_str}</span>")
        
        self.lbl_file.setText(summary)
        self.btn_done.show()

    def update_batch_status(self, current, total):
        """Updates the batch counter label."""
        self.lbl_batch.setText(f"Batch {current} of {total}")
        # Force a repaint to ensure the UI doesn't lag behind the logic
        self.lbl_batch.repaint()

    def update_data(self, pct, speed, current_file):
        self.bar.setValue(int(pct))
        self.lbl_speed.setText(f"⚡ {speed}")
        self.lbl_file.setText(f"Active: {current_file}")
        
class AssetManager(QMainWindow):
    def __init__(self, shot_path="", engine=None, project_label="Generic Project"):
        super().__init__()
        self.engine = engine
        
        self.project_label = project_label

        self.catalog_provider = CatalogProvider(self.engine)
        
        self.setWindowTitle(f"[{self.project_label}] - Pipeline Asset Manager")
        self.resize(1800, 1200)

        # Status Options Logic
        status_raw = self.engine.settings.get('status_options', "")
        if status_raw:
            self.status_options = [s.strip() for s in status_raw.split(',') if s.strip()]
            if "" not in self.status_options: self.status_options.insert(0, "")
        else:
            self.status_options = ["", "Review Needed", "Needs Update", "Approved", "RTS", "Pending"]
        
        self.df_master = pd.DataFrame()
        self.df_shots = pd.DataFrame()
        self.ext_checkboxes = {}
        self.session_cache = []
        self.playlist_cache = []
        self.autosave_fuse_lit = False
        self.autosave_timer = QTimer()
        self.autosave_timer.setSingleShot(True)
        self.autosave_timer.timeout.connect(self.run_autosave)

        self.active_dialogs = {}
        
        self.init_ui()
        
        # --- SURGICAL INJECTION: Setup the dock ---
        self.setup_data_dock()
        
        # SURGICAL FIX: We no longer try to setText on self.path_master here.
        # reload_all handles everything now.
        self.reload_all()
        
        # Trigger the warning check immediately after the window renders
        QTimer.singleShot(0, self.check_initial_state)

    def init_ui(self):
        main_widget = QWidget(); self.layout = QVBoxLayout(main_widget)
        self.layout.setContentsMargins(10, 10, 10, 10)

        self.lbl_stats = QLabel("Showing 0 / 0 items")
        self.lbl_stats.setStyleSheet("color: #888; padding-right: 10px; font-family: 'Courier New', 'Menlo', monospace;")
        self.statusBar().addPermanentWidget(self.lbl_stats)

        # --- 0. GLOBAL PROJECT TOOLBAR ---
        self.top_toolbar = QHBoxLayout()
        
        self.btn_project_manager = QPushButton("Project Manager")
        self.btn_project_manager.setStyleSheet("""
            QPushButton { 
                background-color: #3d3d3d; color: #58cc71; 
                font-weight: bold; padding: 6px 15px; border: 1px solid #555;
            }
            QPushButton:hover { background-color: #4d4d4d; }
        """)
        self.btn_project_manager.clicked.connect(self.open_project_manager)
        
        self.btn_notes_manager = QPushButton("Notes Manager")
        self.btn_notes_manager.setStyleSheet("""
            QPushButton { 
                background-color: #5a2e88; color: white; 
                font-weight: bold; height: 35px;
            }
            QPushButton:hover { background-color: #6a3e98; }
        """)
        self.btn_notes_manager.clicked.connect(self.open_global_notes_manager)

        # --- SURGICAL INJECTION: DATA SOURCES TOGGLE ---
        self.btn_data_dock = QPushButton("🗄️ Data Sources")
        self.btn_data_dock.setCheckable(True) # Makes it act like a toggle switch
        self.btn_data_dock.setStyleSheet("""
            QPushButton { 
                background-color: #2e5a88; color: white; 
                font-weight: bold; padding: 6px 15px; border: 1px solid #555;
            }
            QPushButton:hover { background-color: #3b76b3; }
            QPushButton:checked { background-color: #3b82f6; border: 1px solid #fff; }
        """)
        self.btn_data_dock.toggled.connect(self.toggle_data_dock)
        
        self.top_toolbar.addWidget(self.btn_project_manager)
        self.top_toolbar.addWidget(self.btn_notes_manager)
        self.top_toolbar.addWidget(self.btn_data_dock) # Add it to the layout

        self.top_toolbar.addStretch()
        self.layout.addLayout(self.top_toolbar)

        # Subtle separator
        line = QFrame(); line.setFrameShape(QFrame.HLine); line.setFrameShadow(QFrame.Sunken)
        self.layout.addWidget(line)

        # --- 1. SIMPLE SEARCH BAR (Now starts here) ---
        self.proxy_model = MultiFilterProxy()

        # --- 1. SIMPLE SEARCH BAR ---
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("<b>Simple Search:</b>"))
        self.edit_search = QLineEdit()
        self.edit_search.setPlaceholderText("Type to filter...")
        self.edit_search.setClearButtonEnabled(True)
        self.edit_search.textChanged.connect(self.update_simple_search)
        search_layout.addWidget(self.edit_search, 2)

        search_layout.addWidget(QLabel("In Columns:"))
        btn_all_search = QPushButton("All"); btn_none_search = QPushButton("None")
        btn_all_search.setFixedWidth(40); btn_none_search.setFixedWidth(50)
        btn_all_search.clicked.connect(lambda: self.toggle_simple_cols(True))
        btn_none_search.clicked.connect(lambda: self.toggle_simple_cols(False))
        search_layout.addWidget(btn_all_search); search_layout.addWidget(btn_none_search)

        self.simple_col_container = QHBoxLayout()
        self.simple_checkboxes = {}
        for col in ["FILENAME", "LOCALPATH", "SEQUENCE", "SHOTNAME", "ALTSHOTNAME", "SUBNOTES"]:
            cb = QCheckBox(col)
            cb.setChecked(True if col in ["FILENAME", "LOCALPATH"] else False)
            cb.stateChanged.connect(self.update_simple_search)
            self.simple_col_container.addWidget(cb)
            self.simple_checkboxes[col] = cb
            
        search_layout.addLayout(self.simple_col_container)
        search_layout.addStretch()

        self.layout.addLayout(search_layout)

        advanced_search_layout = QHBoxLayout()        
        # --- ADVANCED LAUNCH BUTTON ---
        self.btn_advanced_search = QPushButton("Advanced Search...")
        self.btn_advanced_search.setStyleSheet("background-color: #444; color: white; font-weight: bold; padding: 5px 15px;")
        self.btn_advanced_search.clicked.connect(self.open_advanced_search)
        advanced_search_layout.addWidget(self.btn_advanced_search)
        
        # --- CLEAR ADVANCED SEARCH BUTTON ---
        self.btn_clear_advanced = QPushButton("Clear Adv Search")
        self.btn_clear_advanced.setStyleSheet("QPushButton { background-color: #882e2e; color: white; padding: 5px 15px; } "
                                              "QPushButton:disabled { background-color: #444; color: #888; }") 
        self.btn_clear_advanced.clicked.connect(self.clear_advanced_search)
        
        # Start disabled!
        self.btn_clear_advanced.setEnabled(False) 
        
        advanced_search_layout.addWidget(self.btn_clear_advanced)

        advanced_search_layout.addStretch()

        # --- Find where you added the Clear Adv Search button ---
        self.btn_clear_advanced.clicked.connect(self.clear_advanced_search)
        advanced_search_layout.addWidget(self.btn_clear_advanced)

        # --- SURGICAL INJECTION: Reset All Button ---
        advanced_search_layout.addStretch() # This pushes the Reset button to the far right
        self.btn_reset_all = QPushButton("Reset All Filters")
        self.btn_reset_all.setStyleSheet("""
            QPushButton { 
                background-color: #333; color: #aaa; 
                padding: 5px 15px; border: 1px solid #555; 
            }
            QPushButton:hover { background-color: #444; color: white; }
        """)
        self.btn_reset_all.clicked.connect(self.reset_all_filters)
        advanced_search_layout.addWidget(self.btn_reset_all)
        
        self.layout.addLayout(advanced_search_layout)

        # --- EXTENSIONS ROW ---
        ext_filter_layout = QHBoxLayout()
        ext_filter_layout.addWidget(QLabel("Extensions:"))
        
        btn_all = QPushButton("All")
        btn_none = QPushButton("None")
        btn_all.clicked.connect(lambda: self.toggle_all_exts(True))
        btn_none.clicked.connect(lambda: self.toggle_all_exts(False))
        
        ext_filter_layout.addWidget(btn_all)
        ext_filter_layout.addWidget(btn_none)
        
        self.ext_container = QHBoxLayout()
        ext_filter_layout.addLayout(self.ext_container)
        
        # Pushes the extension checkboxes tightly to the left
        ext_filter_layout.addStretch() 
        self.layout.addLayout(ext_filter_layout)

        # --- SEQ / SHOT ROW ---
        shot_status_layout = QHBoxLayout()
        
        self.seq_selector = QComboBox()
        self.seq_selector.addItem("All")
        self.seq_selector.setMinimumWidth(100) # Prevents collapsing
        self.seq_selector.currentTextChanged.connect(self.apply_sequence_filter)
        
        self.shot_selector = QComboBox()
        self.shot_selector.addItem("All")
        self.shot_selector.setMinimumWidth(250) # Gives room for long dual-names
        self.shot_selector.currentTextChanged.connect(self.filter_table)
        
        shot_status_layout.addWidget(QLabel("Seq:"))
        shot_status_layout.addWidget(self.seq_selector)
        shot_status_layout.addWidget(QLabel("Shot:"))
        shot_status_layout.addWidget(self.shot_selector)
        
        # Pushes the dropdowns tightly to the left
        shot_status_layout.addStretch() 
        self.layout.addLayout(shot_status_layout)

        # --- DATE RANGE ROW ---
        date_layout = QHBoxLayout()
        date_layout.addWidget(QLabel("Date Range:"))
        
        self.date_preset = QComboBox()
        self.date_preset.addItems(["All Time", "Today", "Yesterday", "Last 2 Days", "Last 3 Days", "Last Week", "Last Month"])
        self.date_preset.currentTextChanged.connect(self.apply_date_preset)
        
        self.date_from = QDateEdit()
        self.date_from.setCalendarPopup(True)
        self.date_from.setDisplayFormat("yyyy-MM-dd")
        self.date_from.dateChanged.connect(self.update_date_filter)
        
        self.date_to = QDateEdit()
        self.date_to.setCalendarPopup(True)
        self.date_to.setDisplayFormat("yyyy-MM-dd")
        self.date_to.setDate(QDate.currentDate())
        self.date_to.dateChanged.connect(self.update_date_filter)
        
        date_layout.addWidget(self.date_preset)
        date_layout.addWidget(QLabel("From:"))
        date_layout.addWidget(self.date_from)
        date_layout.addWidget(QLabel("To:"))
        date_layout.addWidget(self.date_to)
        
        # --- SURGICAL INJECTION: Latest Only Checkbox ---
        self.cb_latest_only = QCheckBox("Latest (per Shot)")
        self.cb_latest_only.setToolTip("Only show the most recent file for each unique Shotname")
        self.cb_latest_only.stateChanged.connect(self.update_latest_filter)
        date_layout.addWidget(self.cb_latest_only)
        
        # Pushes the date controls tightly to the left
        date_layout.addStretch()

        self.btn_validate_ranges = QPushButton("Flag Range Mismatches")
        self.btn_validate_ranges.setCheckable(True)
        self.btn_validate_ranges.setStyleSheet("""
            QPushButton:checked { background-color: #882e2e; color: white; font-weight: bold; }
        """)
        self.btn_validate_ranges.toggled.connect(self.toggle_range_validation)
        # Add it to your search_layout or actions_layout
        date_layout.addWidget(self.btn_validate_ranges)

        self.layout.addLayout(date_layout)
        
        line = QFrame(); line.setFrameShape(QFrame.HLine); line.setFrameShadow(QFrame.Sunken); self.layout.addWidget(line)
        
        actions_layout = QHBoxLayout()
        self.btn_rv = QPushButton("Play in RV"); self.btn_rv.setStyleSheet("background-color: #2e5a88; font-weight: bold; color: white;"); self.btn_rv.clicked.connect(self.launch_rv)
        self.btn_rv_add_scans = QPushButton("Play with Scans in RV"); self.btn_rv_add_scans.setStyleSheet("background-color: #2e5a88; font-weight: bold; color: white;"); self.btn_rv_add_scans.clicked.connect(self.launch_rv_with_added_scans)
        self.btn_nuke = QPushButton("Open in Nuke")
        self.btn_nuke.setStyleSheet("background-color: #f37321; font-weight: bold; color: white;")
        self.btn_nuke.clicked.connect(self.launch_nuke)
        self.btn_send = QPushButton("Send What's Ready"); self.btn_send.setStyleSheet("background-color: #2e885a; font-weight: bold; color: white;"); self.btn_send.clicked.connect(self.send_ready_assets)
        # Inside init_ui
        self.btn_sessions = QPushButton("Session Manager")
        self.btn_sessions.setStyleSheet("background-color: #5a2e88; font-weight: bold; color: white;")
        self.btn_sessions.clicked.connect(self.open_session_manager)

        self.btn_os_open = QPushButton("Open using OS"); self.btn_os_open.setStyleSheet("background-color: #2e5a88; font-weight: bold; color: white;"); self.btn_os_open.clicked.connect(self.open_file_os_default)
        
        actions_layout.addWidget(self.btn_rv)
        actions_layout.addWidget(self.btn_rv_add_scans)
        actions_layout.addWidget(self.btn_nuke)
        actions_layout.addWidget(self.btn_os_open)
        actions_layout.addWidget(self.btn_send)
        actions_layout.addWidget(self.btn_sessions)
        actions_layout.addStretch()
        self.layout.addLayout(actions_layout)

        # Inside AssetManager.init_ui
        self.table = QTableView()
        self.main_model = PandasModel(self.df_master, self, read_only=True)

        self.proxy_model.setSourceModel(self.main_model)
        self.table.setModel(self.proxy_model)
        
        self.proxy_model.setSourceModel(self.main_model)
        self.table.setModel(self.proxy_model)
        self.proxy_model.layoutChanged.connect(self.update_status_stats)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        self.table.setSelectionBehavior(QTableView.SelectItems)
        self.table.setSelectionMode(QTableView.ExtendedSelection)
        self.table.setSortingEnabled(True)
        self.table.selectionModel().selectionChanged.connect(self.update_status_stats)
        self.layout.addWidget(self.table)
        
        # --- SLEEK BOTTOM CONTROL SECTION ---
        bottom_row = QHBoxLayout()

        # Left Side: The "Action" Buttons (Scraping)
        self.btn_update_scrape = QPushButton("Update Data")
        self.btn_update_scrape.setStyleSheet("background-color: #b07030; color: white; font-weight: bold; height: 35px;")
        self.btn_update_scrape.clicked.connect(self.trigger_quick_scrape)

        bottom_row.addWidget(self.btn_update_scrape)
        bottom_row.addStretch()

        # Right Side: The "Config" Dropdown
        self.btn_edit_config = QPushButton("Project Config ▾")
        self.btn_edit_config.setStyleSheet("background-color: #444; color: white; font-weight: bold; height: 35px; padding: 0 15px;")

        # --- NEW: CONFIG HUB BUTTON ---
        self.btn_config_hub = QPushButton("Config Hub")
        self.btn_config_hub.setStyleSheet("""
            QPushButton { 
                background-color: #5a2e88; 
                color: white; 
                font-weight: bold; 
                height: 35px; 
                padding: 0 15px; 
                border-radius: 4px;
            }
            QPushButton:hover { background-color: #6a3e98; }
        """)
        self.btn_config_hub.clicked.connect(self.open_config_hub)
        # bottom_row.addWidget(self.btn_config_hub) # Add it to the layout
        
        # Build the Menu
        config_menu = QMenu(self)
        import_menu = config_menu.addMenu("Import...")
        import_menu.addAction(f"Import Shots data", self.action_import_shots)
        config_menu.addSeparator()
        config_menu.addAction("Edit Project Settings", self.open_settings_editor)
        config_menu.addSeparator()
        
        # Nested App Configs (Nuke, RV, etc.)
        app_menu = config_menu.addMenu("App Executables...")
        for app_name in sorted(self.engine.apps.keys()):
            app_menu.addAction(f"Edit {app_name} Paths", lambda a=app_name: self.open_app_config_editor(a))
        config_menu.addSeparator()
        config_menu.addAction("Edit Shots CSV", self.open_shots_editor)
        config_menu.addSeparator()
        config_menu.addAction("Edit Path Subs", self.open_pathsubs_editor)
        config_menu.addSeparator()
        config_menu.addAction("Edit Naming Templates", self.open_templates_editor)
        config_menu.addSeparator()
        config_menu.addAction("Edit Notes Config", self.open_notes_config_editor)
        config_menu.addSeparator()
        config_menu.addAction("Force Reload All Data", self.live_refresh_config)
        
        self.btn_edit_config.setMenu(config_menu)
        bottom_row.addWidget(self.btn_edit_config)

        self.layout.addLayout(bottom_row)

        self.setCentralWidget(main_widget)

        # Initialize the numbers
        self.update_status_stats()

    def check_rclone_presence(self):
        """Returns True if rclone is found in the system PATH."""
        import shutil
        return shutil.which("rclone") is not None

    def show_rclone_install_help(self):
        """Presents a friendly installation guide based on the OS."""
        import sys
        from PySide6.QtWidgets import QMessageBox

        msg = QMessageBox(self)
        msg.setWindowTitle("Rclone Not Found")
        msg.setIcon(QMessageBox.Information)
        
        # Determine OS-specific instructions
        if sys.platform == "win32":
            install_cmd = "winget install Rclone.Rclone"
            guide = "<b>Windows 11:</b> Open PowerShell and run:<br><code>" + install_cmd + "</code>"
        elif sys.platform == "darwin":
            install_cmd = "brew install rclone"
            guide = "<b>macOS:</b> Open Terminal and run:<br><code>" + install_cmd + "</code>"
        else:
            install_cmd = "sudo dnf install rclone  # or apt"
            guide = "<b>Linux:</b> Use your package manager:<br><code>" + install_cmd + "</code>"

        msg.setText("🚀 <b>GDrive Warp Copy requires 'rclone' to be installed.</b>")
        msg.setInformativeText(
            f"This utility enables high-speed, multi-threaded transfers.<br><br>{guide}"
            "<br><br>Once installed, please restart the application."
        )
        msg.setStandardButtons(QMessageBox.Ok)
        msg.exec()

    def start_warp_copy(self):
        if not self.check_rclone_presence():
            self.show_rclone_install_help(); return
            
        selection = self.get_selected_paths()
        hub = WarpHubDialog(selection, self)
        if hub.exec() != QDialog.Accepted: return
        
        mappings = hub.get_mappings()
        
        # --- 1. TOTAL FILE PRE-CALCULATION (The "Honesty" Phase) ---
        grouped_tasks = {} # (anchor, dest, force) -> [LIST OF ACTUAL FILES]
        total_files_in_job = 0
        
        import glob, re
        for m in mappings:
            key = (m['anchor'], m['dest'], m['force'])
            if key not in grouped_tasks: grouped_tasks[key] = []
            
            # Expand sequences immediately to get the real count
            expanded = glob.glob(re.sub(r'%0?\d*d', '*', m['path'])) if '%' in m['path'] else [m['path']]
            grouped_tasks[key].extend(expanded)
            total_files_in_job += len(expanded)
            QApplication.processEvents()

        if total_files_in_job == 0: return

        # 2. HUD Setup
        self.hud = WarpProgressHUD(self)
        self.hud.move(self.rect().center() - self.hud.rect().center())
        self.hud.show()

        # 3. EXECUTION WITH GLOBAL FILE MATH
        files_completed_before_current_batch = 0
        total_batches_run = len(grouped_tasks)

        for (anchor, dest, force), all_files in grouped_tasks.items():
            if not anchor or not dest: continue

            self.warp_proc = RcloneCopyTask(self)
            
            def handle_hud_update(batch_finished_count, speed, filename):
                # THE REAL MATH:
                # (Files from previous batches + files done in this batch) / Total Files
                current_total_done = files_completed_before_current_batch + batch_finished_count
                global_pct = (current_total_done / total_files_in_job) * 100
                self.hud.update_data(global_pct, speed, filename)

            self.warp_proc.progress_update.connect(handle_hud_update)
            self.warp_proc.run_copy(all_files, dest, anchor, force_overwrite=force)
            
            while self.warp_proc.state() != QProcess.NotRunning:
                QApplication.processEvents()
                if not self.hud.isVisible():
                    self.warp_proc.kill(); return 

            # Batch finished! Add this batch's count to the "Completed" tally
            files_completed_before_current_batch += len(all_files)

        # 4. FINAL SUMMARY (Now entirely based on user selection)
        # We pass len(mappings) which represents exactly how many rows were in the Warp Hub
        self.hud.show_summary(total_files_in_job, len(mappings), [m['dest'] for m in mappings])

    def _handle_rclone_output(self):
        """Parses rclone --progress to update the UI."""
        out = self.warp_proc.readAllStandardOutput().data().decode()
        # You can regex parse the 'Transferred: 50%' here if you want a real bar!
        print(out)
    
    def setup_data_dock(self):
        self.data_dock = DataSourcesDock(self, self.engine)
        self.addDockWidget(Qt.RightDockWidgetArea, self.data_dock)

        self.resizeDocks([self.data_dock], [275], Qt.Horizontal)

        self.data_dock.hide()
        self.data_dock.visibilityChanged.connect(self.btn_data_dock.setChecked)
        self.data_dock.request_reload.connect(lambda active_ids: self.reload_all())

    def toggle_data_dock(self, checked):
        """Shows or hides the dock based on the toolbar button state."""
        if checked:
            self.data_dock.show()
        else:
            self.data_dock.hide()

    def action_import_shots(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select CSV to Import", "", "CSV Files (*.csv)")
        if not file_path: return

        importer = CSVImporter(file_path)
        incoming_df = importer.get_raw_df()
        if incoming_df is None: return

        target_headers = ['SEQUENCE', 'SHOTNAME', 'ALTSHOTNAME', 'FIRSTFRAME', 'LASTFRAME', 'HEROPLATE']
        
        # Use the NEW advanced mapper
        mapper = AdvancedImportMapperDialog(incoming_df, target_headers, self)
        if mapper.exec() != QDialog.Accepted: return
        
        mapping_dict, index_col = mapper.get_map_config()
        if not mapping_dict: return

        # Transform and Merge (Keep your existing logic here)
        renamed_df = incoming_df[list(mapping_dict.values())].copy()
        inv_map = {v: k for k, v in mapping_dict.items()}
        renamed_df.rename(columns=inv_map, inplace=True)

        self.df_shots = pd.concat([self.df_shots, renamed_df]).drop_duplicates(subset=[index_col], keep='last')
        
        s_path = PathSwapper.translate(str(self.engine.settings.get('shots_csv', '')))
        self.df_shots.to_csv(s_path, index=False)
        
        self.reload_all()
        self.statusBar().showMessage(f"Successfully imported {len(renamed_df)} shots.", 5000)
        
    def toggle_range_validation(self, enabled):
        self.main_model.validation_enabled = enabled
        # Force the table to repaint everything
        self.main_model.layoutChanged.emit()

    def open_config_hub(self):
        """Launches the new unified configuration interface."""
        hub = ConfigHub(self.engine, self)
        # We use exec_() so it's modal (locks the main window while editing)
        if hub.exec_():
            # If they saved things, we might want to refresh the main view
            self.live_refresh_config()

    def update_status_stats(self):
        """Updates the permanent status bar label with row and selection counts."""
        if not hasattr(self, 'proxy_model') or not hasattr(self, 'main_model'):
            return
            
        visible = self.proxy_model.rowCount()
        total = self.main_model.rowCount()
        
        # --- NEW: CALCULATE SELECTED ROWS ---
        selection = self.table.selectionModel().selectedIndexes()
        # We use a set to ensure we only count each row once
        selected_count = len({idx.row() for idx in selection})
        
        # Color code: green if filtering, otherwise standard grey
        color = "#58cc71" if visible < total else "#888"
        self.lbl_stats.setStyleSheet(f"color: {color}; padding-right: 10px; font-family: 'Courier New', 'Menlo', monospace;")
        
        # Build the string: SELECTED | DISPLAYED | TOTAL
        stats_text = ""
        if selected_count > 0:
            stats_text += f"SELECTED: {selected_count} | "
            
        stats_text += f"DISPLAYED: {visible} | TOTAL: {total}  "
        
        self.lbl_stats.setText(stats_text)

    def update_latest_filter(self, state):
        # Qt.Checked is an int (2), we need a True/False
        is_checked = (state == 2 or state == Qt.Checked)
        self.proxy_model.set_latest_only(is_checked)

    def reset_all_filters(self):
        """Surgically restores every filter to its 'factory' state."""
        # 1. Clear Search Text & Checkboxes
        self.edit_search.clear()
        for col, cb in self.simple_checkboxes.items():
            cb.blockSignals(True)
            cb.setChecked(True if col in ["FILENAME", "LOCALPATH"] else False)
            cb.blockSignals(False)

        # 2. Clear Advanced Search
        self.clear_advanced_search()

        # 3. Reset Extensions (Select All)
        self.toggle_all_exts(True)

        # 4. Reset Dropdowns
        self.seq_selector.setCurrentText("All")
        self.shot_selector.setCurrentText("All")
        
        # 5. Reset Date Range
        self.date_preset.setCurrentText("All Time") # This triggers update_date_filter via signal

        # 6. Reset Latest per Shot
        self.cb_latest_only.blockSignals(True)
        self.cb_latest_only.setChecked(False)
        self.proxy_model.set_latest_only(False)
        self.cb_latest_only.blockSignals(False)

        # 7. Final Push to Proxy
        self._exec_simple_search() # Update the proxy with the now-empty search string
        self.statusBar().showMessage("All filters reset to default.", 3000)

    def action_add_note(self, checked=False, reply_to_file=""):
        selection = self.table.selectionModel().selectedIndexes()
        if not selection: return

        # 1. Validation Check (Config Exists?)
        notes_eng = NotesEngine(self.engine)
        if not notes_eng.is_valid():
            if not notes_eng.trigger_sync_ui(self):
                return # User bailed on sync

        # 2. Identify target rows and metadata
        rows = sorted({self.proxy_model.mapToSource(idx).row() for idx in selection})
        shot_names = [str(self.df_master.iat[r, self.df_master.columns.get_loc("SHOTNAME")]) for r in rows]
        
        # --- NEW: PRE-FLIGHT PATH CHECK ---
        user = os.getlogin()
        test_row_data = self.df_master.iloc[rows[0]].to_dict()
        notes_eng.asset_context = test_row_data
        test_path = notes_eng.construct_path(scope="default", task="general", user=user)

        if not test_path or test_path.strip() == "" or test_path == "nan" or test_path == ".":
            from PySide6.QtWidgets import QMessageBox
            QMessageBox.critical(self, "Path Resolution Error", 
                "Cannot construct a valid save path for these notes.\n\n"
                "Please check your Notes_Config.csv and ensure your Project Root is set.")
            return
        # ----------------------------------

        # UI Polish: Change the dialog title if this is a reply
        dlg = NotesEntryDialog(self, shot_names=shot_names)
        if reply_to_file:
            dlg.setWindowTitle("Reply to Note")
            
        if dlg.exec() != QDialog.Accepted: return
        note_text = dlg.get_note_text()
        if not note_text: return
        
        # 4. The Loop: Resolve and Save for every row
        user = os.getlogin()
        success_count = 0
        
        for r in rows:
            row_data = self.df_master.iloc[r].to_dict()
            notes_eng.asset_context = row_data
            
            target_path = notes_eng.construct_path(scope="default", task="general", user=user)
            note_dir, note_filename = os.path.split(target_path)
            os.makedirs(note_dir, exist_ok=True)
            
            # --- THE SNAPSHOT LOGIC ---
            # 1. Create the base note entry
            note_entry = {
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "User": user,
                "Note": note_text,
                "Reply_To": reply_to_file # Empty for new, populated for replies!
            }
            # 2. Append ALL columns from the master row to this dict
            note_entry.update(row_data) 
            
            try:
                pd.DataFrame([note_entry]).to_csv(target_path, index=False, encoding='cp1252')
                self.update_notes_manifest(note_dir, note_filename)
                success_count += 1
            except Exception as e:
                print(f"Failed to save note snapshot: {e}")

        self.statusBar().showMessage(f"Notes & Manifests updated for {success_count} shots.", 5000)

    def update_notes_manifest(self, directory, note_file, status="active"):
        """
        Surgically handles the .notes_info.csv ledger in the target directory.
        Works for New Notes (Insert) or Status Changes (Update).
        """
        manifest_path = os.path.join(directory, ".notes_info.csv")
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        new_row_dict = {"Filename": note_file, "Status": status, "Last_Modified": now_ts}

        if os.path.exists(manifest_path):
            try:
                # Force string type so we don't accidentally mangle any data
                df = pd.read_csv(manifest_path, dtype=str).fillna("")
                
                if note_file in df['Filename'].values:
                    # SCENARIO: Update existing status (Triggered by Notes Manager Dropdown)
                    mask = df['Filename'] == note_file
                    df.loc[mask, 'Status'] = status
                    df.loc[mask, 'Last_Modified'] = now_ts
                else:
                    # SCENARIO: Register a brand new note (Triggered by action_add_note)
                    new_row = pd.DataFrame([new_row_dict])
                    df = pd.concat([df, new_row], ignore_index=True)
                
                df.to_csv(manifest_path, index=False)
            except Exception as e:
                print(f"Manifest Write Error: {e}")
        else:
            # SCENARIO: First note ever written to this specific directory
            pd.DataFrame([new_row_dict]).to_csv(manifest_path, index=False)

    def open_global_notes_manager(self):
        dlg = NotesManagerDialog(parent=self, engine=self.engine, mode="ALL")
        dlg.show() # Non-modal so you can keep it open while working

    def open_notes_manager(self):
        """Gather context from selection and spawn the manager."""
        selection = self.table.selectionModel().selectedIndexes()
        
        # 1. If nothing is selected, bail
        if not selection:
            self.statusBar().showMessage("Selection required to Manage Notes.", 4000)
            return

        # 2. Extract the actual row data from df_master for each selected row
        rows = sorted({self.proxy_model.mapToSource(idx).row() for idx in selection})
        selected_data = [self.df_master.iloc[r].to_dict() for r in rows]

        # 3. SURGICAL FIX: Pass the correct mode and kwarg!
        dlg = NotesManagerDialog(parent=self, engine=self.engine, mode="SELECTION", context_data=selected_data)
        
        dlg.exec()

    def open_project_manager(self):
        """Spawns the Project/Shot Manager tool."""
        if not hasattr(self, 'project_manager_window') or self.project_manager_window is None:
            # We pass 'self' as parent and the engine for settings
            self.project_manager_window = ProjectManagerDialog(self.engine, parent=self)
            
        self.project_manager_window.show()
        self.project_manager_window.raise_()
        self.project_manager_window.activateWindow()

    def toggle_simple_cols(self, state):
        for cb in self.simple_checkboxes.values():
            cb.setChecked(state)
        self.update_simple_search()

    def update_simple_search(self):
        """Uses a 250ms debounce timer to prevent lag while typing."""
        if not hasattr(self, 'search_timer'):
            self.search_timer = QTimer(self)
            self.search_timer.setSingleShot(True)
            self.search_timer.timeout.connect(self._exec_simple_search)
        
        self.search_timer.start(250)

    def _exec_simple_search(self):
        text = self.edit_search.text().strip()
        active_cols = {name for name, cb in self.simple_checkboxes.items() if cb.isChecked()}
        self.proxy_model.set_simple_search(text, active_cols)

    def open_advanced_search(self):
        """Spawns or focuses the floating advanced search palette."""
        if not hasattr(self, 'advanced_dialog') or self.advanced_dialog is None:
            self.advanced_dialog = AdvancedSearchBuilder(self.proxy_model, parent=self)
            
        self.advanced_dialog.show()
        self.advanced_dialog.raise_()
        self.advanced_dialog.activateWindow()

    def clear_advanced_search(self):
        """Wipes the advanced search rules and updates UI state."""
        if hasattr(self, 'advanced_dialog') and self.advanced_dialog is not None:
            self.advanced_dialog.clear_all_rules()
        else:
            self.proxy_model.set_advanced_search([])
        
        # Explicitly disable after clearing
        self.btn_clear_advanced.setEnabled(False)
        self.statusBar().showMessage("Advanced search rules cleared.", 3000)

    def refresh_advanced_button_state(self, rules_exist):
        """Called by the dialog to tell the main UI if the clear button should be on."""
        self.btn_clear_advanced.setEnabled(rules_exist)

    def trigger_quick_scrape(self):
        # 1. Capture the exact state before the scrape
        old_uuids = set(self.df_master['UUID']) if 'UUID' in self.df_master.columns else set()
        old_count = len(self.df_master)

        dlg = UpdateScrapeDialog(self, engine=self.engine)
        if dlg.exec():
            # 2. Scrape finished! Reload the UI with the fresh CSV
            self.reload_all()
            
            # 3. Capture the new state
            new_uuids = set(self.df_master['UUID']) if 'UUID' in self.df_master.columns else set()
            new_count = len(self.df_master)
            
            # 4. Do the math using Python sets
            added = len(new_uuids - old_uuids)
            removed = len(old_uuids - new_uuids)
            
            # 5. Build and show the confirmation popup
            from PySide6.QtWidgets import QMessageBox
            msg = f"Catalog successfully updated and loaded into the UI.\n\n"
            msg += f"Previous Total: {old_count}\n"
            msg += f"New Total: {new_count}\n\n"
            msg += f"Items Added: {added}\n"
            msg += f"Items Removed: {removed}"
            
            QMessageBox.information(self, "Data Summary", msg)
            self.statusBar().showMessage(f"Catalog Updated: +{added} / -{removed}", 5000)

    def open_settings_editor(self):
        """Universal Project Settings using the Dynamic Form."""
        csv_path = os.path.join(self.engine.root, "Project_Settings.csv")
        dlg = DynamicKeyValueEditor(csv_path, engine=self.engine, preset="project_settings", parent=self)
        if dlg.exec():
            self.live_refresh_config()
            self.statusBar().showMessage("Project Settings updated.", 3000)

    def open_templates_editor(self):
        """Naming Templates using the Dynamic Form."""
        t_path = os.path.join(self.engine.root, "Naming_Templates.csv")
        
        # Legacy auto-gen if file is missing
        if not os.path.exists(t_path):
            os.makedirs(os.path.dirname(t_path), exist_ok=True)
            pd.DataFrame([["scan_name_template", "SHOTNAME_plate_vHEROPLATE"]], 
                        columns=['Key', 'Value']).to_csv(t_path, index=False)

        # We don't need a preset here as it's a simple Key/Value, but we pass the engine
        # so it handles {LOCALDOTDIR} and generic styling automatically.
        dlg = DynamicKeyValueEditor(t_path, title="Naming Templates", engine=self.engine, parent=self)
        if dlg.exec():
            self.live_refresh_config()
            self.statusBar().showMessage("Naming Templates reloaded.", 3000)

    def open_app_config_editor(self, app_name):
        """App Executables (Nuke/RV) using the 'app_config' preset."""
        import sys
        plat = "win" if sys.platform == "win32" else "mac" if sys.platform == "darwin" else "linux"
        csv_path = os.path.join(self.engine.apps_dir, app_name, f"{plat}.csv")
        
        dlg = DynamicKeyValueEditor(csv_path, title=f"{app_name} Config", engine=self.engine, preset="app_config", parent=self)
        if dlg.exec():
            self.live_refresh_config()

    def open_notes_config_editor(self):
        """Notes configuration using the Dynamic Form."""
        n_path = os.path.join(self.engine.root, "Notes_Config.csv")
        
        if not os.path.exists(n_path):
            from PySide6.QtWidgets import QMessageBox
            res = QMessageBox.question(self, "Config Missing", 
                "Notes Config missing. Run a Project Sync to generate it?", QMessageBox.Yes | QMessageBox.No)
            if res == QMessageBox.Yes:
                self.engine.bootstrap_template(mode='sync')
                self.live_refresh_config()
            if not os.path.exists(n_path): return

        dlg = DynamicKeyValueEditor(n_path, title="Notes Routing", engine=self.engine, preset="notes_config", parent=self)
        if dlg.exec():
            self.live_refresh_config()

    def open_shots_editor(self):
        """Shots Editor remains a table (GenericCSVEditor) as it is structural data."""
        raw_path = str(self.engine.settings.get('shots_csv', ''))
        if not raw_path or raw_path == "nan": return
            
        s_path = PathSwapper.translate(raw_path)
        if not os.path.exists(s_path):
            # ... (Your existing 'Create Blank CSV?' logic remains here) ...
            pass 

        dlg = GenericCSVEditor(s_path, title="Shots Editor", parent=self, engine=self.engine)
        
        # Keep your surgical hiding logic for ALTSHOTNAME
        is_dual = str(self.engine.settings.get('dual_name', 'False')).lower() == 'true'
        if not is_dual and "ALTSHOTNAME" in dlg.df.columns:
            idx = dlg.df.columns.get_loc("ALTSHOTNAME")
            dlg.table.setColumnHidden(idx, True)

        if dlg.exec():
            self.reload_all()

    def open_pathsubs_editor(self):
        """Path Subs remains a table to support multi-platform columns."""
        pathsubs_path = os.path.join(self.engine.root, "Path_Subs.csv")
        dlg = GenericCSVEditor(pathsubs_path, title="Path Subs", parent=self, allow_add_column=True, engine=self.engine)
        if dlg.exec():
            self.live_refresh_config()

    def update_status_options_from_engine(self):
        """Surgically re-parses the status_options string from the engine settings."""
        status_raw = str(self.engine.settings.get('status_options', ""))
        if status_raw and status_raw != "nan":
            self.status_options = [s.strip() for s in status_raw.split(',') if s.strip()]
            # Ensure an empty option exists for clearing status
            if "" not in self.status_options: 
                self.status_options.insert(0, "")
        else:
            # Fallback defaults
            self.status_options = ["", "Review Needed", "Needs Update", "Approved", "RTS", "Uploaded"]

    def live_refresh_config(self):
        """Re-initializes engine from disk and updates logic."""
        # 1. Re-read disk
        self.engine.__init__(self.engine.root) 
        self.naming_templates = self.engine.naming_templates
        
        # 2. Sync Global Swapper
        PathSwapper.PATHSUBS = self.engine.pathsubs
        
        # 3. Update status options (from the easy fix earlier)
        self.update_status_options_from_engine()
        
        # 4. Trigger the reload/mapping logic
        # Note: reload_all now pulls m_path/s_path from engine.settings automatically
        self.reload_all()
        
        self.setWindowTitle(f"[{self.project_label}] - Pipeline Asset Manager")
        self.statusBar().showMessage(f"Refreshed config for {self.project_label}", 3000)

    def browse_for_csv(self, line_edit, config_key):
        """Browses for a CSV and surgically updates the Project Config file."""
        file_path, _ = QFileDialog.getOpenFileName(self, "Select CSV", line_edit.text(), "CSV Files (*.csv)")
        if file_path:
            line_edit.setText(file_path)
            # Update the engine's memory
            self.engine.settings[config_key] = file_path
            # Write it back to the Project_Settings.csv so it's sticky
            self.save_engine_settings()
            self.reload_all()

    def save_engine_settings(self):
        """Writes current engine settings back to the Project_Settings.csv."""
        csv_path = os.path.join(self.engine.root, "Project_Settings.csv")
        df = pd.DataFrame(list(self.engine.settings.items()), columns=['Key', 'Value'])
        df.to_csv(csv_path, index=False)

    def open_session_manager(self):
        """Surgical Update: Just handle the window visibility."""
        manager_key = "session_manager"
        if manager_key in self.active_dialogs:
            self.active_dialogs[manager_key].raise_()
            self.active_dialogs[manager_key].activateWindow()
            return

        base_dir = self.engine.project_root
        dlg_manager = SessionManagerDialog(base_dir, self)
        self.active_dialogs[manager_key] = dlg_manager
        dlg_manager.finished.connect(lambda: self.active_dialogs.pop(manager_key, None))
        
        # We REMOVED all the 'if result == 1' logic from here because 
        # SessionManagerDialog now calls launch_session_from_manager directly!
        dlg_manager.show()

    def launch_session_from_manager(self, session_name, mode):
        """Now launches Review/Playlist editors as free-floating windows."""
        base_dir = self.engine.project_root

        # 1. PATH RESOLUTION (Same as before)
        if mode == "PLAYLIST":
            session_path = os.path.normpath(os.path.join(base_dir, ".autosaves", "playlists", f"{session_name}.csv"))
        elif mode == "SENT":
            session_path = os.path.normpath(os.path.join(base_dir, "submission_data", f"{session_name}.csv"))
        else: # WIP
            session_path = os.path.normpath(os.path.join(base_dir, ".autosaves", "sessions", f"{session_name}.csv"))

        # 2. THE MULTI-WINDOW GUARD
        # If this exact file is already open, just bring it to the front
        if session_path in self.active_dialogs:
            self.active_dialogs[session_path].raise_()
            self.active_dialogs[session_path].activateWindow()
            return

        # 3. DATA LOADING
        df = pd.read_csv(session_path, encoding='cp1252')
        if 'ABSPATH' in df.columns:
            df['ABSPATH'] = df['ABSPATH'].apply(PathSwapper.translate)

        # 4. INSTANTIATE (No .exec()!)
        if mode == "PLAYLIST":
            dlg = PlaylistReviewEditor(df, self, engine=self.engine, target_path=session_path)
        else:
            is_read_only = (mode == "SENT")
            dlg = SubmissionReviewDialog(df, self, engine=self.engine, target_path=session_path, read_only=is_read_only)
            
            if is_read_only:
                dlg.btn_just_save.setVisible(False)
                dlg.btn_save_exit.setVisible(False)
                dlg.buttons.button(QDialogButtonBox.Ok).setVisible(False)
                dlg.buttons.button(QDialogButtonBox.Cancel).setText("Close")
                dlg.setWindowTitle(f"[{self.project_label}] - History: {session_name}")

        # 5. REGISTER & SHOW
        self.active_dialogs[session_path] = dlg
        
        # When the window closes, remove it from the registry
        dlg.finished.connect(lambda: self.active_dialogs.pop(session_path, None))
        
        # --- NEW: Connect the "Submit" and "Save" signals ---
        # Since .exec() is gone, the dialogs need to call these methods directly
        if mode == "WIP":
            dlg.accepted.connect(lambda: self.handle_floating_submit(session_path))
            # result code 2 was our "Save for Later"
            dlg.finished.connect(lambda code: self.handle_floating_save_later(session_path) if code == 2 else None)

        dlg.show()

    def handle_floating_submit(self, path):
        """Triggered when a floating Submission dialog hits 'Submit'."""
        if path in self.active_dialogs:
            dlg = self.active_dialogs[path]
            request_reveal = getattr(dlg, 'open_folder_requested', False)
            self.process_final_submission(dlg.get_data(), session_path=path, open_folder=request_reveal)
            
            # Refresh Manager if open
            if "session_manager" in self.active_dialogs:
                self.active_dialogs["session_manager"].switch_mode("WIP")

    def handle_floating_save_later(self, path):
        """Triggered when a floating dialog hits 'Save and Exit'."""
        # The dialog already saved the CSV in its internal logic, 
        # so we just need to sync the main app state.
        self.run_autosave()
        if "session_manager" in self.active_dialogs:
            self.active_dialogs["session_manager"].refresh_list()

    def process_final_submission(self, final_df, session_path=None, open_folder=False):
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_dir = self.engine.project_root
        
        if not base_dir:
            self.statusBar().showMessage("Submission Failed: No base directory found.", 5000)
            return

        # --- DYNAMIC OUTPUT COLUMNS ---
        raw_csv_headers = str(self.engine.settings.get('submission_csv_headers', 'LOCALPATH,ALTSHOTNAME,SHOTNAME,FILENAME,FIRST,LAST,SUBNOTES,SUBTYPE'))
        output_cols = [c.strip() for c in raw_csv_headers.split(',') if c.strip()]
        
        is_dual = str(self.engine.settings.get('dual_name', 'False')).lower() == 'true'
        if not is_dual and 'ALTSHOTNAME' in output_cols:
            output_cols.remove('ALTSHOTNAME')
        
        # 1. Write the External Data CSV
        data_dir = os.path.join(base_dir, "submission_data")
        os.makedirs(data_dir, exist_ok=True)
        
        final_filename = f"submission_data_{timestamp}.csv"
        final_data_path = os.path.join(data_dir, final_filename)
        
        existing_output = [c for c in output_cols if c in final_df.columns]
        final_df[existing_output].to_csv(final_data_path, index=False, encoding='cp1252')

        # 2. Update Audit Trail
        if 'UUID' in final_df.columns:
            final_df['UUID'] = final_df.apply(lambda x: generate_uuid(x['LOCALPATH'], x['FILENAME']), axis=1)
            sent_uuids = final_df['UUID'].tolist()
            
            log_dir = os.path.join(base_dir, "submission_logs")
            os.makedirs(log_dir, exist_ok=True)
            
            log_df = final_df.copy()
            log_df['SUBSENT'] = now_str
            save_cols = [c for c in ['UUID', 'LOCALPATH', 'FILENAME', 'SUBSENT'] if c in log_df.columns]
            log_df[save_cols].to_csv(os.path.join(log_dir, f"send_{timestamp}.csv"), index=False)

            # Update Main DataFrame
            mask = self.df_master['UUID'].isin(sent_uuids)
            self.df_master.loc[mask, 'SUBSTATUS'] = ""
            def append_ts(val): return now_str if not val or val == "" else f"{val}, {now_str}"
            self.df_master.loc[mask, 'SUBSENT'] = self.df_master.loc[mask, 'SUBSENT'].apply(append_ts)

        # --- THE REVEAL LOGIC (Simplified to use the argument) ---
        if open_folder:
            self.trigger_os_reveal(data_dir, final_filename)
        
        # 3. Final Step: Cleanup the Session file
        if session_path and os.path.exists(session_path):
            try:
                os.remove(session_path)
                session_name = os.path.basename(session_path).replace(".csv", "")
                if session_name in self.session_cache:
                    self.session_cache.remove(session_name)
            except Exception as e:
                print(f"Cleanup Error: {e}")

        # Refresh the UI
        self.main_model.dataChanged.emit(self.main_model.index(0,0), self.main_model.index(self.main_model.rowCount()-1, self.main_model.columnCount()-1))
        self.run_autosave()
        self.statusBar().showMessage(f"Submission Complete: {timestamp}", 5000)

    def add_to_session(self, row_indices, session_name):
        if session_name == "NEW":
            from PySide6.QtWidgets import QInputDialog
            name, ok = QInputDialog.getText(self, "New Session", "Enter Session Name:")
            if not ok or not name: return
            session_name = name

        # 1. THE DATA GRAB
        cols = ['UUID', 'ABSPATH', 'LOCALPATH', 'FILENAME', 'FIRST', 'LAST', 'SHOTNAME', 'ALTSHOTNAME']
        valid_cols = [c for c in cols if c in self.df_master.columns]
        
        try:
            new_data = self.df_master.iloc[list(row_indices)][valid_cols].copy()
            new_data['SUBNOTES'] = ""
        except Exception as e:
            self.statusBar().showMessage(f"Extraction Error: {e}", 5000)
            return

        # 2. UPDATE MAIN TABLE STATUS
        sub_idx = self.df_master.columns.get_loc("SUBSTATUS")
        for r in row_indices:
            self.df_master.iat[r, sub_idx] = "Pending"

        # 3. FILE HANDLING
        base_dir = self.engine.project_root
        session_dir = os.path.join(base_dir, ".autosaves", "sessions")
        if not os.path.exists(session_dir): os.makedirs(session_dir)
        
        # We normpath here ONLY to ensure the dictionary key matches what the launcher used
        session_path = os.path.normpath(os.path.join(session_dir, f"{session_name}.csv"))
        
        if os.path.exists(session_path):
            existing_df = pd.read_csv(session_path, encoding='cp1252')
            combined = pd.concat([existing_df, new_data]).drop_duplicates(subset=['UUID'], keep='last')
            combined.to_csv(session_path, index=False)
        else:
            new_data.to_csv(session_path, index=False)

        if session_name not in self.session_cache:
            self.session_cache.append(session_name)
            self.session_cache.sort()
            
        # 4. REFRESH UI & PING FLOATING WINDOWS
        self.main_model.dataChanged.emit(
            self.main_model.index(0, sub_idx), 
            self.main_model.index(self.main_model.rowCount()-1, sub_idx)
        )
        self.start_autosave_fuse()
        self.statusBar().showMessage(f"Added {len(row_indices)} items to session: {session_name}", 3000)

        # Sync the specific floating review dialog if it's open
        if session_path in self.active_dialogs:
            self.active_dialogs[session_path].refresh_from_disk()

        # Sync the session manager list
        manager = self.active_dialogs.get("session_manager")
        if manager and manager.current_mode == "WIP":
            manager.refresh_list()

    def add_to_playlist(self, row_indices, playlist_name):
        if playlist_name == "NEW":
            from PySide6.QtWidgets import QInputDialog
            from datetime import datetime
            default_name = f"playlist_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            name, ok = QInputDialog.getText(self, "New Playlist", "Enter Playlist Name:", text=default_name)
            if not ok or not name: return
            playlist_name = name

        # 1. THE DATA GRAB
        cols = ['UUID', 'ABSPATH', 'LOCALPATH', 'FILENAME', 'FIRST', 'LAST', 'SHOTNAME', 'ALTSHOTNAME']
        valid_cols = [c for c in cols if c in self.df_master.columns]
        
        try:
            new_data = self.df_master.iloc[list(row_indices)][valid_cols].copy()
            new_data['SUBNOTES'] = ""
        except Exception as e:
            self.statusBar().showMessage(f"Extraction Error: {e}", 5000)
            return

        # 2. FILE HANDLING
        base_dir = self.engine.project_root
        playlist_dir = os.path.join(base_dir, ".autosaves", "playlists")
        if not os.path.exists(playlist_dir): os.makedirs(playlist_dir)
        
        playlist_path = os.path.normpath(os.path.join(playlist_dir, f"{playlist_name}.csv"))
        
        if os.path.exists(playlist_path):
            existing_df = pd.read_csv(playlist_path, encoding='cp1252')
            combined = pd.concat([existing_df, new_data]).drop_duplicates(subset=['UUID'], keep='last')
            combined.to_csv(playlist_path, index=False)
        else:
            new_data.to_csv(playlist_path, index=False)

        # 3. UPDATE CACHE & UI & PING FLOATING WINDOWS
        if playlist_name not in self.playlist_cache:
            self.playlist_cache.append(playlist_name)
            self.playlist_cache.sort()
            
        self.statusBar().showMessage(f"Added {len(row_indices)} items to playlist: {playlist_name}", 3000)

        # Sync the specific floating playlist editor if it's open
        if playlist_path in self.active_dialogs:
            self.active_dialogs[playlist_path].refresh_from_disk()

        # Sync the session manager list
        manager = self.active_dialogs.get("session_manager")
        if manager and manager.current_mode == "PLAYLIST":
            manager.refresh_list()

    def keyPressEvent(self, event):
        """Surgically intercepts Copy before the Editor can steal the focus."""
        
        # 1. Catch the Copy Command (Cmd+C / Ctrl+C)
        if event.matches(QKeySequence.Copy):
            # 2. FORCE-CLOSE any active editor in the table 
            # This prevents the "last cell is currently being edited" ghosting.
            if self.table.indexWidget(self.table.currentIndex()):
                self.table.commitData(self.table.indexWidget(self.table.currentIndex()))
            
            # 3. Perform the centralized copy
            ClipboardHelper.copy_table_selection(self.table)
            
            # 4. Optional: Feedback via status bar
            curr = self
            while curr:
                if hasattr(curr, 'statusBar') and curr.statusBar():
                    curr.statusBar().showMessage("Copied selection to clipboard", 2000)
                    break
                curr = curr.parent()
            
            # IMPORTANT: Accept the event so it doesn't trigger the default Qt edit behavior
            event.accept()
            return 

        # Let all other keys (Enter, Arrows, etc.) behave normally
        super().keyPressEvent(event)

    def copy_selection(self):
        ClipboardHelper.copy_table_selection(self.table)
        self.statusBar().showMessage("Copied selection to clipboard", 2000)

    def show_context_menu(self, pos):
        selection = self.table.selectionModel().selectedIndexes()
        if not selection: return

        try:
            rows = {self.proxy_model.mapToSource(idx).row() for idx in selection}
        except Exception: return

        main_menu = QMenu(self)

        # 1. FIXED: Removed the conflicting Ctrl+C shortcut from here
        main_menu.addAction("Open File Location").triggered.connect(self.open_file_location)
        
        # --- SURGICAL INJECTION: THE OS OPENER ---
        main_menu.addAction("Open File (OS Default)").triggered.connect(self.open_file_os_default)
        # --- NEW: THE WARP SPEED COPY ---
        main_menu.addSeparator()
        main_menu.addAction("🚀 Native Smart Copy to...").triggered.connect(self.start_warp_copy)

        main_menu.addSeparator()
        copy_action = QAction("Copy Selection", self)
        copy_action.setShortcut(QKeySequence.Copy) # This is the "Real" Ctrl+C
        copy_action.triggered.connect(self.copy_selection)
        main_menu.addAction(copy_action)

        main_menu.addSeparator()
        # Status Menu
        status_menu = main_menu.addMenu("Sub Status")
        for option in self.status_options:
            label = option if option != "" else "Clear Status"
            act = status_menu.addAction(label)
            act.triggered.connect(lambda chk=False, opt=option: self.apply_bulk_status(opt))

        main_menu.addSeparator()
        # Media Actions
        action_menu = main_menu.addMenu("Media Actions")
        action_menu.addAction("Play in RV").triggered.connect(self.launch_rv)
        action_menu.addAction("Play selection + Hero Scans in RV").triggered.connect(self.launch_rv_with_added_scans)
        action_menu.addAction("Open in Nuke").triggered.connect(self.launch_nuke)

        main_menu.addSeparator()
        # 2. Submission Session (Using Cache - No Disk Hit!)
        sub_menu = main_menu.addMenu("Add to Submission Session")
        # --- NEW: PLAYLIST MENU ---
        playlist_menu = main_menu.addMenu("Add to Playlist")
        playlist_menu.addAction("New Playlist...").triggered.connect(lambda: self.add_to_playlist(rows, "NEW"))
        playlist_menu.addSeparator()
        for name in self.playlist_cache:
            act = playlist_menu.addAction(f"Add to: {name}")
            act.triggered.connect(lambda checked=False, n=name: self.add_to_playlist(rows, n))
        # --------------------------

        main_menu.addSeparator()
        
        # --- 1. PRE-FLIGHT GUARD FOR NOTES ---
        notes_eng = NotesEngine(self.engine)
        can_note = True
        note_error_msg = "Add Note to Selection..."

        if not notes_eng.is_valid():
            can_note = False
            note_error_msg = "Cannot add notes (Config Missing/Invalid)"
        else:
            user = os.getlogin()
            for r in rows:
                notes_eng.asset_context = self.df_master.iloc[r].to_dict()
                t_path = notes_eng.construct_path(scope="default", task="general", user=user)

                # A. Check for garbage paths
                if not t_path or str(t_path).strip() == "" or str(t_path).lower() == "nan" or t_path == ".":
                    can_note = False
                    note_error_msg = "Cannot add notes (Path Unresolved)"
                    break

                # B. Check for physical accessibility (Walk up until we find an existing folder)
                check_dir = os.path.dirname(t_path)
                while check_dir and not os.path.exists(check_dir):
                    parent = os.path.dirname(check_dir)
                    if parent == check_dir: break # Hit the OS root
                    check_dir = parent

                # C. Check if that closest existing folder is writable
                if not check_dir or not os.path.exists(check_dir) or not os.access(check_dir, os.W_OK):
                    can_note = False
                    note_error_msg = "Cannot add notes (Path Inaccessible)"
                    break

        # 2. Render the Action (Active or Disabled)
        if can_note:
            main_menu.addAction("Add Note to Selection...").triggered.connect(self.action_add_note)
        else:
            disabled_note_act = main_menu.addAction(note_error_msg)
            disabled_note_act.setEnabled(False)

        main_menu.addAction("Manage Selection Notes...").triggered.connect(self.open_notes_manager)

        # Fast logic check
        all_rts = all(self.df_master.iat[r, self.df_master.columns.get_loc("SUBSTATUS")] == "RTS" for r in rows)
        
        if not all_rts:
            disabled_act = sub_menu.addAction("Selection must be status 'RTS' to add...")
            disabled_act.setEnabled(False)
        else:
            sub_menu.addAction("Create New Session...").triggered.connect(lambda: self.add_to_session(rows, "NEW"))
            sub_menu.addSeparator()
            
            # Use the MEMORY cache, not glob
            for name in self.session_cache:
                act = sub_menu.addAction(f"Add to: {name}")
                act.triggered.connect(lambda checked=False, n=name: self.add_to_session(rows, n))

        main_menu.exec(self.table.viewport().mapToGlobal(pos))

    def apply_bulk_status(self, status_string):
        selection = self.table.selectionModel().selectedIndexes()
        if not selection:
            return

        sub_idx = self.df_master.columns.get_loc("SUBSTATUS")
        
        # We use a set to ensure we only update each row once 
        # even if multiple cells in that row are selected
        rows_to_update = {self.proxy_model.mapToSource(idx).row() for idx in selection}

        for row in rows_to_update:
            self.df_master.iat[row, sub_idx] = status_string

        # Trigger the visual refresh
        self.main_model.dataChanged.emit(
            self.main_model.index(0, sub_idx), 
            self.main_model.index(self.main_model.rowCount() - 1, sub_idx)
        )
        
        # Lit the fuse manually for bulk updates
        self.start_autosave_fuse() 
        self.statusBar().showMessage(f"Bulk updated {len(rows_to_update)} rows", 3000)

    def start_autosave_fuse(self):
        if not self.autosave_fuse_lit:
            # self.autosave_fuse_lit = True; self.statusBar().showMessage("Saving..."); self.autosave_timer.start(500)
            self.autosave_fuse_lit = True; self.autosave_timer.start(500)

    def run_autosave(self):
        # Point directly to engine setting
        base_dir = self.engine.project_root
        
        cache_dir = os.path.join(base_dir, ".autosaves")
        if not os.path.exists(cache_dir): 
            try:
                os.makedirs(cache_dir)
            except Exception as e:
                self.statusBar().showMessage(f"Autosave Dir Error: {e}", 5000)
                return

        # Map current statuses to UUIDs
        wip_df = self.df_master[self.df_master['SUBSTATUS'] != ""]
        session_data = dict(zip(wip_df['UUID'], wip_df['SUBSTATUS']))
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save JSON to local cache folder
        save_file = os.path.join(cache_dir, f"session_{timestamp}.json")
        try:
            with open(save_file, 'w') as f: 
                json.dump(session_data, f)
        except Exception as e:
            self.statusBar().showMessage(f"Save Failed: {e}", 5000)
            return

        all_saves = sorted(glob.glob(os.path.join(cache_dir, "session_*.json")))
        if len(all_saves) > 20: [os.remove(f) for f in all_saves[:-20]]
        
        self.autosave_fuse_lit = False
        self.statusBar().showMessage(f"Submission Status saved", 2000)

    def apply_status_filter(self, t): self.proxy_model.set_status_filter(t); self.table.resizeColumnToContents(1)

    def toggle_all_exts(self, s): [cb.setChecked(s) for cb in self.ext_checkboxes.values()]

    def update_extension_filter(self):
        self.proxy_model.set_extension_filter({ext for ext, cb in self.ext_checkboxes.items() if cb.isChecked()})

    def filter_table(self, t):
        if t == "All": self.proxy_model.set_shot_filter("")
        elif t == "No Shot": self.proxy_model.set_shot_filter("", no_shot=True)
        else: self.proxy_model.set_shot_filter("|".join([re.escape(p.strip()) for p in t.split("|")]))

    def reload_all(self):
        """Audited: Uses CatalogProvider for data, but preserves all original processing logic."""
        raw_s = str(self.engine.settings.get('shots_csv', '')).strip()
        s_path = PathSwapper.translate(raw_s)

        # 1. Catalog Intake: Ask the Authority for the Data
        self.main_model.beginResetModel()
        
        # --- SURGICAL INJECTION: Ask the Dock what's active! ---
        active_ids = self.data_dock.get_active_root_ids() if hasattr(self, 'data_dock') else None
        df_provided = self.catalog_provider.get_raw_csv_df(active_roots=active_ids)
        
        # ADD 'ROOT_ID' TO THE END OF THIS LIST so it survives the cull!
        ui_order = ["LOCALPATH", "FILENAME", "FILETYPE", "FIRST", "LAST", "SUBSTATUS", "SUBSENT", 
                    "SEQUENCE", "SHOTNAME", "ALTSHOTNAME", "CREATION", "MODDATE", "ABSPATH", "HAS_SHOT", "FILE_ID", "UUID", "ROOT_ID"]

        if not df_provided.empty:
            try:
                # --- RESTORED ORIGINAL PROCESSING LOGIC ---
                # This ensures UUID, ABSPATH, and FILE_ID exist before refresh_logic runs
                df_provided['UUID'] = df_provided.apply(lambda x: generate_uuid(x['LOCALPATH'], x['FILENAME']), axis=1)
                df_provided['ABSPATH'] = df_provided['ABSPATH'].apply(lambda x: PathSwapper.translate(str(x)))
                df_provided['FILE_ID'] = df_provided['ABSPATH'].astype(str) + "/" + df_provided['FILENAME'].astype(str)
                
                # Ensure fabric columns exist
                for c in ["SUBSTATUS", "SUBSENT", "HAS_SHOT", "SHOTNAME", "ALTSHOTNAME"]: 
                    if c not in df_provided.columns: df_provided[c] = ""

                # Filter to the column set the UI expects
                existing_cols = [c for c in ui_order if c in df_provided.columns]
                self.df_master = df_provided[existing_cols]
                # -------------------------------------------
            except Exception as e:
                print(f"DEBUG: Data Processing Error: {e}")
                self.df_master = pd.DataFrame(columns=ui_order)
        else:
            self.df_master = pd.DataFrame(columns=ui_order)

        self.main_model._data = self.df_master
        self.main_model.endResetModel()

        # 2. Shots Loading (Unchanged)
        if s_path and s_path != "." and os.path.exists(s_path): 
            try:
                self.df_shots = pd.read_csv(s_path, encoding='cp1252', dtype=str).fillna("")
            except: 
                self.df_shots = pd.DataFrame(columns=['SEQUENCE', 'SHOTNAME', 'ALTSHOTNAME', 'HEROPLATE'])
        else:
            self.df_shots = pd.DataFrame(columns=['SEQUENCE', 'SHOTNAME', 'ALTSHOTNAME', 'HEROPLATE'])

        # 3. Session & Playlist Cache Update (Unchanged)
        m_dir = self.engine.project_root
        if m_dir and os.path.exists(m_dir):
            session_glob = os.path.join(m_dir, ".autosaves/sessions/*.csv")
            self.session_cache = [os.path.basename(f).replace(".csv", "") for f in glob.glob(session_glob)]
            
            playlist_glob = os.path.join(m_dir, ".autosaves/playlists/*.csv")
            self.playlist_cache = sorted([os.path.basename(f).replace(".csv", "") for f in glob.glob(playlist_glob)])
        
        # 4. Refresh Data & Logic
        self.refresh_logic()
        self.update_status_stats()

    def refresh_logic(self):
        base_path = self.engine.project_root
        
        # 1. Update Persistent Truth (Ledger)
        log_dir = os.path.join(base_path, "submission_logs")
        if os.path.exists(log_dir):
            log_files = glob.glob(os.path.join(log_dir, "send_*.csv"))
            if log_files:
                all_log_data = []
                for f in log_files:
                    try:
                        ldf = pd.read_csv(f, dtype=str, encoding='cp1252')
                        if 'LOCALPATH' in ldf.columns and 'FILENAME' in ldf.columns:
                            ldf['UUID'] = ldf.apply(lambda x: generate_uuid(x['LOCALPATH'], x['FILENAME']), axis=1)
                        if 'UUID' in ldf.columns and 'SUBSENT' in ldf.columns:
                            all_log_data.append(ldf[['UUID', 'SUBSENT']])
                    except Exception as e: print(f"Error loading log {f}: {e}")

                if all_log_data:
                    combined = pd.concat(all_log_data).drop_duplicates()
                    combined['UUID'] = combined['UUID'].astype(str).str.strip()
                    audit_map = combined.groupby('UUID')['SUBSENT'].apply(lambda x: ', '.join(x.unique()))
                    self.df_master['SUBSENT'] = self.df_master['UUID'].map(audit_map).fillna("")

        # 2. Update WIP (Autosave)
        cache_dir = os.path.join(base_path, ".autosaves")
        if os.path.exists(cache_dir):
            all_saves = sorted(glob.glob(os.path.join(cache_dir, "session_*.json")))
            if all_saves:
                try:
                    with open(all_saves[-1], 'r') as f:
                        sess_map = json.load(f)
                    sess_map = {str(k).strip(): v for k, v in sess_map.items()}
                    self.df_master['SUBSTATUS'] = self.df_master['UUID'].astype(str).str.strip().map(sess_map).fillna("")
                except Exception as e: print(f"Autosave Load Error: {e}")

        # --- THE SAFETY NET: Ensure the 'fabric' columns exist in df_master ---
        for col in ['SEQUENCE', 'SHOTNAME', 'ALTSHOTNAME', 'HAS_SHOT']:
            if col not in self.df_master.columns:
                self.df_master[col] = "" if col != 'HAS_SHOT' else False

        # --- PHASE 1: INITIALIZE FABRIC COLUMNS ---
        self.df_master['SEQUENCE'] = "" 
        self.df_master['SHOTNAME'] = ""
        self.df_master['ALTSHOTNAME'] = ""
        self.df_master['HAS_SHOT'] = False

        if self.df_master.empty or self.df_shots.empty:
            self.finalize_ui()
            return

        # Clean strings for the shots_df to prevent type-mismatch (int vs str)
        shots_clean = self.df_shots.copy()
        # SURGICAL FIX: Changed INPUTFILE/OUTPUTFILE to SHOTNAME/ALTSHOTNAME
        for col in ['SEQUENCE', 'SHOTNAME', 'ALTSHOTNAME']:
            if col in shots_clean.columns:
                shots_clean[col] = shots_clean[col].astype(str).replace('nan', '').str.strip()

        # --- PHASE 2: SEQUENCE LOCKDOWN (ESTABLISH THE WALLS) ---
        # Identify every unique sequence defined by the Coordinator
        defined_sequences = [s for s in shots_clean['SEQUENCE'].unique() if s]

        for seq in defined_sequences:
            # Match the sequence name as a path component
            seq_pattern = re.escape(seq)
            seq_mask = self.df_master['LOCALPATH'].str.contains(seq_pattern, case=False, na=False)
            
            if seq_mask.any():
                # Lock these rows to THIS sequence and no other
                self.df_master.loc[seq_mask, 'SEQUENCE'] = seq

        # --- PHASE 3: ISOLATED SHOT MAPPING (MATCH WITHIN THE WALLS) ---
        # We process the table sequence-by-sequence to ensure zero crosstalk
        for seq in defined_sequences:
            # 1. Isolate Master Rows belonging to this Sequence only
            master_seq_subset = (self.df_master['SEQUENCE'] == seq)
            if not master_seq_subset.any():
                continue

            # 2. Isolate the list of Shot Names defined ONLY for this Sequence
            shot_pool_for_seq = shots_clean[shots_clean['SEQUENCE'] == seq]

            for _, shot_row in shot_pool_for_seq.iterrows():
                # SURGICAL FIX: Updated to SHOTNAME and ALTSHOTNAME
                input_val = shot_row['SHOTNAME']
                output_val = shot_row['ALTSHOTNAME']
                if not input_val and not output_val:
                    continue
                
                # PRECISE MATCH LOGIC: Use original regex approach
                shot_patterns = [re.escape(p) for p in [input_val, output_val] if p]
                regex_pattern = "|".join(shot_patterns)

                # 3. Final Match: Within the subset, find the shot
                final_match_mask = master_seq_subset & \
                                   (self.df_master['HAS_SHOT'] == False) & \
                                   (self.df_master['LOCALPATH'].str.contains(regex_pattern, case=False, na=False))

                if final_match_mask.any():
                    self.df_master.loc[final_match_mask, 'SHOTNAME'] = input_val
                    self.df_master.loc[final_match_mask, 'ALTSHOTNAME'] = output_val
                    self.df_master.loc[final_match_mask, 'HAS_SHOT'] = True

        # --- PHASE 3.5: DUAL-NAME HEALING (The Hacky Back-fill) ---
        if str(self.engine.settings.get('dual_name', 'False')).lower() == 'true':
            captured_mapped = self.df_master[self.df_master['HAS_SHOT'] == True]
            
            if not captured_mapped.empty:
                prod_lookup = dict(zip(captured_mapped['SHOTNAME'], captured_mapped['SEQUENCE']))
                work_lookup = dict(zip(captured_mapped['ALTSHOTNAME'], captured_mapped['SEQUENCE']))
                shot_to_seq_map = {**prod_lookup, **work_lookup}
                
                for shot_name, confirmed_seq in shot_to_seq_map.items():
                    if not shot_name or not confirmed_seq: continue
                    
                    healing_pattern = re.escape(shot_name)
                    orphan_mask = (self.df_master['SEQUENCE'] == "") & \
                                  (self.df_master['LOCALPATH'].str.contains(healing_pattern, case=False, na=False))
                    
                    if orphan_mask.any():
                        self.df_master.loc[orphan_mask, 'SEQUENCE'] = confirmed_seq
                        
                        # SURGICAL FIX: Updated to match new column names in shots_clean
                        original_row = shots_clean[
                            (shots_clean['SHOTNAME'] == shot_name) | 
                            (shots_clean['ALTSHOTNAME'] == shot_name)
                        ].iloc[0]
                        
                        self.df_master.loc[orphan_mask, 'SHOTNAME'] = original_row['SHOTNAME']
                        self.df_master.loc[orphan_mask, 'ALTSHOTNAME'] = original_row['ALTSHOTNAME']
                        self.df_master.loc[orphan_mask, 'HAS_SHOT'] = True

        # --- PHASE 4: ORPHAN CLEANUP ---
        for _, shot_row in shots_clean.iterrows():
            input_val = shot_row['SHOTNAME']
            output_val = shot_row['ALTSHOTNAME']
            if not input_val and not output_val: 
                continue
            
            # SURGICAL FIX: Changed ( ) to (?: ) to silence the UserWarning
            # This is a non-capturing group. Logic stays the same, warning dies.
            shot_patterns = [re.escape(p) for p in [input_val, output_val] if p]
            regex_pattern = "^(?:" + "|".join(shot_patterns) + ")"
            
            orphan_mask = (self.df_master['HAS_SHOT'] == False) & \
                          (self.df_master['FILENAME'].str.contains(regex_pattern, case=False, na=False))
            
            if orphan_mask.any():
                # If this shot belongs to a sequence, pull it in (in case Phase 2 missed it)
                if shot_row['SEQUENCE']:
                    self.df_master.loc[orphan_mask, 'SEQUENCE'] = shot_row['SEQUENCE']
                    
                self.df_master.loc[orphan_mask, 'SHOTNAME'] = input_val
                self.df_master.loc[orphan_mask, 'ALTSHOTNAME'] = output_val
                self.df_master.loc[orphan_mask, 'HAS_SHOT'] = True

        # --- UI REFRESH (Surgical Polish) ---
        self.finalize_ui()

        # 1. Force find the SUBSTATUS index accurately
        sub_idx = self.get_column_index("SUBSTATUS")
        if sub_idx != -1:
            self.table.setItemDelegateForColumn(sub_idx, StatusDelegate(self.status_options, self.table))
            self.table.setEditTriggers(QTableView.AllEditTriggers)

    def update_shot_dropdown(self):
        """Rebuilds the Shot dropdown based on the active Sequence."""
        self.shot_selector.blockSignals(True)
        current_shot = self.shot_selector.currentText()
        
        self.shot_selector.clear()
        self.shot_selector.addItems(["All", "No Shot"])
        
        seq = self.seq_selector.currentText()
        
        if not self.df_shots.empty:
            # Filter the shot DataFrame to match the chosen sequence
            if seq == "All" or not seq:
                display_df = self.df_shots
            else:
                # We use .fillna("") to safely handle empty sequence fields in the CSV
                display_df = self.df_shots[self.df_shots['SEQUENCE'].fillna("") == seq]
                
            for _, r in display_df.iterrows():
                if pd.notna(r['SHOTNAME']) and pd.notna(r['ALTSHOTNAME']): 
                    self.shot_selector.addItem(f"{r['SHOTNAME']} | {r['ALTSHOTNAME']}")
                    
        # Attempt to restore the previous shot selection if it survived the filter
        idx = self.shot_selector.findText(current_shot)
        self.shot_selector.setCurrentIndex(idx if idx >= 0 else 0)
        self.shot_selector.blockSignals(False)
        
        # Re-apply the shot filter to the table view
        self.filter_table(self.shot_selector.currentText())
 
    def validate_row_range(self, row_data):
        """
        Compares master row (FIRST/LAST) against shot metadata (FIRSTFRAME/LASTFRAME).
        Returns (is_valid, error_cols)
        """
        shot_name = str(row_data.get('SHOTNAME', '')).strip()
        if not shot_name or shot_name == "nan" or self.df_shots.empty:
            return True, []

        # Find the matching shot in metadata
        match = self.df_shots[
            (self.df_shots['SHOTNAME'] == shot_name) | 
            (self.df_shots['ALTSHOTNAME'] == shot_name)
        ]
        
        if match.empty:
            return True, []

        target = match.iloc[0]
        error_cols = []
        
        # Compare FIRST to FIRSTFRAME
        if str(row_data.get('FIRST', '')) != str(target.get('FIRSTFRAME', '')):
            error_cols.append('FIRST')
            
        # Compare LAST to LASTFRAME
        if str(row_data.get('LAST', '')) != str(target.get('LASTFRAME', '')):
            error_cols.append('LAST')

        return len(error_cols) == 0, error_cols
    
    def finalize_ui(self):
        """Restored to original logic: precise FILENAME sizing, no global autosize."""
        fabric_columns = [
            "LOCALPATH", "FILENAME", "FILETYPE", "FIRST", "LAST", 
            "SUBSTATUS", "SUBSENT", "SEQUENCE", "SHOTNAME", "ALTSHOTNAME", 
            "CREATION", "MODDATE", "ABSPATH", "HAS_SHOT", "FILE_ID", "UUID"
        ]
        
        self.main_model.layoutAboutToBeChanged.emit()
        existing = [c for c in fabric_columns if c in self.df_master.columns]
        self.df_master = self.df_master[existing]
        self.main_model._data = self.df_master 
        self.main_model.layoutChanged.emit()

        # Hide technical columns
        for col in ['HAS_SHOT', 'FILE_ID', 'UUID']: 
            if col in self.df_master.columns:
                self.table.setColumnHidden(self.df_master.columns.get_loc(col), True)

        # --- ORIGINAL FILENAME LOGIC ---
        if "FILENAME" in self.df_master.columns:
            fn_idx = self.df_master.columns.get_loc("FILENAME")
            header = self.table.horizontalHeader()
            
            # Auto-size just this one column
            self.table.resizeColumnToContents(fn_idx)
            
            # Immediately flip back to Interactive so user can resize
            header.setSectionResizeMode(fn_idx, QHeaderView.Interactive)
            
            # Enforce the original 250px minimum
            if self.table.columnWidth(fn_idx) < 250:
                self.table.setColumnWidth(fn_idx, 250)

        # Re-apply the dropdown delegate
        sub_idx = self.get_column_index("SUBSTATUS")
        if sub_idx != -1:
            self.table.setItemDelegateForColumn(sub_idx, StatusDelegate(self.status_options, self.table))
            self.table.setEditTriggers(QTableView.AllEditTriggers)

        self.refresh_ui_elements()

    def get_column_index(self, name):
        """Helper to find index by name in df_master."""
        try: return self.df_master.columns.get_loc(name)
        except: return -1

    def apply_sequence_filter(self, t):
        self.proxy_model.set_sequence_filter(t)
        # When sequence changes, cascade the update down to the shot dropdown
        self.update_shot_dropdown()

    def refresh_ui_elements(self):
        while self.ext_container.count():
            w = self.ext_container.takeAt(0).widget()
            if w: w.deleteLater()
        self.ext_checkboxes = {}
        for ext in sorted(self.df_master['FILETYPE'].unique().tolist()):
            cb = QCheckBox(str(ext)); cb.setChecked(True); cb.stateChanged.connect(self.update_extension_filter)
            self.ext_container.addWidget(cb); self.ext_checkboxes[ext] = cb
        
        # Populate Sequence Dropdown
        self.seq_selector.blockSignals(True)
        self.seq_selector.clear(); self.seq_selector.addItem("All")
        if 'SEQUENCE' in self.df_master.columns:
            seqs = sorted([s for s in self.df_master['SEQUENCE'].unique() if s])
            self.seq_selector.addItems(seqs)
        self.seq_selector.blockSignals(False)

        # Let the dynamic helper populate the shots and trigger the table filter
        self.update_shot_dropdown()
        self.update_extension_filter()

    def apply_date_preset(self, t):
        today = QDate.currentDate(); self.date_to.setDate(today)
        if t == "All Time": self.date_from.setDate(QDate(2000, 1, 1))
        elif t == "Today": self.date_from.setDate(today)
        elif t == "Yesterday": self.date_from.setDate(today.addDays(-1)); self.date_to.setDate(today.addDays(-1))
        elif t == "Last 2 Days": self.date_from.setDate(today.addDays(-1)); self.date_to.setDate(today)
        elif t == "Last 3 Days": self.date_from.setDate(today.addDays(-2)); self.date_to.setDate(today)
        elif t == "Last Week": self.date_from.setDate(today.addDays(-7))
        elif t == "Last Month": self.date_from.setDate(today.addDays(-30))
        self.update_date_filter()

    def update_date_filter(self):
        q_s, q_e = self.date_from.date(), self.date_to.date()
        self.proxy_model.set_date_range(datetime(q_s.year(), q_s.month(), q_s.day()), datetime(q_e.year(), q_e.month(), q_e.day(), 23, 59))

    def send_ready_assets(self):
        rts_mask = self.df_master['SUBSTATUS'] == "RTS"
        if not rts_mask.any(): return
        
        session_cols = ['UUID', 'ABSPATH', 'LOCALPATH', 'SHOTNAME', 'ALTSHOTNAME', 'FILENAME', 'FIRST', 'LAST']
        df_review = self.df_master.loc[rts_mask, session_cols].copy()
        
        dlg = SubmissionReviewDialog(df_review, self, engine=self.engine)
        result = dlg.exec()
        
        if result == 0: 
            self.statusBar().showMessage("Submission Cancelled", 2000)
            return
        
        final_submission_df = dlg.get_data()
        # Use the path the dialog was already using!
        actual_session_path = dlg.get_target_path()

        if result == 2: # Save for Later
            remaining_uuids = set(final_submission_df['UUID'])
            for uuid in self.df_master.loc[rts_mask, 'UUID']:
                row_idx = self.df_master.index[self.df_master['UUID'] == uuid][0]
                self.df_master.at[row_idx, 'SUBSTATUS'] = "Pending" if uuid in remaining_uuids else ""
            
            self.run_autosave()
            
            # SURGICAL CHECK: Only write if the file doesn't already exist 
            # (prevents the double-timestamp duplicate)
            if not os.path.exists(actual_session_path):
                session_dir = os.path.dirname(actual_session_path)
                if not os.path.exists(session_dir): os.makedirs(session_dir)
                final_submission_df.to_csv(actual_session_path, index=False, encoding='cp1252')
            
            self.main_model.dataChanged.emit(self.main_model.index(0,0), self.main_model.index(self.main_model.rowCount()-1, 0))
            self.statusBar().showMessage(f"Session saved: {os.path.basename(actual_session_path)}", 3000)

        # HANDLE GOOD TO GO (Result 1)
        elif result == 1:
            # Use the path from the dialog!
            self.process_final_submission(final_submission_df, session_path=dlg.get_target_path())

    def show_error(self, title, message):
        """Unified popup for when things go sideways."""
        from PySide6.QtWidgets import QMessageBox
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Critical)
        msg.setWindowTitle(title)
        msg.setText(message)
        msg.exec()

    def check_initial_state(self):
        """Fires on launch to intelligently onboard the user if the project is empty."""
        if self.df_master.empty:
            from PySide6.QtWidgets import QMessageBox
            
            # 1. The Onboarding Welcome Message
            msg = QMessageBox(self)
            msg.setIcon(QMessageBox.Information)
            msg.setWindowTitle("Welcome to Your Project")
            msg.setText("No catalog data was found for this project.")
            msg.setInformativeText("To get started, you need to define at least one <b>Data Root</b> (a directory to scan for your project files).\n\nWould you like to set this up now?")
            
            btn_setup = msg.addButton("Set Data Roots", QMessageBox.AcceptRole)
            msg.addButton("Later", QMessageBox.RejectRole)
            
            msg.exec()
            
            # 2. Launch the Data Roots Editor
            if msg.clickedButton() == btn_setup:
                # Grab whatever raw data root string exists (even if blank)
                current_data = str(self.engine.settings.get('data_root_raw', self.engine.settings.get('data_root', '')))
                
                dlg = DataRootsEditorDialog(current_data, self)
                if dlg.exec():
                    # 3. Save the new roots to the engine and to the physical CSV
                    new_json = dlg.get_serialized_data()
                    self.engine.settings['data_root'] = new_json
                    self.save_engine_settings()
                    self.live_refresh_config()
                    
                    # 4. Prompt for immediate scrape
                    scrape_msg = QMessageBox.question(
                        self, 
                        "Run Scraper?", 
                        "Data Roots saved successfully!\n\nWould you like to scan this directory now to build your project catalog?",
                        QMessageBox.Yes | QMessageBox.No
                    )
                    
                    if scrape_msg == QMessageBox.Yes:
                        self.trigger_quick_scrape()

    def confirm_missing_files(self, paths, app_name):
        """Surgically checks for missing files and returns True/False based on user consent."""
        missing = []
        for p in paths:
            if '%' in p:
                # It's an image sequence (e.g., %04d). We verify the parent folder exists.
                if not os.path.exists(os.path.dirname(p)):
                    missing.append(p)
            else:
                # It's a single file (e.g., .nk or .mov). We check the exact file path.
                if not os.path.exists(p):
                    missing.append(p)

        if not missing: return True

        from PySide6.QtWidgets import QMessageBox
        msg = f"The following {len(missing)} file(s) or folder(s) are not accessible on disk:\n\n"
        msg += "\n".join(missing[:5]) + ("\n..." if len(missing) > 5 else "")
        msg += f"\n\nWould you like to try opening {app_name} anyway?"
        
        return QMessageBox.question(self, "Missing Files", msg, QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes
    
    def launch_rv(self):
        # Whitelist for RV (STAYS THE SAME)
        rv_exts = {'.rv', '.exr', '.tif', '.tiff', '.jpg', '.jpeg', '.tga', '.png', '.mov', '.mp4'}
        paths = self.get_selected_paths()
        
        # Filter for compatibility (STAYS THE SAME)
        valid_paths = [p for p in paths if os.path.splitext(p)[1].lower() in rv_exts]
        
        if not valid_paths:
            self.show_error("RV Compatibility Error", 
                           "None of the selected files are compatible with RV.\n"
                           "Allowed: .rv, .exr, .tif, .tiff, .jpg, .jpeg, .tga, .png, .mov, .mp4")
            return

        # Check existence (STAYS THE SAME)
        clean_path = valid_paths[0].split('%')[0].rstrip('.')
        if not os.path.exists(os.path.dirname(clean_path)):
            self.show_error("File Not Found", f"The directory or file is missing:\n{valid_paths[0]}")
            return

        # --- SURGICAL INJECTION: THE CONFIRMATION GUARD ---
        if self.confirm_missing_files(valid_paths, "RV"):
            self.trigger_app(valid_paths, "RV")

    def launch_nuke(self):
        # Whitelist for Nuke (STAYS THE SAME)
        paths = [p for p in self.get_selected_paths() if p.lower().endswith(".nk")]
        
        if not paths:
            self.show_error("Nuke Compatibility Error", 
                           "No Nuke scripts (.nk) selected.\n"
                           "Please select a Nuke script to open.")
            return

        # Your original single-file check (STAYS THE SAME)
        if not os.path.exists(paths[0]):
            self.show_error("File Not Found", f"The Nuke script is missing:\n{paths[0]}")
            return

        # NEW: Accessibility guard for the final trigger
        if self.confirm_missing_files(paths, "Nuke"):
            self.trigger_app(paths, "Nuke")

    def open_file_location(self):
        selection = self.table.selectionModel().selectedIndexes()
        if not selection: return
        row = self.proxy_model.mapToSource(selection[0]).row()
        
        folder = str(self.df_master.iat[row, self.df_master.columns.get_loc("ABSPATH")])
        filename = str(self.df_master.iat[row, self.df_master.columns.get_loc("FILENAME")])
        
        # Final safety check before calling the OS trigger
        target = os.path.join(folder, filename).split('%')[0].rstrip('.')
        if not os.path.exists(os.path.dirname(target)):
             self.show_error("Path Not Found", f"The directory is no longer accessible:\n{folder}")
             return
             
        self.trigger_os_reveal(folder, filename)

    def open_file_os_default(self):
        """Opens the selected files in the host OS's default application."""
        import sys, subprocess, os
        
        paths = self.get_selected_paths()
        if not paths: return
        
        for p in paths:
            # The OS doesn't know how to "double click" a sequence pattern
            if '%' in p:
                self.statusBar().showMessage("Cannot open sequence patterns directly. Use 'Open File Location' instead.", 4000)
                continue
                
            if not os.path.exists(p):
                self.statusBar().showMessage(f"File not found: {p}", 3000)
                continue
                
            try:
                if sys.platform == "win32":
                    os.startfile(p) # Windows native 'double-click'
                elif sys.platform == "darwin":
                    subprocess.run(['open', p]) # Mac native
                else:
                    subprocess.run(['xdg-open', p]) # Linux native
            except Exception as e:
                self.statusBar().showMessage(f"Failed to open file: {e}", 4000)

    def get_selected_paths(self):
        """Surgical helper to grab absolute paths from selection."""
        selection = self.table.selectionModel().selectedIndexes()
        if not selection: return []
        rows = sorted({self.proxy_model.mapToSource(idx).row() for idx in selection})
        return [os.path.join(
            str(self.df_master.iat[r, self.df_master.columns.get_loc("ABSPATH")]),
            str(self.df_master.iat[r, self.df_master.columns.get_loc("FILENAME")])
        ) for r in rows]

    def trigger_app(self, paths, app_name="RV"):
        """Launches apps using the new nested OS-specific folder logic."""
        # The engine already filtered the correct OS CSV during discovery
        cfg = self.engine.apps.get(app_name, {})
        if not cfg:
            self.show_error("Config Missing", f"No config found for {app_name} on this OS.")
            return

        # 1. Resolve Binary (Takes the first 'bin' entry)
        raw_bin = cfg.get('bin', [""])[0]
        bin_path = PathSwapper.translate(raw_bin)
        
        if not bin_path or not os.path.exists(bin_path):
            self.show_error("Binary Not Found", f"Check {app_name} config:\n{bin_path}")
            return

        # 2. Build Environment (The 'Stacked' Logic)
        env = os.environ.copy()
        env_entries = cfg.get('env', [])
        for entry in env_entries:
            if "=" in entry:
                k, v = entry.split("=", 1)
                # Diligently PathSwap the value in case it's a path
                env[k.strip()] = PathSwapper.translate(v.strip())

        # 3. Build Flags (Stacked or single row)
        flags = []
        for f_row in cfg.get('flags', []):
            flags.extend([f.strip() for f in f_row.replace(',', ' ').split() if f.strip()])
        
        # 4. Final Command
        full_command = [bin_path] + flags + paths

        try:
            creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
            subprocess.Popen(
                full_command, 
                env=env, 
                creationflags=creation_flags,
                stdout=subprocess.DEVNULL, 
                stderr=subprocess.DEVNULL
            )
            self.statusBar().showMessage(f"Launched {app_name}", 3000)
        except Exception as e:
            self.show_error("Launch Failed", str(e))

    def trigger_os_reveal(self, folder, filename):
        """Unified OS-specific folder/file reveal logic."""
        # Join and normalize to fix slashes for the current OS
        if "%" in filename:
            target_path = os.path.normpath(folder)
        else:
            target_path = os.path.normpath(os.path.join(folder, filename))

        try:
            if sys.platform == "win32":
                # SURGICAL WINDOWS FIX: Explorer REQUIRE backslashes
                win_path = os.path.normpath(target_path).replace("/", "\\")
                # Note the comma after /select is critical on Windows
                subprocess.run(['explorer', '/select,', win_path])
            elif sys.platform == "darwin":
                subprocess.run(['open', '-R', target_path])
            else:
                # Linux fallback (xdg-open usually needs the directory)
                subprocess.run(['xdg-open', os.path.dirname(target_path)])
        except Exception as e:
            self.statusBar().showMessage(f"OS Reveal Error: {e}", 5000)

    def resolve_scan_path(self, shot_name, target_ext="exr"):
        """
        DIR / NAME . % [0] PADDING d . EXT
        Surgically handles single-digit padding and kills NaN ghosts.
        """
        if self.df_shots.empty: return None

        # 1. Resolve Shot Data
        shot_info = self.df_shots[(self.df_shots['SHOTNAME'] == shot_name) | 
                                  (self.df_shots['ALTSHOTNAME'] == shot_name)]
        if shot_info.empty: return None
        shot_dict = shot_info.iloc[0].to_dict()

        # 2. Grab Templates
        dir_tpl = self.engine.naming_templates.get('scan_directory_template', '')
        fn_tpl = self.engine.naming_templates.get('scan_name_template', '')
        if not dir_tpl or not fn_tpl: return None

        # 3. THE PADDING LOGIC (Delegated to Builder)
        raw_pad = self.engine.settings.get('padding_scans', '')
        padding_nomenclature = PaddingNomBuilder.build(raw_pad, style="printf")

        # 4. Strict Resolution
        ctx = {**self.engine.settings, **shot_dict}
        def resolve_strict(template, context):
            res = template
            for k in sorted(context.keys(), key=len, reverse=True):
                placeholder = f"{{{k}}}"
                if placeholder in res:
                    res = res.replace(placeholder, str(context[k]))
            return res

        resolved_dir = resolve_strict(dir_tpl, ctx)
        resolved_fn = resolve_strict(fn_tpl, ctx)

        # 5. Final Construction
        full_path = f"{resolved_dir}/{resolved_fn}.{padding_nomenclature}.{target_ext}"
        
        return PathSwapper.translate(full_path)
    
    def launch_rv_with_added_scans(self):
        """Launches selection + the Hero Scans defined in the Project Config."""
        selection = self.table.selectionModel().selectedIndexes()
        if not selection: return

        # Get extension from the first item
        first_row = self.proxy_model.mapToSource(selection[0]).row()
        target_ext = str(self.df_master.iat[first_row, self.df_master.columns.get_loc("FILETYPE")])

        final_paths = []
        target_shots = set()

        # 1. Add explicitly selected items
        for idx in selection:
            src_row = self.proxy_model.mapToSource(idx).row()
            shot = str(self.df_master.iat[src_row, self.df_master.columns.get_loc("SHOTNAME")])
            path = os.path.join(str(self.df_master.iat[src_row, self.df_master.columns.get_loc("ABSPATH")]),
                                str(self.df_master.iat[src_row, self.df_master.columns.get_loc("FILENAME")]))
            final_paths.append(path)
            if shot and shot != "nan" and shot != "":
                target_shots.add(shot)

        # 2. Append the calculated Hero Scan paths
        for shot in target_shots:
            scan_path = self.resolve_scan_path(shot, target_ext=target_ext)
            if scan_path:
                final_paths.append(scan_path)

        # --- THE SAFETY GUARD FIX ---
        if final_paths:
            # Check if files exist; if not, ask user before triggering
            if self.confirm_missing_files(final_paths, "RV"):
                self.trigger_app(final_paths, "RV")
                self.statusBar().showMessage(f"Sent {len(final_paths)} items to RV.")

    def closeEvent(self, event):
        """Disarm ghost timers, kill persistent child windows, and scavenge temp data."""
        if self.autosave_timer.isActive():
            self.autosave_timer.stop()
            self.run_autosave()

        # Kill the Session Manager if it's still floating
        if hasattr(self, 'session_manager_win') and self.session_manager_win:
            self.session_manager_win.close()

        # --- SURGICAL INJECTION: THE TEMP ROOT SCAVENGER ---
        # --- THE NEW ORPHAN SCAVENGER ---
        catalog_dir = os.path.join(self.engine.project_root, "catalogs")
        if os.path.exists(catalog_dir):
            # Parse the permanent names directly
            raw_dr = str(self.engine.settings.get('data_root_raw', ''))
            permanent_names = []
            try:
                import json
                parsed = json.loads(raw_dr)
                if isinstance(parsed, list):
                    permanent_names = [str(item[0]) for item in parsed if isinstance(item, list) and len(item) > 0]
            except:
                if str(self.engine.settings.get('data_root', '')):
                    permanent_names = ["default"]

            for filename in os.listdir(catalog_dir):
                if filename.endswith(".csv"):
                    if filename[:-4] not in permanent_names:
                        try:
                            os.remove(os.path.join(catalog_dir, filename))
                        except: pass

        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    # This ensures the app doesn't quit just because one window closes
    app.setQuitOnLastWindowClosed(True) 
    
    launcher = ProjectLauncherDialog()
    launcher.show()
    
    sys.exit(app.exec())