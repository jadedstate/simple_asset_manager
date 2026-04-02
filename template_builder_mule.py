import sys, re
from PySide6.QtWidgets import (QApplication, QDialog, QVBoxLayout, QHBoxLayout, 
                               QLabel, QLineEdit, QPushButton, QTreeWidget, QTreeWidgetItem, 
                               QFrame, QGridLayout, QAbstractItemView, QScrollArea, QWidget)
from PySide6.QtCore import Qt, Signal

# --- THE LEGO BLOCKS ---
class TokenBlock(QFrame):
    block_changed = Signal()
    block_removed = Signal(object)
    block_moved = Signal(object, int)

    def __init__(self, block_type, value, parent=None):
        super().__init__(parent)
        self.block_type = block_type # 'var', 'const', or 'user'
        self.value = value
        
        self.setFrameShape(QFrame.StyledPanel)
        
        # --- THE STYLING TRIAD ---
        if self.block_type == 'var':
            # BLUE: System Presets (Immutable)
            self.setStyleSheet("TokenBlock { background-color: #2e5a88; border: 1px solid #3b749e; border-radius: 4px; }")
        elif self.block_type == 'const':
            # GREY: Predefined Glue (Immutable value)
            self.setStyleSheet("TokenBlock { background-color: #444; border: 1px solid #555; border-radius: 4px; }")
        else:
            # GOLD/ORANGE: User Input (Editable with Sanitization)
            self.setStyleSheet("TokenBlock { background-color: #d19a66; border: 1px solid #b3824d; border-radius: 4px; }")
            
        lay = QHBoxLayout(self)
        lay.setContentsMargins(2, 2, 2, 2)
        lay.setSpacing(3)

        # 1. Navigation
        btn_left = QPushButton("◀")
        btn_left.setFixedSize(16, 20)
        btn_left.setStyleSheet("background: transparent; color: #aaa; border: none;")
        btn_left.clicked.connect(lambda: self.block_moved.emit(self, -1))
        lay.addWidget(btn_left)

        # 2. THE PAYLOAD LOGIC
        if self.block_type in ('var', 'const'):
            lbl = QLabel(f"{self.value}")
            color = "white" if self.block_type == 'var' else "#bbb"
            lbl.setStyleSheet(f"color: {color}; font-weight: bold; border: none; padding: 0px 4px;")
            lay.addWidget(lbl)
        else:
            # USER TYPE: Editable but Sanitized
            self.edit = QLineEdit(self.value)
            self.edit.setStyleSheet("background-color: #222; color: #fff; border: none; padding: 2px; font-weight: bold;")
            self.edit.setFixedWidth(max(30, len(self.value) * 10 + 10))
            self.edit.textChanged.connect(self._on_user_text_changed)
            lay.addWidget(self.edit)

        btn_right = QPushButton("▶")
        btn_right.setFixedSize(16, 20)
        btn_right.setStyleSheet("background: transparent; color: #aaa; border: none;")
        btn_right.clicked.connect(lambda: self.block_moved.emit(self, 1))
        lay.addWidget(btn_right)
        
        # 3. Delete
        btn_del = QPushButton("✖")
        btn_del.setFixedSize(16, 20)
        btn_del.setStyleSheet("QPushButton { background: transparent; color: #ff6666; border: none; } QPushButton:hover { color: red; }")
        btn_del.clicked.connect(lambda: self.block_removed.emit(self))
        lay.addWidget(btn_del)

    def _on_user_text_changed(self, text):
        # SANITIZATION RULE: Strip illegal file-system characters and spaces
        clean_text = re.sub(r'[\\/*?:"<>| ]', '_', text) 
        if clean_text != text:
            self.edit.setText(clean_text)
            return
            
        self.value = clean_text
        self.edit.setFixedWidth(max(30, len(self.value) * 10 + 10))
        self.block_changed.emit()

# --- THE MAIN APP ---
class RigidTemplateBuilder(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("🧱 Rigid Schema Builder (Lego Mode)")
        self.resize(1000, 700)
        self.setStyleSheet("background-color: #2b2b2b; color: #abb2bf; font-size: 13px;")

        self.mock_context = {
            "Project": "Rage", "Sequence": "seq_124", "Shot": "shot_0253",
            "Task": "comp", "Version": "v001", "Ext": "exr",
        }

        layout = QVBoxLayout(self)

        # --- 1. THE "FINDER" TREE ---
        tree_lay = QHBoxLayout()
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Template (The Schema)", "Resolved (The Output)"])
        self.tree.setColumnWidth(0, 450)
        self.tree.setSelectionMode(QAbstractItemView.SingleSelection)
        self.tree.setStyleSheet("""
            QTreeWidget { background-color: #1e1e1e; border: 1px solid #444; border-radius: 4px; padding: 5px; }
            QTreeWidget::item { padding: 5px; }
            QTreeWidget::item:selected { background-color: #3b749e; color: white; }
        """)
        
        self.tree.setDragEnabled(True)
        self.tree.setAcceptDrops(True)
        self.tree.setDropIndicatorShown(True)
        self.tree.setDragDropMode(QAbstractItemView.InternalMove)
        tree_lay.addWidget(self.tree)

        btn_v_lay = QVBoxLayout()
        btn_add_dir = QPushButton("📁 Add Folder")
        btn_add_file = QPushButton("📄 Add File")
        btn_remove = QPushButton("❌ Remove Node")
        for btn in (btn_add_dir, btn_add_file, btn_remove):
            btn.setStyleSheet("background-color: #444; padding: 8px; border-radius: 3px;")
            btn_v_lay.addWidget(btn)
        
        btn_add_dir.clicked.connect(lambda: self.add_node(True))
        btn_add_file.clicked.connect(lambda: self.add_node(False))
        btn_remove.clicked.connect(self.remove_node)
        btn_v_lay.addStretch()
        tree_lay.addLayout(btn_v_lay)
        layout.addLayout(tree_lay, stretch=3)

        # --- 2. THE LEGO BUILDER (Node Inspector) ---
        self.inspector_frame = QFrame()
        self.inspector_frame.setStyleSheet("background-color: #333; border-radius: 4px;")
        insp_lay = QVBoxLayout(self.inspector_frame)
        
        self.lbl_insp_title = QLabel("<b>Select a node to build its name...</b>")
        insp_lay.addWidget(self.lbl_insp_title)

        scroll = QScrollArea()
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("QScrollArea { border: 2px dashed #555; background-color: #1e1e1e; border-radius: 4px; min-height: 60px; }")
        
        self.block_container = QWidget()
        self.block_lay = QHBoxLayout(self.block_container)
        self.block_lay.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.block_lay.setContentsMargins(5, 5, 5, 5)
        scroll.setWidget(self.block_container)
        insp_lay.addWidget(scroll)

        # --- 3. THE TD TOOLBOX (Palette) ---
        palette_lay = QVBoxLayout()
        
        # Row 1: Constants & Custom
        const_row = QHBoxLayout()
        const_row.addWidget(QLabel("<b>Constants / Glue:</b>"))
        for p in ["_", "-", ".", "v", "img"]:
            btn = QPushButton(p)
            btn.setFixedWidth(30)
            btn.setStyleSheet("background-color: #444; color: #bbb; font-weight: bold;")
            btn.clicked.connect(lambda checked=False, val=p: self.append_block('const', val))
            const_row.addWidget(btn)
        
        const_row.addSpacing(20)
        
        btn_user = QPushButton("➕ Add User String Block")
        btn_user.setStyleSheet("background-color: #d19a66; color: black; font-weight: bold; padding: 4px 10px; border-radius: 3px;")
        btn_user.clicked.connect(lambda: self.append_block('user', 'custom_text'))
        const_row.addWidget(btn_user)
        const_row.addStretch()
        palette_lay.addLayout(const_row)

        # Row 2: Variables
        palette_lay.addWidget(QLabel("<b>System Variables:</b>"))
        var_grid = QGridLayout()
        col = 0
        row = 0
        for token in self.mock_context.keys():
            btn = QPushButton(f"[{token}]")
            btn.setStyleSheet("QPushButton { background-color: #2e5a88; color: white; border-radius: 3px; padding: 5px; } QPushButton:hover { background-color: #3b76b3; }")
            btn.clicked.connect(lambda checked=False, t=token: self.append_block('var', t))
            var_grid.addWidget(btn, row, col)
            col += 1
            if col > 5: col = 0; row += 1
        palette_lay.addLayout(var_grid)
        
        insp_lay.addLayout(palette_lay)
        layout.addWidget(self.inspector_frame, stretch=2)

        self.tree.itemSelectionChanged.connect(self.on_selection_changed)
        root = self.create_item([{'type': 'var', 'value': 'Project'}, {'type': 'const', 'value': '_delivery'}], True)
        self.tree.addTopLevelItem(root)
        root.setExpanded(True)
        self.tree.setCurrentItem(root)

    def create_item(self, token_list, is_folder=True):
        item = QTreeWidgetItem()
        item.setData(0, Qt.UserRole, is_folder)
        item.setData(0, Qt.UserRole + 1, token_list)
        flags = Qt.ItemIsSelectable | Qt.ItemIsEnabled | Qt.ItemIsDragEnabled
        if is_folder: flags |= Qt.ItemIsDropEnabled
        item.setFlags(flags)
        self.refresh_item_display(item)
        return item

    def refresh_item_display(self, item):
        token_list = item.data(0, Qt.UserRole + 1)
        is_folder = item.data(0, Qt.UserRole)
        schema_str = "".join([f"{{{b['value']}}}" if b['type'] == 'var' else b['value'] for b in token_list])
        resolved_str = "".join([self.mock_context.get(b['value'], f"<{b['value']}>") if b['type'] == 'var' else b['value'] for b in token_list])
        icon = "📁 " if is_folder else "📄 "
        item.setText(0, icon + schema_str)
        item.setText(1, resolved_str)

    def on_selection_changed(self):
        while self.block_lay.count():
            child = self.block_lay.takeAt(0)
            if child.widget(): child.widget().deleteLater()
        selected = self.tree.selectedItems()
        if not selected:
            self.lbl_insp_title.setText("<b>Select a node to build its name...</b>")
            return
        item = selected[0]
        self.lbl_insp_title.setText(f"<b>Building {'Folder' if item.data(0, Qt.UserRole) else 'File'}:</b>")
        for block_data in item.data(0, Qt.UserRole + 1):
            self._add_block_widget(block_data)

    def _add_block_widget(self, block_data):
        block_ui = TokenBlock(block_data['type'], block_data['value'])
        block_ui.block_changed.connect(self.sync_ui_to_data)
        block_ui.block_removed.connect(self.remove_block_widget)
        block_ui.block_moved.connect(self.move_block_widget)
        self.block_lay.addWidget(block_ui)

    def append_block(self, b_type, value):
        if not self.tree.selectedItems(): return
        self._add_block_widget({'type': b_type, 'value': value})
        self.sync_ui_to_data()

    def remove_block_widget(self, block_widget):
        self.block_lay.removeWidget(block_widget)
        block_widget.deleteLater()
        self.sync_ui_to_data()

    def move_block_widget(self, block_widget, direction):
        current_idx = self.block_lay.indexOf(block_widget)
        new_idx = current_idx + direction
        if 0 <= new_idx < self.block_lay.count():
            self.block_lay.removeWidget(block_widget)
            self.block_lay.insertWidget(new_idx, block_widget)
            self.sync_ui_to_data()

    def sync_ui_to_data(self):
        selected = self.tree.selectedItems()
        if not selected: return
        item = selected[0]
        new_token_list = []
        for i in range(self.block_lay.count()):
            widget = self.block_lay.itemAt(i).widget()
            if isinstance(widget, TokenBlock):
                new_token_list.append({'type': widget.block_type, 'value': widget.value})
        item.setData(0, Qt.UserRole + 1, new_token_list)
        self.refresh_item_display(item)

    def add_node(self, is_folder):
        selected = self.tree.selectedItems()
        parent = selected[0] if selected else self.tree.invisibleRootItem()
        if parent != self.tree.invisibleRootItem() and not parent.data(0, Qt.UserRole):
            parent = parent.parent() or self.tree.invisibleRootItem()
        new_item = self.create_item([{'type': 'user', 'value': 'new_node'}], is_folder)
        parent.addChild(new_item)
        parent.setExpanded(True)
        self.tree.setCurrentItem(new_item)

    def remove_node(self):
        selected = self.tree.selectedItems()
        if selected: (selected[0].parent() or self.tree.invisibleRootItem()).removeChild(selected[0])

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = RigidTemplateBuilder()
    window.show()
    sys.exit(app.exec())