import os
import sys
import subprocess
import shutil
import time

def ensure_dependencies():
    """The 'Self-Repair' feature: Ensures PyInstaller and core libs are present."""
    required = ["pyinstaller", "pandas", "PySide6"]
    print("--- Checking Environment Dependencies ---")
    for lib in required:
        try:
            # We check if the module can be loaded by the current python
            if lib == "pyinstaller":
                import PyInstaller
            else:
                __import__(lib)
        except ImportError:
            print(f"--- {lib} not found. Installing into {sys.executable}... ---")
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib])
            print(f"--- {lib} installed successfully. ---")

def build():
    # 0. The 'Forgotten' Step: Self-Repair
    ensure_dependencies()

    # 1. Setup Paths (Normalized for Win/Mac/Linux)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    source_script = os.path.normpath(os.path.join(base_dir, "table_view_simple_projects.py"))
    app_name = "PipeManager"
    
    dist_path = os.path.normpath(os.path.join(base_dir, "dist"))
    build_path = os.path.normpath(os.path.join(base_dir, "build"))

    print(f"\n--- Starting UNIVERSAL Build for {app_name} ---")
    print(f"Platform: {sys.platform} | Python: {sys.version.split()[0]}")

    # 2. Exclusions (Keeping the 'Total Control' over bundle size)
    excludes = [
        "PyQt5", "PyQt6", "tkinter", 
        "PySide6.QtWebEngineCore", 
        "PySide6.QtWebEngineWidgets",
        "PySide6.QtDesigner"
    ]

    # 3. Construct Command
    # Using 'sys.executable -m' ensures we use the same environment that just passed the check
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--noconsole",
        "--onedir",        # Instant launch (Standard for VFX/Pipeline apps)
        "--clean",
        f"--name={app_name}",
        f"--distpath={dist_path}",
        f"--workpath={build_path}",
    ]

    for mod in excludes:
        cmd.extend(["--exclude-module", mod])

    # Core data collection
    cmd.extend(["--collect-all", "pandas"])
    
    # OS-Specific Collection Logic
    if sys.platform == "win32":
        # Windows can be picky about PySide6 recursion; hidden-imports are safer
        cmd.extend(["--hidden-import", "PySide6.QtCore", "--hidden-import", "PySide6.QtWidgets"])
    else:
        # Mac/Linux handle the full collection better
        cmd.extend(["--collect-all", "PySide6"])

    cmd.append(source_script)

    # 4. Run Build
    try:
        # Use shell=True on Windows to resolve environment variables
        is_windows = (sys.platform == "win32")
        subprocess.check_call(cmd, shell=is_windows)
        
        print(f"\n" + "="*40)
        print("BUILD SUCCESSFUL!")
        print(f"Location: {dist_path}")
        print("="*40)

    except subprocess.CalledProcessError as e:
        print(f"\nBUILD FAILED (Exit Code {e.returncode})")
        print("Try closing any running instances of the app before rebuilding.")
    except Exception as e:
        print(f"\nAN UNEXPECTED ERROR OCCURRED: {e}")

    # 5. Cleanup
    print("\nCleaning up build artifacts...")
    time.sleep(2) 
    spec_file = os.path.normpath(os.path.join(base_dir, f"{app_name}.spec"))
    if os.path.exists(build_path):
        try: shutil.rmtree(build_path)
        except: pass
    if os.path.exists(spec_file):
        try: os.remove(spec_file)
        except: pass

if __name__ == "__main__":
    build()