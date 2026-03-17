import os
import shutil
import re
from pathlib import Path

# --- CONFIGURATION ---
SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
# We'll create a folder in the system temp directory
DEST_BASE = os.environ.get('TEMP', '/tmp')
DEST_DIR = os.path.join(DEST_BASE, "AI_Radio_Github_Export")

# Directories and files to IGNORE during copy
IGNORE_PATTERNS = [
    ".git",
    "venv",
    ".venv",
    "__pycache__",
    "output",          # HLS segments
    "cache",           # TTS cache
    "data",            # SQLite databases, logs
    "*.pyc",
    "*.log",
    ".DS_Store",
    "node_modules",
    "assets/music/*",  # Don't upload the whole music library to Github!
]

def prepare_export():
    print(f"🚀 Starting export process...")
    print(f"Source: {SOURCE_DIR}")
    print(f"Destination: {DEST_DIR}")

    # 1. Clean destination if it exists
    if os.path.exists(DEST_DIR):
        print("Cleaning up old export directory...")
        shutil.rmtree(DEST_DIR)

    # 2. Copy files with ignore
    print("Copying files (excluding large assets and caches)...")
    shutil.copytree(SOURCE_DIR, DEST_DIR, ignore=shutil.ignore_patterns(
        '.git', 'venv', '.venv', '__pycache__', 'output', 'cache', 'data', '*.pyc', '*.log'
    ))
    
    # Manually clean assets/music but keep the folder
    music_dir = os.path.join(DEST_DIR, "assets", "music")
    if os.path.exists(music_dir):
        for f in os.listdir(music_dir):
            file_path = os.path.join(music_dir, f)
            if os.path.isfile(file_path):
                os.remove(file_path)
        # Add a placeholder so the folder is tracked
        with open(os.path.join(music_dir, ".gitkeep"), "w") as f:
            f.write("")

    # 3. Sanitize config.yaml
    config_path = os.path.join(DEST_DIR, "config.yaml")
    if os.path.exists(config_path):
        print("Sanitizing config.yaml (Removing API keys)...")
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()

        # More precise approach: iterate lines
        lines = content.splitlines()
        new_lines = []
        in_sports_section = False
        
        for line in lines:
            # Detect section
            if "thesportsdb:" in line:
                in_sports_section = True
            elif ":" in line and line.strip().endswith(":") and not line.strip().startswith("-"):
                # Started a new section that isn't sports
                if in_sports_section and (not line.startswith("  ")): 
                    in_sports_section = False
            
            if "api_key:" in line:
                if in_sports_section:
                    # Keep it (it's public 123 for thesportsdb)
                    new_lines.append(line)
                else:
                    # Strip it
                    key_part = line.split("api_key:")[0]
                    new_lines.append(f"{key_part}api_key: 'YOUR_API_KEY_HERE'")
            else:
                new_lines.append(line)

        with open(config_path, "w", encoding="utf-8") as f:
            f.write("\n".join(new_lines))
    else:
        print("Warning: config.yaml not found in copy!")

    print(f"\n✅ SUCCESS! Project files ready for Github at:\n{DEST_DIR}")
    print("\nNext steps:")
    print(f"1. Go to {DEST_DIR}")
    print("2. Initialize git: git init")
    print("3. Add files: git add .")
    print("4. Commit and push to your repository.")

if __name__ == "__main__":
    prepare_export()
