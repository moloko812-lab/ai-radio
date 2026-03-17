import os
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple


@dataclass
class TrackSlot:
    track_path: str
    id: str  # Unique track ID (stem)
    title: str
    artist: str
    duration: float
    start_time: float
    end_time: float
    show_type: str = "regular"
    has_dj_intro: bool = True
    has_dj_outro: bool = True
    long_monologue: bool = False


def _parse_artist_title(path: Path) -> Tuple[str, str]:
    name = path.stem
    # Common patterns:
    # "01 - Artist - Title", "Artist - Title", "Artist_Title", etc.
    name = re.sub(r"^\d+\s*[-._]\s*", "", name).strip()
    parts = [p.strip() for p in re.split(r"\s*-\s*", name) if p.strip()]
    if len(parts) >= 2:
        return parts[0], " - ".join(parts[1:])
    return "", name


def _scan_music_files(music_dirs: List[str], limit: int = 1000) -> List[Path]:
    """Scan music directories for audio files with a safety limit."""
    files: List[Path] = []
    extensions = (".mp3", ".wav", ".ogg", ".flac", ".m4a")
    for d in music_dirs:
        if not d: continue
        p = Path(d)
        if not p.exists(): continue
        
        if p.is_file():
            if p.suffix.lower() in extensions:
                files.append(p)
            continue
        
        # Iterative scan instead of rglob to allow limit and avoid hanging on huge folders
        try:
            for root, _, filenames in os.walk(str(p)):
                for filename in filenames:
                    if any(filename.lower().endswith(ext) for ext in extensions):
                        files.append(Path(os.path.join(root, filename)))
                        if len(files) >= limit:
                            return files
        except Exception as e:
            print(f"Error scanning {d}: {e}")
            
    return files


def _probe_duration_seconds(path: Path) -> float:
    # Quick check for WAV
    try:
        if path.suffix.lower() == ".wav":
            import wave
            with wave.open(str(path), "rb") as w:
                return float(w.getnframes()) / float(w.getframerate())
    except: pass
    
    # Try using ffprobe with a strict timeout
    import subprocess
    cmd = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", str(path)
    ]
    try:
        # 2s timeout is plenty for a local disk, keeps UI snappy if drive is slow
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=2.0)
        output = result.stdout.strip()
        if output:
            return float(output)
    except: pass
        
    return 180.0 # Default 3 min placeholder


def build_music_schedule(duration_minutes: int, music_dirs: List[str]) -> List[TrackSlot]:
    """
    Build a sequential track schedule for a given duration.
    """
    total_budget = max(float(duration_minutes) * 60.0, 0.0)
    
    # Try using the library database first
    candidates = []
    try:
        from services.music.library import library
        import sqlite3
        conn = sqlite3.connect(library.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tracks")
        rows = cursor.fetchall()
        conn.close()
        
        if rows:
            valid_prefixes = [str(Path(d).absolute()) for d in music_dirs if d and Path(d).exists()]
            for r in rows:
                path_str = r['path']
                if not path_str: continue
                path_abs = str(Path(path_str).absolute())
                
                # Only include if it belongs to one of the allowed music_dirs
                for prefix in valid_prefixes:
                    if path_abs.startswith(prefix):
                        candidates.append(Path(path_str))
                        break
    except Exception:
        pass
        
    if not candidates:
        print(f"[PLANNER] Library empty or no matches. Scanning disk directly: {music_dirs}")
        candidates = _scan_music_files(music_dirs)

    if not candidates:
        print(f"[PLANNER] NO MUSIC FOUND in any of these folders: {music_dirs}")
        return []

    print(f"[PLANNER] Found {len(candidates)} track candidates. Building 60min schedule...")
    random.shuffle(candidates)
    
    schedule: List[TrackSlot] = []
    current_time_offset = 0.0
    
    while current_time_offset < total_budget and candidates:
        path = candidates.pop()
        duration = _probe_duration_seconds(path)
        if duration <= 0:
            duration = 180.0  # Placeholder 3 min
            
        artist, title = _parse_artist_title(path)
        
        slot = TrackSlot(
            track_path=str(path.absolute()),
            id=path.stem,
            title=title,
            artist=artist,
            duration=duration,
            start_time=current_time_offset,
            end_time=current_time_offset + duration
        )
        schedule.append(slot)
        current_time_offset += duration
        
    return schedule
