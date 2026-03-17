import sqlite3
import os
import json
import re
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any


class MusicLibrary:
    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            # Use absolute path relative to project root
            base_dir = Path(__file__).parent.parent.parent
            self.db_path = str((base_dir / "data" / "library.db").absolute())
        else:
            self.db_path = db_path
            
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()

        # Enable WAL mode for better concurrency
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")

        # Tracks table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                id TEXT PRIMARY KEY,
                path TEXT UNIQUE,
                title TEXT,
                artist TEXT,
                lyrics TEXT,
                genre TEXT,
                style TEXT,
                mood TEXT,
                tempo TEXT,
                instruments TEXT,
                structure TEXT,
                artist_info TEXT,
                play_count INTEGER DEFAULT 0,
                last_played TIMESTAMP,
                last_scanned TIMESTAMP
            )
        """)

        # Track statistics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS track_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                track_id TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                listeners INTEGER,
                FOREIGN KEY (track_id) REFERENCES tracks(id)
            )
        """)

        conn.commit()
        conn.close()

    def get_track(self, track_id: str) -> Optional[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tracks WHERE id = ?", (track_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def get_all_tracks(self, limit: int = 1000) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM tracks ORDER BY last_scanned DESC LIMIT ?", (limit,)
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def update_track(self, track_id: str, data: Dict[str, Any]):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        fields = []
        values = []
        for k, v in data.items():
            if k == "id":
                continue
            fields.append(f"{k} = ?")
            values.append(v)

        values.append(track_id)
        query = f"UPDATE tracks SET {', '.join(fields)} WHERE id = ?"
        cursor.execute(query, tuple(values))
        conn.commit()
        conn.close()

    def record_play(self, track_id: str, listeners: int):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Update track totals
        cursor.execute(
            """
            UPDATE tracks 
            SET play_count = play_count + 1, last_played = ? 
            WHERE id = ?
        """,
            (datetime.now().isoformat(), track_id),
        )

        # Log specific play event
        cursor.execute(
            """
            INSERT INTO track_stats (track_id, listeners) 
            VALUES (?, ?)
        """,
            (track_id, listeners),
        )

        conn.commit()
        conn.close()

    def get_track_stats(self, track_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT timestamp, listeners 
            FROM track_stats 
            WHERE track_id = ? 
            ORDER BY timestamp DESC 
            LIMIT ?
        """,
            (track_id, limit),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in reversed(rows)]

    def scan_track_metadata(self, track_path: str) -> Dict[str, Any]:
        """Try to find and parse .txt file for the track."""
        p = Path(track_path)
        txt_path = p.with_suffix(".txt")

        metadata = {
            "lyrics": "",
            "genre": "",
            "style": "",
            "mood": "",
            "tempo": "",
            "instruments": "",
            "structure": "",
            "artist_info": "",
        }

        if not txt_path.exists():
            return metadata

        try:
            with open(txt_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Parse top-level metadata blocks first
            genres = re.search(r"\[genre:\s*(.*?)\]", content, re.IGNORECASE)
            if genres:
                metadata["genre"] = genres.group(1).strip()

            tempos = re.search(r"\[tempo:\s*(.*?)\]", content, re.IGNORECASE)
            if tempos:
                metadata["tempo"] = tempos.group(1).strip()

            def get_block(name: str):
                m = re.search(
                    rf"\[{name}\]\n(.*?)(?=\n\[|$)", content, re.IGNORECASE | re.DOTALL
                )
                return m.group(1).strip() if m else ""

            metadata["style"] = get_block("style")
            metadata["mood"] = get_block("mood")
            metadata["instruments"] = get_block("instruments")

            # Explicit structure block if exists
            struct_block = get_block("structure")

            # Extract lyrics and inline structure tags
            # We want to keep lines that AREN'T metadata blocks
            lines = content.split("\n")
            clean_lyrics = []
            inline_structure = []

            # Skip first line if it's the title (often repeated in Suno txt)
            first_line = lines[0].strip()
            start_idx = (
                1
                if (
                    p.stem.lower() in first_line.lower()
                    or first_line.lower() in p.stem.lower()
                )
                else 0
            )

            in_metadata_block = False
            for line in lines[start_idx:]:
                line_strip = line.strip()
                if not line_strip:
                    clean_lyrics.append("")
                    continue

                # Detect start of a block like [style]
                if re.match(
                    r"^\[(style|mood|instruments|structure|genre:|tempo:)\]",
                    line_strip,
                    re.IGNORECASE,
                ):
                    in_metadata_block = True
                    continue

                # Detect start of any other block to stop metadata capture
                if line_strip.startswith("[") and in_metadata_block:
                    # If it's something like [verse], it's lyrics-related
                    if not re.match(
                        r"^\[(style|mood|instruments|structure|genre:|tempo:)\]",
                        line_strip,
                        re.IGNORECASE,
                    ):
                        in_metadata_block = False

                if in_metadata_block:
                    continue

                # Detect inline structure like [verse 1] or [chorus]
                if line_strip.startswith("[") and line_strip.endswith("]"):
                    inline_structure.append(line_strip)
                else:
                    clean_lyrics.append(line)

            metadata["lyrics"] = "\n".join(clean_lyrics).strip()

            # Combine explicit structure block with discovered inline tags
            if struct_block:
                metadata["structure"] = struct_block
            elif inline_structure:
                metadata["structure"] = "\n".join(inline_structure)

        except Exception as e:
            print(f"Error parsing metadata for {track_path}: {e}")

        return metadata

    def cleanup_broken_links(self) -> int:
        """Remove tracks from DB if their files no longer exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, path FROM tracks")
        rows = cursor.fetchall()
        
        removed_count = 0
        for track_id, path in rows:
            if not path or not os.path.exists(path):
                cursor.execute("DELETE FROM tracks WHERE id = ?", (track_id,))
                removed_count += 1
        
        if removed_count > 0:
            conn.commit()
        conn.close()
        return removed_count

    def prune_orphaned_tracks(self, valid_folders: List[str]) -> int:
        """Remove tracks from DB if they do not belong to an active synced folder."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        valid_prefixes = [str(Path(f).absolute()) for f in valid_folders if f]
        
        cursor.execute("SELECT id, path FROM tracks")
        rows = cursor.fetchall()
        
        removed_count = 0
        for track_id, path in rows:
            if not path: continue
            path_abs = str(Path(path).absolute())
            
            is_valid = False
            for prefix in valid_prefixes:
                if path_abs.startswith(prefix):
                    is_valid = True
                    break
                    
            if not is_valid:
                cursor.execute("DELETE FROM tracks WHERE id = ?", (track_id,))
                removed_count += 1
                
        if removed_count > 0:
            conn.commit()
            print(f"Library DB pruned: removed {removed_count} orphaned tracks.")
        conn.close()
        return removed_count

    def sync_external_folders(self, folders: List[str]) -> Dict[str, int]:
        """Scan folders, add new tracks, and return stats."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        stats = {"added": 0, "processed": 0}
        extensions = (".mp3", ".wav", ".ogg", ".flac", ".m4a")
        
        for folder in folders:
            p = Path(folder)
            if not p.exists():
                print(f"Directory not found during scan: {folder}")
                continue

            for ext in extensions:
                for track_file in p.rglob(f"*{ext}"):
                    stats["processed"] += 1
                    track_path = str(track_file.absolute())
                    track_id = track_file.stem

                    # Check if exists by path
                    cursor.execute(
                        "SELECT id FROM tracks WHERE path = ?", (track_path,)
                    )
                    if not cursor.fetchone():
                        meta = self.scan_track_metadata(track_path)

                        # Default artist/title from filename
                        name = track_file.stem
                        name = re.sub(r"^\d+\s*[-._]\s*", "", name).strip()
                        parts = [
                            p.strip() for p in re.split(r"\s*-\s*", name) if p.strip()
                        ]
                        artist = parts[0] if len(parts) >= 2 else "Unknown Artist"
                        title = " - ".join(parts[1:]) if len(parts) >= 2 else name

                        cursor.execute(
                            """
                            INSERT OR REPLACE INTO tracks (id, path, title, artist, lyrics, genre, style, mood, tempo, instruments, structure, last_scanned)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                            (
                                track_id,
                                track_path,
                                title,
                                artist,
                                meta["lyrics"],
                                meta["genre"],
                                meta["style"],
                                meta["mood"],
                                meta["tempo"],
                                meta["instruments"],
                                meta["structure"],
                                datetime.now().isoformat(),
                            ),
                        )
                        stats["added"] += 1

        conn.commit()
        conn.close()
        return stats


# Singleton instance
library = MusicLibrary()
