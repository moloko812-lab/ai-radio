import json
import os
import time
from pathlib import Path
from typing import List, Dict, Optional
import logging
logger = logging.getLogger(__name__)

class PlaylistManager:
    def __init__(self, storage_path: Optional[str] = None):
        if storage_path is None:
            # Use absolute path based on project root
            root = Path(__file__).parent.parent
            self.storage_path = root / "data" / "playlist.json"
        else:
            self.storage_path = Path(storage_path)
            
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.history: List[Dict] = []
        self.ratings: Dict[str, List[int]] = {} # track_id -> list of ratings
        self._load()

    def _load(self):
        if self.storage_path.exists():
            try:
                with open(self.storage_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.history = data.get("history", [])
                    self.ratings = data.get("ratings", {})
            except Exception as e:
                logger.error(f"Failed to load playlist: {e}")

    def _save(self):
        try:
            with open(self.storage_path, "w", encoding="utf-8") as f:
                json.dump({
                    "history": self.history[-500:], # Keep only last 500 for storage
                    "ratings": self.ratings
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Failed to save playlist: {e}")

    def add_to_history(self, segment: Dict):
        """Add a segment to history. Only music segments usually."""
        seg_type = str(segment.get("segment_type", "")).upper()
        # Capture MUSIC and MUSIC_FILL or any music-related types
        if "MUSIC" not in seg_type:
            return

        track_id = self._get_track_id(segment)
        logger.info(f"Adding track to history: {track_id} (type: {seg_type})")
        entry = {
            "id": track_id,
            "title": segment.get("track_title", "Unknown"),
            "artist": segment.get("track_artist", "Unknown"),
            "timestamp": time.time(),
            "duration": segment.get("duration_sec", 0)
        }
        
        # Don't add duplicate of the very last track if it's the same
        if self.history and self.history[-1]["id"] == track_id:
             return

        self.history.append(entry)
        if len(self.history) > 500: # More history for hourly filtering
            self.history = self.history[-500:]
        self._save()

    def _get_track_id(self, segment: Dict) -> str:
        title = segment.get("track_title", "Unknown")
        artist = segment.get("track_artist", "")
        if artist:
            return f"{artist} - {title}".lower().strip()
        return title.lower().strip()

    def add_rating(self, track_id: str, rating: int, user_id: str):
        """rating should be 1-5 or similar. user_id prevents multiple votes."""
        if track_id not in self.ratings:
            self.ratings[track_id] = {}
        
        # Store as dict: user_id -> rating
        self.ratings[track_id][user_id] = rating
        self._save()

    def get_rating(self, track_id: str) -> float:
        user_ratings = self.ratings.get(track_id, {})
        if not user_ratings:
            return 0.0
        
        # Backward compatibility: handle old list format
        if isinstance(user_ratings, list):
            if not user_ratings: return 0.0
            return sum(user_ratings) / len(user_ratings)

        # Calculate avg from all unique users who voted
        vals = list(user_ratings.values())
        if not vals: return 0.0
        return sum(vals) / len(vals)

    def get_rating_count(self, track_id: str) -> int:
        data = self.ratings.get(track_id, {})
        return len(data)

    def get_recent(self, hours: float = 1.0, current_track_id: Optional[str] = None) -> List[Dict]:
        limit_time = time.time() - (hours * 3600)
        recent = [item.copy() for item in self.history if item["timestamp"] >= limit_time]
        
        # We only want to mark the LATEST instance of current_track_id as playing
        found_playing = False
        
        # Enrich with rating
        # Iterate in REVERSE to find the newest one first
        enriched = []
        for item in reversed(recent):
            item["rating"] = self.get_rating(item["id"])
            item["rating_count"] = self.get_rating_count(item["id"])
            
            # Check is_playing
            if not found_playing and current_track_id and item["id"] == current_track_id:
                item["is_playing"] = True
                found_playing = True
            else:
                item["is_playing"] = False
                
            # Add relative time string
            diff = time.time() - item["timestamp"]
            if diff < 60:
                item["time_ago"] = "Just now"
            else:
                item["time_ago"] = f"{int(diff // 60)}m ago"
            
            enriched.append(item)
                
        return enriched

# Singleton
playlist_manager = PlaylistManager()
