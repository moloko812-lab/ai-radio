"""Runtime pointers for the single-process MVP.

This module enables multiple Flask servers (front at :8000 and dashboard at
:8001) running in the same Python process to share references to the same
orchestrator instance and in-memory state.

Limitations:
  - In-memory only; everything resets on restart.
  - Not safe for multi-process deployments.
"""

from __future__ import annotations

from collections import deque
from typing import Any, Deque, Dict, Optional, List


orchestrator: Any = None
orch_loop: Any = None

status_data: Dict[str, Any] = {}
metadata: Dict[str, Any] = {}

transcript: Deque[Dict[str, Any]] = deque(maxlen=200)

streamer: Any = None


music_folders: List[str] = []

music_schedule: List[Dict] = []

current_track_info: Dict[str, Any] = {}

current_playing_segment: Dict[str, Any] = {}

# Store listener messages to be used by the dialogue generator
listener_messages: Deque[Dict[str, Any]] = deque(maxlen=100)
