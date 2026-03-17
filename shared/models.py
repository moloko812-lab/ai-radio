from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class State(str, Enum):
    WARMUP = "WARMUP"
    TALK_GEN = "TALK_GEN"
    TTS_GEN = "TTS_GEN"
    RENDER = "RENDER"
    PUBLISH = "PUBLISH"
    MUSIC_FILL = "MUSIC_FILL"
    DEGRADED = "DEGRADED"


class SegmentType(str, Enum):
    TALK = "talk"
    MUSIC = "music"
    TALK_OVER_MUSIC = "talk_over_music"
    MUSIC_TRANSITION = "music_transition"
    JINGLE = "jingle"
    NEWS = "news"
    FALLBACK = "fallback"


@dataclass
class ScriptLine:
    speaker: str
    text: str
    speaker_name: str = ""
    source: str = "ai"  # "ai", "script", "news"
    style: Dict = field(default_factory=lambda: {"energy": 0.5, "warmth": 0.5, "pace": 1.0})
    pause_after_ms: int = 200
    voice: str = ""
    voice_id: str = ""
    is_chat: bool = False


@dataclass
class ScriptBlock:
    show_id: str
    block_id: str
    language: str = "en"
    topic_tags: List[str] = field(default_factory=list)
    target_duration_sec: int = 60
    lines: List[ScriptLine] = field(default_factory=list)
    mix_notes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TTSResult:
    line: ScriptLine
    audio_path: str
    duration_sec: float
    sample_rate: int


@dataclass
class RenderedSegment:
    segment_id: str
    segment_type: SegmentType
    file_path: str
    duration_sec: float
    lufs: float
    peak: float
    transcript: str = ""
    source: str = "ai"  # "ai", "script", "news"
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    show_type: str = "regular"
    has_dj_intro: bool = False
    has_dj_outro: bool = False
    long_monologue: bool = False
    track_title: str = ""
    track_artist: str = ""
    track_id: str = ""


@dataclass
class BufferStatus:
    minutes_ahead: float
    segments_count: int
    total_duration_sec: float
    last_updated: datetime = field(default_factory=datetime.utcnow)


@dataclass
class SystemStatus:
    state: State
    buffer: BufferStatus
    llm_latency_ms: float = 0
    tts_latency_ms: float = 0
    render_latency_ms: float = 0
    current_dj: str = ""
    current_track: str = ""
    error_message: Optional[str] = None


@dataclass
class NowPlaying:
    dj: str
    track: str
    segment_type: SegmentType
    energy: float
    timestamp: int
