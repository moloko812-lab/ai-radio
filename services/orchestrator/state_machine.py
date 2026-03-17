from enum import Enum
from typing import Optional, Callable, List, Dict
from datetime import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from shared.models import State, BufferStatus, SystemStatus
from services.orchestrator.config import config


class StateMachine:
    def __init__(self):
        self._state: State = State.WARMUP
        self._listeners: Dict[State, List[Callable]] = {
            state: [] for state in State
        }
        self._buffer_status: Optional[BufferStatus] = None
        self._llm_latency_ms: float = 0
        self._tts_latency_ms: float = 0
        self._render_latency_ms: float = 0
        self._current_dj: str = ""
        self._current_track: str = ""
        self._error_message: Optional[str] = None

    @property
    def state(self) -> State:
        return self._state

    def transition(self, new_state: State) -> None:
        if self._state != new_state:
            self._state = new_state
            for listener in self._listeners.get(new_state, []):
                listener()

    def on(self, state: State, callback: Callable) -> None:
        self._listeners[state].append(callback)

    def update_buffer(self, status: BufferStatus) -> None:
        self._buffer_status = status

    def update_latencies(self, llm: Optional[float] = None, tts: Optional[float] = None, render: Optional[float] = None) -> None:
        if llm is not None:
            self._llm_latency_ms = llm
        if tts is not None:
            self._tts_latency_ms = tts
        if render is not None:
            self._render_latency_ms = render

    def set_current_track(self, dj: str = "", track: str = "") -> None:
        self._current_dj = dj
        self._current_track = track

    def set_error(self, message: Optional[str]) -> None:
        self._error_message = message
        if message:
            self.transition(State.DEGRADED)

    def clear_error(self) -> None:
        self._error_message = None

    def get_system_status(self) -> SystemStatus:
        return SystemStatus(
            state=self._state,
            buffer=self._buffer_status or BufferStatus(0, 0, 0),
            llm_latency_ms=self._llm_latency_ms,
            tts_latency_ms=self._tts_latency_ms,
            render_latency_ms=self._render_latency_ms,
            current_dj=self._current_dj,
            current_track=self._current_track,
            error_message=self._error_message
        )

    def should_generate_talk(self) -> bool:
        if not self._buffer_status:
            return False
        return self._buffer_status.minutes_ahead < self._buffer_status_min

    def should_use_fallback(self) -> bool:
        if not self._buffer_status:
            return True
        return self._buffer_status.minutes_ahead < 1.0  # critical

    @property
    def _buffer_status_min(self) -> float:
        return float(config.buffer_min)
