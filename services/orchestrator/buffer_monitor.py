from collections import deque
from datetime import datetime, timedelta
from typing import Optional, List
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from shared.models import RenderedSegment, BufferStatus
from services.orchestrator.config import config


class BufferMonitor:
    def __init__(self):
        self._segments: deque = deque()
        self._target_minutes: float = config.buffer_target
        self._min_minutes: float = config.buffer_min
        self._critical_seconds: float = config.buffer_critical

    def reload_thresholds(self) -> None:
        """Reload buffer thresholds from the global config.

        MVP note: this does not touch queued segments; only thresholds.
        """
        self._target_minutes = float(config.buffer_target)
        self._min_minutes = float(config.buffer_min)
        self._critical_seconds = float(config.buffer_critical)

    def clear(self) -> None:
        """Clear all queued segments in the buffer."""
        self._segments.clear()

    def add_segment(self, segment: RenderedSegment) -> None:
        self._segments.append(segment)
        self._cleanup_old_segments()

    def get_segments_ready(self) -> List[RenderedSegment]:
        now = datetime.utcnow()
        ready = []
        for seg in self._segments:
            age = (now - seg.created_at).total_seconds()
            if age >= 0:  # Segment is ready (past its intended start)
                ready.append(seg)
        return ready

    def peek_next(self) -> Optional[RenderedSegment]:
        ready = self.get_segments_ready()
        return ready[0] if ready else None

    def pop_ready(self) -> Optional[RenderedSegment]:
        ready = self.get_segments_ready()
        if ready:
            segment = ready[0]
            self._segments.remove(segment)
            return segment
        return None

    def peek_current(self) -> Optional[RenderedSegment]:
        if self._segments:
            return self._segments[0]
        return None

    def get_status(self) -> BufferStatus:
        total_duration = sum(seg.duration_sec for seg in self._segments)
        minutes_ahead = total_duration / 60.0
        return BufferStatus(
            minutes_ahead=minutes_ahead,
            segments_count=len(self._segments),
            total_duration_sec=total_duration
        )

    def _cleanup_old_segments(self) -> None:
        # Increase safety cutoff to 180 minutes so long hour-schedules aren't purged accidentally
        cutoff = datetime.utcnow() - timedelta(minutes=180)
        while self._segments and self._segments[0].created_at < cutoff:
            self._segments.popleft()

    @property
    def is_low(self) -> bool:
        status = self.get_status()
        return status.minutes_ahead < self._min_minutes

    @property
    def is_critical(self) -> bool:
        status = self.get_status()
        return status.total_duration_sec < self._critical_seconds

    @property
    def target_reached(self) -> bool:
        status = self.get_status()
        return status.minutes_ahead >= self._target_minutes
