import asyncio
import logging
import subprocess
from pathlib import Path
from typing import Optional, Callable, Any
import sys
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.orchestrator.buffer_monitor import BufferMonitor
from services.orchestrator.config import config
from services.orchestrator.config import config
from shared import runtime_state
from shared.playlist import playlist_manager

logger = logging.getLogger(__name__)


def _run_in_executor(func, *args):
    """Cross-platform replacement for asyncio.to_thread (Python 3.9+)."""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, lambda: func(*args))


class Streamer:
    """Continuously consumes ready segments from BufferMonitor and outputs HLS.

    MVP constraints:
    - Single-process, single ffmpeg encoder instance.
    - Segments are decoded/resampled per-file and pushed as raw PCM into ffmpeg stdin.
    """

    def __init__(self, buffer: BufferMonitor):
        self._buffer = buffer
        self._stream_config = config.stream_config
        self._running = False

        self._ffmpeg: Optional[subprocess.Popen] = None
        self._output_dir: Path = Path("./output/hls")
        self._segment_duration: int = 4

        self._sample_rate: int = int(config.tts_sample_rate)
        self._channels: int = 2
        self._sample_width_bytes: int = 2  # s16le

        self._realtime_chunk_sec: float = 0.25

        self._last_silence_push = 0.0

        self._current_segment: Optional[Any] = None
        self._current_segment_start_time: float = 0.0
        self._current_segment_remaining_sec: float = 0.0
        self._track_ending_callback: Optional[Callable[[], None]] = None
        self._track_ending_notified: bool = False
        self._skip_current = False
        
        # Crossfade state
        self._crossfade_sec = float(config.get("audio.crossfade_sec", 2.0))
        self._bytes_per_sec = self._sample_rate * self._channels * self._sample_width_bytes
        self._crossfade_bytes = int(self._crossfade_sec * self._bytes_per_sec)
        self._overlap_pcm: bytes = b""

    def _apply_crossfade(self, head_pcm: bytes, tail_pcm: bytes) -> bytes:
        """Mix tail_pcm (fading out) with head_pcm (fading in)."""
        import struct
        
        n_samples = min(len(head_pcm), len(tail_pcm)) // 2
        if n_samples <= 0:
            return head_pcm
            
        fmt = f"<{n_samples}h"
        try:
            samples_in = struct.unpack(fmt, head_pcm[:n_samples*2])
            samples_out = struct.unpack(fmt, tail_pcm[:n_samples*2])
        except Exception as e:
            logger.warning(f"Crossfade unpack failed: {e}")
            return head_pcm
            
        mixed = []
        for i in range(n_samples):
            # i=0: out at 1.0, in at 0.0
            # i=n_samples: out at 0.0, in at 1.0
            weight_in = i / n_samples
            weight_out = 1.0 - weight_in
            
            # Simple linear crossfade
            m = int(samples_out[i] * weight_out + samples_in[i] * weight_in)
            
            # Clip
            if m > 32767: m = 32767
            elif m < -32768: m = -32768
            mixed.append(m)
            
        return struct.pack(fmt, *mixed) + head_pcm[n_samples*2:]

    def skip_current(self) -> None:
        self._skip_current = True

    def set_track_ending_callback(self, callback: Callable[[], None]) -> None:
        self._track_ending_callback = callback

    async def start(self) -> None:
        stream_type = (self._stream_config.get("type") or "hls").lower()

        if stream_type != "hls":
            raise ValueError(
                f"MVP streamer supports only HLS right now (got: {stream_type})"
            )

        await self._start_hls()
        self._running = True
        logger.info(
            f"Streamer started: {stream_type} → {self._output_dir / 'index.m3u8'}"
        )

    async def stop(self) -> None:
        self._running = False
        await _run_in_executor(self._stop_ffmpeg)
        logger.info("Streamer stopped")

    async def _start_hls(self) -> None:
        hls_cfg = self._stream_config.get("hls", {}) or {}
        self._output_dir = Path(hls_cfg.get("output_dir", "./output/hls"))
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._segment_duration = int(hls_cfg.get("segment_duration_sec", 4))

        # Clean old playlist/segments so players don't get stale data.
        for p in (
            list(self._output_dir.glob("*.m3u8"))
            + list(self._output_dir.glob("*.ts"))
            + list(self._output_dir.glob("*.m4s"))
            + list(self._output_dir.glob("*.mp4"))
        ):
            try:
                p.unlink()
            except Exception:
                pass

        self._start_ffmpeg_hls()

    def _start_ffmpeg_hls(self) -> None:
        if self._ffmpeg and self._ffmpeg.poll() is None:
            return

        playlist = str(self._output_dir / "index.m3u8")
        seg_pattern = str(self._output_dir / "seg_%08d.ts")

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "warning",
            "-nostdin",
            "-f",
            "s16le",
            "-ar",
            str(self._sample_rate),
            "-ac",
            str(self._channels),
            "-i",
            "pipe:0",
            "-c:a",
            "aac",
            "-b:a",
            "256k",
            "-f",
            "hls",
            "-hls_time",
            str(self._segment_duration),
            # Keep a longer playlist and avoid deleting segments in MVP.
            # This prevents clients from hitting 404 if the encoder ...
            "-hls_list_size",
            "30",
            "-hls_flags",
            "delete_segments+append_list",
            "-hls_segment_filename",
            seg_pattern,
            playlist,
        ]

        self._ffmpeg = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )

        # Wait briefly and check if ffmpeg started successfully
        time.sleep(0.5)
        if self._ffmpeg.poll() is not None:
            # ffmpeg exited immediately - capture and log the error
            stdout, stderr = self._ffmpeg.communicate()
            logger.error(
                f"ffmpeg failed to start: returncode={self._ffmpeg.returncode}"
            )
            if stdout:
                logger.error(
                    f"ffmpeg stdout: {stdout.decode('utf-8', errors='replace')}"
                )
            if stderr:
                logger.error(
                    f"ffmpeg stderr: {stderr.decode('utf-8', errors='replace')}"
                )
            self._ffmpeg = None

    def _stop_ffmpeg(self) -> None:
        if not self._ffmpeg:
            return

        try:
            if self._ffmpeg.stdin:
                try:
                    self._ffmpeg.stdin.close()
                except Exception:
                    pass
            self._ffmpeg.terminate()
            try:
                self._ffmpeg.wait(timeout=5)
            except Exception:
                self._ffmpeg.kill()
        finally:
            self._ffmpeg = None

    def _write_pcm_realtime(self, pcm: bytes) -> bool:
        """Write raw PCM into encoder stdin paced close to real-time.

        Without pacing, ffmpeg will generate HLS segments as fast as we feed
        it, and with segment deletion a browser client will eventually start
        getting 404s. Pacing keeps wall-clock roughly aligned to media time.
        """
        if not self._ffmpeg or not self._ffmpeg.stdin:
            return False

        bytes_per_sec = self._sample_rate * self._channels * self._sample_width_bytes
        if bytes_per_sec <= 0:
            return False

        chunk_bytes = max(1, int(bytes_per_sec * float(self._realtime_chunk_sec)))

        try:
            i = 0
            n = len(pcm)

            # Use absolute time for pacing to avoid drift
            start_wall_time = time.time()
            bytes_written = 0

            while i < n:
                if getattr(self, "_skip_current", False):
                    self._skip_current = False
                    logger.info(
                        "Streamer gracefully skipping the remainder of the current track"
                    )
                    break

                part = pcm[i : i + chunk_bytes]
                self._ffmpeg.stdin.write(part)
                self._ffmpeg.stdin.flush()

                i += len(part)
                bytes_written += len(part)

                # Target wall time for the progress we've made
                expected_wall_time = start_wall_time + (bytes_written / bytes_per_sec)

                # Sleep if we are ahead of real-time
                # We stay slightly ahead (0.1s) to keep the pipe full but avoid buffer bloat
                now = time.time()
                sleep_dur = (expected_wall_time - now) - 0.1
                if sleep_dur > 0:
                    time.sleep(sleep_dur)

            return True
        except BrokenPipeError:
            logger.warning("ffmpeg stdin broken pipe — will restart")
            return False
        except Exception as e:
            logger.warning(f"Failed writing PCM realtime: {e}")
            return False

    async def stream_loop(self) -> None:
        """Run forever until stop() is called."""
        while self._running:
            if not self._ffmpeg or self._ffmpeg.poll() is not None:
                logger.warning("ffmpeg encoder not running — restarting")
                await _run_in_executor(self._stop_ffmpeg)
                await _run_in_executor(self._start_ffmpeg_hls)

                # Check if ffmpeg started successfully
                if not self._ffmpeg:
                    logger.error("ffmpeg failed to start - waiting before retry...")
                    await asyncio.sleep(2)
                    continue

            segment = None
            try:
                segment = self._buffer.pop_ready()
            except Exception:
                segment = None

            if segment and Path(segment.file_path).exists():
                self._start_segment(segment)
                seg_path = segment.file_path
                
                # Check if we should crossfade (only if buffer has another segment waiting)
                # This ensures we don't return early if there's a gap in the broadcast
                has_next = self._buffer.get_status().segments_count > 0
                
                ok = await _run_in_executor(self._push_wav_segment, seg_path, has_next)
                if not ok:
                    await _run_in_executor(self._push_silence, 1.0)
                    self._overlap_pcm = b""
                
                self._current_segment = None
                self._track_ending_notified = False
                # Clear current playing state
                runtime_state.current_playing_segment = None
                logger.debug("Cleared current_playing_segment")

                # Delete the rendered segment file to prevent disk bloat
                try:
                    p = Path(seg_path)
                    if p.exists():
                        p.unlink()
                        logger.debug(f"Cleaned up played segment: {p.name}")
                except Exception as e:
                    logger.warning(f"Failed to clean up segment file: {e}")
            else:
                # If buffer is empty, flush any remaining overlap pcm
                if self._overlap_pcm:
                    # Fade out the overlap buffer to silence
                    fade_out = self._apply_crossfade(b"\x00" * len(self._overlap_pcm), self._overlap_pcm)
                    await _run_in_executor(self._write_pcm_realtime, fade_out)
                    self._overlap_pcm = b""
                
                now = time.time()
                if now - self._last_silence_push >= 1.0:
                    await _run_in_executor(self._push_silence, 1.0)
                    self._last_silence_push = now
                await asyncio.sleep(0.2)

    def _start_segment(self, segment: Any) -> None:
        self._current_segment = segment
        self._current_segment_start_time = time.time()
        self._current_segment_remaining_sec = getattr(segment, "duration_sec", 0.0)
        self._track_ending_notified = False

        seg_type = getattr(segment, "segment_type", "")
        seg_title = getattr(segment, "track_title", "")
        seg_id = getattr(segment, "segment_id", "")
        logger.info(
            f"Streamer started playing: type={seg_type}, title={seg_title}, id={seg_id}"
        )

        transcript = getattr(segment, "transcript", "")

        # Write to comprehensive log file
        try:
            from datetime import datetime

            curr_prog = config.current_program
            prog_title = curr_prog.get("title", "Unknown Show")
            prog_type = curr_prog.get("type", "Unknown Type")
            show_info = f"[{prog_title} | {prog_type.upper()}]"

            with open("tts_log.txt", "a", encoding="utf-8") as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if seg_type == "music":
                    artist = getattr(segment, "track_artist", "Unknown Artist")
                    title = seg_title if seg_title else "Unknown Title"
                    dur = int(getattr(segment, "duration_sec", 0))
                    f.write(
                        f"[{timestamp}] {show_info} 🎵 MUSIC: {artist} - {title} ({dur}s)\n"
                    )

                if transcript:
                    # Clean up the format slightly for the log
                    clean_transcript = transcript.replace(" | ", "\n")
                    f.write(
                        f"[{timestamp}] {show_info} 🎤 DIALOGUE ({seg_type}):\n{clean_transcript}\n"
                        + "-" * 40
                        + "\n"
                    )
                elif seg_type != "music":
                    dur = int(getattr(segment, "duration_sec", 0))
                    f.write(
                        f"[{timestamp}] {show_info} ▶ OTHER: {seg_type} - {seg_title} ({dur}s)\n"
                    )
        except Exception as e:
            logger.error(f"Failed to write to tts_log.txt: {e}")

        if transcript:
            logger.info(f"🎤 ON AIR NOW:\n{transcript}\n" + "-" * 40)

        # Update shared state
        runtime_state.current_playing_segment = {
            "segment_id": seg_id,
            "segment_type": seg_type,
            "track_title": seg_title,
            "track_artist": getattr(segment, "track_artist", ""),
            "duration_sec": getattr(segment, "duration_sec", 0.0),
            "start_time": self._current_segment_start_time,
            "has_dj_intro": getattr(segment, "has_dj_intro", False),
            "has_dj_outro": getattr(segment, "has_dj_outro", False),
            "long_monologue": getattr(segment, "long_monologue", False),
            "show_type": getattr(segment, "show_type", "regular"),
            "script_lines": getattr(segment, "metadata", {}).get("script_lines", [])
            if hasattr(segment, "metadata")
            else [],
        }

        # Record history if it's music
        if "MUSIC" in str(seg_type).upper():
            playlist_manager.add_to_history(runtime_state.current_playing_segment)

        logger.debug(
            f"Updated runtime_state.current_playing_segment: {runtime_state.current_playing_segment}"
        )

        # Update shared state
        runtime_state.current_track_info = {
            "segment_id": getattr(segment, "segment_id", ""),
            "segment_type": getattr(segment, "segment_type", ""),
            "track_title": getattr(segment, "track_title", ""),
            "track_artist": getattr(segment, "track_artist", ""),
            "duration_sec": getattr(segment, "duration_sec", 0.0),
            "start_time": self._current_segment_start_time,
            "has_dj_intro": getattr(segment, "has_dj_intro", False),
            "has_dj_outro": getattr(segment, "has_dj_outro", False),
            "long_monologue": getattr(segment, "long_monologue", False),
            "show_type": getattr(segment, "show_type", "regular"),
            "script_lines": getattr(segment, "metadata", {}).get("script_lines", [])
            if hasattr(segment, "metadata")
            else [],
        }

    def get_current_track_info(self) -> dict:
        if not self._current_segment:
            return {
                "playing": False,
                "track_title": "",
                "track_artist": "",
                "remaining_sec": 0.0,
                "segment_type": "",
                "show_type": "regular",
            }

        return {
            "playing": True,
            "track_title": getattr(self._current_segment, "track_title", ""),
            "track_artist": getattr(self._current_segment, "track_artist", ""),
            "remaining_sec": self._current_segment_remaining_sec,
            "segment_type": getattr(self._current_segment, "segment_type", ""),
            "show_type": getattr(self._current_segment, "show_type", "regular"),
            "has_dj_intro": getattr(self._current_segment, "has_dj_intro", True),
            "has_dj_outro": getattr(self._current_segment, "has_dj_outro", True),
            "long_monologue": getattr(self._current_segment, "long_monologue", False),
        }

    def _push_wav_segment(self, file_path: str, has_next: bool = False) -> bool:
        if not self._ffmpeg or not self._ffmpeg.stdin:
            logger.warning("Cannot push segment - ffmpeg encoder not available")
            return False

        # Stream decoded PCM
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-i",
            file_path,
            "-f",
            "s16le",
            "-ar",
            str(self._sample_rate),
            "-ac",
            str(self._channels),
            "pipe:1",
        ]

        try:
            decoder = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                bufsize=0,
            )

            chunk_bytes = max(1, int(self._bytes_per_sec * float(self._realtime_chunk_sec)))
            start_wall_time = time.time()
            bytes_written = 0
            
            # Temporary buffer for the whole segment to handle tail-stealing
            # (In production we could use a sliding window, but for segments ~4-5min it's manageable)
            segment_pcm = decoder.stdout.read()
            decoder.wait()
            
            if not segment_pcm:
                return False
                
            total_len = len(segment_pcm)
            pos = 0
            
            # Handle start crossfade with head
            head_len = min(len(self._overlap_pcm), total_len)
            if head_len > 0:
                mixed_head = self._apply_crossfade(segment_pcm[:head_len], self._overlap_pcm)
                # replace original head with mixed head
                segment_pcm = mixed_head + segment_pcm[head_len:]
                self._overlap_pcm = b""
            
            # If we crossfade into the next one, steal the tail
            printable_len = total_len
            if has_next and total_len > self._crossfade_bytes:
                printable_len = total_len - self._crossfade_bytes
                self._overlap_pcm = segment_pcm[printable_len:]
            else:
                self._overlap_pcm = b""

            # Push the playable part in chunks
            while pos < printable_len:
                if getattr(self, "_skip_current", False):
                    self._skip_current = False
                    self._overlap_pcm = b"" # cancel crossfade if skipping
                    break
                
                chunk = segment_pcm[pos : pos + chunk_bytes]
                if not chunk: break
                
                # Check if we've reached the printable limit
                if pos + len(chunk) > printable_len:
                    chunk = segment_pcm[pos : printable_len]
                
                self._ffmpeg.stdin.write(chunk)
                self._ffmpeg.stdin.flush()
                
                pos += len(chunk)
                bytes_written += len(chunk)

                # Pace
                expected_wall_time = start_wall_time + (bytes_written / self._bytes_per_sec)
                now = time.time()
                sleep_dur = (expected_wall_time - now) - 0.1
                if sleep_dur > 0:
                    time.sleep(sleep_dur)

            return bytes_written > 0

        except Exception as e:
            logger.warning(f"Failed pushing segment to encoder: {e}")
            return False

    def _push_silence(self, seconds: float) -> None:
        if not self._ffmpeg or not self._ffmpeg.stdin:
            return

        frames = int(self._sample_rate * max(seconds, 0.05))
        total_bytes = frames * self._channels * self._sample_width_bytes
        buf = b"\x00" * total_bytes

        # Use paced writer to keep media-time aligned with wall-clock.
        self._write_pcm_realtime(buf)


class Metadata:
    def __init__(self):
        self._current_dj: str = ""
        self._current_track: str = ""
        self._segment_type: str = "music"
        self._energy: float = 0.5

    def update(
        self,
        dj: str = "",
        track: str = "",
        segment_type: str = "music",
        energy: float = 0.5,
    ) -> None:
        self._current_dj = dj
        self._current_track = track
        self._segment_type = segment_type
        self._energy = energy

    def to_dict(self) -> dict:
        from datetime import datetime

        return {
            "type": "now_playing",
            "dj": self._current_dj,
            "track": self._current_track,
            "segment_type": self._segment_type,
            "energy": self._energy,
            "timestamp": int(datetime.utcnow().timestamp()),
        }
