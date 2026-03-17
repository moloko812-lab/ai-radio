import asyncio
import os
# TEST WRITE
import logging
import random
from collections import deque
from datetime import datetime, timedelta
import sys
from pathlib import Path
from typing import List, Dict, Optional, Any, Union, Tuple
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def _run_in_executor(func, *args):
    """Cross-platform replacement for asyncio.to_thread (Python 3.9+)."""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, lambda: func(*args))


from services.orchestrator.state_machine import StateMachine
from services.orchestrator.buffer_monitor import BufferMonitor
from services.orchestrator.config import config
from services.dialogue.planner import DialoguePlanner
from services.dialogue.program_engine import ProgramEngine
from services.music.planner import build_music_schedule, TrackSlot
from services.music.library import library
from services.tts_kokoro.worker import TTSWorker
from services.render.renderer import AudioRenderer
from shared.models import State, RenderedSegment, SegmentType
from shared import runtime_state

# Ensure logs directory exists
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/orchestrator.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self):
        self.state_machine = StateMachine()
        self.buffer: Optional[BufferMonitor] = None
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._kokoro_process = None

        # Workers will be initialized after config is loaded
        self._planner: DialoguePlanner = None
        self._tts: TTSWorker = None
        self._renderer: AudioRenderer = None

        # Current working data flowing through the pipeline
        self._music_schedule: List[TrackSlot] = []
        self._music_schedule_idx: int = 0
        self._last_music_track_path: Optional[str] = None
        self._current_track_info: Optional[Dict] = None
        self._last_bg_track_path: Optional[str] = None
        self._force_next_track: bool = False
        self._engine = ProgramEngine()
        self._last_cleanup_time: float = 0.0

        self._last_news_hour: int = datetime.now().hour

        # Current working data flowing through the pipeline
        self._current_script = None
        self._current_tts_results = None
        self._show_id = ""

        self._force_greeting = True

        # Live transcript (in-memory, last N lines)
        self.transcript = deque(maxlen=200)

        self._silent_track_history = []
        self._sparse_track_count = 0

    async def start(self) -> None:
        logger.info("Starting orchestrator...")
        config.load("config.yaml")

        # Start Kokoro TTS service
        await self._start_kokoro_service()

        # Buffer thresholds depend on config — initialize after load
        self.buffer = BufferMonitor()

        # Initialize workers
        self._planner = DialoguePlanner()
        self._tts = TTSWorker(cache_dir=config.get("tts.cache_dir", "./cache/tts"))
        self._renderer = AudioRenderer(output_dir="./output/segments")

        self._show_id = f"show_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"

        # Initialize library and sync in background to not block startup
        async def _sync_bg():
            try:
                # Import here to avoid circular dependencies
                from services.music.library import library

                folders = set(config.get("music.folders", []) or [])
                programs = config.get("schedule.programs", []) or []
                for prog in programs:
                    p_folders = prog.get("music_folders", [])
                    if isinstance(p_folders, list):
                        folders.update(p_folders)

                cleaned_folders = [f for f in folders if f]
                if cleaned_folders:
                    logger.info(
                        f"Syncing library with folders in background (threaded): {cleaned_folders}"
                    )
                    # CRITICAL: Use run_in_executor to avoid blocking the asyncio event loop!
                    await _run_in_executor(
                        library.sync_external_folders, cleaned_folders
                    )
                    await _run_in_executor(
                        library.prune_orphaned_tracks, cleaned_folders
                    )
                    logger.info("Library sync and prune complete.")
            except Exception as e:
                logger.error(f"Failed to sync music library: {e}")

        asyncio.create_task(_sync_bg())

        self._rebuild_music_schedule(60)
        try:
            runtime_state.music_schedule = self.get_hour_schedule()
        except Exception:
            pass

        # Expose this orchestrator instance for other in-process servers
        runtime_state.orchestrator = self

        self.state_machine.transition(State.WARMUP)
        await self._warmup()

        # Rebuild schedule after warmup/sync to ensure we picked up new tracks
        self._rebuild_music_schedule(60)
        try:
            runtime_state.music_schedule = self.get_hour_schedule()
        except Exception:
            pass

        self._running = True
        self._tasks.append(asyncio.create_task(self._run_loop()))
        self._tasks.append(asyncio.create_task(self._buffer_monitor_loop()))

        logger.info("Orchestrator started — pipeline is live")

    async def _start_kokoro_service(self) -> None:
        import httpx
        kokoro_url = config.get("tts.kokoro_url", "http://localhost:8003")

        logger.info(f"Connecting to Kokoro TTS service at {kokoro_url}...")
        
        # Wait for service to be ready (it's started by start_radio.py)
        for i in range(30):
            try:
                async with httpx.AsyncClient(timeout=2) as client:
                    resp = await client.get(f"{kokoro_url}/health")
                    if resp.status_code == 200:
                        logger.info("Kokoro TTS service is READY")
                        return
            except Exception:
                pass
            
            if i % 5 == 0:
                logger.info(f"Waiting for Kokoro TTS service to respond... ({i+1}/30)")
            await asyncio.sleep(2)
        
        logger.warning("Kokoro TTS service did not respond. Will try to continue anyway.")

    async def restart_kokoro(self) -> None:
        """Restart the Kokoro TTS service process."""
        logger.warning("Restarting Kokoro TTS service...")
        if self._kokoro_process:
            try:
                self._kokoro_process.terminate()
                # Wait a bit for it to die
                for _ in range(5):
                    if self._kokoro_process.poll() is not None:
                        break
                    await asyncio.sleep(1)
                if self._kokoro_process.poll() is None:
                    self._kokoro_process.kill()
            except Exception as e:
                logger.error(f"Error killing Kokoro process: {e}")
            self._kokoro_process = None

        await self._start_kokoro_service()

    async def stop(self) -> None:
        logger.info("Stopping orchestrator...")
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("Orchestrator stopped")

    async def _warmup(self) -> None:
        logger.info("Warming up — checking dependencies...")

        # Absolute root path to ensure we find assets
        root_dir = Path(__file__).parent.parent.parent.absolute()
        music_dir = root_dir / "assets" / "music"
        
        music_files = []
        if music_dir.exists():
            for ext in ["mp3", "wav", "ogg", "flac"]:
                music_files.extend(list(music_dir.rglob(f"*.{ext}")))
                music_files.extend(list(music_dir.rglob(f"*.{ext.upper()}")))
        
        logger.info(f"Music library: {len(music_files)} tracks found in {music_dir}")

        # Register found tracks in the music library object so they show up in UI
        if music_files:
            try:
                from services.music.library import library
                # Ensure the library sync sees these folders and adds tracks
                folders = list(set([str(p.parent.absolute()) for p in music_files]))
                # Filter out system or hidden folders if any
                folders = [f for f in folders if not f.split(os.sep)[-1].startswith('.')]
                await _run_in_executor(library.sync_external_folders, folders)
                logger.info(f"Library sync triggered for {len(folders)} folders.")
            except Exception as e:
                logger.error(f"Failed to sync tracks to library UI: {e}")
        else:
            logger.warning("!!! NO MUSIC FOUND !!! System will start in TALK-only mode.")

        # Check fallback directory
        fallback_dir = root_dir / "assets" / "fallback"
        fallback_files = list(fallback_dir.glob("*.mp3")) + list(fallback_dir.glob("*.wav"))
        logger.info(f"Fallback clips: {len(fallback_files)} clips")

        logger.info("Warmup complete")
        if self._force_greeting:
            logger.info("Initial greeting requested — transitioning to TALK_GEN")
            self.state_machine.transition(State.TALK_GEN)
        elif music_files:
            self.state_machine.transition(State.MUSIC_FILL)
        else:
            self.state_machine.transition(State.TALK_GEN)


    # ── main loops ────────────────────────────────────────────
    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self._process_state()
            except Exception as e:
                logger.error(f"Error in run loop: {e}", exc_info=True)
                self.state_machine.set_error(str(e))
                # Don't get stuck — wait and try to recover
                await asyncio.sleep(5)
                self.state_machine.clear_error()
                self.state_machine.transition(State.MUSIC_FILL)
            await asyncio.sleep(0.1)

    async def _buffer_monitor_loop(self) -> None:
        while self._running:
            try:
                status = self.buffer.get_status()
                self.state_machine.update_buffer(status)
                logger.info(
                    f"Buffer: {status.minutes_ahead:.1f} min "
                    f"({status.segments_count} segments, "
                    f"{status.total_duration_sec:.0f}s)  "
                    f"State: {self.state_machine.state.value}"
                )

                # Buffer generation trigger
                if self.state_machine.state == State.PUBLISH:
                    # Priority 1: Forced News/Talk blocks (e.g. hourly transitions)
                    # We trigger TALK_GEN only if handover is already handled or not needed
                    if getattr(self, "_force_news_block", False) and not getattr(
                        self, "_handover_pending", False
                    ):
                        logger.warning("Forcing TALK_GEN for hourly news")
                        self.state_machine.transition(State.TALK_GEN)

                    # Priority 2: Sparse Mode / Show logic
                    else:
                        curr_prog = config.current_program
                        prog_type = curr_prog.get("type", "music")

                        if prog_type == "talk":
                            # TALK shows are strictly scheduled via ProgramEngine
                            if not self.buffer.target_reached:
                                try:
                                    next_seg = self._engine.get_next_segment(show_id=self._show_id)
                                    if next_seg.kind == "talk":
                                        logger.info(f"Buffer Filling: NEXT IS TALK Segment '{next_seg.topic}'")
                                        self.state_machine.transition(State.TALK_GEN)
                                    else:
                                        self.state_machine.transition(State.MUSIC_FILL)
                                except Exception as e:
                                    logger.error(f"Program engine error: {e}")
                                    self.state_machine.transition(State.MUSIC_FILL)
                        else:
                            # MUSIC shows with Sparse Mode support
                            if config.talk_sparse_mode:
                                # Trigger talk if we reached the count, regardless of buffer fullness (to ensure variety)
                                if self._sparse_track_count >= 3:
                                    logger.info(f"═══ Sparse Mode Trigger: {self._sparse_track_count} tracks played, transitioning to TALK_GEN ═══")
                                    self._sparse_track_count = 0
                                    self.state_machine.transition(State.TALK_GEN)
                                elif not self.buffer.target_reached:
                                    # Increment only when we are actually adding a new music track to the buffer
                                    self._sparse_track_count += 1
                                    logger.info(f"Sparse Mode: Adding music track ({self._sparse_track_count}/3)")
                                    self.state_machine.transition(State.MUSIC_FILL)
                            elif not self.buffer.target_reached:
                                logger.info("Buffer target not reached, adding music segment")
                                self.state_machine.transition(State.MUSIC_FILL)

                # Check for hourly news trigger
                if config.get(
                    "hourly_news.enabled", False
                ) and self.state_machine.state in (State.MUSIC_FILL, State.PUBLISH):
                    # When will the buffer run out?
                    projected_end = datetime.now() + timedelta(
                        seconds=status.total_duration_sec
                    )

                    # If the end of the buffer crosses into a new hour (or we're at e.g. XX:59)
                    # Let's check if the minute is > 45 and we haven't generated news for the UPCOMING hour yet
                    # Actually, if projected_end.minute < 15 and we haven't generated news for projected_end.hour
                    # This means the playhead just crossed the hour boundary!

                    # We consider the news to belong to `projected_end.hour`
                    if projected_end.hour != self._last_news_hour:
                        # Guard against buffer oscillation double-triggering news
                        hour_key = f"{projected_end.strftime('%Y-%m-%d')}_{projected_end.hour}"
                        processed = getattr(self, "_processed_news_hours", set())
                        
                        if hour_key not in processed:
                            logger.info(f"Hourly News Trigger - Transitioning to hour {projected_end.hour}")
                            if not hasattr(self, "_processed_news_hours"):
                                self._processed_news_hours = set()
                            self._processed_news_hours.add(hour_key)
                            
                            # Cleanup old logs (keep last 48 items)
                            if len(self._processed_news_hours) > 48:
                                self._processed_news_hours = set(list(self._processed_news_hours)[-24:])

                            self._last_news_hour = projected_end.hour
                            self._news_target_time = projected_end
                            self._handover_pending = True
                            self._force_news_block = True
                            self._force_intro_block = True
                        else:
                            # Hour changed but already handled this session
                            self._last_news_hour = projected_end.hour
                        # The actual state transition to news will happen after the handover track is added
                        # We don't transition(State.TALK_GEN) here directly anymore to allow the handover track to be picked first

            except Exception as e:
                logger.error(f"Error in buffer monitor: {e}")

            await asyncio.sleep(5)

            # Periodic cleanup of old segment files and TTS cache (every 10 minutes)
            import time as _time

            now_t = _time.time()
            if now_t - self._last_cleanup_time > 600:
                self._last_cleanup_time = now_t
                try:
                    await _run_in_executor(self._cleanup_old_segment_files)
                except Exception as e:
                    logger.error(f"Segment cleanup error: {e}")
                try:
                    await _run_in_executor(self._tts.cleanup_cache)
                except Exception as e:
                    logger.error(f"TTS cache cleanup error: {e}")
                # Trim planner dialogue history to prevent memory growth
                try:
                    if len(self._planner._global_dialogue_history) > 50:
                        self._planner._global_dialogue_history = (
                            self._planner._global_dialogue_history[-20:]
                        )
                except Exception:
                    pass

    # ── state processing ──────────────────────────────────────
    async def _process_state(self) -> None:
        state = self.state_machine.state

        if state == State.TALK_GEN:
            await self._generate_talk()
        elif state == State.TTS_GEN:
            await self._generate_tts()
        elif state == State.RENDER:
            await self._render_segment()
        elif state == State.PUBLISH:
            # Nothing to do — buffer monitor will trigger next generation
            await asyncio.sleep(2)
        elif state == State.MUSIC_FILL:
            await self._fill_with_music()
        elif state == State.DEGRADED:
            await self._degraded_mode()

    # ── TALK_GEN ──────────────────────────────────────────────
    async def _generate_talk(self) -> None:
        is_greeting = getattr(self, "_force_greeting", False)
        is_news = getattr(self, "_force_news_block", False)
        is_intro = getattr(self, "_force_intro_block", False)

        if is_news:
            logger.info("═══ TALK_GEN: generating HOURLY NEWS script ═══")
            self._force_news_block = False
        elif is_greeting:
            logger.info("═══ TALK_GEN: generating INITIAL GREETING script ═══")
            self._force_greeting = False
        elif is_intro:
            logger.info("═══ TALK_GEN: generating NEW HOUR INTRO script ═══")
            self._force_intro_block = False
        else:
            logger.info("═══ TALK_GEN: generating dialogue script ═══")

        import time

        t0 = time.time()

        topics = config.topics or ["general"]

        curr_prog = config.current_program
        prog_type = curr_prog.get("type", "music")

        # Determine duration based on show type
        if prog_type == "talk":
            # TALK shows: use ProgramEngine for structured timeline
            try:
                planned_seg = self._engine.get_next_segment(show_id=self._show_id)
                if planned_seg.kind == "talk":
                    base_dur = planned_seg.duration_sec
                    duration = random.randint(int(base_dur * 0.9), int(base_dur * 1.1))
                    topics = [planned_seg.topic] if planned_seg.topic else topics
                    logger.info(
                        f"Using PLANNED talk segment: topic='{planned_seg.topic}' dur={duration}s"
                    )
                else:
                    duration = 60
            except Exception as e:
                logger.warning(f"Engine error: {e}. Using defaults.")
                if is_intro:
                    duration = 45
                else:
                    duration = random.randint(300, 600)
        else:
            # MUSIC shows: talk is only for news/intros — keep it short
            if is_intro:
                duration = 45
            else:
                duration = random.randint(
                    config.talk_min_duration, config.talk_max_duration
                )

        try:
            if is_greeting:
                from shared.models import ScriptBlock, ScriptLine
                sl_args = {
                    "speaker": "Olga",
                    "text": "A I Radio is live! All systems are operational and we are starting the broadcast.",
                    "voice": "af_sky"
                }
                # Defensive creation
                try:
                    sl_args["is_chat"] = False
                    line = ScriptLine(**sl_args)
                except TypeError:
                    sl_args.pop("is_chat", None)
                    line = ScriptLine(**sl_args)
                
                self._current_script = ScriptBlock(
                    show_id="init_boot",
                    block_id="greeting_v2",
                    lines=[line]
                )
                self.state_machine.transition(State.TTS_GEN)
                return

            if is_news:
                # Ensure we also consume intro flag since they are functionally merged if both are true
                if is_intro:
                    self._force_intro_block = False

                # ... existing news logic ... (I'll keep it as is in the next chunk)
                # ----------------
                # Custom news block generation
                # ----------------
                from services.news.news_fetcher import fetch_all_hourly_news
                from shared.models import ScriptBlock, ScriptLine

                dj_id = config.get("hourly_news.dj", "DJ_5")
                # We need the actual voice id from the list
                voice_id = "am_onyx"  # fallback
                for d in config.djs_config.get("list", []):
                    if d.get("id") == dj_id:
                        voice_id = d.get("voice", voice_id)

                target_dt = getattr(self, "_news_target_time", None)
                news_prompt = await fetch_all_hourly_news(config._config, target_time=target_dt)

                # Call LLM through the planner (unified wrapper)
                news_text = await self._planner._llm_generate(
                    news_prompt, role="model_a"
                )
                if not news_text:
                    news_text = "I'm having a bit of trouble reaching my news sources right now, but stay tuned!"

                news_text = news_text.strip()

                # Get display name for DJ
                spk_name = dj_id
                for d in config.djs_config.get("list", []):
                    if d.get("id") == dj_id:
                        spk_name = d.get("name", dj_id)
                        break

                sl_args = {
                    "speaker": dj_id,
                    "speaker_name": spk_name,
                    "text": news_text,
                    "source": "news",
                    "voice_id": voice_id,
                    "style": {"energy": 0.6, "pace": 1.0},
                }
                try:
                    sl_args["is_chat"] = False
                    line = ScriptLine(**sl_args)
                except TypeError:
                    sl_args.pop("is_chat", None)
                    line = ScriptLine(**sl_args)

                script_block = ScriptBlock(
                    show_id=self._show_id,
                    block_id=f"news_{int(t0)}",
                    target_duration_sec=60,
                    lines=[line],
                )
                self._current_script = script_block
                
                # Add to history so DJs know news happened
                self._planner._add_to_global_history(dj_id, news_text)

            else:
                # Normal or Intro talk
                if is_intro:
                    self._planner._is_intro = True
                    self._planner._is_handover = False
                else:
                    self._planner._is_intro = False
                    self._planner._is_handover = False

                script_block = await asyncio.wait_for(
                    self._planner.plan_block(
                        topics,
                        duration,
                        self._show_id,
                        current_track=self._get_current_track_info(),
                        next_track=self._get_next_track_info(),
                        program=curr_prog,
                        recent_tracks=self._silent_track_history
                    ),
                    timeout=600,
                )
                self._current_script = script_block
                self._silent_track_history = [] # Clear after talk

                # Reset flags
                self._planner._is_intro = False
            latency = (time.time() - t0) * 1000
            self.state_machine.update_latencies(llm=latency)
            logger.info(
                f"  Script ready: {len(self._current_script.lines)} lines, "
                f"target {self._current_script.target_duration_sec}s  "
                f"(LLM {latency:.0f}ms)"
            )

            # Update live transcript with the generated lines (UI wants a stream)
            ts = int(datetime.utcnow().timestamp())
            for line in self._current_script.lines:
                entry = {
                    "dj": line.speaker,
                    "speaker_name": line.speaker_name,
                    "source": line.source,
                    "text": line.text,
                    "timestamp": ts,
                }
                self.transcript.append(entry)
                runtime_state.transcript.append(entry)
            if self._current_script.lines:
                # Keep a "current DJ" for UI
                self.state_machine.set_current_track(
                    dj=self._current_script.lines[-1].speaker
                )

            self.state_machine.transition(State.TTS_GEN)
        except asyncio.TimeoutError:
            logger.error("  Script generation timed out — falling back to music")
            self.state_machine.transition(State.MUSIC_FILL)
        except Exception as e:
            logger.error(f"  Script generation error: {e}")
            self.state_machine.transition(State.MUSIC_FILL)

    def _apply_dj_voices_to_tts(self) -> None:
        if not self._tts:
            return
        try:
            tts_cfg = config.tts_config or {}
            if isinstance(tts_cfg, dict):
                voices_map = tts_cfg.get("voices")
                if isinstance(voices_map, dict):
                    self._tts._voices.update(voices_map)

            dj_list = config.get("djs.list", []) or []
            if isinstance(dj_list, list):
                for dj in dj_list:
                    if isinstance(dj, dict):
                        dj_id = (dj.get("id") or "").strip()
                        voice = (dj.get("voice") or "").strip()
                        if dj_id and voice:
                            self._tts._voices[dj_id.lower()] = voice
        except Exception as e:
            logger.error(f"Failed to apply DJ voices to TTS worker: {e}")

    # ── TTS_GEN ───────────────────────────────────────────────
    async def _generate_tts(self) -> None:
        if self._current_script is None or not self._current_script.lines:
            logger.warning("  No script to synthesize — skipping to MUSIC_FILL")
            self.state_machine.transition(State.MUSIC_FILL)
            return

        logger.info(
            f"═══ TTS_GEN: synthesizing {len(self._current_script.lines)} lines ═══"
        )
        import time

        t0 = time.time()

        try:
            self._apply_dj_voices_to_tts()

            # Check if previous attempt timed out
            if getattr(runtime_state, "kokoro_timed_out", False):
                logger.warning("  Detected Kokoro timeout state — triggering restart")
                runtime_state.kokoro_timed_out = False
                await self.restart_kokoro()

            self._current_tts_results = await asyncio.wait_for(
                self._tts.synthesize_batch(self._current_script.lines),
                timeout=300, # 5 minutes max
            )

            # Check if ANY of the lines timed out during this batch
            if getattr(runtime_state, "kokoro_timed_out", False):
                logger.warning(
                    "  Kokoro timed out during batch — triggering restart for future use"
                )
                runtime_state.kokoro_timed_out = False
                await self.restart_kokoro()

            latency = (time.time() - t0) * 1000
            self.state_machine.update_latencies(tts=latency)

            # Filter out results with missing files
            valid = [
                r for r in self._current_tts_results if Path(r.audio_path).exists()
            ]
            if not valid:
                logger.error("  All TTS outputs missing — falling back to music")
                self.state_machine.transition(State.MUSIC_FILL)
                return

            self._current_tts_results = valid
            logger.info(f"  TTS done: {len(valid)} audio files  (TTS {latency:.0f}ms)")
            self.state_machine.transition(State.RENDER)
        except asyncio.TimeoutError:
            logger.error(
                "  TTS batch timed out — restarting Kokoro and falling back to music"
            )
            runtime_state.kokoro_timed_out = False
            await self.restart_kokoro()
            self.state_machine.transition(State.MUSIC_FILL)
        except Exception as e:
            logger.error(f"  TTS error: {e}")
            self.state_machine.transition(State.MUSIC_FILL)

    # ── RENDER ────────────────────────────────────────────────
    async def _render_segment(self) -> None:
        if not self._current_script or not self._current_tts_results:
            logger.warning("  Nothing to render — skipping to PUBLISH")
            self.state_machine.transition(State.PUBLISH)
            return

        logger.info("═══ RENDER: mixing audio segment ═══")
        import time

        t0 = time.time()

        # Optionally pick a music bed
        dialogue_duration = 0.0
        try:
            dialogue_duration = float(
                sum(
                    getattr(r, "duration_sec", 0.0)
                    for r in (self._current_tts_results or [])
                )
            )
        except Exception:
            dialogue_duration = 0.0

        music_bed = self._pick_background_track(dialogue_duration)

        try:
            segment = await asyncio.wait_for(
                self._renderer.render_block(
                    self._current_script,
                    self._current_tts_results,
                    music_bed=music_bed,
                ),
                timeout=900,
            )
            latency = (time.time() - t0) * 1000
            self.state_machine.update_latencies(render=latency)

            if segment.duration_sec < 1.0:
                logger.warning(
                    f"  Segment too short ({segment.duration_sec:.1f}s) — discarding"
                )
            else:
                if self._current_script and self._current_script.block_id.startswith(
                    "news_"
                ):
                    segment.segment_type = SegmentType.NEWS
                    segment.track_title = "News Bulletin"
                    segment.track_artist = "News Desk"

                self.buffer.add_segment(segment)
                logger.info(
                    f"  Segment rendered: {segment.duration_sec:.1f}s  "
                    f"LUFS={segment.lufs:.1f}  "
                    f"(render {latency:.0f}ms)"
                )
                # Advance the program engine cursor as we just fulfilled a segment
                try:
                    self._engine.advance()
                except Exception as e:
                    logger.error(f"Engine advance failed: {e}")

                # Update now-playing metadata for the UI (talk blocks)
                if (
                    segment.segment_type == SegmentType.TALK
                    and self._current_script
                    and self._current_script.lines
                ):
                    self.state_machine.set_current_track(
                        dj=self._current_script.lines[-1].speaker, track="AI Talk"
                    )

            # Clear pipeline data
            self._current_script = None
            self._current_tts_results = None

            # Decide what to do next
            if self.buffer.target_reached:
                self.state_machine.transition(State.PUBLISH)
            else:
                curr_prog = config.current_program
                prog_type = curr_prog.get("type", "music")

                if prog_type == "talk":
                    # TALK shows: check engine for next segment type
                    try:
                        next_seg = self._engine.get_next_segment(show_id=self._show_id)
                        if next_seg.kind == "music":
                            logger.info(
                                f"  Post-render: engine says NEXT is MUSIC ({next_seg.duration_sec}s)"
                            )
                            self.state_machine.transition(State.MUSIC_FILL)
                        else:
                            logger.info(
                                f"  Post-render: engine says NEXT is TALK: '{next_seg.topic}' ({next_seg.duration_sec}s)"
                            )
                            self.state_machine.transition(State.TALK_GEN)
                    except Exception:
                        self.state_machine.transition(State.PUBLISH)
                else:
                    # MUSIC shows: after a rare talk block (news/intro), go back to music
                    self.state_machine.transition(State.MUSIC_FILL)

        except Exception as e:
            logger.error(f"  Render error: {e}")
            self._current_script = None
            self._current_tts_results = None
            # Do NOT always transition to MUSIC_FILL here.
            # Let the buffer monitor decide the next state in the next cycle.
            self.state_machine.transition(State.PUBLISH)

    # ── MUSIC_FILL ────────────────────────────────────────────
    async def _fill_with_music(self) -> None:
        logger.info("═══ MUSIC_FILL: adding music to buffer ═══")

        music_file, track_info = self._pick_scheduled_music_track_with_info()
        if not music_file:
            logger.warning("  No music files available — generating silent placeholder")
            await self._generate_silence_segment(10)
            self.state_machine.transition(State.PUBLISH)
            return

        show_type = track_info.get("show_type", "regular") if track_info else "regular"
        has_dj_intro = track_info.get("has_dj_intro", True) if track_info else True
        has_dj_outro = track_info.get("has_dj_outro", True) if track_info else True
        long_monologue = (
            track_info.get("long_monologue", False) if track_info else False
        )

        try:
            output_path = (
                Path("./output/segments")
                / f"music_{datetime.utcnow().strftime('%M%S%f')}.wav"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)

            logger.debug(
                f"  Measuring loudness of {music_file.name} for transparent normalization..."
            )
            # Switch back to static gain for transparency (no pumping)
            lufs, peak = await self._renderer._get_loudness_metrics(str(music_file))
            
            # target = -14 LUFS. If track is -18 LUFS, gain is +4dB.
            gain_offset = config.target_loudness - lufs
            
            # Safety: cap gain if peak is already high to avoid clipping before limiter
            if (peak + gain_offset) > 0:
                gain_offset = -peak - 0.5  # target -0.5dB peak
                
            filter_str = f"volume={gain_offset:.2f}dB,alimiter=limit=-1.0dB"

            cmd = [
                "ffmpeg",
                "-y",
                "-i",
                str(music_file),
                "-af",
                filter_str,
                "-ar",
                "48000",
                "-ac",
                "2",
                str(output_path),
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()

            if proc.returncode != 0:
                logger.error(f"  Music transcode failed: {stderr.decode()[:200]}")
                await self._generate_silence_segment(5)
            else:
                genre = self._renderer._detect_genre_from_filename(music_file.name)
                output_path_with_eq = await self._renderer.apply_eq_to_music(
                    str(output_path), genre
                )

                duration = self._renderer._get_duration(str(output_path_with_eq))

                # Prepare base segment info
                seg_id = f"music_{datetime.utcnow().strftime('%M%S%f')}"
                track_title = (
                    track_info.get("title", music_file.stem)
                    if track_info
                    else music_file.stem
                )
                track_artist = track_info.get("artist", "") if track_info else ""
                track_id = track_info.get("id", "") if track_info else ""
                script_lines = []

                # Generate DJ outro and mix it (Optional - wrap in another try block)
                if duration and duration > 40 and has_dj_outro:
                    logger.info(
                        f"Generating DJ outro to mix at end of {music_file.stem}"
                    )
                    try:
                        topics = config.topics or ["general"]
                        dj_duration = config.dj_outro_duration_sec

                        current_track = {"title": track_title, "artist": track_artist}
                        next_track_info = self._get_next_track_info()

                        is_handover_time = getattr(
                            self, "_handover_pending", False
                        ) or getattr(self, "_is_last_track_of_hour", False)
                        if is_handover_time:
                            self._planner._is_handover = True
                            self._planner._next_program = config.get_next_program()
                            self._handover_pending = False
                            self._is_last_track_of_hour = False
                            self._goto_news_after_this = True
                        else:
                            self._planner._is_handover = False

                        script = await asyncio.wait_for(
                            self._planner.plan_block(
                                topics,
                                dj_duration,
                                self._show_id,
                                current_track=current_track,
                                next_track=next_track_info,
                                program=config.current_program,
                            ),
                            timeout=90,  # Increased timeout for lyrics analysis
                        )
                        self._planner._is_handover = False

                        if script and script.lines:
                            self._apply_dj_voices_to_tts()
                            tts_results = await asyncio.wait_for(
                                self._tts.synthesize_batch(script.lines),
                                timeout=120,
                            )
                            valid = [
                                r for r in tts_results if Path(r.audio_path).exists()
                            ]
                            if valid:
                                tts_concat_path = str(
                                    Path(output_path_with_eq).parent
                                    / f"tts_{datetime.utcnow().strftime('%M%S%f')}.wav"
                                )
                                await self._renderer._render_simple(
                                    [r.audio_path for r in valid], tts_concat_path
                                )
                                tts_dur = self._renderer._get_duration(tts_concat_path)

                                if tts_dur > 0:
                                    mix_path = str(output_path_with_eq).replace(
                                        ".wav", "_mixed.wav"
                                    )
                                    overlap_sec = config.dj_intro_before_end_sec
                                    if show_type != "talk" and tts_dur > 15:
                                        overlap_sec = min(tts_dur, duration - 5)

                                    delay = max(0, duration - overlap_sec)
                                    delay_ms = int(delay * 1000)
                                    fade_dur = 3.0
                                    duck_start = max(0, delay - fade_dur)
                                    duck_end = delay + tts_dur
                                    duck_vol = 0.15
                                    duck_drop = 1.0 - duck_vol

                                    # Smooth ducking: fade down before talk, stay at duck_vol during talk, fade back up after talk.
                                    # We extend the ducking period to allow a 2s fade up after the voice ends.
                                    fade_up_dur = 2.0
                                    duck_end_total = duck_end + fade_up_dur
                                    
                                    v_expr = (
                                        f"if(lt(t,{duck_start}),1,"
                                        f"if(lt(t,{duck_start}+{fade_dur}),1-{duck_drop}*(t-{duck_start})/{fade_dur},"
                                        f"if(lt(t,{duck_end}),{duck_vol},"
                                        f"if(lt(t,{duck_end_total}),{duck_vol}+{duck_drop}*(t-{duck_end})/{fade_up_dur},1))))"
                                    )

                                    cmd_mix = [
                                        "ffmpeg",
                                        "-y",
                                        "-i",
                                        output_path_with_eq,
                                        "-i",
                                        tts_concat_path,
                                        "-filter_complex",
                                        # Boost DJ voice by 4dB to ensure it's heard well over music
                                        f"[0:a]volume='{v_expr}':eval=frame[m]; "
                                        f"[1:a]adelay={delay_ms}|{delay_ms},volume=1.5[v]; "
                                        f"[m][v]amix=inputs=2:duration=longest:normalize=0,alimiter=limit=-1.0dB[out]",
                                        "-map",
                                        "[out]",
                                        "-c:a",
                                        "pcm_s16le",
                                        "-ar",
                                        str(config.tts_sample_rate),
                                        "-ac",
                                        "2",
                                        mix_path,
                                    ]

                                    proc_mix = await asyncio.create_subprocess_exec(
                                        *cmd_mix,
                                        stdout=asyncio.subprocess.DEVNULL,
                                        stderr=asyncio.subprocess.DEVNULL,
                                    )
                                    await proc_mix.communicate()

                                    if proc_mix.returncode == 0:
                                        output_path_with_eq = mix_path
                                        duration = self._renderer._get_duration(
                                            mix_path
                                        )
                                        curr_t = float(delay)
                                        for r in valid:
                                            script_lines.append(
                                                {
                                                    "speaker": r.line.speaker,
                                                    "speaker_name": r.line.speaker_name,
                                                    "text": r.line.text,
                                                    "start": round(curr_t, 2),
                                                    "duration": round(
                                                        r.duration_sec, 2
                                                    ),
                                                }
                                            )
                                            curr_t += r.duration_sec
                                tts_concat_path_obj = Path(tts_concat_path)
                                if tts_concat_path_obj.exists():
                                    tts_concat_path_obj.unlink()
                    except Exception as e:
                        logger.error(
                            f"  DJ generation failed for this track, playing clean: {e}"
                        )

                # Final check and add to buffer
                if duration and duration > 0:
                    segment = RenderedSegment(
                        segment_id=seg_id,
                        segment_type=SegmentType.MUSIC,
                        file_path=str(output_path_with_eq),
                        duration_sec=duration,
                        lufs=config.target_loudness,
                        peak=-1.0,
                        transcript=f"[Music: {music_file.name}]",
                        show_type=show_type,
                        track_title=track_title,
                        track_artist=track_artist,
                        track_id=track_id,
                        metadata={"script_lines": script_lines},
                    )

                    if segment.track_id:
                        try:
                            count = runtime_state.status_data.get("listeners_count", 0)
                            library.record_play(segment.track_id, count)
                        except Exception as e:
                            logger.error(f"Error recording play statistics: {e}")

                    self.buffer.add_segment(segment)
                    logger.info(f"  Music added: {music_file.name} ({duration:.1f}s)")
                    self.state_machine.set_current_track(track=music_file.stem)

                    # Advance the program engine cursor after adding music
                    try:
                        self._engine.advance()
                    except Exception as e:
                        logger.error(f"Engine advance failed after music: {e}")
        except Exception as e:
            logger.error(f"  Critical music fill error: {e}")

        if getattr(self, "_goto_news_after_this", False):
            self._goto_news_after_this = False
            self.state_machine.transition(State.TALK_GEN)
        else:
            self.state_machine.transition(State.PUBLISH)

    # ── DEGRADED ──────────────────────────────────────────────
    async def _degraded_mode(self) -> None:
        logger.warning("═══ DEGRADED MODE — using fallback content ═══")

        fallback_dir = Path("./assets/fallback")
        files = list(fallback_dir.glob("*.wav")) + list(fallback_dir.glob("*.mp3"))

        if files:
            fb = random.choice(files)
            duration = self._renderer._get_duration(str(fb))
            segment = RenderedSegment(
                segment_id=f"fallback_{datetime.utcnow().strftime('%M%S%f')}",
                segment_type=SegmentType.FALLBACK,
                file_path=str(fb),
                duration_sec=duration or 5.0,
                lufs=config.target_loudness,
                peak=-1.0,
                transcript="[Fallback audio]",
            )
            self.buffer.add_segment(segment)

        await asyncio.sleep(1)
        self.state_machine.clear_error()

        # If we just finished a handover track, go to NEWS/TALK_GEN
        if getattr(self, "_goto_news_after_this", False):
            self._goto_news_after_this = False
            self.state_machine.transition(State.TALK_GEN)
        else:
            self.state_machine.transition(State.MUSIC_FILL)

    # ── helpers ───────────────────────────────────────────────
    def _cleanup_old_segment_files(self) -> None:
        """Delete rendered segment WAV files older than 15 minutes.

        This prevents unbounded disk growth (~1.5 GB/hour) which was
        the root cause of stream crashes every 1-2 hours.
        """
        import time as _time

        segments_dir = Path("./output/segments")
        if not segments_dir.exists():
            return

        # Collect file paths that are still in the buffer (don't delete those!)
        protected_paths = set()
        if self.buffer:
            for seg in self.buffer._segments:
                protected_paths.add(str(Path(seg.file_path).resolve()))

        cutoff = _time.time() - 900  # 15 minutes ago
        deleted_count = 0
        freed_bytes = 0

        for f in segments_dir.iterdir():
            if f.suffix not in (".wav", ".mp3", ".ogg"):
                continue
            try:
                resolved = str(f.resolve())
                if resolved in protected_paths:
                    continue
                stat = f.stat()
                if stat.st_mtime < cutoff:
                    size = stat.st_size
                    f.unlink()
                    deleted_count += 1
                    freed_bytes += size
            except Exception:
                continue

        if deleted_count > 0:
            logger.info(
                f"Cleanup: deleted {deleted_count} old segment files, freed {freed_bytes / (1024 * 1024):.1f} MB"
            )

        # Also trim tts_log.txt if it gets too large (>5MB)
        try:
            log_path = Path("tts_log.txt")
            if log_path.exists() and log_path.stat().st_size > 5 * 1024 * 1024:
                lines = log_path.read_text(
                    encoding="utf-8", errors="ignore"
                ).splitlines()
                # Keep only the last 500 lines
                log_path.write_text("\n".join(lines[-500:]) + "\n", encoding="utf-8")
                logger.info("Trimmed tts_log.txt to last 500 lines")
        except Exception:
            pass

    def _music_dirs_from_config(self) -> List[str]:
        # Prefer program-specific configured music folders; fallback to global; fallback to assets/music
        dirs = []
        try:
            curr_prog = config.current_program or {}
            cfg_dirs = curr_prog.get("music_folders", None)
            
            # If not in program, or empty list in program, look at global music.folders
            if not cfg_dirs or not isinstance(cfg_dirs, list) or len(cfg_dirs) == 0:
                cfg_dirs = config.get("music.folders", None)
            
            # Legacy/other global fallback
            if not cfg_dirs:
                cfg_dirs = config.get("music", {}).get("folders", None)

            if isinstance(cfg_dirs, list):
                dirs = [str(x) for x in cfg_dirs if isinstance(x, str) and x.strip()]
            
            # Filter non-existent dirs to avoid wasting time and failing on Windows paths on Ubuntu
            valid_dirs = []
            for d in dirs:
                if Path(d).exists():
                    valid_dirs.append(d)
                else:
                    logger.warning(f"[MUSIC_SCAN] Folder from config DOES NOT EXIST: {d}")
            dirs = valid_dirs

            logger.info(f"[MUSIC_SCAN] Resolved {len(dirs)} valid dirs (Current Prog: {bool(curr_prog)})")
        except Exception as e:
            logger.error(f"[MUSIC_SCAN] Error resolving dirs: {e}")
            dirs = []

        if not dirs:
            # Absolute path from root to assets/music as a final fallback
            fallback = str(Path(__file__).parent.parent.parent / "assets" / "music")
            if Path(fallback).exists():
                dirs = [fallback]
            else:
                dirs = ["./assets/music"]
        return dirs

    def _rebuild_music_schedule(self, duration_minutes: int = 60) -> None:
        try:
            self._music_schedule = build_music_schedule(
                duration_minutes, self._music_dirs_from_config()
            )
            self._music_schedule_idx = 0
            self._last_music_track_path = None
        except Exception as e:
            logger.error(f"Failed to build music schedule: {e}")
            self._music_schedule = []
            self._music_schedule_idx = 0

    def _get_current_track_info(self) -> Optional[Dict]:
        # Last track that was actually played (or None if unknown)
        try:
            return getattr(self, "_current_track_info", None)
        except Exception:
            return None

    def _get_next_track_info(self) -> Optional[Dict]:
        # Next scheduled music track
        try:
            if self._music_schedule and 0 <= self._music_schedule_idx < len(
                self._music_schedule
            ):
                s = self._music_schedule[self._music_schedule_idx]
                return {
                    "id": s.id,
                    "track_path": s.track_path,
                    "title": s.title,
                    "artist": s.artist,
                    "duration": s.duration,
                    "start_time": s.start_time,
                    "end_time": s.end_time,
                }
        except Exception:
            pass
        return None

    def get_hour_schedule(self) -> List[Dict]:
        out: List[Dict] = []
        try:
            for i, s in enumerate(self._music_schedule or []):
                out.append(
                    {
                        "index": i,
                        "start": float(s.start_time),
                        "end": float(s.end_time),
                        "type": "MUSIC",
                        "title": s.title,
                        "artist": s.artist,
                        "track_path": s.track_path,
                        "duration": float(s.duration),
                        # The current index points to the NEXT track to play,
                        # so the currently playing one is usually index - 1
                        "is_current": bool(i == (self._music_schedule_idx - 1)),
                    }
                )
        except Exception:
            pass
        return out

    def regenerate_hour_schedule(self) -> None:
        self._rebuild_music_schedule(60)

    def skip_current_track(self) -> None:
        if self._music_schedule_idx < len(self._music_schedule):
            self._music_schedule_idx += 1
            if self._music_schedule_idx >= len(self._music_schedule):
                self._music_schedule_idx = len(self._music_schedule)

        from shared import runtime_state

        if getattr(runtime_state, "streamer", None):
            runtime_state.streamer.skip_current()
        if self.buffer:
            self.buffer.clear()

    def force_next_track(self) -> None:
        # Skip and request an immediate transition to music fill.
        self.skip_current_track()
        self._force_next_track = True
        self.state_machine.transition(State.MUSIC_FILL)

    def decide_background(self, dialogue_duration: float) -> bool:
        return float(dialogue_duration or 0.0) >= 15.0

    def _pick_background_track(self, dialogue_duration: float) -> Optional[str]:
        if not self.decide_background(dialogue_duration):
            return None
        bg_dir = Path("./assets/Background")
        files = []
        if bg_dir.exists():
            files.extend(list(bg_dir.glob("*.mp3")))
            files.extend(list(bg_dir.glob("*.wav")))
            files.extend(list(bg_dir.glob("*.ogg")))
        if not files:
            return None
        # no immediate repeats
        files_sorted = sorted(files)
        pick = random.choice(files_sorted)
        if self._last_bg_track_path and len(files_sorted) > 1:
            attempts = 0
            while str(pick) == str(self._last_bg_track_path) and attempts < 5:
                pick = random.choice(files_sorted)
                attempts += 1
        self._last_bg_track_path = str(pick)
        return str(pick)

    def _pick_scheduled_music_track(self) -> Optional[Path]:
        path, _ = self._pick_scheduled_music_track_with_info()
        return path

    def _pick_scheduled_music_track_with_info(
        self,
    ) -> Tuple[Optional[Path], Optional[Dict]]:
        try:
            curr_prog = config.current_program
            prog_title = curr_prog.get("title", "Unknown")

            if getattr(self, "_current_program_title", None) != prog_title:
                logger.info(
                    f"Program changed to '{prog_title}', rebuilding schedule..."
                )
                self._current_program_title = prog_title
                self._rebuild_music_schedule(60)

            if self._force_next_track:
                self._force_next_track = False

            if (
                not self._music_schedule
                or self._music_schedule_idx >= len(self._music_schedule)
            ) and not self._force_next_track:
                logger.info(
                    "Music schedule reached the end — rebuilding for another hour..."
                )
                self._rebuild_music_schedule(60)

            if self._music_schedule and 0 <= self._music_schedule_idx < len(
                self._music_schedule
            ):
                # Check if this is the last track of the hour schedule
                if self._music_schedule_idx == len(self._music_schedule) - 1:
                    self._is_last_track_of_hour = True
                else:
                    self._is_last_track_of_hour = False

                slot = self._music_schedule[self._music_schedule_idx]
                p = Path(slot.track_path)
                if p.exists():
                    self._music_schedule_idx += 1
                    track_info = {
                        "id": slot.id,
                        "track_path": slot.track_path,
                        "title": slot.title,
                        "artist": slot.artist,
                        "duration": slot.duration,
                        "start_time": slot.start_time,
                        "end_time": slot.end_time,
                        "show_type": slot.show_type,
                        "has_dj_intro": slot.has_dj_intro,
                        "has_dj_outro": slot.has_dj_outro,
                        "long_monologue": slot.long_monologue,
                    }

                    # --- Sparse mode logic: songs play clean, history is recorded for standalone Talk Block ---
                    if config.talk_sparse_mode:
                        track_info["has_dj_intro"] = False
                        track_info["has_dj_outro"] = False
                        self._silent_track_history.append(track_info)
                        logger.debug(f"Sparse Mode: '{slot.title}' will play clean (added to history)")

                    self._current_track_info = track_info
                    self._last_music_track_path = slot.track_path
                    return p, track_info
        except Exception as e:
            logger.error(f"Scheduled music pick failed: {e}")

        p = self._pick_music_track()
        if (
            p
            and self._last_music_track_path
            and str(p) == str(self._last_music_track_path)
        ):
            p2 = self._pick_music_track()
            if p2 and str(p2) != str(self._last_music_track_path):
                p = p2
        if p:
            self._last_music_track_path = str(p)
            self._current_track_info = {
                "track_path": str(p),
                "title": p.stem,
                "artist": "",
                "duration": 0.0,
            }
        return p, self._current_track_info

    def _pick_music_track(self) -> Optional[Path]:
        music_dirs = self._music_dirs_from_config()
        extensions = (".mp3", ".wav", ".ogg", ".flac", ".m4a")

        files = []
        for d in music_dirs:
            music_dir = Path(d)
            if not music_dir.exists():
                continue
            for ext in extensions:
                files.extend(list(music_dir.rglob(f"*{ext}")))
        return random.choice(files) if files else None

    async def _generate_silence_segment(self, duration_sec: float = 5) -> None:
        """Generate a short silent WAV as a placeholder."""
        output_path = (
            Path("./output/segments")
            / f"silence_{datetime.utcnow().strftime('%M%S%f')}.wav"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)

        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"anullsrc=r={config.tts_sample_rate}:cl=stereo",
            "-t",
            str(duration_sec),
            str(output_path),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()

        segment = RenderedSegment(
            segment_id=f"silence_{datetime.utcnow().strftime('%M%S%f')}",
            segment_type=SegmentType.FALLBACK,
            file_path=str(output_path),
            duration_sec=duration_sec,
            lufs=-60.0,
            peak=-60.0,
            transcript="[Silence placeholder]",
        )
        self.buffer.add_segment(segment)


async def main():
    orchestrator = Orchestrator()
    await orchestrator.start()

    try:
        while True:
            await asyncio.sleep(10)
    except KeyboardInterrupt:
        await orchestrator.stop()


if __name__ == "__main__":
    asyncio.run(main())
