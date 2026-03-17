import asyncio
from typing import List, Dict, Any, Optional
import hashlib
import json
import logging
import subprocess
from pathlib import Path
from datetime import datetime
import sys
import re
import httpx

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from shared.models import ScriptLine, TTSResult
from services.orchestrator.config import config

logger = logging.getLogger(__name__)


class TTSWorker:
    """TTS worker that calls a Kokoro TTS HTTP service (example.py on port 8001).

    Fallback chain:
    1. Kokoro HTTP API (POST /tts) — best quality
    2. pyttsx3 (Windows SAPI) — acceptable quality
    3. ffmpeg sine tone — placeholder only
    """

    def __init__(self, cache_dir: str = "./cache/tts"):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._tts_config = config.tts_config
        self._sample_rate = self._tts_config.get("sample_rate", 24000)  # Kokoro uses 24 kHz
        self._voices = self._tts_config.get("voices", {"dj_a": "af_sarah", "dj_b": "am_michael"})
        self._speed = self._tts_config.get("speed", 1.0)
        self._cache_enabled = self._tts_config.get("cache_enabled", True)

        # Kokoro TTS service URL (example.py)
        self._kokoro_url = self._tts_config.get("kokoro_url", "http://localhost:8003")
        self._client = None
        self._vosk_client = None  # Persistent Vosk client
        self._loop = None
        self._last_cache_cleanup = 0.0
        self._cache_max_files = 500  # Keep at most 500 cached TTS files
        self._cache_max_age_sec = 7200  # Delete cache entries older than 2 hours

    def _get_client(self):
        """Get or create a persistent httpx client."""
        # Ensure we are in the correct event loop (important for threaded environments)
        current_loop = asyncio.get_event_loop()
        if self._client is None or self._loop != current_loop:
            if self._client:
                # Close old client if loop changed
                asyncio.create_task(self._client.aclose())
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(400, connect=10, read=300),
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
            )
            self._loop = current_loop
        return self._client

    def _log_to_test_file(self, msg: str):
        try:
            from datetime import datetime
            with open("tts_log.txt", "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ⚙️ TTS: {msg}\n")
        except:
            pass

    def _get_vosk_client(self):
        """Get or create a persistent httpx client for Vosk."""
        current_loop = asyncio.get_event_loop()
        if self._vosk_client is None or self._loop != current_loop:
            if self._vosk_client:
                try:
                    asyncio.create_task(self._vosk_client.aclose())
                except Exception:
                    pass
            self._vosk_client = httpx.AsyncClient(
                timeout=httpx.Timeout(300, connect=5, read=300),
                limits=httpx.Limits(max_keepalive_connections=3, max_connections=5)
            )
        return self._vosk_client

    def cleanup_cache(self) -> None:
        """Remove old TTS cache files to prevent unbounded disk growth.
        
        The cache was growing to 1.4 GB+ after 6 hours with 3900+ files,
        causing system slowdowns and crashes.
        """
        import time as _time
        try:
            files = list(self._cache_dir.glob("*.wav"))
            if len(files) <= self._cache_max_files:
                return
            
            # Sort by modification time (oldest first)
            files_with_time = []
            for f in files:
                try:
                    files_with_time.append((f, f.stat().st_mtime))
                except Exception:
                    continue
            
            files_with_time.sort(key=lambda x: x[1])
            
            cutoff = _time.time() - self._cache_max_age_sec
            deleted = 0
            freed = 0
            
            # Delete files that are either too old or exceed the max count
            files_to_keep = self._cache_max_files
            files_to_delete = len(files_with_time) - files_to_keep
            
            for f, mtime in files_with_time:
                if files_to_delete <= 0 and mtime >= cutoff:
                    break
                try:
                    size = f.stat().st_size
                    f.unlink()
                    deleted += 1
                    freed += size
                    files_to_delete -= 1
                except Exception:
                    continue
            
            if deleted > 0:
                logger.info(f"TTS cache cleanup: deleted {deleted} files, freed {freed / (1024*1024):.1f} MB")
        except Exception as e:
            logger.error(f"TTS cache cleanup error: {e}")

    async def synthesize(self, line: ScriptLine) -> TTSResult:
        # Determine language for TTS
        text = getattr(line, "text", "")
        # Always check for Cyrillic letters.
        # - If Cyrillic is present, we must invoke Russian TTS (Vosk).
        # - If no Cyrillic (e.g., pure English or numbers), we must invoke English TTS (Kokoro).
        # This prevents sending pure English text to a Russian model that can't read it.
        is_ru = bool(re.search(r'[а-яА-ЯёЁ]', text))

        if is_ru:
            voice_id = self._resolve_vosk_voice_id(line)
        else:
            voice_id = self._resolve_voice_id(line)

        cache_key = self._get_cache_key(line, str(voice_id))
        cached_path = self._cache_dir / f"{cache_key}.wav"

        if self._cache_enabled and cached_path.exists():
            logger.info(f"Cache hit TTS: speaker={line.speaker} voice={voice_id} is_ru={is_ru} text={line.text[:40]}...")
            return TTSResult(
                line=line,
                audio_path=str(cached_path),
                duration_sec=self._get_duration(str(cached_path)),
                sample_rate=self._sample_rate,
            )

        output_path = str(cached_path)

        logger.info(f"TTS request: speaker={line.speaker} voice={voice_id} is_ru={is_ru} text={line.text[:60]}...")

        # Try backends in order
        result = None

        if is_ru:
            # 1a. Vosk HTTP API
            if result is None:
                result = await self._try_vosk_http(line.text, int(voice_id), output_path)
        else:
            # 1b. Kokoro HTTP API
            if result is None:
                result = await self._try_kokoro_http(line.text, str(voice_id), output_path)

        # 2. pyttsx3 (Windows SAPI)
        if result is None:
            result = await self._try_pyttsx3(line.text, voice_id, output_path)

        # 3. ffmpeg sine tone placeholder
        if result is None:
            result = await self._try_ffmpeg_tone(line.text, output_path)

        if result is None:
            raise RuntimeError(f"All TTS backends failed for: {line.text[:40]}")

        return TTSResult(
            line=line,
            audio_path=result["path"],
            duration_sec=result["duration"],
            sample_rate=self._sample_rate,
        )

    async def synthesize_batch(self, lines: List[ScriptLine]) -> List[TTSResult]:
        # Perform synthesis in parallel with a small semaphore to avoid overloading the TTS service
        # But for Kokoro (especially CPU), serial might be safer. 
        # However, to avoid "lags" reported by user, we can try 2-3 at a time.
        sem = asyncio.Semaphore(2)
        
        async def _safe_synth(line):
            async with sem:
                try:
                    return await self.synthesize(line)
                except Exception as e:
                    logger.error(f"TTS failed for '{line.text[:40]}...': {e}")
                    return None

        tasks = [_safe_synth(line) for line in lines]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    # ── cache key ─────────────────────────────────────────────

    def _resolve_voice_id(self, line: ScriptLine) -> str:
        speaker = getattr(line, "speaker", "") or ""
        voice = getattr(line, "voice", None)
        
        logger.info(f"[VOICE DEBUG] speaker={speaker}, line.voice={voice}")
        
        # Priority 1: Use voice directly from line if set by planner
        if voice:
            logger.info(f"[VOICE DEBUG] Using line.voice: {voice}")
            return str(voice).strip()
        
        # Priority 2: Use voice_id from line
        line_voice_id = getattr(line, "voice_id", None)
        if line_voice_id:
            logger.info(f"[VOICE DEBUG] Using line.voice_id: {line_voice_id}")
            return str(line_voice_id).strip()
        
        # Priority 3: Look up in djs.list config (More specific)
        speaker_key = speaker.lower()
        try:
            dj_list = config.get('djs.list', []) or []
            if isinstance(dj_list, list):
                for dj in dj_list:
                    if isinstance(dj, dict) and (dj.get('id') or '').lower() == speaker_key:
                        voice = (dj.get('voice') or "").strip()
                        if voice:
                            logger.info(f"[VOICE DEBUG] Using djs.list: {voice}")
                            return voice
        except Exception:
            pass
            
        # Priority 4: Look up in tts.voices config (Fallback)
        if isinstance(self._voices, dict) and speaker_key:
            voice_id = self._voices.get(speaker_key)
            if voice_id:
                logger.info(f"[VOICE DEBUG] Using tts.voices: {voice_id}")
                return str(voice_id).strip()

        logger.info(f"[VOICE DEBUG] Using default: af_sarah")
        return "af_sarah"

    def _resolve_vosk_voice_id(self, line: ScriptLine) -> int:
        # Priority 1: line.voice_id override
        line_voice_id = getattr(line, "voice_id", None)
        if line_voice_id is not None and str(line_voice_id).isdigit():
            return int(line_voice_id)

        speaker_key = getattr(line, "speaker", "").lower()
        
        # Look up in djs.list config for voice_ru
        try:
            dj_list = config.get('djs.list', []) or []
            if isinstance(dj_list, list):
                for dj in dj_list:
                    if isinstance(dj, dict) and (dj.get('id') or '').lower() == speaker_key:
                        voice_ru = dj.get('voice_ru')
                        if voice_ru is not None:
                            return int(voice_ru)
        except Exception:
            pass

        # Fallback based on typical mapping
        if speaker_key == "dj_a":
            return 53 # default female (vedi_irina)
        elif speaker_key == "dj_b":
            return 14 # default male (or whatever integer in Vosk multi)
        return 53

    def _get_cache_key(self, line: ScriptLine, voice_id: str) -> str:
        content = (
            f"{getattr(line, 'speaker', '')}:{voice_id}:{getattr(line, 'text', '')}:"
            f"{json.dumps(getattr(line, 'style', {}), sort_keys=True)}:{self._speed}:{self._sample_rate}"
        )
        return hashlib.md5(content.encode()).hexdigest()

    # ──────────────────────────────────────────────────────────
    #  Backend 1: Kokoro HTTP API  (example.py on port 8001)
    # ──────────────────────────────────────────────────────────
    async def _try_kokoro_http(self, text: str, voice_id: str, output_path: str) -> Optional[Dict]:
        client = self._get_client()

        try:
            # Quick health check if the client seems stale or every N requests
            # But for simplicity, just do the request
            resp = await client.post(
                f"{self._kokoro_url}/tts",
                json={
                    "text": text,
                    "model_id": "kokoro",
                    "voice_id": voice_id,
                },
            )
            if resp.status_code != 200:
                err_msg = f"Kokoro HTTP returned {resp.status_code}: {resp.text}"
                logger.warning(err_msg)
                self._log_to_test_file(f"❌ Kokoro HTTP Error: {resp.status_code}")
                return None

            # Response is raw WAV bytes
            Path(output_path).write_bytes(resp.content)
            duration = self._get_duration(output_path)
            logger.info(f"Kokoro TTS OK: {duration:.1f}s  voice={voice_id}  '{text[:30]}...'")
            self._log_to_test_file(f"✅ Kokoro OK [{voice_id}] (Duration: {duration:.1f}s)")
            return {"path": output_path, "duration": duration}

        except httpx.ConnectError:
            logger.warning("Kokoro TTS service not reachable — trying next backend")
            self._log_to_test_file("⚠️ Kokoro API Unreachable (Connection Error)")
            return None
        except httpx.TimeoutException:
            logger.warning("Kokoro TTS Timeout (Took > 300s)")
            self._log_to_test_file("❌ Kokoro Timeout (>300s)")
            # Set a flag for orchestrator to restart the service
            from shared import runtime_state
            runtime_state.kokoro_timed_out = True
            return None
        except Exception as e:
            err_msg = f"{type(e).__name__}: {str(e)}"
            logger.warning(f"Kokoro HTTP error: {err_msg}")
            self._log_to_test_file(f"❌ Kokoro Exception: {err_msg}")
            return None

    # ──────────────────────────────────────────────────────────
    #  Backend 1.5: Vosk HTTP API (port 8002)
    # ──────────────────────────────────────────────────────────
    async def _try_vosk_http(self, text: str, voice_id: int, output_path: str) -> Optional[Dict]:
        try:
            client = self._get_vosk_client()
            resp = await client.post(
                 "http://localhost:8002/tts",
                json={
                    "text": text,
                    "voice": voice_id,
                },
            )
            if resp.status_code != 200:
                err_msg = f"Vosk HTTP returned {resp.status_code}: {resp.text}"
                logger.warning(err_msg)
                self._log_to_test_file(f"❌ Vosk HTTP Error: {resp.status_code}")
                return None

            # Response is raw WAV bytes
            Path(output_path).write_bytes(resp.content)
            duration = self._get_duration(output_path)
            logger.info(f"Vosk TTS OK: {duration:.1f}s  voice={voice_id}  '{text[:30]}...'")
            self._log_to_test_file(f"✅ Vosk OK [ID:{voice_id}] (Duration: {duration:.1f}s)")
            return {"path": output_path, "duration": duration}

        except Exception as e:
            err_name = type(e).__name__
            if 'ConnectError' in err_name:
                logger.warning("Vosk TTS service not reachable — trying next backend")
                self._log_to_test_file("⚠️ Vosk API Unreachable (Connection Error)")
            elif 'Timeout' in err_name:
                logger.warning("Vosk TTS Timeout (Took > 300s)")
                self._log_to_test_file("❌ Vosk Timeout (>300s)")
            else:
                logger.warning(f"Vosk HTTP error: {err_name}: {e}")
                self._log_to_test_file(f"❌ Vosk Exception: {err_name}: {e}")
            return None

    # ──────────────────────────────────────────────────────────
    #  Backend 2: pyttsx3 (Windows SAPI)
    # ──────────────────────────────────────────────────────────
    async def _try_pyttsx3(self, text: str, voice_id: str, output_path: str) -> Optional[Dict]:
        try:
            import pyttsx3
        except ImportError:
            return None

        def _synth():
            engine = pyttsx3.init()
            
            try:
                voices = engine.getProperty('voices')
                voices = list(voices) if voices else []
            except:
                voices = []
            
            target_voice = voice_id.lower().replace('_', ' ')
            
            selected_voice = None
            for voice in voices:
                voice_name = getattr(voice, 'name', '') or ''
                voice_id_attr = getattr(voice, 'id', '') or ''
                if target_voice in voice_name.lower() or voice_id_attr.lower() == voice_id.lower():
                    selected_voice = voice
                    break
            
            if selected_voice:
                try:
                    engine.setProperty('voice', selected_voice.id)
                    logger.info(f"pyttsx3 using voice: {getattr(selected_voice, 'name', 'unknown')}")
                except Exception as e:
                    logger.warning(f"pyttsx3 could not set voice: {e}")
            else:
                logger.warning(f"pyttsx3 could not find voice {voice_id}, using default")
            
            engine.setProperty("rate", int(150 * self._speed))
            engine.save_to_file(text, output_path)
            engine.runAndWait()

        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _synth)
            duration = self._get_duration(output_path)
            logger.info(f"pyttsx3 TTS OK: {duration:.1f}s  '{text[:30]}...'")
            return {"path": output_path, "duration": duration}
        except Exception as e:
            logger.warning(f"pyttsx3 error: {e}")
            return None

    # ──────────────────────────────────────────────────────────
    #  Backend 3: FFmpeg sine-tone placeholder
    # ──────────────────────────────────────────────────────────
    async def _try_ffmpeg_tone(self, text: str, output_path: str) -> Optional[Dict]:
        word_count = max(len(text.split()), 1)
        duration = min(max(word_count * 0.3, 1.0), 15.0)
        freq = 440

        cmd = [
            "ffmpeg", "-y",
            "-f", "lavfi",
            "-i", f"sine=frequency={freq}:duration={duration}:sample_rate={self._sample_rate}",
            "-af", f"volume=0.08,afade=t=in:d=0.1,afade=t=out:st={max(duration - 0.2, 0.1):.2f}:d=0.2",
            output_path,
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode != 0:
                logger.warning(f"ffmpeg tone failed: {stderr.decode()[:200]}")
                return None
            logger.info(f"ffmpeg tone placeholder: {duration:.1f}s")
            return {"path": output_path, "duration": duration}
        except Exception as e:
            logger.warning(f"ffmpeg tone error: {e}")
            return None

    # ── helpers ───────────────────────────────────────────────
    def _get_duration(self, path: str) -> float:
        if not path or not Path(path).exists():
            return 0.0
            
        cmd = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            path,
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            output = result.stdout.strip()
            if not output:
                return 0.0
            duration = float(output)
            return duration if duration > 0 else 0.0
        except Exception:
            return 0.0
