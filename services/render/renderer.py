import asyncio
import json
import logging
import subprocess
import random
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from shared.models import ScriptBlock, TTSResult, RenderedSegment, SegmentType
from services.orchestrator.config import config

logger = logging.getLogger(__name__)

EQ_PRESETS = {
    "flat": "equalizer=f=60:width=20:g=0,equalizer=f=170:width=20:g=0,equalizer=f=310:width=20:g=0,equalizer=f=600:width=20:g=0,equalizer=f=1000:width=20:g=0,equalizer=f=3000:width=20:g=0,equalizer=f=6000:width=20:g=0,equalizer=f=11000:width=20:g=0",
    "lofi": "equalizer=f=80:width=50:g=-3,equalizer=f=200:width=50:g=-2,equalizer=f=400:width=50:g=-1,equalizer=f=800:width=50:g=0,equalizer=f=1600:width=50:g=-1,equalizer=f=3200:width=50:g=-2,equalizer=f=6400:width=50:g=-3",
    "chill": "equalizer=f=60:width=20:g=2,equalizer=f=150:width=20:g=1,equalizer=f=300:width=20:g=0,equalizer=f=600:width=20:g=0,equalizer=f=1000:width=20:g=0,equalizer=f=3000:width=20:g=0,equalizer=f=6000:width=20:g=1,equalizer=f=11000:width=20:g=2",
    "electronic": "equalizer=f=60:width=20:g=-2,equalizer=f=100:width=20:g=-1,equalizer=f=200:width=20:g=0,equalizer=f=400:width=20:g=2,equalizer=f=800:width=20:g=3,equalizer=f=1600:width=20:g=2,equalizer=f=3200:width=20:g=1,equalizer=f=6400:width=20:g=2,equalizer=f=11000:width=20:g=3",
    "bass_boost": "equalizer=f=60:width=20:g=6,equalizer=f=80:width=20:g=5,equalizer=f=100:width=20:g=4,equalizer=f=150:width=20:g=2,equalizer=f=200:width=20:g=1,equalizer=f=400:width=20:g=0,equalizer=f=800:width=20:g=0",
    "vocal": "equalizer=f=80:width=20:g=3,equalizer=f=150:width=20:g=4,equalizer=f=300:width=20:g=3,equalizer=f=600:width=20:g=2,equalizer=f=100:width=20:g=2,equalizer=f=2000:width=20:g=1,equalizer=f=4000:width=20:g=0,equalizer=f=8000:width=20:g=-1",
    "night": "equalizer=f=60:width=20:g=2,equalizer=f=120:width=20:g=1,equalizer=f=250:width=20:g=0,equalizer=f=500:width=20:g=-1,equalizer=f=1000:width=20:g=-1,equalizer=f=2000:width=20:g=0,equalizer=f=4000:width=20:g=1,equalizer=f=8000:width=20:g=2",
    "warm": "equalizer=f=60:width=20:g=3,equalizer=f=100:width=20:g=2,equalizer=f=200:width=20:g=1,equalizer=f=400:width=20:g=0,equalizer=f=800:width=20:g=0,equalizer=f=1600:width=20:g=-1,equalizer=f=3200:width=20:g=-2,equalizer=f=6400:width=20:g=-3",
    "bright": "equalizer=f=1000:width=20:g=2,equalizer=f=2000:width=20:g=3,equalizer=f=4000:width=20:g=4,equalizer=f=6000:width=20:g=4,equalizer=f=8000:width=20:g=5,equalizer=f=10000:width=20:g=5,equalizer=f=11000:width=20:g=4",
    "rock": "equalizer=f=60:width=20:g=-2,equalizer=f=100:width=20:g=0,equalizer=f=200:width=20:g=2,equalizer=f=400:width=20:g=3,equalizer=f=800:width=20:g=3,equalizer=f=1600:width=20:g=2,equalizer=f=3200:width=20:g=3,equalizer=f=6400:width=20:g=4,equalizer=f=8000:width=20:g=3",
}


class AudioRenderer:
    def __init__(self, output_dir: str = "./output/segments"):
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._audio_config = config.get('audio', {})
        self._target_lufs = self._audio_config.get('target_loudness_lufs', -16)
        self._duck_db = self._audio_config.get('duck_db', 10)
        self._crossfade_sec = self._audio_config.get('crossfade_sec', 2.0)
        self._last_bg_track: Optional[str] = None
        self._last_eq_preset: str = "flat"

    def _detect_genre_from_filename(self, filename: str) -> str:
        name_lower = filename.lower()
        
        if any(x in name_lower for x in ['lofi', 'lo-fi', 'lof', 'chill']):
            return "lofi"
        elif any(x in name_lower for x in ['electronic', 'edm', 'techno', 'house', 'trance']):
            return "electronic"
        elif any(x in name_lower for x in ['bass', 'bassboost', 'boom']):
            return "bass_boost"
        elif any(x in name_lower for x in ['rock', 'metal', 'punk']):
            return "rock"
        elif any(x in name_lower for x in ['vocal', 'voice', 'speech']):
            return "vocal"
        elif any(x in name_lower for x in ['warm', 'jazz', 'soul', 'smooth']):
            return "warm"
        elif any(x in name_lower for x in ['bright', 'pop', 'dance']):
            return "bright"
        elif any(x in name_lower for x in ['night', 'ambient', 'sleep', 'relax']):
            return "night"
        elif any(x in name_lower for x in ['chill', 'relax', 'easy']):
            return "chill"
        
        return "flat"

    def _get_eq_preset(self, genre: str = None) -> str:
        if genre and genre in EQ_PRESETS:
            self._last_eq_preset = genre
            return EQ_PRESETS[genre]
        return EQ_PRESETS.get(self._last_eq_preset, EQ_PRESETS["flat"])

    async def render_block(
        self,
        script: ScriptBlock,
        tts_results: List[TTSResult],
        music_bed: Optional[str] = None
    ) -> RenderedSegment:
        segment_id = f"seg_{script.block_id}_{datetime.utcnow().strftime('%M%S%f')}"
        output_path = self._output_dir / f"{segment_id}.wav"

        input_files = [r.audio_path for r in tts_results if Path(r.audio_path).exists()]

        if not input_files:
            raise ValueError("No valid input audio files for rendering")


        # Auto background music for TALK blocks if not explicitly provided
        if not music_bed:
            bg_dir = Path("assets/Background")
            if bg_dir.exists():
                bg_files = list(bg_dir.glob("*.mp3"))
                if bg_files:
                    candidates = [str(f) for f in bg_files]
                    if self._last_bg_track in candidates and len(candidates) > 1:
                        candidates.remove(self._last_bg_track)
                    music_bed = random.choice(candidates)
                    self._last_bg_track = music_bed

        if music_bed and Path(music_bed).exists():
            rendered_path = await self._render_with_music(input_files, music_bed, str(output_path))
        else:
            rendered_path = await self._render_simple(input_files, str(output_path))

        normalized_path = await self._normalize_loudness(rendered_path)

        lufs, peak = await self._get_loudness_metrics(normalized_path)

        transcript = " | ".join(f"{r.line.speaker}: {r.line.text}" for r in tts_results)

        duration = self._get_duration(normalized_path)

        
        script_timeline = []
        curr_t = 0.0
        for r in tts_results:
            if Path(r.audio_path).exists():
                script_timeline.append({
                    "speaker": r.line.speaker,
                    "speaker_name": r.line.speaker_name,
                    "source": r.line.source,
                    "text": r.line.text,
                    "is_chat": r.line.is_chat,
                    "start": round(curr_t, 2),
                    "duration": round(r.duration_sec, 2)
                })
                curr_t += r.duration_sec

        return RenderedSegment(
            segment_id=segment_id,
            segment_type=SegmentType.TALK,
            file_path=normalized_path,
            duration_sec=duration,
            lufs=lufs,
            peak=peak,
            transcript=transcript,
            metadata={
                "show_id": script.show_id,
                "block_id": script.block_id,
                "topic_tags": script.topic_tags,
                "script_lines": script_timeline
            }
        )

    async def render_talk_over_music(
        self,
        script: ScriptBlock,
        tts_results: List[TTSResult],
        music_bed: Optional[str] = None
    ) -> RenderedSegment:
        segment_id = f"talk_over_{script.block_id}_{datetime.utcnow().strftime('%M%S%f')}"
        output_path = self._output_dir / f"{segment_id}.wav"

        input_files = [r.audio_path for r in tts_results if Path(r.audio_path).exists()]
        if not input_files:
            raise ValueError("No valid input audio files for rendering")

        if not music_bed:
            bg_dir = Path("assets/Background")
            if bg_dir.exists():
                bg_files = list(bg_dir.glob("*.mp3")) + list(bg_dir.glob("*.wav")) + list(bg_dir.glob("*.ogg"))
                if bg_files:
                    music_bed = str(random.choice(bg_files))

        if music_bed and Path(music_bed).exists():
            rendered_path = await self._render_with_music(input_files, music_bed, str(output_path))
        else:
            rendered_path = await self._render_simple(input_files, str(output_path))

        normalized_path = await self._normalize_loudness(rendered_path)
        lufs, peak = await self._get_loudness_metrics(normalized_path)
        transcript = " | ".join(f"{r.line.speaker}: {r.line.text}" for r in tts_results)
        duration = self._get_duration(normalized_path)
        
        script_timeline = []
        curr_t = 0.0
        for r in tts_results:
            if Path(r.audio_path).exists():
                script_timeline.append({
                    "speaker": r.line.speaker,
                    "speaker_name": r.line.speaker_name,
                    "source": r.line.source,
                    "text": r.line.text,
                    "is_chat": getattr(r.line, "is_chat", False),
                    "start": round(curr_t, 2),
                    "duration": round(r.duration_sec, 2)
                })
                curr_t += r.duration_sec

        return RenderedSegment(
            segment_id=segment_id,
            segment_type=SegmentType.TALK_OVER_MUSIC,
            file_path=normalized_path,
            duration_sec=duration,
            lufs=lufs,
            peak=peak,
            transcript=transcript,
            metadata={
                "script_lines": script_timeline,
                "show_id": script.show_id,
                "block_id": script.block_id,
                "topic_tags": script.topic_tags,
                "music_bed": music_bed
            }
        )

    async def _render_simple(self, input_files: List[str], output_path: str) -> str:
        """Concatenate multiple audio files into one."""
        if not input_files:
            return output_path

        # Single file — just copy
        if len(input_files) == 1:
            cmd = [
                "ffmpeg", "-y",
                "-i", input_files[0],
                "-c:a", "pcm_s16le",
                "-ar", str(config.tts_sample_rate),
                "-ac", "2",
                output_path,
            ]
            await self._run_ffmpeg(cmd)
            return output_path

        # Multiple files — build a proper concat filter
        # e.g. [0:a][1:a][2:a]concat=n=3:v=0:a=1[out]
        inputs = []
        for f in input_files:
            inputs.extend(["-i", f])

        filter_parts = "".join(f"[{i}:a]" for i in range(len(input_files)))
        filter_complex = f"{filter_parts}concat=n={len(input_files)}:v=0:a=1[out]"

        cmd = [
            "ffmpeg", "-y",
            *inputs,
            "-filter_complex", filter_complex,
            "-map", "[out]",
            "-c:a", "pcm_s16le",
            "-ar", str(config.tts_sample_rate),
            "-ac", "2",
            output_path,
        ]

        await self._run_ffmpeg(cmd)
        return output_path

    async def _render_with_music(
        self,
        voice_files: List[str],
        music_file: str,
        output_path: str
    ) -> str:
        """Concatenate voice files, mix with music bed (ducked)."""
        # Step 1 – concatenate voice files into a temp file
        voice_concat_path = output_path.replace(".wav", "_voice.wav")
        await self._render_simple(voice_files, voice_concat_path)

        # Step 2 – mix voice + music
        # Apply fade-in and fade-out to background music
        duration = self._get_duration(voice_concat_path)
        fade_out_start = max(duration - 1.5, 0)

        # Convert duck_db (e.g. 10dB) to linear gain (e.g. 0.31)
        # We use a base level for the music bed. If duck_db is 10, music is at -10dB relative to 1.0
        duck_linear = 10 ** (-abs(self._duck_db) / 20)

        cmd = [
            "ffmpeg", "-y",
            "-i", voice_concat_path,
            "-i", music_file,
            "-filter_complex",
            # Voice boosted to 1.5, music ducked by duck_linear
            # Added normalize=0 to amix to prevent volume jumps when voice ends
            "[0:a]volume=1.5[v];"
            f"[1:a]volume={duck_linear:.3f},afade=t=in:st=0:d=1.0,afade=t=out:st={fade_out_start}:d=1.5[m];"
            "[v][m]amix=inputs=2:duration=first:dropout_transition=2:normalize=0[out]",
            "-map", "[out]",
            "-c:a", "pcm_s16le",
            "-ar", str(config.tts_sample_rate),
            "-ac", "2",
            output_path,
        ]

        await self._run_ffmpeg(cmd)

        # Clean up temp
        Path(voice_concat_path).unlink(missing_ok=True)

        return output_path

    async def _normalize_loudness(self, input_path: str) -> str:
        output_path = input_path.replace(".wav", "_norm.wav")

        # Measure current loudness to apply correct gain
        lufs, peak = await self._get_loudness_metrics(input_path)
        
        # Target from config
        target_lufs = float(self._target_lufs)
        gain = target_lufs - lufs
        
        # Avoid radical gain changes that might cause distortion
        gain = max(-20, min(20, gain))
        
        # Safety: if peak + gain > -1.0, reduce gain
        if peak + gain > -1.0:
            gain = -1.0 - peak

        cmd = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-af", f"volume={gain:.2f}dB,alimiter=limit=-1.0dB",
            "-c:a", "pcm_s16le",
            "-ar", str(config.tts_sample_rate),
            "-ac", "2",
            output_path,
        ]

        await self._run_ffmpeg(cmd)

        # Remove unnormalized original
        if input_path != output_path:
            Path(input_path).unlink(missing_ok=True)

        return output_path

    async def _get_loudness_metrics(self, path: str) -> Tuple[float, float]:
        cmd = [
            "ffmpeg",
            "-i", path,
            "-af", "volumedetect",
            "-f", "null", "-"
        ]

        try:
            result = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            _, stderr = await result.communicate()

            output = stderr.decode()

            import re
            lufs_match = re.search(r"mean_volume: ([-\d.]+) dB", output)
            if not lufs_match:
                lufs_match = re.search(r"Mean volume: ([-\d.]+) dB", output)
            peak_match = re.search(r"max_volume: ([-\d.]+) dB", output)
            if not peak_match:
                peak_match = re.search(r"Peak: ([-\d.]+) dB", output)

            lufs = float(lufs_match.group(1)) if lufs_match else self._target_lufs
            peak = float(peak_match.group(1)) if peak_match else -1.0

            return lufs, peak

        except Exception as e:
            logger.error(f"Failed to get loudness: {e}")
            return self._target_lufs, -1.0

    def _get_duration(self, path: str) -> float:
        if not path or not Path(path).exists():
            logger.warning(f"File does not exist: {path}")
            return 0.0
            
        cmd = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            path
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            output = result.stdout.strip()
            if not output:
                logger.warning(f"ffprobe returned empty duration for: {path}")
                return 0.0
            duration = float(output)
            if duration <= 0:
                logger.warning(f"Invalid duration {duration} for: {path}")
                return 0.0
            return duration
        except subprocess.TimeoutExpired:
            logger.warning(f"ffprobe timeout for: {path}")
            return 0.0
        except Exception as e:
            logger.warning(f"ffprobe error for {path}: {e}")
            return 0.0

    async def _apply_eq(self, input_path: str, genre: str = None) -> str:
        eq_filter = self._get_eq_preset(genre)
        output_path = input_path.replace(".wav", "_eq.wav")
        
        cmd = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-af", eq_filter,
            "-c:a", "pcm_s16le",
            "-ar", str(config.tts_sample_rate),
            "-ac", "2",
            output_path,
        ]
        
        try:
            await self._run_ffmpeg(cmd)
            logger.info(f"Applied EQ preset '{genre or self._last_eq_preset}' to {Path(input_path).name}")
            Path(input_path).unlink(missing_ok=True)
            return output_path
        except Exception as e:
            logger.warning(f"EQ failed, using original: {e}")
            return input_path

    async def apply_eq_to_music(self, music_file_path: str, genre: str = None) -> str:
        if not genre:
            genre = self._detect_genre_from_filename(music_file_path)
        
        output_path = str(Path(music_file_path).with_suffix('.eq.wav'))
        
        eq_filter = self._get_eq_preset(genre)
        
        cmd = [
            "ffmpeg", "-y",
            "-i", music_file_path,
            "-af", eq_filter,
            "-ar", str(config.tts_sample_rate),
            "-ac", "2",
            output_path,
        ]
        
        try:
            await self._run_ffmpeg(cmd)
            logger.info(f"Applied EQ preset '{genre}' to {Path(music_file_path).name}")
            return output_path
        except Exception as e:
            logger.warning(f"EQ failed: {e}")
            return music_file_path

    async def _run_ffmpeg(self, cmd: List[str]) -> None:
        logger.debug(f"Running: {' '.join(cmd)}")
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            err_msg = stderr.decode(errors='replace')
            raise Exception(f"FFmpeg failed (rc={process.returncode}): {err_msg[-500:]}")
