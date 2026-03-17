import json
import logging
import random
import re
from dataclasses import dataclass
from typing import Optional, Literal, Any, List, Dict, Union, Tuple
from pathlib import Path
import httpx

from services.orchestrator.config import config

logger = logging.getLogger(__name__)

SegmentKind = Literal["talk", "music"]

@dataclass
class PlanSegment:
    kind: SegmentKind
    duration_sec: int
    topic: str = ""

@dataclass
class ProgramPlan:
    program_name: str
    target_total_sec: int
    djs: List[str]
    topics: List[str]
    segments: List[PlanSegment]
    cursor: int = 0

class ProgramEngine:
    """Hybrid (B) program execution.

    - Generates a structured 1-hour timeline (talk/music) up front.
    - Generates the talk script only for the *next* talk segment when needed.
    - Advances the plan cursor only after a segment is actually published to the buffer.
    """

    def __init__(self):
        self._plan: Optional[ProgramPlan] = None

    def reset(self) -> None:
        self._plan = None

    @property
    def plan(self) -> Optional[ProgramPlan]:
        return self._plan

    def ensure_plan(self, show_id: str = "") -> ProgramPlan:
        if self._plan is not None and self._plan.cursor < len(self._plan.segments):
            return self._plan

        program_name, program = self._pick_program()
        djs = list(program.get("djs", [])) or self._get_on_air_djs() or self._get_all_djs()
        topics = list(program.get("topics", [])) or list(config.get("topics.rotation", [])) or []
        target_min = int(config.get("program.duration_min", 60) or 60)
        target_total_sec = max(10 * 60, target_min * 60)

        if not djs:
            raise ValueError("No DJs configured (djs/on_air/programs)")

        segments = self._generate_timeline(program_name, djs, topics, target_total_sec, program=program, show_id=show_id)
        self._plan = ProgramPlan(
            program_name=program_name,
            target_total_sec=target_total_sec,
            djs=djs,
            topics=topics,
            segments=segments,
            cursor=0,
        )
        logger.info(f"Program plan ready: program='{program_name}' segments={len(segments)} target={target_total_sec}s")
        return self._plan

    def get_next_segment(self, show_id: str = "") -> PlanSegment:
        plan = self.ensure_plan(show_id=show_id)
        if plan.cursor >= len(plan.segments):
            self.reset()
            plan = self.ensure_plan(show_id=show_id)
        return plan.segments[plan.cursor]

    def advance(self) -> None:
        if self._plan is None:
            return
        self._plan.cursor += 1
        if self._plan.cursor >= len(self._plan.segments):
            # Plan finished; next call will regenerate
            logger.info("Program plan finished")
            # keep plan for UI, but next ensure_plan() regenerates
        return

    # ── internals ─────────────────────────────────────────────


    def _get_all_djs(self) -> List[str]:
        # Supports both schemas:
        # 1) djs: {DJ_A: {personality, voice}, ...}
        # 2) djs: {list: [{id,name,personality,voice}, ...], personalities: {...}, voices: {...}}
        djs_obj = config.get("djs", {}) or {}
        if isinstance(djs_obj, dict):
            # schema 1
            direct_keys = [k for k in djs_obj.keys() if k not in ("list", "personalities", "voices")]
            if direct_keys:
                return sorted(direct_keys)
            # schema 2
            lst = djs_obj.get("list") or []
            out = []
            if isinstance(lst, list):
                for it in lst:
                    if isinstance(it, dict):
                        dj_id = it.get("id") or it.get("name")
                        if dj_id:
                            out.append(str(dj_id))
            return out
        return []

    def _get_on_air_djs(self) -> List[str]:
        on_air = config.get("on_air", None)
        if isinstance(on_air, list) and on_air:
            return [str(x) for x in on_air if str(x).strip()]
        # fallback: legacy schema might have djs.on_air
        legacy = config.get("djs.on_air", None)
        if isinstance(legacy, list) and legacy:
            return [str(x) for x in legacy if str(x).strip()]
        return []
    def _pick_program(self) -> Tuple[str, Dict]:
        programs = config.get("programs", {}) or {}
        current = config.get("current_program")
        if current and current in programs:
            return current, programs[current]
        if programs:
            name = next(iter(programs.keys()))
            return name, programs[name]
        return "Default", {"djs": self._get_on_air_djs() or self._get_all_djs(), "topics": []}

    def _generate_timeline(self, program_name: str, djs: List[str], topics: List[str], target_total_sec: int, program: Optional[Dict] = None, show_id: str = "") -> List[PlanSegment]:
        topics_hint = ", ".join(topics[:10]) if topics else "general radio banter, music intros/outros"
        djs_hint = ", ".join(djs)
        prog_type = program.get('type', 'music') if program else 'music'

        if prog_type == 'talk':
            # Specialized prompt for Talk shows as per user request
            prompt = f"""You are a radio producer. Create a structured timeline for a {target_total_sec//60}-minute TALK SHOW.
Program name: {program_name}
DJs on-air: {djs_hint}
Topics pool: {topics_hint}

REQUIRED CORE STRUCTURE:
1. Intro (30-60s)
2. Talk block 1 (60-120s) -> Topic from pool
3. Music (180-300s)
4. Talk block 2 (60-120s) -> Continue topic or new angle
5. Music (180-300s)
6. Talk block 3 (60-120s) -> Listener style discussion/Micro talk
7. Music (180-300s)
8. Talk block 4 (60-120s) -> Surprise fact/Deep discussion
9. Music (180-300s)
10. Outro (20-40s)

Return ONLY valid JSON (no markdown) as a list of segments.
Each segment object:
- type: "talk" or "music"
- duration_sec: integer
- topic: short string (required for talk, empty for music)

Total duration must be close to {target_total_sec} seconds.
"""
        else:
            # Standard music show prompt
            prompt = f"""You are a radio producer. Create a structured timeline for a {target_total_sec//60}-minute radio program.

Program name: {program_name}
DJs on-air: {djs_hint}
Topics pool: {topics_hint}

Return ONLY valid JSON (no markdown) as a list of segments.
Each segment object:
- type: \"talk\" or \"music\"
- duration_sec: integer
- topic: short string (required for talk, empty for music)

Rules:
- Total duration should be close to {target_total_sec} seconds.
- Alternate talk and music frequently.
- Talk segments: 30-90 seconds.
- Music segments: 180-420 seconds.
- First segment must be talk (intro).
"""

        url = config.get("llm.model_a.endpoint", "http://localhost:11434/api/generate")
        model = config.get("llm.model_a.model", "llama3")
        api_key = config.get("llm.model_a.api_key", "") or config.get("llm.api_key", "")
        api_type = config.get("llm.model_a.api_type", "") or config.get("llm.api_type", "")
        if not api_type:
            api_type = "openai" if ("openrouter.ai" in url or "openai.com" in url or "/v1/" in url) else "ollama"

        raw = None
        try:
            with httpx.Client(timeout=120) as client:
                if api_type == "openai":
                    headers = {"Content-Type": "application/json"}
                    if api_key:
                        headers["Authorization"] = f"Bearer {api_key}"
                    payload = {
                        "model": model,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.5,
                        "max_tokens": 2048,
                    }
                    r = client.post(url, json=payload, headers=headers)
                    r.raise_for_status()
                    data = r.json()
                    choices = data.get("choices", [])
                    raw = choices[0]["message"]["content"] if choices else ""
                else:
                    payload = {
                        "model": model,
                        "prompt": prompt,
                        "stream": False,
                    }
                    r = client.post(url, json=payload)
                    r.raise_for_status()
                    data = r.json()
                    raw = data.get("response") or data.get("text") or ""
        except Exception as e:
            logger.error(f"Timeline LLM call failed: {e}")
            raw = ""

        items = self._extract_json_list(raw)
        segments: List[PlanSegment] = []
        if items:
            for it in items:
                try:
                    t = (it.get("type") or it.get("kind") or "").strip().lower()
                    dur = int(it.get("duration_sec") or it.get("duration") or 0)
                    topic = (it.get("topic") or "").strip()
                    if t not in ("talk", "music") or dur <= 0:
                        continue
                    if t == "talk" and not topic:
                        # assign a topic from pool if missing
                        topic = random.choice(topics) if topics else "radio banter"
                    if t == "music":
                        topic = ""
                    segments.append(PlanSegment(kind=t, duration_sec=dur, topic=topic))
                except Exception:
                    continue

        if not segments:
            # fallback deterministic plan
            segments = self._fallback_timeline(topics, target_total_sec)

        # Ensure first segment is talk
        if segments and segments[0].kind != "talk":
            segments.insert(0, PlanSegment(kind="talk", duration_sec=120, topic=random.choice(topics) if topics else "intro"))

        # Normalize total duration (simple scaling on music blocks)
        total = sum(s.duration_sec for s in segments)
        if total <= 0:
            segments = self._fallback_timeline(topics, target_total_sec)
            total = sum(s.duration_sec for s in segments)

        target = target_total_sec
        diff = target - total
        # If too short/long by more than 5%, adjust music durations proportionally
        if abs(diff) > target * 0.05:
            music = [s for s in segments if s.kind == "music"]
            if music:
                music_total = sum(s.duration_sec for s in music)
                if music_total > 0:
                    scale = (music_total + diff) / music_total
                    scale = max(0.5, min(1.5, scale))
                    for s in segments:
                        if s.kind == "music":
                            s.duration_sec = int(max(120, min(600, s.duration_sec * scale)))
        return segments

    def _fallback_timeline(self, topics: List[str], target_total_sec: int) -> List[PlanSegment]:
        segs = []
        remaining = target_total_sec
        # Intro
        segs.append(PlanSegment(kind="talk", duration_sec=120, topic=random.choice(topics) if topics else "intro"))
        remaining -= 120
        toggle = "music"
        while remaining > 0:
            if toggle == "music":
                dur = min(300, max(180, remaining if remaining < 300 else 240))
                segs.append(PlanSegment(kind="music", duration_sec=dur))
                remaining -= dur
                toggle = "talk"
            else:
                dur = min(150, max(60, remaining if remaining < 150 else 90))
                segs.append(PlanSegment(kind="talk", duration_sec=dur, topic=random.choice(topics) if topics else "radio banter"))
                remaining -= dur
                toggle = "music"
        return segs

    def _extract_json_list(self, raw: str) -> Optional[List[Dict[str, Any]]]:
        if not raw or not raw.strip():
            return None
        cleaned = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL)
        cleaned = cleaned.strip()
        # try direct
        try:
            obj = json.loads(cleaned)
            if isinstance(obj, list):
                return obj
        except Exception:
            pass
        # find first [...]
        m = re.search(r'\[.*\]', cleaned, flags=re.DOTALL)
        if not m:
            return None
        try:
            obj = json.loads(m.group(0))
            if isinstance(obj, list):
                return obj
        except Exception:
            return None
        return None
