import sys
import asyncio
import time
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
import httpx

# Ensure shared models and config are accessible
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from shared.models import ScriptBlock, ScriptLine
from services.orchestrator.config import config
from services.music.library import library
from shared import runtime_state

logger = logging.getLogger(__name__)


class DialoguePlanner:
    def __init__(self):
        self._dj_a_prompt = self._load_prompt("dj_a.md")
        self._dj_b_prompt = self._load_prompt("dj_b.md")
        
        # Load base template, try language-specific one first
        lang = config.get("language", "en").upper()
        self._prompt_template = self._load_prompt(f"planner - {lang}.md")
        if not self._prompt_template:
            self._prompt_template = self._load_prompt("planner.md")
            
        self._llm_config = config.llm_config
        self._recent_topics: List[str] = []
        self._max_recent_topics = 10
        self._global_dialogue_history = []
        self._is_intro = False
        self._is_handover = False
        self._next_program = {}

    def _load_prompt(self, name: str) -> str:
        prompt_path = Path(__file__).parent / "prompts" / name
        if prompt_path.exists():
            with open(prompt_path, 'r', encoding='utf-8') as f:
                return f.read()
        return ""

    async def plan_block(
        self,
        topics: List[str],
        target_duration: int,
        show_id: str,
        current_track: Optional[Dict] = None,
        next_track: Optional[Dict] = None,
        program: Optional[Dict] = None,
        recent_tracks: Optional[List[Dict]] = None
    ) -> ScriptBlock:
        block_id = f"talk_{datetime.utcnow().strftime('%M%S%f')}"

        # 1. Check if program has a fixed script (e.g. for ads)
        if program and (program.get('script') or program.get('fixed_script')):
            raw_script = program.get('script') or program.get('fixed_script')
            logger.info(f"Using fixed script for program: {program.get('title')}")
            plan = self._convert_script_to_plan(raw_script, program)
            return await self._generate_lines(
                plan, show_id, block_id, target_duration, 
                current_track=current_track, next_track=next_track, program=program,
                recent_tracks=recent_tracks
            )

        prompt = self._build_planning_prompt(topics, target_duration, program, current_track, next_track, recent_tracks)

        plan = None
        # Try up to 2 times as requested
        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"LLM Planning attempt {attempt}/{max_attempts} for {show_id}")
                plan_raw = await self._call_llm(prompt, "planner")
                plan = self._extract_json(plan_raw)
                if plan:
                    logger.info(f"Successfully generated plan on attempt {attempt}")
                    break
                else:
                    logger.warning(f"Attempt {attempt}: LLM returned success but JSON extraction failed.")
            except Exception as e:
                logger.error(f"Attempt {attempt} failed with error: {e}")
            
            if attempt < max_attempts:
                # Optional: small delay or log
                logger.info("Retrying...")

        if plan is None:
            logger.warning("All LLM attempts failed. Using fallback plan.")
            plan = self._fallback_plan(target_duration, program)

        script_block = await self._generate_lines(
            plan, show_id, block_id, target_duration, 
            current_track=current_track, next_track=next_track, program=program,
            recent_tracks=recent_tracks
        )

        self._update_recent_topics(plan.get("topic_tags", []))

        return script_block

    def _convert_script_to_plan(self, raw_script: Union[str, List], program: Dict) -> Dict:
        """Converts a raw string or list script into a structured plan object."""
        flow = []
        djs = program.get('djs', ["DJ_A"])
        default_speaker = djs[0] if djs else "DJ_A"

        if isinstance(raw_script, str):
            # Try to parse line by line: "DJ_NAME: Text"
            lines = raw_script.split('\n')
            for line in lines:
                if not line.strip(): continue
                if ':' in line:
                    parts = line.split(':', 1)
                    spk = parts[0].strip()
                    txt = parts[1].strip()
                    flow.append({"speaker": spk, "text": txt, "style_hint": "neutral"})
                else:
                    flow.append({"speaker": default_speaker, "text": line.strip(), "style_hint": "neutral"})
        elif isinstance(raw_script, list):
            for item in raw_script:
                if isinstance(item, str):
                    flow.append({"speaker": default_speaker, "text": item, "style_hint": "neutral"})
                elif isinstance(item, dict):
                    flow.append({
                        "speaker": item.get("speaker", default_speaker),
                        "text": item.get("text", ""),
                        "style_hint": item.get("style_hint", "neutral")
                    })

        return {
            "block_id": "scheduled_script",
            "topic_tags": ["scheduled"],
            "dialogue": flow,
            "mix_notes": {"bed_music": "low"}
        }

    def _extract_json(self, raw: str) -> Optional[dict]:
        """Extract JSON from LLM response that may contain <think> tags,
        markdown code blocks, or other surrounding text."""
        if not raw or not raw.strip():
            return None

        # 1. Remove <think>...</think> blocks (deepseek-r1)
        cleaned = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL)

        # 2. Try to find JSON inside ```json ... ``` code blocks
        code_block = re.search(r'```(?:json)?\s*\n?([\s\S]*?)```', cleaned)
        if code_block:
            try:
                return json.loads(code_block.group(1).strip())
            except json.JSONDecodeError:
                pass

        # 3. Try to find a JSON object { ... } anywhere in the text
        brace_match = re.search(r'\{[\s\S]*\}', cleaned)
        if brace_match:
            try:
                return json.loads(brace_match.group(0))
            except json.JSONDecodeError:
                pass

        # 4. Try parsing the whole cleaned string
        try:
            return json.loads(cleaned.strip())
        except json.JSONDecodeError:
            return None

    def _build_planning_prompt(self, topics: List[str], duration: int, program: Optional[Dict] = None, current_track: Optional[Dict] = None, next_track: Optional[Dict] = None, recent_tracks: Optional[List[Dict]] = None) -> str:
        recent = ", ".join(self._recent_topics[-5:]) or "none"
        prompt = self._prompt_template
        prompt = prompt.replace("{{topics}}", ", ".join(topics))
        prompt = prompt.replace("{{duration}}", str(duration))
        prompt = prompt.replace("{{words}}", str(int(duration * 3.5)))
        prompt = prompt.replace("{{recent_topics}}", recent)
        
        # Inject language directive so the LLM knows which language to use
        lang = config.get("language", "en")
        if lang == "ru":
            # Only add the directive if we didn't already load the RU-specific prompt
            if "УСЛОВИЕ ЯЗЫКА" not in prompt:
                prompt += "\n\nУСЛОВИЕ ЯЗЫКА: ДИАЛОГ ДОЛЖЕН БЫТЬ НАПИСАН ИСКЛЮЧИТЕЛЬНО НА РУССКОМ ЯЗЫКЕ. CRITICAL: Write ALL dialogue text ONLY in Russian. Do NOT write in English."
        else:
            prompt += "\n\nLANGUAGE: All dialogue must be written in English only."
        
        dj_personalities = ""
        try:
            participating_djs = program.get('djs', []) if program else []
            if not participating_djs:
                participating_djs = ['DJ_A', 'DJ_B']
                
            for dj in config.get('djs.list', []):
                if isinstance(dj, dict) and 'id' in dj and 'personality' in dj:
                    if dj['id'] in participating_djs:
                        dj_name = dj.get('name', dj['id'])
                        dj_personalities += f"- Speaker ID: {dj['id']} (Name: {dj_name}) => {dj['personality']}\n"
        except Exception:
            pass
            
        if dj_personalities:
            prompt += f"\n\n--- DJ PERSONALITIES & NAMES ---\n{dj_personalities}\nCRITICAL: When writing the dialogue text, characters MUST address each other using their REAL NAMES (e.g. 'Hey Milka'), NOT their Speaker IDs (like DJ_A)."
        
        if program:
            is_talk = program.get('type') == 'talk'
            prog_type_str = "TALK SHOW / PODCAST (Deep discussion mode)" if is_talk else "MUSIC SHOW (Standard radio flow)"
            
            prog_info = f"\n\n--- CURRENT PROGRAM CONTEXT ---\n"
            prog_info += f"Program Title: {program.get('title')}\n"
            prog_info += f"Show Type: {prog_type_str}\n"
            prog_info += f"Description: {program.get('description')}\n"
            prog_info += f"Show Directives: {program.get('prompt')}\n"
            
            if is_talk:
                prog_info += "CLASSIFICATION: This is a TALK SHOW. Follow the 'Professional Radio Format' in the System Prompt. Use Hooks, Facts, Reactions, and the specific Talk Block structure. Keep segments fast and energetic!\n"
            else:
                prog_info += "CLASSIFICATION: This is a MUSIC SHOW. Follow the [Recap] -> [Trivia] -> [Creative Announce] structure.\n"
                
            prog_info += f"Participating DJs: {', '.join(program.get('djs', []))}\n"
            prompt += prog_info
            
        # --- NEW: Listener Interaction Integration ---
        try:
            if hasattr(runtime_state, 'listener_messages') and runtime_state.listener_messages:
                # Get last few valid (non-rejected, non-processed) messages
                valid_msgs = [m for m in runtime_state.listener_messages if not m.get('rejected') and not m.get('processed')]
                if valid_msgs:
                    recent_msgs = valid_msgs[-5:] # Last 5
                    
                    # Mark these as processed so they aren't read again
                    for m in recent_msgs:
                        m['processed'] = True
                        
                    msg_text = "\n\n--- RECENT LISTENER MESSAGES ---\n"
                    msg_text += "DJs: You can Choice to respond to one or more of these. If a message is a request, feedback, or a business promo (promo marked with [PROMO]), work it naturally into the flow.\n"
                    for m in recent_msgs:
                        p_label = " [PROMO]" if m.get('is_promo') else ""
                        msg_text += f"- From {m.get('author')}{p_label}: \"{m.get('text')}\"\n"
                    prompt += msg_text
        except Exception as e:
            logger.warning(f"Failed to inject listener messages: {e}")
            
        import random
        # --- NEW: Rich Music & Lyric Discussion System ---
        lyric_tactics = [
            "Relate a line to a famous philosophical concept like Stoicism or Existentialism.",
            "Describe what cinematic scene this would be in a movie about the listeners' life.",
            "Deconstruct a specific metaphor and explain why it works so well for the night.",
            "Talk about how the specific rhymes or sounds create a 'late-night texture'.",
            "Share a 'fake' nostalgia memory of a night in a rainy city where these words hit hard.",
            "Directly challenge the other DJ to interpret a specific cryptic line from the text.",
            "Describe a specific usage scenario: 'If you're making coffee right now, listen to the bridge...'",
            "Discuss how the melody masks a potentially deep or lonely meaning in the lyrics.",
            "Focus on a repeating phrase and discuss its hypnotic, loop-like effect.",
            "Pick one specific word from the snippet and gush about its perfect placement.",
            "Ask a rhetorical question to the audience based on the song's core message.",
            "Imagine you are listening to this track in a specific secret, underground location.",
            "Explain how these specific lyrics 'lock in' the mood for the rest of the show.",
            "Relate the lyrics to the artist's personal struggles or known background.",
            "Why these words specifically belong to the '3 AM night crawlers'.",
            "Describe a city street at night that perfectly matches the vibe of these lyrics.",
            "Share a playful joke or pun based on the song title or a specific line.",
            "Talk about the 'inner whisper' quality of the vocals and what they are saying to us.",
            "Contrast the energy of the track with the stillness of the current hour.",
            "Pick the most 'visual' line and describe the image it puts in your head."
        ]

        def get_track_context(track_dict, is_recap=False):
            if not isinstance(track_dict, dict) or not (track_dict.get('title') or track_dict.get('artist')):
                return ""
            
            tid = track_dict.get('id')
            meta = library.get_track(tid) if tid else None
            
            label = "RECAP (Just Ended)" if is_recap else "ANNOUNCE (Upcoming)"
            ctx = f"\n--- {label} TRACK INFO ---\n"
            artist = track_dict.get('artist', 'Unknown')
            title = track_dict.get('title', 'Unknown')
            ctx += f"Artist: {artist}\nTitle: {title}\n"
            
            if meta:
                ctx += f"STYLE: {meta.get('genre', 'N/A')}, {meta.get('mood', 'N/A')}\n"
                if meta.get('lyrics'):
                    lines = [L.strip() for L in meta['lyrics'].split('\n') if L.strip()]
                    # Take up to 20 lines
                    snippet = "\n    ".join(lines[:20])
                    ctx += f"LYRICS SNIPPET:\n    {snippet}\n"
                    tactic = random.choice(lyric_tactics)
                    ctx += f"DISCUSSION TACTIC: {tactic}\n"
            
            if is_recap:
                ctx += "INSTRUCTION: Discuss this track briefly using its lyrics and the tactic above. Avoid generic praise.\n"
            else:
                ctx += f"INSTRUCTION: Introduce this track creatively using the lyrics/tactic above. MANDATORY: The very last line of the script MUST be: NEXT SONG: {artist} - {title}\n"
            return ctx

        if current_track:
            prompt += get_track_context(current_track, is_recap=True)
            
        if recent_tracks:
            prompt += "\n\n--- RECENTLY PLAYED SILENT TRACKS (Last few songs) ---\n"
            prompt += "The following tracks played in silence before this talk block. Briefly mention them or the vibe they created:\n"
            for t in recent_tracks:
                prompt += f"- {t.get('artist')} - {t.get('title')}\n"
            prompt += "INSTRUCTION: Transition between these and the current recap smoothly.\n"

        if next_track:
            prompt += get_track_context(next_track, is_recap=False)

        # Proportional dialogue instructions
        prompt += f"\n\n--- DURATION TARGET ---\n"
        if duration < 40:
            # Short mixed-in talk (intro/outro)
            prompt += f"This is a QUICK transition of {duration} seconds. \n"
            prompt += f"Keep it brief and punchy. Maximum 1-2 short sentences per speaker.\n"
            prompt += f"Target Word Count: approximately {int(duration * 2.2)} - {int(duration * 2.8)} words total.\n"
        else:
            # Standalone talk block
            min_turns = max(2, int(duration / 40)) # e.g. 60s -> 2 turns, 120s -> 3 turns
            prompt += f"The user wants a SUBSTANTIAL dialogue of {duration // 60}m {duration % 60}s. \n"
            prompt += f"Write a deep, multi-turn conversation. Each DJ should speak at least {min_turns} times.\n"
            prompt += f"TURN LENGTH: Every individual DJ turn/paragraph MUST be 250-500 characters long (at least 3-5 sentences). ELABORATE on your thoughts. Don't just agree; discuss, explain, and share anecdotes.\n"
            prompt += f"Target Word Count: approximately {int(duration * 2.8)} - {int(duration * 3.5)} words total.\n"

        prompt += "\n\n--- CRITICAL CONSTRAINTS ---\n"
        prompt += "- NEVER use cliches: 'captured that midnight feeling', 'speaking of spaces between', 'essence of', 'it's all about'.\n"
        prompt += "- DO NOT start with 'Did you know', 'Fun fact', or 'Let's roll with'.\n"
        prompt += "- No robotic transitions. Every segment must be a fresh, human-like conversation.\n"

        # --- NEW: Handover & Intro Logic ---
        is_handover = getattr(self, "_is_handover", False)
        is_intro = getattr(self, "_is_intro", False)
        
        if is_handover:
            next_prog = getattr(self, "_next_program", {})
            next_dj_names = []
            if next_prog:
                for dj_id in next_prog.get('djs', []):
                    for d in config.get('djs.list', []):
                        if d.get('id') == dj_id:
                            next_dj_names.append(d.get('name', dj_id))
            
            next_dj_str = ", ".join(next_dj_names) or "the next host"
            prompt += f"\n\n--- CRITICAL: SHOW HANDOVER (END OF HOUR) ---\n"
            prompt += f"This is the END of your shift. You must:\n"
            prompt += f"1. Say goodbye to the listeners.\n"
            prompt += f"2. Mention the current track briefly using the context above.\n"
            prompt += f"3. Announce that after the news, {next_dj_str} will be taking over with the show '{next_prog.get('title', 'the next program')}'.\n"
            prompt += f"4. Say a final goodbye."
            
        if is_intro:
            prompt += f"\n\n--- CRITICAL: SHOW INTRO (NEW HOUR) ---\n"
            prompt += f"This is the START of a new hour. You must:\n"
            prompt += f"1. Greet the listeners and welcome them to the show '{program.get('title', 'AIR')}'.\n"
            if program.get('type') == 'talk':
                prompt += f"2. Explicitly state the THEME of this hour: {program.get('description', 'various topics')}.\n"
                prompt += f"3. Mention what you'll be discussing based on: {program.get('prompt', '')}.\n"
            
            prompt += f"4. Introduce the first track using the DETAILED INFO provided above."
            
        if self._global_dialogue_history:
            lines = [f"[{h['time'].strftime('%H:%M')}] {h['speaker']}: {h['text']}" for h in self._global_dialogue_history[-15:]]
            prompt += "\n\n--- GLOBAL RECENT SHOW DIALOGUE HISTORY ---\n"
            prompt += "\n".join(lines)
            if program and program.get('type') == 'talk':
                prompt += "\n\nКРИТИЧЕСКОЕ ТРЕБОВАНИЕ: Это ПРОДОЛЖАЮЩЕЕСЯ разговорное шоу! ВЫ ОБЯЗАНЫ прочитать историю выше и ПРОДОЛЖИТЬ разговор логично с того места, где он остановился. НЕ НАЧИНАЙТЕ новый выпуск и НЕ ЗДОРОВАЙТЕСЬ снова. Продолжайте обсуждение напрямую!\n"
            else:
                prompt += "\n\nCRITICAL REQUIREMENT: Avoid repeating topics, jokes, specific opinions, or key phrases from the recent history above. DO NOT greed the listeners again if you already did.\n"
                
        # --- NEW: Listener Messages Integration ---
        messages_to_process = []
        if hasattr(runtime_state, "listener_messages"):
            while runtime_state.listener_messages:
                messages_to_process.append(runtime_state.listener_messages.popleft())
                
        if messages_to_process:
            prompt += "\n\n--- LISTENER INCOMING MESSAGES ---\n"
            prompt += "The following messages were just sent by listeners through the visual chat interface. Incorporate them into your dialogue dynamically! Read the author's name, their message, and react to it.\n\n"
            
            for m in messages_to_process:
                if m.get("rejected"):
                    reason = m.get("rejection_reason", "")
                    if reason == "promo_joke":
                        prompt += f"- [SYSTEM NOTE]: A business apparently tried to sneak an ad or phone number ({m['text']}). React with a joke, subtly making fun of them trying to slip a promo past the AI DJ moderation.\n"
                    else:
                        prompt += f"- [SYSTEM NOTE]: A listener ({m['author']}) sent an inappropriate message ({reason}). Read their name and make a witty, sarcastic joke about them failing the vibe check, without repeating the bad words: '{m['text']}'\n"
                else:
                    is_promo = m.get("is_promo")
                    if is_promo:
                        prompt += f"- Listener '{m['author']}' says: '{m['text']}' (Note: this contains business/promo info, mention it casually as requested by the user!)\n"
                    else:
                        prompt += f"- Listener '{m['author']}' says: '{m['text']}' (React naturally to this, answer their question or validate their vibe!)\n"
            
            prompt += "\nINSTRUCTION: Engage with these messages EARLY in the dialogue. Don't just 'read' them—DISCUSS them.\n"
            prompt += "- If it's a song request, talk about that artist.\n"
            prompt += "- If it's a greeting, ask them how their night is going.\n"
            prompt += "- If it's a joke, react with a real laugh and a follow-up joke.\n"
            prompt += "- RESPONSE LENGTH: Each reaction to a listener should be a FULL paragraph (4+ sentences).\n"
            prompt += "CRITICAL: For every line in your 'dialogue' JSON where a DJ responds to a listener message, you MUST set 'is_chat': true.\n"
        
        prompt += "\n\nFINAL INSTRUCTION: Return the dialogue ONLY as a VALID JSON object matching the requested schema. No conversational filler or markdown outside the JSON."
        return prompt

    async def _call_llm(self, prompt: str, role: str) -> str:
        llm_cfg = self._llm_config.get(role, self._llm_config.get("model_a", {}))
        endpoint = llm_cfg.get("endpoint", "http://localhost:11434/api/generate")
        model = llm_cfg.get("model", "llama3")
        timeout = llm_cfg.get("timeout_sec", 600)
        api_key = llm_cfg.get("api_key", "") or self._llm_config.get("api_key", "")
        
        # Auto-detect API type: "openai" for OpenRouter/OpenAI, "ollama" for local Ollama
        api_type = llm_cfg.get("api_type", "") or self._llm_config.get("api_type", "")
        if not api_type:
            api_type = "openai" if ("openrouter.ai" in endpoint or "openai.com" in endpoint or "/v1/" in endpoint) else "ollama"

        logger.info(f"===> Calling LLM Model: {model} (Role: {role}, API: {api_type}) <===")
        import time
        t_start = time.time()
        
        try:
            # Diagnostics for Ubuntu/Linux
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "AIRadio/1.0 (Ubuntu; Linux) Python/Httpx",
                "HTTP-Referer": "https://github.com/alexey-pelykh/ai-radio",
                "X-Title": "AI Radio Project"
            }
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"
            
            k_preview = f"{api_key[:6]}...{api_key[-4:]}" if (api_key and len(api_key) > 10) else "MISSING"
            logger.info(f"LLM Call Start: model={model}, endpoint={endpoint}, key_preview={k_preview}")
            
            if api_type == "openai":
                payload = {
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": llm_cfg.get("temperature", 0.7),
                    "max_tokens": llm_cfg.get("max_tokens", 4096),
                }
            else:
                payload = {
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": llm_cfg.get("temperature", 0.7),
                        "num_predict": llm_cfg.get("max_tokens", 4096),
                    }
                }
                if role == "planner": payload["format"] = "json"

            def _sync_request():
                t_req_start = time.time()
                try:
                    # Use trust_env=False to ignore system proxies which can cause hangs on Linux
                    # This is more compatible with older httpx versions than proxies={}
                    with httpx.Client(timeout=httpx.Timeout(timeout, connect=10.0), trust_env=False) as client:
                        logger.info(f"Connecting to {endpoint}...")
                        resp = client.post(endpoint, json=payload, headers=headers)
                        
                        if resp.status_code != 200:
                            logger.error(f"LLM API ERROR {resp.status_code}: {resp.text[:500]}")
                        
                        logger.info(f"Request completed in {time.time() - t_req_start:.2f}s")
                        return resp
                except httpx.ConnectError as ce:
                    logger.error(f"LLM Connection Error (DNS/Network): {ce}")
                    raise
                except httpx.TimeoutException as te:
                    logger.error(f"LLM Timeout Error: {te}")
                    raise
                except Exception as ex:
                    logger.error(f"LLM Internal Httpx Error: {ex}")
                    raise

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, _sync_request)
            response.raise_for_status()
            data = response.json()
            
            if api_type == "openai":
                choices = data.get("choices", [])
                result = choices[0].get("message", {}).get("content", "") if choices else ""
            else:
                result = data.get("response", "")
                
            duration = time.time() - t_start
            logger.info(f"LLM Success in {duration:.2f}s (Result length: {len(result)})")
            return result

        except Exception as e:
            logger.error(f"LLM FATAL FAIL ({model}): {type(e).__name__} - {e}")
            return "" # Return empty so fallback logic can trigger

    async def _llm_generate(self, prompt: str, role: str = "model_a", max_tokens: int = 2048) -> str:
        """Generic LLM generation for other services (e.g. news)"""
        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"LLM generate attempt {attempt}/{max_attempts} for role: {role}")
                res = await self._call_llm(prompt, role)
                if res and len(res.strip()) > 5:
                    return res
            except Exception as e:
                logger.error(f"DialoguePlanner._llm_generate attempt {attempt} failed: {e}")
        return ""

    async def _generate_lines(
        self,
        plan: Dict,
        show_id: str,
        block_id: str,
        target_duration: int,
        current_track: Optional[Dict] = None,
        next_track: Optional[Dict] = None,
        program: Optional[Dict] = None,
        recent_tracks: Optional[List[Dict]] = None
    ) -> ScriptBlock:
        script_block = ScriptBlock(
            show_id=show_id,
            block_id=block_id,
            language=config.get("language", "en"),
            topic_tags=plan.get("topic_tags", []),
            target_duration_sec=target_duration,
            mix_notes=plan.get("mix_notes", {})
        )

        flow = plan.get("dialogue", [])
        
        # Backward compatibility if LLM still formats as main_points
        if not flow and "conversation_flow" in plan and "main_points" in plan["conversation_flow"]:
            flow = plan["conversation_flow"]["main_points"]

        seen_normalized_texts = set()

        for i, point in enumerate(flow):
            speaker = point.get("speaker", "DJ_A")
            style_hint = point.get("style_hint", "neutral")
            text = point.get("text", point.get("point", ""))
            is_chat = point.get("is_chat", False)
            
            # Heuristic backup: if LLM forgets is_chat, but text clearly references a listener
            if not is_chat:
                chat_triggers = [
                    "message from", "got a message", "listener", "listening", "says:", 
                    "asking", "asked about", "comment from", "вопрос от", "сообщение от",
                    "пишет нам", "спрашивает", "в эфире", "на проводе", "чат"
                ]
                text_lower = text.lower()
                if any(t in text_lower for t in chat_triggers):
                    is_chat = True
                    logger.info(f"Auto-detected is_chat for line: {text[:50]}...")
            
            if not text:
                continue

            # Try to map hallucinatory names back to Speaker IDs if needed
            matched_id = None
            dj_name_for_regex = speaker
            try:
                for dj in config.get('djs.list', []):
                    if isinstance(dj, dict):
                        did = dj.get('id', '')
                        dname = dj.get('name', '')
                        if speaker == did or did in speaker:
                            matched_id = did
                            dj_name_for_regex = dname
                            break
                        if dname and (speaker == dname or dname in speaker):
                            matched_id = did
                            dj_name_for_regex = dname
                            break
            except Exception:
                pass
                
            if matched_id:
                speaker = matched_id
            
            # --- NEW: Force mapping to participating DJs if LLM hallucinated DJ_A/DJ_B ---
            participating_djs = program.get('djs', []) if program else []
            if participating_djs:
                # If the chosen speaker is not in participating list, pick the best substitute
                if speaker not in participating_djs:
                    # If it's a "DJ_A" style ID, map to the first participating DJ
                    # If it's a "DJ_B" style ID, map to the second participating DJ (if exists)
                    if "A" in speaker or len(participating_djs) == 1:
                        new_spk = participating_djs[0]
                    else:
                        new_spk = participating_djs[min(1, len(participating_djs)-1)]
                    
                    logger.warning(f"Speaker '{speaker}' not in program DJs {participating_djs}. Remapping to '{new_spk}'.")
                    speaker = new_spk

            import re
            # Remove potential speaker prefixes that LLMs sometimes hallucinate into the text
            text = re.sub(r'^\*?\*?DJ[_\s]?[A-Z0-9]\*?\*?\s*[:\-]?\s*', '', text, flags=re.IGNORECASE)
            if dj_name_for_regex != speaker:
                text = re.sub(rf'^\*?\*?{dj_name_for_regex}\*?\*?\s*[:\-]?\s*', '', text, flags=re.IGNORECASE)
            
            # Additional safety: generic "Name:" cleanup at the start if it looks like a script prefix
            text = re.sub(r'^[\*\s]*[A-Za-zА-Яа-я0-9_]{2,15}[\*\s]*:\s*', '', text)
            
            # Remove any leading bullet points, minuses, or pluses that LLMs use for lists
            text = re.sub(r'^[\s\*\+\-•]+', '', text)
            
            # Remove stage directions or metadata in parentheses/brackets, e.g. (MILKA), (short transition), [Music plays]
            text = re.sub(r'\([^)]*\)', '', text)
            text = re.sub(r'\[[^\]]*\]', '', text)
            
            # Remove trailing JSON fragment hallucinations (e.g. "something!'}, ,{ }, {")
            text = re.sub(r'[\'"]?\s*\}\s*,\s*\{?.*$', '', text)
            
            text = text.strip(' "\'')
            
            # If line became empty after stripping (e.g. it was just "(MILKA)"), skip it
            if len(text.strip()) < 2:
                continue
            
            # Anti-loop check: If the LLM generates the exact same phrase, skip it
            normalized_text = re.sub(r'[^a-zA-Zа-яА-Я0-9]', '', text).lower()
            if len(normalized_text) > 20:  # Only check substantial phrases
                # Check within current block only (local dedup)
                if normalized_text in seen_normalized_texts:
                    logger.warning(f"Duplicate within block, skipping: {text[:60]}")
                    continue
                
                # Check recent global history (last 10 entries only — not too aggressive)
                is_global_repeat = False
                for h in self._global_dialogue_history[-10:]:
                    h_norm = re.sub(r'[^a-zA-Zа-яА-Я0-9]', '', h['text']).lower()
                    if len(h_norm) > 20 and normalized_text == h_norm:
                        is_global_repeat = True
                        break
                
                if is_global_repeat:
                    logger.warning(f"Global history repeat, skipping line: {text[:60]}")
                    continue  # Skip this line but keep processing others
                    
                seen_normalized_texts.add(normalized_text)

            self._add_to_global_history(speaker, text)
            spk_name = self._get_speaker_name(speaker)
            voice = self._get_voice(speaker)
            
            # --- Determine source for UI ---
            # Default is "ai". 
            line_source = "ai"
            plan_id = str(plan.get("block_id", "")).lower()
            
            # Robust check for fallback scripts
            is_fallback_plan = "fallback" in plan_id or "script" in plan_id
            
            # Known fallback phrases that should ALWAYS be labeled as script
            fallback_phrases = [
                "That was some great energy", 
                "Welcome back",
                "NEXT SONG: Coming right up",
                "Let's keep it rolling",
                "Hey music lovers",
                "Keeping the energy high",
                "Right into the flow",
                "Fresh beats, late nights",
                "This is your companion",
                "There's something uniquely peaceful",
                "Taking a moment here to just appreciate",
                "Cruising through the night",
                "The night has its own rhythm",
                "Finding that perfect groove",
                "Let's take a deep breath"
            ]
            
            is_fallback_phrase = any(p.lower() in text.lower() for p in fallback_phrases)
            
            if block_id.startswith("news") or plan_id.startswith("news"):
                line_source = "news"
            elif is_fallback_plan or "scheduled" in plan_id or "fixed" in plan_id or is_fallback_phrase:
                line_source = "script"

            logger.info(f"[PLANNER DEBUG] speaker={speaker}, final_source={line_source}, plan_id={plan_id}, fallback_match={is_fallback_phrase}")

            # Create ScriptLine with basic fields first
            script_line_args = {
                "speaker": speaker,
                "speaker_name": spk_name,
                "text": text,
                "source": line_source,
                "style": self._get_style(speaker, style_hint),
                "pause_after_ms": 250 if i < len(flow) - 1 else 200,
                "voice_id": voice,
                "is_chat": is_chat
            }
            
            # Defensive check: if ScriptLine was somehow loaded from an old version of models.py
            # we try to remove is_chat if it fails, but better to just use kwargs
            try:
                line = ScriptLine(**script_line_args)
            except TypeError:
                # Fallback for old version of ScriptLine
                script_line_args.pop("is_chat", None)
                line = ScriptLine(**script_line_args)

            script_block.lines.append(line)
            
        if not script_block.lines:
            logger.warning("No lines generated in plan — clearing stale global history to break repetition cycle.")
            # Clear stale history so the next LLM call has a chance to produce fresh content
            self._global_dialogue_history.clear()
            spk = "DJ_A"
            spk_name = spk
            for dj in config.get('djs.list', []):
                if dj.get('id') == spk:
                    spk_name = dj.get('name', spk)
                    break
            script_block.lines.append(ScriptLine(
                speaker=spk, 
                speaker_name=spk_name,
                text=self._fallback_line(spk, ""), 
                source="script", # Fallback is "script"
                style=self._get_style(spk, "neutral"), 
                pause_after_ms=200, 
                voice=self._get_voice(spk)
            ))

        return script_block

    async def _generate_line(self, speaker: str, point: str, style_hint: str, conversation_history: List[str], current_track: Optional[Dict] = None, next_track: Optional[Dict] = None, program: Optional[Dict] = None) -> str:
        char_prompt = self._dj_a_prompt if "A" in speaker else self._dj_b_prompt

        personality = ""
        try:
            dj_list = config.get('djs.list', []) or []
            if isinstance(dj_list, list):
                for dj in dj_list:
                    if isinstance(dj, dict) and dj.get('id') == speaker:
                        personality = (dj.get('personality') or "").strip()
                        break
            if not personality:
                personality = (config.get(f'djs.personalities.{speaker}', '') or '').strip()
        except Exception:
            personality = ""


        prompt_parts = []
        prompt_parts.append(f"{char_prompt}\n\n")
        if personality:
            prompt_parts.append(f"DJ personality: {personality}\n")
        if isinstance(current_track, dict) and (current_track.get('title') or current_track.get('artist')):
            prompt_parts.append(f"Current track just ended: {current_track.get('artist','')} - {current_track.get('title','')}\n")
        if isinstance(next_track, dict) and (next_track.get('title') or next_track.get('artist')):
            prompt_parts.append(f"Next track to announce: {next_track.get('artist','')} - {next_track.get('title','')}\n")
            
        if self._global_dialogue_history:
            if program and program.get('type') == 'talk':
                prompt_parts.append("\n--- GLOBAL RECENT SHOW DIALOGUE (READ AND CONTINUE FROM HERE) ---\n")
            else:
                prompt_parts.append("\n--- GLOBAL RECENT SHOW DIALOGUE (DO NOT REPEAT PREVIOUS JOKES OR PHRASES) ---\n")
            for h in self._global_dialogue_history[-15:]:
                prompt_parts.append(f"[{h['time'].strftime('%H:%M')}] {h['speaker']}: {h['text']}\n")
            prompt_parts.append("--- END GLOBAL HISTORY ---\n\n")

        prompt_parts.append(f"Generate a short radio DJ line for: {point}\n")
        
        if conversation_history:
            prompt_parts.append("\n--- DIALOGUE HISTORY FOR THIS SEGMENT ---\n")
            for h in conversation_history:
                prompt_parts.append(f"{h}\n")
            prompt_parts.append(f"--- END HISTORY ---\n\nMake sure to actively REACT to, ANSWER, or ACKNOWLEDGE what the other DJ just said if applicable, so it feels like a real conversation. Continue the flow naturally.\n")
            
        prompt_parts.append(f"Style hint: {style_hint}\n")
        
        if program and program.get('type') == 'talk':
            prompt_parts.append("Keep it natural, conversational, and avoid overly short summary responses. If a previous discussion was happening, continue digging into it. You MUST NOT add stage directions or metadata like [DJ A]. Return ONLY the spoken text, nothing else.")
        else:
            prompt_parts.append("Keep it natural, conversational, and under 30 words. Return ONLY the spoken text, nothing else.")
            
        prompt = "".join(prompt_parts)

        max_attempts = 2
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"LLM generate_line attempt {attempt}/{max_attempts} for {speaker}")
                raw = await self._call_llm(prompt, "model_a" if "A" in speaker else "model_b")
                text = self._clean_line(raw)
                if text and len(text) > 3:
                    return text
            except Exception as e:
                logger.error(f"Failed to generate line attempt {attempt}: {e}")
        
        return self._fallback_line(speaker, point)

    def _clean_line(self, raw: str) -> str:
        """Clean LLM output for a single DJ line."""
        if not raw:
            return ""
        # Remove <think> blocks
        cleaned = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL)
        # Strip markdown, quotes, etc.
        cleaned = cleaned.strip().strip('"').strip("'").strip()
        # Take only the first meaningful line
        for line in cleaned.split('\n'):
            line = line.strip()
            if line and not line.startswith('#') and not line.startswith('```'):
                return line
        return cleaned.split('\n')[0].strip() if cleaned else ""

    def _get_style(self, speaker: str, hint: str) -> dict:
        base = {"energy": 0.5, "warmth": 0.5, "pace": 1.0}
        if "A" in speaker:
            base.update({"energy": 0.7, "warmth": 0.5, "pace": 1.05})
        else:
            base.update({"energy": 0.5, "warmth": 0.7, "pace": 0.98})

        if hint == "energetic":
            base["energy"] = 0.9
        elif hint == "thoughtful":
            base["energy"] = 0.4
            base["warmth"] = 0.8

        return base

    def _get_speaker_name(self, speaker: str) -> str:
        try:
            dj_list = config.get('djs.list', []) or []
            if isinstance(dj_list, list):
                for dj in dj_list:
                    if isinstance(dj, dict) and dj.get('id') == speaker:
                        return dj.get('name', speaker)
        except Exception:
            pass
        return speaker

    def _get_voice(self, speaker: str) -> str:
        voice = ""
        try:
            dj_list = config.get('djs.list', []) or []
            logger.info(f"[GET_VOICE DEBUG] Looking for speaker={speaker}, dj_list={[d.get('id') for d in dj_list]}")
            if isinstance(dj_list, list):
                for dj in dj_list:
                    if isinstance(dj, dict) and dj.get('id') == speaker:
                        voice = (dj.get('voice') or "").strip()
                        logger.info(f"[GET_VOICE DEBUG] Found match: {dj.get('id')} -> voice={voice}")
                        break
            if not voice:
                voice = (config.get(f'djs.voices.{speaker}', '') or '').strip()
                logger.info(f"[GET_VOICE DEBUG] Fallback to djs.voices.{speaker}: {voice}")
        except Exception as e:
            logger.error(f"[GET_VOICE DEBUG] Error: {e}")
            voice = ""
        return voice

    def _fallback_plan(self, duration: int, program: Optional[Dict] = None) -> Dict:
        import random
        openers = ["DJ_A", "DJ_B"]
        if program and program.get('djs'):
            openers = program['djs']
        
        spk1 = random.choice(openers)
        spk2 = openers[1] if len(openers) > 1 else spk1
        
        # Determine if we should do a "Safety Music Handover"
        # If duration is short, it's likely a transition.
        if duration < 30:
            return {
                "block_id": "fallback_script",
                "dialogue": [
                    {"speaker": spk1, "text": "That was some great energy! Let's keep it rolling.", "style_hint": "energetic"},
                    {"speaker": spk1, "text": "NEXT SONG: Coming right up.", "style_hint": "quick"}
                ]
            }

        return {
            "block_id": "fallback_script",
            "topic_tags": ["night_vibe", "fallback"],
            "dialogue": [
                {"speaker": spk1, "text": self._fallback_line(spk1, ""), "style_hint": "energetic", "source": "script"},
                {"speaker": spk2, "text": self._fallback_line(spk2, ""), "style_hint": "thoughtful", "source": "script"},
                {"speaker": spk1, "text": "Let's dive back into the music. Stay tuned.", "style_hint": "smooth", "source": "script"}
            ],
            "mix_notes": {"bed_music": "low"}
        }

    def _fallback_line(self, speaker: str, point: str) -> str:
        import random
        # DJ_A style: Energetic, high-vibe
        fallbacks_a = [
            "Hey music lovers, staying with you through the night!",
            "Keeping the energy high here on the frequency!",
            "Right into the flow, let's keep those vibes moving!",
            "Fresh beats, late nights, and you — perfect combo.",
            "Welcome back! You're tuned into the best AI radio in town!",
            "This is your companion for the night, keeping the tracks spinning!",
        ]
        # DJ_B style: Thoughtful, smooth
        fallbacks_b = [
            "There's something uniquely peaceful about this time of day.",
            "Taking a moment here to just appreciate these smooth frequencies.",
            "Cruising through the night with the perfect soundtrack.",
            "The night has its own rhythm, and we're just here to follow it.",
            "Finding that perfect groove as we move through the playlist.",
            "Let's take a deep breath and enjoy where this music takes us.",
        ]
        
        lower_spk = speaker.lower()
        if "a" in lower_spk or "1" in lower_spk:
            return random.choice(fallbacks_a)
        if "b" in lower_spk or "2" in lower_spk or "3" in lower_spk:
            return random.choice(fallbacks_b)
        
        return random.choice(fallbacks_a + fallbacks_b)

    def _update_recent_topics(self, topics: List[str]) -> None:
        self._recent_topics.extend(topics)
        if len(self._recent_topics) > self._max_recent_topics:
            self._recent_topics = self._recent_topics[-self._max_recent_topics:]

    def _add_to_global_history(self, speaker: str, text: str) -> None:
        self._global_dialogue_history.append({
            "time": datetime.utcnow(),
            "speaker": speaker,
            "text": text
        })
        # Prune older than 1 hour (3600 seconds)
        cutoff = datetime.utcnow().timestamp() - 3600
        self._global_dialogue_history = [
            h for h in self._global_dialogue_history 
            if h["time"].timestamp() > cutoff
        ]
