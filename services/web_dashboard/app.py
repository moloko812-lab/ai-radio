from pathlib import Path
from typing import List, Dict, Any, Optional
import os
import sys
# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Flask, render_template, jsonify, request, send_from_directory
import sqlite3
import yaml
import json
import time
import logging
import asyncio
import uuid
import werkzeug.utils

# Add this for more direct logging
from shared import runtime_state
from services.orchestrator.config import config
from services.music.library import library
from shared.playlist import playlist_manager

logger = logging.getLogger(__name__)
# Force more verbose logging
logging.basicConfig(level=logging.DEBUG)

app_root = os.path.abspath(os.path.dirname(__file__))
app = Flask(__name__, root_path=app_root, template_folder='templates')
from flask_cors import CORS
CORS(app)

SHOW_IMAGES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'services', 'web_front', 'static', 'images', 'shows')
os.makedirs(SHOW_IMAGES_DIR, exist_ok=True)
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}

@app.route('/static/images/shows/<path:filename>')
def serve_show_images(filename):
    return send_from_directory(SHOW_IMAGES_DIR, filename)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

config_path = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config.yaml'))
voices_dir = Path(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'voices')).resolve()

def load_config():
    try:
        if not os.path.exists(config_path):
            print(f"[ERROR] Dashboard config NOT FOUND: {config_path}")
            return {}
        with open(config_path, 'r', encoding='utf-8') as f:
            res = yaml.safe_load(f) or {}
            
            # Print DJ list to console for verification
            djs = res.get('djs', {}).get('list', [])
            dj_names = [d.get('name', d.get('id', '??')) for d in djs]
            print(f"[Dashboard] Config loaded. Found {len(djs)} DJs: {dj_names}")
            
            # Ensure JSON serializability (sanitizes time/dates/weird types to strings)
            return json.loads(json.dumps(res, default=str))
    except Exception as e:
        print(f"[FATAL ERROR] Dashboard failed to load config: {e}")
        return {}

def save_config(data):
    try:
        existing = load_config()

        # Handle Language Switch
        new_lang = data.get('language')
        if new_lang and new_lang in ['en', 'ru'] and new_lang != existing.get('language'):
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            
            # Switch config
            template_path = os.path.join(base_dir, f'config - {new_lang.upper()}.yaml')
            if os.path.exists(template_path):
                import shutil
                shutil.copyfile(template_path, config_path)
                existing = load_config() # Reload from the new template
            
            # Switch planner prompt
            prompts_dir = os.path.join(base_dir, 'services', 'dialogue', 'prompts')
            planner_template = os.path.join(prompts_dir, f'planner - {new_lang.upper()}.md')
            planner_dest = os.path.join(prompts_dir, 'planner.md')
            if os.path.exists(planner_template):
                import shutil
                shutil.copyfile(planner_template, planner_dest)

        def _deep_update(dst: dict, src: dict) -> dict:
            for k, v in (src or {}).items():
                if isinstance(v, dict) and isinstance(dst.get(k), dict):
                    _deep_update(dst[k], v)
                else:
                    dst[k] = v
            return dst

        _deep_update(existing, data)
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(existing, f, default_flow_style=False)
        return True
    except Exception as e:
        print(f"Save config error: {e}")
        return False

def _get_orchestrator():
    orch = getattr(runtime_state, 'orchestrator', None)
    # If the orchestrator thread reported a crash, treat as None for data loading
    if orch == "CRASHED":
        return None
    if not orch:
        logger.debug("runtime_state.orchestrator is STILL None")
    return orch


def _get_status_dict() -> dict:
    """Return a dashboard-compatible status dict."""
    try:
        orch = _get_orchestrator()
        online_count = getattr(runtime_state, 'status_data', {}).get("online_count", 0)
        listener_ips = getattr(runtime_state, 'status_data', {}).get("listener_ips", [])
        listeners_detailed = getattr(runtime_state, 'status_data', {}).get("listeners_detailed", [])
        
        if not orch:
            is_crashed = getattr(runtime_state, 'orchestrator', None) == "CRASHED"
            return {
                "buffer_minutes": 0.0,
                "segments": 0,
                "llm_latency_ms": 0,
                "tts_latency_ms": 0,
                "render_latency_ms": 0,
                "state": "FATAL_ERROR" if is_crashed else "WARMUP",
                "online_count": online_count,
                "listener_ips": listener_ips,
                "listeners_detailed": listeners_detailed,
                "error": "orchestrator thread crashed" if is_crashed else "orchestrator not started"
            }

        st = orch.state_machine.get_system_status()
        res = {
            "buffer_minutes": float(st.buffer.minutes_ahead) if st.buffer else 0.0,
            "segments": int(st.buffer.segments_count) if st.buffer else 0,
            "llm_latency_ms": int(round(st.llm_latency_ms or 0)),
            "tts_latency_ms": int(round(st.tts_latency_ms or 0)),
            "render_latency_ms": int(round(st.render_latency_ms or 0)),
            "state": st.state.value if st.state else "UNKNOWN",
            "online_count": online_count,
            "listener_ips": listener_ips,
            "listeners_detailed": listeners_detailed,
            "error": st.error_message or ""
        }
        # Safely update instead of reassigning the whole object to avoid breaking refs
        if not hasattr(runtime_state, 'status_data') or not isinstance(runtime_state.status_data, dict):
            runtime_state.status_data = {}
        runtime_state.status_data.update(res)
        return res
    except Exception as e:
        logger.error(f"Error building status dict: {e}")
        return {"state": "ERROR", "error": str(e), "buffer_minutes": 0}

@app.route('/api/status')
def get_status():
    return jsonify(_get_status_dict())


def _music_folders_from_config() -> List[str]:
    """Returns ONLY the global music folders from the 'music' section."""
    cfg = load_config()
    folders = cfg.get('music', {}).get('folders', [])
    if not isinstance(folders, list):
        return []
    return sorted(list(filter(None, folders)))


def _all_active_music_folders() -> List[str]:
    """Returns a merged list of global and per-program music folders."""
    cfg = load_config()
    all_folders = set(cfg.get('music', {}).get('folders', []) or [])
    
    # Check folders in programs too
    programs = cfg.get('schedule', {}).get('programs', [])
    for p in programs:
        pf = p.get('music_folders', [])
        if isinstance(pf, list):
            all_folders.update(pf)
            
    return sorted(list(filter(None, all_folders)))


@app.route('/')
def index():
    return render_template('index.html')
@app.route('/library')
def library_page():
    return render_template('library.html')


@app.route('/schedule')
def schedule_page():
    return render_template('schedule.html')

@app.route('/api/transcript')
def get_transcript():
    # UI expects a single object (latest)
    try:
        if runtime_state.transcript:
            return jsonify(runtime_state.transcript[-1])
    except Exception:
        pass
    return jsonify({"dj": "", "text": "", "timestamp": 0})




@app.route('/api/voices')
def get_voices():
    try:
        # Get voices from filesystem
        voices = sorted([p.stem for p in voices_dir.glob('*.pt')])
        if not voices:
            # Fallback if voices dir is empty or wrong path
            print(f"[WARN] No .pt voices found in {voices_dir}")
        return jsonify({"voices": voices})
    except Exception as e:
        print(f"[ERROR] Failed to list voices: {e}")
        return jsonify({"voices": []})


from flask import send_file
@app.route('/api/test-voice', methods=['POST'])
def test_voice():
    import uuid
    from services.tts_kokoro.worker import TTSWorker
    from shared.models import ScriptLine
    
    data = request.json
    lang = data.get('lang', 'en')
    voice_id = data.get('voice_id', '')
    
    if not voice_id:
        return jsonify({"error": "No voice selected"}), 400

    # Set up text depending on language
    text = "Hello there! This is a test of my voice. How do I sound?"
    if lang == 'ru':
        text = "Всем привет! Это проверка моего голоса для эфира. Как я звучу?"
    
    # We create a dummy script line where we force the voice
    line = ScriptLine(
        speaker="DJ_Test",
        text=text,
        style={"energy": 0.5, "warmth": 0.5, "pace": 1.0},
        voice_id=str(voice_id),
        voice=str(voice_id)
    )
    
    try:
        base_dir = Path(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
        cache_dir = base_dir / "cache" / "tts"
        worker = TTSWorker(cache_dir=str(cache_dir))
        is_ru = (lang == 'ru')
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        output_path = str(worker._cache_dir / f"test_{uuid.uuid4().hex}.wav")
        try:
            if is_ru:
                result = loop.run_until_complete(worker._try_vosk_http(text, int(voice_id), output_path))
            else:
                result = loop.run_until_complete(worker._try_kokoro_http(text, str(voice_id), output_path))
        finally:
            loop.close()
            
        if not result:
            return jsonify({"error": "Voice test generation failed"}), 500
            
        return send_file(result["path"], mimetype='audio/wav')
        
    except Exception as e:
        logger.error(f"Voice test failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/music-folders')
def get_music_folders():
    return jsonify({"folders": _music_folders_from_config()})


@app.route('/api/music-folders', methods=['POST'])
def add_music_folder():
    data = request.json
    folder = data.get('path', '').strip()
    folders = _music_folders_from_config()
    if folder and folder not in folders:
        folders.append(folder)
        save_config({"music": {"folders": folders}})
        _reload_config_in_orchestrator()
    return jsonify({"folders": _music_folders_from_config()})


@app.route('/api/music-folders', methods=['DELETE'])
def remove_music_folder():
    data = request.json
    folder = data.get('path', '')
    folders = _music_folders_from_config()
    if folder in folders:
        folders.remove(folder)
        save_config({"music": {"folders": folders}})
        _reload_config_in_orchestrator()
    return jsonify({"folders": _music_folders_from_config()})


@app.route('/api/config')
def get_config():
    print("DEBUG Dashboard fetching config...")
    return jsonify(load_config())


@app.route('/api/config', methods=['POST'])
def update_config():
    data = request.json
    if save_config(data):
        _reload_config_in_orchestrator()
        return jsonify({"status": "ok", "message": "Config saved"})
    return jsonify({"status": "error", "message": "Failed to save"}), 500


@app.route('/api/upload-show-image', methods=['POST'])
def upload_show_image():
    if 'image' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['image']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    if file and allowed_file(file.filename):
        filename = werkzeug.utils.secure_filename(f"{uuid.uuid4().hex}_{file.filename}")
        file_path = os.path.join(SHOW_IMAGES_DIR, filename)
        file.save(file_path)
        # Return the public URL for the front-end to use in config
    return jsonify({"url": f"/static/images/shows/{filename}"})

@app.route('/api/playlist')
def get_playlist():
    playing_seg = getattr(runtime_state, 'current_playing_segment', None)
    curr_id = ""
    if playing_seg:
        curr_id = playlist_manager._get_track_id(playing_seg)
    return jsonify({"items": playlist_manager.get_recent(hours=1.0, current_track_id=curr_id)})

@app.route('/api/rate', methods=['POST'])
def rate_track():
    data = request.json
    track_id = data.get('track_id')
    rating = data.get('rating')
    client_id = data.get("client_id")
    
    user_id = client_id
    if not user_id:
        user_id = request.headers.get("X-Forwarded-For", request.remote_addr)
        if user_id:
            user_id = user_id.split(',')[0].strip()
            
    if track_id and rating:
        playlist_manager.add_rating(track_id, int(rating), user_id)
        return jsonify({"status": "ok", "new_rating": playlist_manager.get_rating(track_id)})
    return jsonify({"status": "error"}), 400

@app.route('/api/listeners_history')
def get_listeners_history():
    period = request.args.get('period', 'hour')
    try:
        db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'listeners.db')
        if not os.path.exists(db_path):
             return jsonify({"status": "ok", "history": []})
             
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        now = int(time.time())
        
        if period == 'hour':
            start_time = now - 3600
        elif period == 'day':
            start_time = now - 86400
        elif period == 'week':
            start_time = now - 86400 * 7
        else:
            start_time = now - 86400 * 30
            
        c.execute("SELECT timestamp, count FROM listeners_log WHERE timestamp > ? ORDER BY timestamp ASC", (start_time,))
        rows = c.fetchall()
        conn.close()
        
        history = [{"time": r[0]*1000, "count": r[1]} for r in rows]
        return jsonify({"status": "ok", "history": history})
    except Exception as e:
        print(f"Failed to fetch listeners history: {e}")
        return jsonify({"status": "error", "message": str(e)})


@app.route('/api/test-news-fetch')
def test_news_fetch():
    from services.news.news_fetcher import fetch_all_hourly_news
    cfg = load_config()
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            prompt = loop.run_until_complete(fetch_all_hourly_news(cfg))
            return jsonify({"status": "ok", "prompt": prompt})
        finally:
            loop.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/test-news-full', methods=['POST'])
def test_news_full():
    import uuid
    from services.news.news_fetcher import fetch_all_hourly_news
    from services.tts_kokoro.worker import TTSWorker
    from shared.models import ScriptLine
    
    cfg = load_config()
    orch = _get_orchestrator()
    if not orch:
         return jsonify({"error": "Orchestrator not running"}), 400

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            # 1. Fetch raw data
            prompt = loop.run_until_complete(fetch_all_hourly_news(cfg))
            
            # 2. Generate text via LLM
            news_text = "News data unavailable."
            try:
                # Need to run orchestrator/planner methods in a thread-safe way or ensure they don't block
                # Since planner._llm_generate is likely async, we run it in our loop
                news_text = loop.run_until_complete(orch._planner._llm_generate(prompt, max_tokens=1000))
            except Exception as e:
                # Fallback direct call if planner fails
                import httpx
                llm_cfg = cfg.get('llm', {}).get('planner', cfg.get('llm', {}).get('model_a', {}))
                endpoint = llm_cfg.get('endpoint', 'http://localhost:11434/api/generate')
                mod = llm_cfg.get('model', 'qwen2:7b') # Use slightly larger if possible, fallback to qwen2:1.5b
                
                async def _direct_call():
                    async with httpx.AsyncClient() as client:
                        r = await client.post(endpoint, json={"model": mod, "prompt": prompt, "stream": False}, timeout=60)
                        return r.json().get("response", "Could not generate news.")
                news_text = loop.run_until_complete(_direct_call())

            # 3. Generate Audio
            dj_id = cfg.get("hourly_news", {}).get("dj", "DJ_5")
            voice_id = "am_onyx" # fallback
            for d in cfg.get("djs", {}).get("list", []):
                if d.get("id") == dj_id:
                    voice_id = d.get("voice", voice_id)

            base_dir = Path(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
            cache_dir = base_dir / "cache" / "tts"
            worker = TTSWorker(cache_dir=str(cache_dir))
            
            output_path = str(worker._cache_dir / f"test_news_{uuid.uuid4().hex}.wav")
            result = loop.run_until_complete(worker._try_kokoro_http(news_text, str(voice_id), output_path))
            
            if not result:
                 return jsonify({"error": "News TTS failed"}), 500
                 
            return send_file(result["path"], mimetype='audio/wav')
        finally:
            loop.close()

    except Exception as e:
        logger.error(f"News test failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/buffer')
def get_buffer():
    orch = _get_orchestrator()
    if not orch or not orch.buffer:
        return jsonify({"minutes": 0.0, "segments": 0, "status": "starting"})
    st = orch.buffer.get_status()
    return jsonify({
        "minutes": float(st.minutes_ahead),
        "segments": int(st.segments_count),
        "status": "ok"
    })

@app.route('/api/schedule_details')
def get_schedule_details():
    orch = _get_orchestrator()
    if not orch or not orch.buffer:
        return jsonify({"status": "starting", "items": []})
        
    items = []
    
    # Check current playing
    from shared import runtime_state
    if runtime_state.current_playing_segment:
        cps = runtime_state.current_playing_segment
        lines = []
        if isinstance(cps, dict) and "script_lines" in cps:
            lines = cps["script_lines"]
            
        items.append({
            "type": cps.get("segment_type", "unknown"),
            "title": cps.get("track_title", ""),
            "artist": cps.get("track_artist", ""),
            "duration": cps.get("duration_sec", 0),
            "lines": lines,
            "status": "playing"
        })

    # Add buffer items
    for seg in orch.buffer._segments:
        lines = seg.metadata.get("script_lines", []) if isinstance(seg.metadata, dict) else []
        items.append({
            "type": seg.segment_type.value,
            "title": seg.track_title,
            "artist": seg.track_artist,
            "duration": seg.duration_sec,
            "lines": lines,
            "status": "queued"
        })

    resp = jsonify({"status": "ok", "items": items})
    # Diagnostic: using print instead of logger to be safe
    if items and items[0].get('lines'):
        line0 = items[0]['lines'][0]
        print(f"[DASHBOARD DEBUG] First line source: {line0.get('source')} | Text: {line0.get('text')[:20]}")
    return resp


@app.route('/api/workers')
def get_workers():
    s = _get_status_dict()
    
    # Quick LLM check
    llm_status = "Active"
    model_name = "Ollama"
    try:
        from services.orchestrator.config import config
        import requests
        llm_cfg = config.llm_config.get("model_a", {})
        model_name = llm_cfg.get("model", "Ollama")
        endpoint = llm_cfg.get("endpoint", "http://localhost:11434/api/generate")
        base_url = endpoint.rsplit("/api/", 1)[0] if "/api/" in endpoint else "http://localhost:11434"
        
        # very short timeout so dashboard doesn't hang
        res = requests.get(base_url, timeout=1.0)
        if res.status_code == 200:
            llm_status = "Active"
        else:
            llm_status = "Error"
    except Exception:
        llm_status = "Offline"
        
    return jsonify({
        "llm": {"status": llm_status, "latency_ms": s["llm_latency_ms"], "model": model_name},
        "tts": {"status": "Active", "latency_ms": s["tts_latency_ms"]},
        "render": {"status": "Active", "latency_ms": s["render_latency_ms"]}
    })


@app.route('/api/library')
def get_library():
    # Return all tracks with basic filtering
    search = request.args.get('search', '').lower()
    conn = sqlite3.connect(library.db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    if search:
        cursor.execute("""
            SELECT * FROM tracks 
            WHERE lower(title) LIKE ? OR lower(artist) LIKE ? OR lower(genre) LIKE ?
            ORDER BY artist, title
        """, (f"%{search}%", f"%{search}%", f"%{search}%"))
    else:
        cursor.execute("SELECT * FROM tracks ORDER BY artist, title")
        
    rows = cursor.fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/library/track/<track_id>', methods=['GET'])
def get_track_detail(track_id):
    track = library.get_track(track_id)
    if not track:
        return jsonify({"error": "Track not found"}), 404
    
    stats = library.get_track_stats(track_id)
    return jsonify({"track": track, "stats": stats})

@app.route('/api/library/update', methods=['POST'])
def update_library_track():
    data = request.json
    track_id = data.get('id')
    if not track_id:
        return jsonify({"error": "Missing ID"}), 400
        
    library.update_track(track_id, data)
    return jsonify({"status": "ok"})

@app.route('/api/schedule')
def get_schedule():
    orch = _get_orchestrator()
    if not orch:
        return jsonify({'items': [], 'status': 'orchestrator not started'}), 200
    try:
        return jsonify({'items': orch.get_hour_schedule(), 'status': 'ok'})
    except Exception as e:
        return jsonify({'items': [], 'status': 'error', 'error': str(e)}), 200


@app.route('/api/programs')
def get_programs():
    try:
        cfg = load_config()
        programs = cfg.get('schedule', {}).get('programs', [])
        return jsonify({'programs': programs, 'status': 'ok'})
    except Exception as e:
        return jsonify({'programs': [], 'status': 'error', 'error': str(e)}), 200

def _run_on_orch_loop(fn):
    try:
        loop = runtime_state.orch_loop
        orch = runtime_state.orchestrator
        if not loop or not orch:
            return False
        def _call():
            try:
                fn(orch)
            except Exception as e:
                logger.error(f'Failed to run orchestrator action: {e}')
        loop.call_soon_threadsafe(_call)
        return True
    except Exception as e:
        logger.error(f'Failed to dispatch to orchestrator loop: {e}')
        return False


@app.route('/api/schedule/regenerate', methods=['POST'])
def schedule_regenerate():
    ok = _run_on_orch_loop(lambda o: o.regenerate_hour_schedule())
    return jsonify({'status': 'queued' if ok else 'unavailable'})


@app.route('/api/schedule/skip', methods=['POST'])
def schedule_skip():
    ok = _run_on_orch_loop(lambda o: o.skip_current_track())
    return jsonify({'status': 'queued' if ok else 'unavailable'})


@app.route('/api/schedule/force_next', methods=['POST'])
def schedule_force_next():
    def _action(o):
        o.force_next_track()
        try:
            from shared.models import State
            o.state_machine.transition(State.MUSIC_FILL)
        except Exception:
            pass
    ok = _run_on_orch_loop(_action)
    return jsonify({'status': 'queued' if ok else 'unavailable'})

@app.route('/api/restart/tts', methods=['POST'])
def restart_tts():
    # MVP: no separate service manager; keep endpoint for UI.
    return jsonify({"status": "noop"})


@app.route('/api/force/music', methods=['POST'])
def force_music():
    orch = _get_orchestrator()
    if orch:
        from shared.models import State
        orch.state_machine.transition(State.MUSIC_FILL)
    return jsonify({"status": "queued"})


@app.route('/api/force/talk', methods=['POST'])
def force_talk():
    orch = _get_orchestrator()
    if orch:
        from shared.models import State
        orch.state_machine.transition(State.TALK_GEN)
    return jsonify({"status": "queued"})


def _reload_config_in_orchestrator() -> None:
    """Reload config.yaml inside the orchestrator thread/event-loop."""
    try:
        loop = runtime_state.orch_loop
        orch = runtime_state.orchestrator
        if not loop or not orch:
            return

        async def _reload():
            try:
                config.load("config.yaml")
                if orch.buffer:
                    orch.buffer.reload_thresholds()
                try:
                    orch._apply_dj_voices_to_tts()
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"Config reload failed: {e}")

        # Schedule coroutine in orchestrator loop
        asyncio.run_coroutine_threadsafe(_reload(), loop)
    except Exception as e:
        logger.error(f"Failed to schedule config reload: {e}")


if __name__ == '__main__':
    # Use SSL context if certificates exist
    # Ensure absolute pathing with .resolve() for Windows stability
    from pathlib import Path
    root = Path(__file__).parent.parent.parent.resolve()
    cert_path = root / "certs" / "radio.crt"
    key_path = root / "certs" / "radio.key"
    
    ssl_context = None
    if cert_path.exists() and key_path.exists():
        ssl_context = (str(cert_path), str(key_path))
        print(f" * Running dashboard with HTTPS (SSL) on port 8001")
    
    app.run(host='0.0.0.0', port=8001, debug=True, ssl_context=ssl_context)
