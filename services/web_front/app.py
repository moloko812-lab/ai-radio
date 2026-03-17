from flask import (
    Flask,
    render_template,
    jsonify,
    request,
    Response,
    send_from_directory,
    redirect,
)
from flask_cors import CORS
import asyncio
import threading
import random
import time
import os
import io
import wave
import struct
import logging
from pathlib import Path
from datetime import datetime

# ── Ensure project root is on sys.path ────────────────────
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.orchestrator.config import config
from shared import runtime_state
from shared.playlist import playlist_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Absolute path to config.yaml
CONFIG_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config.yaml"
    )
)
config.load(CONFIG_PATH)

app_root = os.path.abspath(os.path.dirname(__file__))
app = Flask(__name__, root_path=app_root, template_folder="templates")
CORS(app)

@app.route('/robots.txt')
def robots_txt():
    return send_from_directory(os.path.join(app_root, '../../'), 'robots.txt')

@app.route('/sitemap.xml')
def sitemap_xml():
    return send_from_directory(os.path.join(app_root, '../../'), 'sitemap.xml')

# ── Shared state (written by orchestrator thread) ─────────
status_data = {
    "buffer_minutes": 0,
    "llm_latency_ms": 0,
    "tts_latency_ms": 0,
    "render_latency_ms": 0,
    "render_latency_ms": 0,
    "state": "WARMUP",
    "online_count": 0,
}

# Structure: { ip: {"last_seen": timestamp, "joined_at": timestamp} }
active_listeners = {}

metadata = {"dj": "", "track": "", "segment_type": "music", "energy": 0.5}

# Expose shared dicts for the dashboard server
runtime_state.status_data = status_data
runtime_state.metadata = metadata

# Reference to orchestrator — set by _start_orchestrator
_orchestrator = None
_orch_loop = None  # asyncio event loop in the orchestrator thread


# ───────────────────────────────────────────────────────────
#  Orchestrator background thread
# ───────────────────────────────────────────────────────────
def _start_orchestrator():
    """Run the Orchestrator in its own asyncio event loop / thread."""
    global _orchestrator, _orch_loop

    from services.orchestrator.app import Orchestrator

    async def _run():
        try:
            import sqlite3
            import os
            from datetime import datetime

            os.makedirs("data", exist_ok=True)
            try:
                conn = sqlite3.connect("data/listeners.db")
                c = conn.cursor()
                c.execute(
                    "CREATE TABLE IF NOT EXISTS listeners_log (timestamp INTEGER, count INTEGER)"
                )
                c.execute(
                    "CREATE TABLE IF NOT EXISTS connections_log (timestamp INTEGER, ip TEXT, event TEXT)"
                )
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"[ERROR] Failed to init DB: {e}")
            
            # Ensure folder logging exists
            if not os.path.exists("data/connections.log"):
                with open("data/connections.log", "w") as f:
                    f.write("timestamp,ip,event\n")

            # --- Library Sync at Startup ---
            try:
                from services.music.library import library
                logger.info("Startup: Cleaning up broken music links...")
                removed = library.cleanup_broken_links()
                if removed > 0:
                    logger.info(f"Startup: Removed {removed} broken tracks from DB.")

                music_folders = config.get("music.folders", [])
                if music_folders:
                    logger.info(f"Startup: Scanning music folders: {music_folders}")
                    stats = library.sync_external_folders(music_folders)
                    logger.info(f"Startup: Sync complete. Added {stats['added']} new tracks (Processed {stats['processed']} files).")
            except Exception as e:
                logger.error(f"Startup: Failed to sync music library: {e}")
            # -------------------------------

            global _orchestrator
            from services.orchestrator.app import Orchestrator

            orch = Orchestrator()
            _orchestrator = orch
            runtime_state.orchestrator = orch

            # Start radio services in background so we don't block the status/metadata loop
            async def _start_radio():
                try:
                    logger.info("Background: Starting Orchestrator...")
                    await orch.start()
                    logger.info("Background: Orchestrator started. Buffer is ready.")

                    # CRITICAL: Create Streamer AFTER orch.start() so orch.buffer exists!
                    from services.streamer.streamer import Streamer

                    streamer = Streamer(orch.buffer)
                    runtime_state.streamer = streamer

                    logger.info("Background: Starting Streamer...")
                    await streamer.start()
                    asyncio.create_task(streamer.stream_loop())
                    logger.info("Background: Radio services started successfully.")
                except Exception as e:
                    logger.error(f"BACKGROUND STARTUP CRASH: {e}")
                    import traceback

                    logger.error(traceback.format_exc())
                    runtime_state.orchestrator = "CRASHED"

            asyncio.create_task(_start_radio())

            # Keep running loop inside the try to maintain scope and catch errors
            # This loop starts IMMEDIATELY now.
            while True:
                try:
                    # If radio isn't started yet, orch is just an object, but state_machine might be there
                    # If orch or its state_machine is missing, get_system_status might fail.
                    # _get_status_dict in dashboard app handles None orch, but here we update status_data.
                    st = orch.state_machine.get_system_status()
                    status_data["state"] = st.state.value
                    now_ts = time.time()
                    # Increase timeout to 45s for stability
                    stale_ips = [
                        ip for ip, data in active_listeners.items() if now_ts - data["last_seen"] > 45
                    ]
                    for ip in stale_ips:
                        del active_listeners[ip]
                    
                    online_count = len(active_listeners)
                    # Create a list of objects with duration [ {ip, duration_sec}, ... ]
                    current_listeners_data = []
                    for ip, data in active_listeners.items():
                        current_listeners_data.append({
                            "ip": ip,
                            "duration_sec": int(now_ts - data["joined_at"])
                        })

                    status_data["online_count"] = online_count
                    status_data["listener_ips"] = [l["ip"] for l in current_listeners_data]
                    status_data["listeners_detailed"] = current_listeners_data
                    
                    # Update metadata with latencies and online count
                    metadata["buffer_minutes"] = round(getattr(st.buffer, "minutes_ahead", 0), 2)
                    metadata["llm_latency_ms"] = round(st.llm_latency_ms, 0)
                    metadata["tts_latency_ms"] = round(st.tts_latency_ms, 0)
                    metadata["render_latency_ms"] = round(st.render_latency_ms, 0)
                    metadata["online_count"] = online_count
                    metadata["listeners_detailed"] = current_listeners_data
                    
                    if online_count > 0:
                        logger.debug(f"Listeners: {online_count}")
                    
                    curr_djs = config.current_program.get("djs", [])

                    def get_formatted_dj_name(dj_id):
                        djs_list = config.djs_config.get("list", [])
                        dj_info = next(
                            (d for d in djs_list if d.get("id") == dj_id), None
                        )
                        if dj_info and dj_info.get("name"):
                            name = str(dj_info.get("name")).strip()
                            if name.startswith("_"):
                                name = name[1:]
                            if name.startswith("DJ "):
                                return name
                            return f"DJ {name}"
                        return f"DJ {dj_id}"

                    if curr_djs:
                        metadata["dj"] = " & ".join(
                            [get_formatted_dj_name(d) for d in curr_djs]
                        )
                    else:
                        metadata["dj"] = (
                            get_formatted_dj_name(st.current_dj)
                            if st.current_dj
                            else "AI DJ"
                        )
                    metadata["energy"] = round(random.uniform(0.3, 0.9), 2)
                    metadata["program_title"] = config.current_program.get(
                        "title", "Night Vibes • 24/7"
                    )

                    playing_seg = getattr(
                        runtime_state, "current_playing_segment", None
                    )
                    if playing_seg:
                        seg_type = str(playing_seg.get("segment_type", "music")).upper()
                        metadata["segment_type"] = seg_type

                        if "MUSIC" in seg_type or "TALK" in seg_type:
                            title = playing_seg.get("track_title", "AI Radio Mix")
                            artist = playing_seg.get("track_artist", "")
                            if artist:
                                metadata["track"] = f"{artist} - {title}"
                            else:
                                metadata["track"] = title
                        else:
                            metadata["track"] = "AI Audio"

                            track_id = playing_seg.get("track_id", "")
                            if track_id:
                                metadata["cover_url"] = f"/api/library/cover/{track_id}"
                            else:
                                metadata["cover_url"] = ""

                        metadata["start_time"] = playing_seg.get("start_time", 0)
                        metadata["duration_sec"] = playing_seg.get("duration_sec", 0)
                        metadata["script_lines"] = playing_seg.get("script_lines", [])
                    else:
                        metadata["track"] = "AI Radio Mix"
                        metadata["segment_type"] = "BUFFERING"
                        metadata["script_lines"] = []

                    # Add status info into metadata for listener UI
                    metadata["buffer_minutes"] = status_data["buffer_minutes"]
                    metadata["llm_latency_ms"] = status_data["llm_latency_ms"]
                    metadata["tts_latency_ms"] = status_data["tts_latency_ms"]
                    metadata["render_latency_ms"] = status_data["render_latency_ms"]
                    metadata["online_count"] = status_data["online_count"]

                except Exception as e:
                    logger.error(f"Error in metadata loop: {e}")

                # Log listeners count every minute
                try:
                    import sqlite3

                    current_time = int(time.time())
                    if not hasattr(_run, "last_log_time"):
                        _run.last_log_time = 0
                    if current_time - _run.last_log_time >= 60:
                        conn = sqlite3.connect("data/listeners.db")
                        c = conn.cursor()
                        c.execute(
                            "INSERT INTO listeners_log (timestamp, count) VALUES (?, ?)",
                            (current_time, len(active_listeners)),
                        )
                        conn.commit()
                        conn.close()
                        _run.last_log_time = current_time
                except Exception as e:
                    logger.error(f"Failed to log to listeners DB: {e}")

                await asyncio.sleep(3)
        except Exception as e:
            import traceback

            print(f"[FATAL ERROR] Orchestrator thread CRASHED:")
            print(traceback.format_exc())
            runtime_state.orchestrator = "CRASHED"

    loop = asyncio.new_event_loop()
    _orch_loop = loop
    runtime_state.orch_loop = loop
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_run())


# Start orchestrator in a daemon thread so Flask doesn't block it
_orch_thread = threading.Thread(target=_start_orchestrator, daemon=True)
_orch_thread.start()


# ───────────────────────────────────────────────────────────
#  Dashboard server (:8001) — run in-process
# ───────────────────────────────────────────────────────────
def _start_dashboard_server():
    """Run the dashboard Flask app on :8001 in the same Python process."""
    try:
        from werkzeug.serving import make_server
        from services.web_dashboard.app import app as dash_app

        # Use SSL context if certificates exist
        # Make the search path absolute relative to this file
        service_dir = Path(__file__).parent.resolve()
        project_root = service_dir.parent.parent
        # Disabling SSL for the internal dashboard to run behind Nginx/Proxy.
        # Nginx will provide SSL on 443 or user can access via clean HTTP on 8001.
        ssl_context = None
        # cert_path = project_root / "certs" / "radio.crt"
        # key_path = project_root / "certs" / "radio.key"
        # if cert_path.exists() and key_path.exists():
        #     ssl_context = (str(cert_path.resolve()), str(key_path.resolve()))
        #     logger.info(f"Dashboard server starting with HTTPS (SSL) on {cert_path.resolve()}")

        port = int(config.get("server.web_dashboard_port", 8001))
        server = make_server("0.0.0.0", port, dash_app, ssl_context=ssl_context)
        server.serve_forever()
    except Exception as e:
        logger.error(f"Failed to start dashboard server: {e}")
        import traceback

        logger.error(traceback.format_exc())


_dash_thread = threading.Thread(target=_start_dashboard_server, daemon=True)
_dash_thread.start()


# ───────────────────────────────────────────────────────────
#  Audio stream endpoint
# ───────────────────────────────────────────────────────────
@app.route("/")
def index():
    spotify_url = config.get("social.spotify_url", "https://spotify.com")
    return render_template("index.html", spotify_url=spotify_url)


@app.route("/download")
def download():
    return render_template("download.html")


@app.route("/hls/<path:filename>")
def hls_files(filename: str):
    # Get real listener IP (behind Nginx/proxy)
    ip = request.headers.get("X-Forwarded-For", request.remote_addr)
    if ip and "," in ip:
        ip = ip.split(",")[0].strip()

    if (
        filename.endswith(".m3u8")
        or filename.endswith(".ts")
        or filename.endswith(".m4s")
    ):
        if ip not in active_listeners:
            print(f"[Radio] New metadata/HLS listener from IP: {ip}")
            active_listeners[ip] = {"last_seen": time.time(), "joined_at": time.time()}
        else:
            active_listeners[ip]["last_seen"] = time.time()

    hls_dir = Path(config.get("stream.hls.output_dir", "./output/hls")).resolve()
    return send_from_directory(str(hls_dir), filename)


@app.route("/radio.mp3")
def stream():
    """Legacy endpoint. Use HLS playlist instead."""
    return redirect("/hls/index.m3u8")


# ───────────────────────────────────────────────────────────
#  REST API
# ───────────────────────────────────────────────────────────
@app.route("/api/now-playing")
def now_playing():
    # Return a copy to avoid thread-safety issues during iteration
    return jsonify(metadata.copy())


@app.route("/api/ping", methods=["GET", "POST"])
def listener_ping():
    """Heartbeat endpoint to track listeners reliably even if HLS is served by Nginx."""
    ip = request.headers.get("X-Forwarded-For", request.remote_addr)
    if ip and "," in ip:
        ip = ip.split(",")[0].strip()
    
    is_new = ip not in active_listeners
    if is_new:
        active_listeners[ip] = {"last_seen": time.time(), "joined_at": time.time()}
        print(f"[Radio] Connection established (API Ping) from: {ip}")
        try:
            with open("data/connections.log", "a") as f:
                f.write(f"{datetime.now().isoformat()},{ip},connected\n")
        except Exception:
            pass
    else:
        active_listeners[ip]["last_seen"] = time.time()
            
    return jsonify({"status": "ok", "online_count": len(active_listeners)})


@app.route("/api/show-info")
def show_info():
    return jsonify(
        {"show_id": "night_radio", "schedule": "24/7", "djs": ["DJ Alex", "DJ Natasha"]}
    )


@app.route("/api/config")
def get_config():
    return jsonify(status_data)


@app.route("/api/schedule")
def get_schedule():
    # Return limited info for the frontend
    # Force reload config using absolute path to be sure
    try:
        config.load(CONFIG_PATH)
    except Exception as e:
        logger.error(f"Failed to reload config in get_schedule: {e}")

    # Use a direct fetch from the config dictionary for robustness
    programs_list = config.get("schedule.programs", [])
    if not programs_list:
        # Fallback to config.schedule property
        programs_list = config.schedule.get("programs", [])

    logger.info(f"Fetched {len(programs_list)} programs from config")

    formatted = []
    for prog in programs_list:
        formatted.append(
            {
                "title": prog.get("title", "Untitled Show"),
                "description": prog.get("description", ""),
                "image_url": prog.get("image_url", ""),
                "start_time": prog.get("start_time", ""),
                "days": prog.get("days", []),
            }
        )
    return jsonify({"programs": formatted})


@app.route("/api/config", methods=["POST"])
def update_config():
    return jsonify({"status": "ok"})


@app.route("/api/buffer")
def get_buffer():
    return jsonify(
        {"minutes": status_data["buffer_minutes"], "segments": 0, "status": "ok"}
    )


@app.route("/logo.png")
def serve_logo():
    project_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    return send_from_directory(project_root, "logo.png")


@app.route("/api/workers")
def get_workers():
    return jsonify(
        {
            "llm": {"status": "running", "latency_ms": status_data["llm_latency_ms"]},
            "tts": {"status": "running", "latency_ms": status_data["tts_latency_ms"]},
            "render": {
                "status": "running",
                "latency_ms": status_data["render_latency_ms"],
            },
        }
    )


@app.route("/api/restart/tts", methods=["POST"])
def restart_tts():
    return jsonify({"status": "restarting"})


@app.route("/api/playlist")
def get_playlist():
    playing_seg = getattr(runtime_state, "current_playing_segment", None)
    curr_id = ""
    if playing_seg:
        curr_id = playlist_manager._get_track_id(playing_seg)
    return jsonify(
        {"items": playlist_manager.get_recent(hours=1.0, current_track_id=curr_id)}
    )


@app.route("/api/rate", methods=["POST"])
def rate_track():
    data = request.json
    track_id = data.get("track_id")
    rating = data.get("rating")
    client_id = data.get("client_id")
    
    user_id = client_id
    if not user_id:
        user_id = request.headers.get("X-Forwarded-For", request.remote_addr)
        if user_id:
            user_id = user_id.split(',')[0].strip()

    if track_id and rating:
        playlist_manager.add_rating(track_id, int(rating), user_id)
        return jsonify(
            {"status": "ok", "new_rating": playlist_manager.get_rating(track_id)}
        )
    return jsonify({"status": "error"}), 400


@app.route("/api/force/music", methods=["POST"])
def force_music():
    if _orchestrator:
        from shared.models import State

        _orchestrator.state_machine.transition(State.MUSIC_FILL)
    return jsonify({"status": "queued"})


@app.route("/api/force/talk", methods=["POST"])
def force_talk():
    if _orchestrator:
        from shared.models import State

        _orchestrator.state_machine.transition(State.TALK_GEN)
    return jsonify({"status": "queued"})


@app.route("/library")
def library_page():
    from services.music.library import library
    track_id = request.args.get('track')
    og_data = None
    
    from urllib.parse import quote
    base_url = request.host_url.rstrip('/')
    if track_id:
        track = library.get_track(track_id)
        if track:
            # Absolute URL for the cover image with proper encoding
            safe_track_id = quote(track_id)
            cover_url = f"{base_url}/api/library/cover/{safe_track_id}"
            
            og_data = {
                "title": f"{track.get('title', 'Unknown')} - {track.get('artist', 'AI Artist')}",
                "description": f"Listen to this {track.get('genre', 'generative')} track on AI Radio.",
                "image": cover_url,
                "url": f"{base_url}/library?track={safe_track_id}"
            }
            # Log for debugging
            logger.info(f"Generated OG data for track: {track_id} -> {cover_url}")
        else:
            logger.warning(f"Track not found for OG data: {track_id}")
            
    return render_template("library.html", og_data=og_data, base_url=base_url)


@app.route("/api/library/tracks")
def get_library_tracks():
    from services.music.library import library
    from shared.playlist import playlist_manager

    db_tracks = library.get_all_tracks(limit=1000)
    tracks = []
    for track in db_tracks:
        # Try to find a cover image in the same directory
        cover_url = ""
        track_path = track.get("path", "")
        if track_path:
            p = Path(track_path)
            has_external = False
            for ext in [".jpg", ".jpeg", ".png", ".webp"]:
                cover_file = p.with_suffix(ext)
                if cover_file.exists():
                    has_external = True
                    break
                    
            if has_external or p.suffix.lower() == ".mp3":
                cover_url = f"/api/library/cover/{track.get('id')}"

        title = track.get("title", "Unknown")
        artist = track.get("artist", "Unknown")
        
        if artist and artist != "Unknown Artist":
            score_id = f"{artist} - {title}".lower().strip()
        else:
            score_id = title.lower().strip()
            
        rating = playlist_manager.get_rating(score_id)
        rating_count = playlist_manager.get_rating_count(score_id)

        tracks.append(
            {
                "id": track.get("id", ""),
                "title": title,
                "artist": artist,
                "album": track.get("artist_info", ""),
                "genre": track.get("genre", ""),
                "tempo": track.get("tempo", ""),
                "mood": track.get("mood", ""),
                "cover_url": cover_url,
                "spotify_url": track.get("spotify_url", ""), # Future proof
                "file_path": track_path,
                "rating": rating,
                "rating_count": rating_count,
            }
        )
    return jsonify({"tracks": tracks})


@app.route("/api/library/top")
def get_top_library_tracks():
    from services.music.library import library
    from shared.playlist import playlist_manager

    db_tracks = library.get_all_tracks(limit=1000)
    tracks_with_scores = []
    
    for track in db_tracks:
        title = track.get("title", "Unknown")
        artist = track.get("artist", "Unknown")
        
        if artist and artist != "Unknown Artist":
            score_id = f"{artist} - {title}".lower().strip()
        else:
            score_id = title.lower().strip()
            
        rating = playlist_manager.get_rating(score_id)
        rating_count = playlist_manager.get_rating_count(score_id)
        
        tracks_with_scores.append({
            "track": track,
            "rating": rating,
            "rating_count": rating_count
        })
        
    # Sort primarily by rating (descending), then rating_count (descending)
    tracks_with_scores.sort(key=lambda x: (x["rating"], x["rating_count"]), reverse=True)
    top_tracks = tracks_with_scores[:15]
    
    frontend_tracks = []
    for item in top_tracks:
        track = item["track"]
        track_path = track.get("path", "")
        # fallback aesthetic cover
        cover_url = "https://images.unsplash.com/photo-1614613535308-eb5fbd3d2c17?w=500&q=80"
        
        if track_path:
            p = Path(track_path)
            has_external = False
            for ext in [".jpg", ".jpeg", ".png", ".webp"]:
                cover_file = p.with_suffix(ext)
                if cover_file.exists():
                    has_external = True
                    break
                    
            if has_external or p.suffix.lower() == ".mp3":
                cover_url = f"/api/library/cover/{track.get('id')}"

        genre = track.get("genre") or track.get("mood") or "AI Music"

        frontend_tracks.append({
            "id": track.get("id", ""),
            "title": track.get("title", "Unknown"),
            "artist": track.get("artist", "Unknown"),
            "genre": genre,
            "img": cover_url,
            "audio": f"/api/library/play/{track.get('id')}"
        })

    return jsonify({"tracks": frontend_tracks})


@app.route("/api/library/play/<track_id>")
def play_library_track(track_id):
    from services.music.library import library
    track = library.get_track(track_id)
    if not track or not track.get("path"):
        return jsonify({"error": "Track not found"}), 404
    
    path = track.get("path")
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404
        
    return send_from_directory(os.path.dirname(path), os.path.basename(path))


@app.route("/api/library/cover/<track_id>")
def serve_library_cover(track_id):
    from services.music.library import library
    import io
    from flask import send_file
    
    track = library.get_track(track_id)
    if not track or not track.get("path"):
        return jsonify({"error": "Track not found"}), 404
    
    p = Path(track.get("path"))
    
    # Optional external images check
    for ext in [".jpg", ".jpeg", ".png", ".webp"]:
        cover_file = p.with_suffix(ext)
        if cover_file.exists():
            return send_from_directory(cover_file.parent, cover_file.name)
            
    # Fallback: Extract embedded cover from MP3 tags
    if p.suffix.lower() == ".mp3":
        try:
            from mutagen.mp3 import MP3
            from mutagen.id3 import ID3, APIC
            
            audio = MP3(str(p), ID3=ID3)
            if audio.tags:
                for tag in audio.tags.values():
                    if isinstance(tag, APIC):
                        # APIC tag found (Attached Picture)
                        return Response(
                            tag.data,
                            mimetype=tag.mime
                        )
        except Exception as e:
            logger.debug(f"Failed to extract embedded cover from {p.name}: {e}")

    # Final fallback: return the logo directly instead of a redirect
    # Scrapers often ignore redirected images
    project_root = Path(__file__).parent.parent.parent
    logo_path = project_root / "logo.png"
    if logo_path.exists():
        return send_from_directory(str(project_root), "logo.png")
        
    return jsonify({"error": "Cover not found"}), 404
            
import re
import random

chat_cooldowns = {}

@app.route("/api/chat", methods=["POST"])
def receive_chat():
    user_ip = request.headers.get("X-Forwarded-For", request.remote_addr).split(',')[0].strip()
    now = time.time()
    
    # Enforce personal/dynamic cooldown if exists, otherwise 120s
    default_cooldown = 120
    user_cooldown = chat_cooldowns.get(f"cooldown_{user_ip}", default_cooldown)
    
    if user_ip in chat_cooldowns:
        last_time = chat_cooldowns[user_ip]
        if isinstance(last_time, (int, float)):
            elapsed = now - last_time
            if elapsed < user_cooldown:
                remaining = int(user_cooldown - elapsed)
                return jsonify({
                    "status": "error", 
                    "error": "cooldown", 
                    "remaining": remaining,
                    "message": f"Please wait {remaining} seconds before sending another message."
                }), 429

    data = request.json
    if not data or "message" not in data:
        return jsonify({"error": "No message provided"}), 400
        
    message = data.get("message", "").strip()
    author = data.get("author", "Listener").strip()
    
    if not message:
        return jsonify({"error": "Empty message"}), 400
        
    if len(message) > 500:
        return jsonify({"error": "Message too long"}), 400
        
    # Check config for listener chat settings
    cfg = config.get("interactions.listener", {})
    enabled = cfg.get("enabled", True)
    if not enabled:
        return jsonify({"status": "disabled"}), 403
        
    filter_profanity = cfg.get("filter_profanity", True)
    
    # --- MODERATION LOGIC ---
    rejected = False
    rejection_reason = ""
    
    # 1. Quick Profanity Stem Check (Fast rejection)
    if filter_profanity:
        profanity_stems = [
            "fuck", "shit", "bitch", "asshole", "cunt", "nigger", "faggot", "dick", "pussy", "idiot",
            "хуй", "пизд", "ебал", "ебат", "сук", "бля", "говн", "гонд", "уеб", "шлюх", "член", "залуп"
        ]
        
        def normalize(t):
            t = (t or "").lower()
            replacements = {'0': 'o', '1': 'i', '3': 'e', '4': 'a', '5': 's', '7': 't', '@': 'a', '$': 's', '!': 'i'}
            for char, rep in replacements.items():
                t = t.replace(char, rep)
            return re.sub(r'[^a-zA-Zа-яА-Я0-9]', '', t)
    
        norm_message = normalize(message)
        norm_author = normalize(author)
        check_text = norm_message + norm_author
        
        for stem in profanity_stems:
            if stem in check_text:
                rejected = True
                rejection_reason = "profanity"
                break

    # 2. AI-Based Moderation (Deep context check)
    if not rejected:
        try:
            llm_cfg = config.llm_config.get("moderator", config.llm_config.get("model_a", {}))
            endpoint = llm_cfg.get("endpoint", "http://localhost:11434/api/generate")
            model = llm_cfg.get("model", "llama3")
            api_key = llm_cfg.get("api_key", "")
            
            prompt = f"""You are a radio broadcast moderator. Your task is to analyze listener messages for suitability on-air.
Rule:
- PROHIBITED: Insults, hate speech, inappropriate sexual content, criminal solicitation, severe profanity (hidden or direct). 
- ALLOWED: Greetings, song requests, feedback, constructive criticism, business mentions, and phone numbers if they are for legitimate contact.

Analyze:
Author: "{author}"
Message: "{message}"

Respond ONLY with 'PASS' if the message is safe, or 'FAIL: [Reason]' if it violates rules.
"""
            import httpx
            with httpx.Client(timeout=10.0) as client:
                data_payload = {
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1, "num_predict": 20}
                }
                headers = {}
                if api_key: headers["Authorization"] = f"Bearer {api_key}"
                
                is_openai = "openai.com" in endpoint or "openrouter.ai" in endpoint or "/v1/" in endpoint
                if is_openai:
                    data_payload = {
                        "model": model,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.1,
                        "max_tokens": 20
                    }
                    if "/v1/" not in endpoint and "openrouter.ai" not in endpoint:
                        endpoint = endpoint.rstrip("/") + "/v1/chat/completions"

                resp = client.post(endpoint, json=data_payload, headers=headers)
                if resp.status_code == 200:
                    res_json = resp.json()
                    ai_res = ""
                    if is_openai:
                        ai_res = res_json.get("choices", [{}])[0].get("message", {}).get("content", "")
                    else:
                        ai_res = res_json.get("response", "")
                    
                    if "FAIL" in ai_res.upper():
                        rejected = True
                        rejection_reason = "ai_moderation"
                        logger.warning(f"AI Modal Rejected message: {message} | Reason: {ai_res}")
        except Exception as e:
            logger.error(f"AI Moderation failed: {e}")

    # 3. Handle Violations (25-minute ban)
    if rejected and rejection_reason in ["profanity", "ai_moderation"]:
        chat_cooldowns[user_ip] = now + 1500
        chat_cooldowns[f"cooldown_{user_ip}"] = 1500
        return jsonify({
            "status": "error",
            "error": "violation",
            "remaining": 1500,
            "message": "Security protocol: Your message violates broadcast rules. You are restricted for 25 minutes."
        }), 403

    # 4. Legit Check for Promo
    has_phone = re.search(r'\+?\d{1,4}?[-.\s]?\(?\d{1,3}?\)?[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}', message) or sum(c.isdigit() for c in message) > 6
    is_promo = bool(has_phone)
    msg_id = f"msg_{int(time.time())}_{random.getrandbits(16)}"

    msg_obj = {
        "id": msg_id,
        "text": message,
        "author": author,
        "timestamp": time.time(),
        "rejected": rejected,
        "rejection_reason": rejection_reason,
        "processed": False,
        "is_promo": is_promo,
        "ip": user_ip
    }
    
    # 5. Queue Position & ETA Calculation
    queue_pos = 1
    eta_sec = 120
    if hasattr(runtime_state, "listener_messages"):
        unprocessed = [m for m in runtime_state.listener_messages if not m.get('rejected') and not m.get('processed')]
        queue_pos = len(unprocessed) + 1
        orch = getattr(runtime_state, 'orchestrator', None)
        if orch:
            is_sparse = config.get("talk.sparse_mode", False)
            blocks_ahead = (queue_pos - 1) // 5
            if is_sparse:
                tracks_to_next = max(0, 3 - getattr(orch, '_sparse_track_count', 0))
                eta_sec = (tracks_to_next * 240) + (blocks_ahead * 840)
            else:
                eta_sec = 300 + (blocks_ahead * 420)
        
        eta_sec = max(120, (eta_sec // 60) * 60)
        runtime_state.listener_messages.append(msg_obj)
        
    # 6. Set personal cooldown for next time
    personal_cooldown = max(120, min(eta_sec // 2, 900))
    chat_cooldowns[user_ip] = now
    chat_cooldowns[f"cooldown_{user_ip}"] = personal_cooldown
    
    return jsonify({
        "status": "ok", 
        "msg_id": msg_id,
        "rejected": rejected,
        "reason": rejection_reason,
        "queue_position": queue_pos,
        "eta_seconds": eta_sec,
        "cooldown_seconds": personal_cooldown
    })

@app.route("/api/chat/status/<msg_id>")
def get_chat_status(msg_id):
    if not hasattr(runtime_state, "listener_messages"):
        return jsonify({"error": "No messages"}), 404
        
    pos = 0
    found = False
    is_processed = False
    
    msgs = list(runtime_state.listener_messages)
    for i, m in enumerate(msgs):
        if m.get("id") == msg_id:
            found = True
            is_processed = m.get("processed", False)
            unprocessed_ahead = [x for x in msgs[:msgs.index(m)] if not x.get('rejected') and not x.get('processed')]
            pos = len(unprocessed_ahead) + 1
            break
            
    if not found:
        return jsonify({"error": "Message not found or rotated out"}), 404
        
    return jsonify({
        "msg_id": msg_id,
        "processed": is_processed,
        "queue_position": pos,
        "status": "queued" if not is_processed else "voiced"
    })


if __name__ == "__main__":
    # When running behind Nginx, Flask should usually run in HTTP mode.
    # Nginx will handle SSL termination on port 443.
    port = int(config.get("server.web_front_port", 8000))
    print(f" * Starting Radio Web Front on port {port} (HTTP)")
    print(f" * SERVER IS READY! Go to http://127.0.0.1:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
