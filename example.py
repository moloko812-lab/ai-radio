import os
import sys
import io
import re

import torch

# Auto-detect CUDA - use GPU if available, otherwise CPU
if torch.cuda.is_available():
    os.environ["CUDA_VISIBLE_DEVICES"] = "0"
    DEVICE = "cuda"
    torch.cuda.is_available = lambda: True
    torch.cuda.is_bf16_supported = lambda: True
else:
    os.environ["CUDA_VISIBLE_DEVICES"] = ""
    DEVICE = "cpu"
    torch.cuda.is_available = lambda: False
    torch.cuda.is_bf16_supported = lambda: False

import numpy as np
import soundfile as sf
from flask import Flask, request, send_file, jsonify

KOKORO_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "Kokoro-TTS-Local-master"
)
sys.path.insert(0, KOKORO_DIR)

from models import build_model

app = Flask(__name__)

MODEL_PATH = os.path.join(KOKORO_DIR, "kokoro-v1_0.pth")
VOICES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "voices")
SAMPLE_RATE = 24000

MODEL = None
# DEVICE is already set by the detection logic at the top of the file.
# Do not overwrite it with None here.

MODELS = {
    "kokoro": {
        "name": "Kokoro-TTS-Local",
        "voices": [
            "af_sarah",
            "af_nova",
            "am_michael",
            "af_bella",
            "af_sky",
            "am_onyx",
        ],
    },
    "orpheus": {
        "name": "Orpheus-FastAPI",
        "voices": ["orpo", "troy", "dan"],
        "note": "Requires separate Orpheus service",
    },
}

DEFAULT_VOICE = "af_nicole"


def get_model():
    global MODEL, DEVICE
    if MODEL is None:
        print(f"Loading Kokoro model on {DEVICE}...")
        try:
            MODEL = build_model(MODEL_PATH, DEVICE)
        except RuntimeError as e:
            print(f"Error loading model: {e}, falling back to CPU")
            DEVICE = "cpu"
            MODEL = build_model(MODEL_PATH, DEVICE)
        print("Kokoro model loaded")
    return MODEL


@app.route("/health", methods=["GET"])
def health():
    return jsonify(
        {"status": "ok", "model_loaded": MODEL is not None, "device": DEVICE}
    )


@app.route("/models", methods=["GET"])
def list_models():
    return jsonify(MODELS)


@app.route("/voices/<model_id>", methods=["GET"])
def list_voices(model_id: str):
    if model_id not in MODELS:
        return jsonify({"error": "Model not found"}), 404
    return jsonify({"model": model_id, "voices": MODELS[model_id]["voices"]})


@app.route("/tts", methods=["POST"])
def tts():
    data = request.get_json()
    text = data.get("text", "")
    model_id = data.get("model_id", "kokoro")
    voice_id = data.get("voice_id", DEFAULT_VOICE)

    if not text:
        return jsonify({"error": "Text is required"}), 400

    try:
        audio_data = _generate_tts(text, model_id, voice_id)

        return send_file(
            io.BytesIO(audio_data),
            mimetype="audio/wav",
            as_attachment=False,
            download_name="output.wav",
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _generate_tts(text: str, model_id: str, voice_id: str) -> bytes:
    if model_id == "kokoro":
        return _generate_kokoro(text, voice_id)
    elif model_id == "orpheus":
        return _generate_orpheus(text, voice_id)
    else:
        raise ValueError(f"Unknown model: {model_id}")


import threading

MODEL_LOCK = threading.Lock()


def _generate_kokoro(text: str, voice_id: str) -> bytes:
    model = get_model()

    voice_file = os.path.join(VOICES_DIR, f"{voice_id}.pt")
    if not os.path.exists(voice_file):
        voice_file = os.path.join(VOICES_DIR, f"{DEFAULT_VOICE}.pt")

    print(f"Generating TTS: voice={voice_id}, text_length={len(text)}")

    # Split text into sentences to avoid model hangs and improve prosody
    # We use a threshold of 400 chars to decide when to split, but we ALWAYS
    # try to split at sentence boundaries (punctuation)
    if len(text) > 400:
        # split by punctuation while keeping the punctuation
        # This matches . ! or ? followed by a space or end of string
        sentences = re.split(r"(?<=[.!?])\s+", text)
        chunks = []
        current = ""
        for s in sentences:
            s = s.strip()
            if not s:
                continue

            if len(current) + len(s) < 500:
                current = (current + " " + s).strip()
            else:
                if current:
                    chunks.append(current)
                current = s
        if current:
            chunks.append(current)
    else:
        chunks = [text]

    all_audio = []

    try:
        with MODEL_LOCK:
            for i, chunk in enumerate(chunks):
                if not chunk.strip():
                    continue
                
                print(f"Synthesizing chunk {i+1}/{len(chunks)} ({len(chunk)} chars)...", flush=True)
                
                # Check if voice file exists
                v_path = os.path.join(VOICES_DIR, f"{voice_id}.pt")
                if not os.path.exists(v_path):
                    print(f"Warning: Voice {voice_id} not found at {v_path}, using {DEFAULT_VOICE}")
                    v_path = os.path.join(VOICES_DIR, f"{DEFAULT_VOICE}.pt")
                
                # Kokoro model call
                # Note: internal split_pattern can sometimes cause issues with very short sentences
                # We use a simple pattern or None if the chunk is already a single sentence.
                generator = model(
                    chunk,
                    voice=v_path,
                    speed=1.0,
                    split_pattern=r"\n+",
                )

                chunk_audio = []
                for gs, ps, audio in generator:
                    if audio is not None:
                        if isinstance(audio, np.ndarray):
                            audio = torch.from_numpy(audio).float()
                        chunk_audio.append(audio)
                
                if chunk_audio:
                    all_audio.extend(chunk_audio)
                    print(f"Chunk {i+1} done.", flush=True)
                else:
                    print(f"Warning: No audio generated for chunk {i+1}", flush=True)
                
                # Small sleep to prevent CPU overheating/maxing out on long texts
                if len(chunks) > 1:
                    import time
                    time.sleep(0.05)

    except Exception as e:
        print(f"Error during generation: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise

    if not all_audio:
        raise ValueError("Failed to generate any audio segments")

    final_audio = torch.cat(all_audio, dim=0)

    buffer = io.BytesIO()
    # Ensure numpy array for soundfile
    audio_np = final_audio.numpy()
    sf.write(buffer, audio_np, SAMPLE_RATE, format="WAV")
    buffer.seek(0)

    print(f"TTS generation complete. Total duration: {len(audio_np)/SAMPLE_RATE:.2f}s", flush=True)
    return buffer.getvalue()


def _generate_orpheus(text: str, voice_id: str) -> bytes:
    import requests

    orpheus_url = os.environ.get("ORPHEUS_URL", "http://localhost:8004")

    payload = {"input": text, "voice": voice_id}

    response = requests.post(
        f"{orpheus_url}/v1/audio/speech", json=payload, timeout=300
    )

    if response.status_code != 200:
        raise ValueError(f"Orpheus service error: {response.status_code}")

    return response.content


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8003)
