import os
import re
import sys
import wave
import traceback
import json
import uuid
from datetime import datetime
from flask import Flask, request, send_file, jsonify

try:
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
except:
    pass

from vosk_tts import Model, Synth

app = Flask(__name__)

# ==============================
# CONFIG
# ==============================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.abspath(os.path.join(BASE_DIR, "vosk-model-tts-ru-0.10-multi"))
OUTPUT_DIR = os.path.join(BASE_DIR, "tts_output_temp")

if not os.path.exists(MODEL_PATH):
    print(f"!!! CRITICAL ERROR: Vosk Russian TTS Model NOT FOUND at: {MODEL_PATH}")
    # We don't exit immediately to allow potential debugging, but it will fail later at load_model
else:
    print(f"Confirmed: Using Russian TTS Model at: {MODEL_PATH}")

MAX_CHUNK_LEN = 300

SPEAKERS = {}

def load_speakers_from_config():
    global SPEAKERS
    config_path = os.path.join(MODEL_PATH, "config.json")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        if "speaker_id_map" in config:
            SPEAKERS = config["speaker_id_map"]
            print(f"Loaded {len(SPEAKERS)} speakers from config.json")
            return
    print("Warning: Could not load speakers from config, using empty list")

MODEL = None
SYNTH = None

def load_model():
    global MODEL, SYNTH
    if MODEL is None:
        print(f"Loading Vosk model from: {MODEL_PATH}")
        try:
            MODEL = Model(model_path=MODEL_PATH)
            SYNTH = Synth(MODEL)
            print("Model loaded successfully!")
        except Exception as e:
            print(f"ERROR loading model: {e}")
            traceback.print_exc()
            sys.exit(1)
    return MODEL, SYNTH

# ==============================
# TEXT NORMALIZATION
# ==============================
def transliterate_to_ru(text: str) -> str:
    # 1. Сначала заменяем частые буквосочетания (регистрозависимо)
    combos = {
        "sh": "ш", "Sh": "Ш", "SH": "Ш",
        "ch": "ч", "Ch": "Ч", "CH": "Ч",
        "th": "з", "Th": "З", "TH": "З",
        "ph": "ф", "Ph": "Ф", "PH": "Ф",
        "ee": "и", "Ee": "И", "EE": "И",
        "oo": "у", "Oo": "У", "OO": "У",
        "ck": "к", "Ck": "К", "CK": "К",
        "qu": "кв", "Qu": "Кв", "QU": "КВ",
    }
    for k, v in combos.items():
        text = text.replace(k, v)

    # 2. Посимвольная транслитерация (основное преобразование)
    mapping = {
        'a': 'а', 'b': 'б', 'c': 'к', 'd': 'д', 'e': 'е', 'f': 'ф', 'g': 'г', 'h': 'х',
        'i': 'и', 'j': 'дж', 'k': 'к', 'l': 'л', 'm': 'м', 'n': 'н', 'o': 'о', 'p': 'п',
        'q': 'к', 'r': 'р', 's': 'с', 't': 'т', 'u': 'у', 'v': 'в', 'w': 'в', 'x': 'кс',
        'y': 'и', 'z': 'з',
        'A': 'А', 'B': 'Б', 'C': 'К', 'D': 'Д', 'E': 'Е', 'F': 'Ф', 'G': 'Г', 'H': 'Х',
        'I': 'И', 'J': 'Дж', 'K': 'К', 'L': 'Л', 'M': 'М', 'N': 'Н', 'O': 'О', 'P': 'П',
        'Q': 'К', 'R': 'Р', 'S': 'С', 'T': 'Т', 'U': 'У', 'V': 'В', 'W': 'В', 'X': 'Кс',
        'Y': 'И', 'Z': 'З'
    }
    
    result = []
    for char in text:
        result.append(mapping.get(char, char))
    
    return "".join(result)

def normalize_text(text: str) -> str:
    # 1. Основные замены пунктуации
    replacements = {
        "…": "...", "—": "-", "–": "-",
        "«": '"', "»": '"', "“": '"', "”": '"', "„": '"', "’": "'", "‘": "'",
        "№": "номер", "\u00A0": " ",
    }
    for k, v in replacements.items():
        text = text.replace(k, v)

    # 2. Транслитерация английских названий
    text = transliterate_to_ru(text)

    # 3. Очистка лишних пробелов
    text = re.sub(r"\s+", " ", text)
    return text.strip()

# ==============================
# UTILS
# ==============================
def split_text(text: str, max_len: int):
    sentences = re.split(r'(?<=[.!?])\s+', text)
    chunks = []
    current = ""

    for sentence in sentences:
        if len(current) + len(sentence) <= max_len:
            current += " " + sentence
        else:
            if current:
                chunks.append(current.strip())
            current = sentence

    if current:
        chunks.append(current.strip())

    return chunks

def concat_wavs(wav_files, output_file):
    data = []
    params = None

    for wf in wav_files:
        with wave.open(wf, 'rb') as w:
            if not params:
                params = w.getparams()
            data.append(w.readframes(w.getnframes()))

    with wave.open(output_file, 'wb') as out:
        out.setparams(params)
        for d in data:
            out.writeframes(d)

# ==============================
# ROUTES
# ==============================
@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok",
        "model": "vosk-0.10-multi",
        "model_path": MODEL_PATH,
        "speakers": SPEAKERS,
        "default_speaker": 53
    })

@app.route('/speakers', methods=['GET'])
def speakers():
    return jsonify(SPEAKERS)

@app.route('/tts', methods=['POST'])
def tts():
    data = request.get_json()
    text = data.get("text", "")
    speaker_id = int(data.get("voice", 53))
    
    if not text:
        return jsonify({"error": "Text is required"}), 400
    
    try:
        model, synth = load_model()
        
        # Получаем имя диджея (опционально, для логов и имени файла)
        speaker_name = "unknown"
        for name, sid in SPEAKERS.items():
            if sid == speaker_id:
                speaker_name = name
                break
                
        print(f"Generating TTS: {len(text)} chars, speaker={speaker_id} ({speaker_name})")
        
        text = normalize_text(text)
        chunks = split_text(text, MAX_CHUNK_LEN)
        print(f"Chunks: {len(chunks)}")
        
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            
        temp_files = []
        req_id = uuid.uuid4().hex
        
        for i, chunk in enumerate(chunks):
            temp_file = os.path.join(OUTPUT_DIR, f"temp_{req_id}_{i}.wav")
            print(f"[{i+1}/{len(chunks)}] Synth chunk")
            synth.synth(chunk, temp_file, speaker_id=speaker_id)
            temp_files.append(temp_file)
            
        final_output = os.path.join(OUTPUT_DIR, f"VOSK_{speaker_name}_{req_id}.wav")
        concat_wavs(temp_files, final_output)
        
        for f in temp_files:
            if os.path.exists(f):
                os.remove(f)
                
        # Send physical file directly (Flask will serve it and then we really shouldn't delete it instantly,
        # but the request is synchronous and send_file can be finicky. 
        # Using a stable file path works around BytesIO problems)
        with open(final_output, "rb") as f:
            audio_bytes = f.read()
            
        # Clean up final output immediately after reading bytes into memory
        os.remove(final_output)
        
        import io
        return send_file(
            io.BytesIO(audio_bytes),
            mimetype='audio/wav',
            as_attachment=False,
            download_name='output.wav'
        )
        
    except Exception as e:
        print("ERROR during synthesis:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("=" * 50)
    print("VOSK TTS RU SERVICE (Exact Match to run_57)")
    print(f"Model: {MODEL_PATH}")
    print("=" * 50)
    load_speakers_from_config()
    load_model()
    # threaded=False IS CRITICAL FOR VOSK TTS. Vosk synth is NOT thread-safe
    # and will generate distorted audio if concurrent requests hit it.
    app.run(host='0.0.0.0', port=8002, threaded=False)
