import os
import sys
import torch
import soundfile as sf
from flask import Flask, request, jsonify, send_file
import numpy as np
from pathlib import Path
import logging

# Детальные логи
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KokoroServer")

# Путь к espeak-ng для Phonemizer
os.environ["PHONEMIZER_ESPEAK_LIBRARY"] = "/usr/lib/x86_64-linux-gnu/libespeak-ng.so.1"

# Базовая директория — где лежит этот файл
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

from kokoro import KPipeline

app = Flask(__name__)
device = 'cuda' if torch.cuda.is_available() else 'cpu'

pipeline = None
try:
    pipeline = KPipeline(lang_code='a', device=device)
    logger.info(f"KPipeline ready on {device}")
except Exception as e:
    logger.error(f"Failed to initialize: {e}")

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "pipeline": pipeline is not None})

@app.route('/tts', methods=['POST'])
def tts():
    # Путь к выходному файлу (всегда абсолютный)
    out_file = os.path.join(BASE_DIR, 'output.wav')
    voices_dir = os.path.join(BASE_DIR, "voices")
    
    data = request.json
    text = data.get('text', '')
    voice_name = data.get('voice_id', data.get('voice', 'af_sky')).replace('.pt', '')
    
    logger.info(f"Synthesizing: [{voice_name}] {text[:50]}...")
    
    try:
        if pipeline is None: raise Exception("Pipeline not loaded")
        
        # Полный путь к файлу голоса
        voice_path = os.path.join(voices_dir, f"{voice_name}.pt")
        
        # Если файла нет в папке voices/, передаем просто имя (для встроенных голосов)
        if not os.path.exists(voice_path):
            logger.warning(f"Voice file {voice_path} not found. Trying built-in voice: {voice_name}")
            voice_data = voice_name
        else:
            voice_data = voice_path
            
        # Генерация (передаем ПОЛНЫЙ путь к голосу ИЛИ его имя)
        generator = pipeline(text, voice=voice_data, speed=1)
        
        audio_segments = []
        for gs, ps, audio in generator:
            if audio is not None:
                audio_segments.append(audio)
        
        if not audio_segments: raise Exception("No audio")
            
        full_audio = np.concatenate(audio_segments)
        sf.write(out_file, full_audio, 24000)
        
        return send_file(out_file, mimetype='audio/wav')
        
    except Exception as e:
        logger.error(f"TTS Error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8003, threaded=False)
