#!/bin/bash
# AI Radio Installation Script for Ubuntu 22.04+
# This script automates the setup of the entire AI Radio project.

set -e # Exit on error

echo "🎙️ Starting AI Radio Installation..."

# 1. Update System
echo "Updating system packages..."
sudo apt update && sudo apt upgrade -y

# 2. Install System Dependencies
echo "Installing system dependencies (FFmpeg, Redis, Python, Espeak, etc.)..."
sudo apt install -y \
    python3 \
    python3-pip \
    python3-venv \
    ffmpeg \
    redis-server \
    libespeak-ng1 \
    phonemizer \
    git \
    wget \
    unzip \
    build-essential \
    libbz2-dev \
    libncurses5-dev \
    libgdbm-dev \
    liblzma-dev \
    libsqlite3-dev \
    libssl-dev \
    uuid-dev \
    zlib1g-dev

# 3. Enable and Start Redis
echo "Setting up Redis..."
sudo systemctl enable redis-server
sudo systemctl start redis-server

# 4. Create Project Virtual Environment
echo "Creating Python virtual environment..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi

# 5. Activate venv and install dependencies
echo "Installing Python dependencies (this may take a while)..."
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# 6. Setup Directory Structure
echo "Creating necessary directories..."
mkdir -p logs cache/tts output/segments data voices assets/music assets/Background assets/fallback

# 7. Check for Kokoro TTS Local
if [ ! -d "Kokoro-TTS-Local-master" ]; then
    echo "⚠️ Warning: Kokoro-TTS-Local-master directory not found."
    echo "Please ensure you have the Kokoro TTS files in the project root."
else
    echo "Setting up Kokoro TTS venv..."
    mkdir -p Kokoro-TTS-Local-master/venv
    python3 -m venv Kokoro-TTS-Local-master/venv
    source Kokoro-TTS-Local-master/venv/bin/activate
    pip install --upgrade pip
    # Kokoro often needs specific torch versions for CUDA, but we'll try standard first
    # or follow the start_radio.py logic
    pip install typing-extensions httpx torch numpy soundfile flask einops kokoro
    deactivate
fi

# 8. Check for Vosk Model
if [ ! -d "vosk-model-tts-ru-0.10-multi" ]; then
    echo "Downloading Vosk Russian TTS model..."
    wget https://alphacephei.com/vosk/models/vosk-model-tts-ru-0.10-multi.zip
    unzip vosk-model-tts-ru-0.10-multi.zip
    rm vosk-model-tts-ru-0.10-multi.zip
fi

echo "✅ Installation Complete!"
echo "--------------------------------------------------"
echo "To start the AI Radio system, run:"
echo "source .venv/bin/activate"
echo "python3 start_radio.py"
echo "--------------------------------------------------"
