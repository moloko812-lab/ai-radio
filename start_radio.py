import subprocess
import time
import sys
import os
import platform


def ensure_kokoro_venv():
    """Create venv for Kokoro if it doesn't exist and install dependencies."""
    import sys

    if platform.system() == "Windows":
        venv_python = os.path.join(
            "Kokoro-TTS-Local-master", "venv", "Scripts", "python.exe"
        )
        venv_pip = os.path.join("Kokoro-TTS-Local-master", "venv", "Scripts", "pip.exe")
    else:
        venv_python = os.path.join("Kokoro-TTS-Local-master", "venv", "bin", "python")
        venv_pip = os.path.join("Kokoro-TTS-Local-master", "venv", "bin", "pip")

    # Check if venv exists
    if not os.path.exists(venv_python):
        print("Creating Kokoro venv...")
        subprocess.run([sys.executable, "-m", "venv", "Kokoro-TTS-Local-master/venv"])

        # Install dependencies
        print("Installing Kokoro dependencies...")
        # Check if CUDA is available
        try:
            import torch

            cuda_available = torch.cuda.is_available()
        except:
            cuda_available = False

        # Install compatible versions for older Python
        deps = [
            "typing-extensions",
            "httpx",
            "torch",
            "numpy",
            "soundfile",
            "flask",
            "einops",
            "kokoro",
            "vosk-tts"
        ]

        if cuda_available:
            subprocess.run([venv_pip, "install"] + deps)
        else:
            subprocess.run(
                [venv_pip, "install"]
                + deps
                + ["--index-url", "https://download.pytorch.org/whl/cpu"]
            )

        print("Kokoro dependencies installed.")

    return venv_python


def start_radio():
    print("=== STARTING RADIO SYSTEM ===")

    # Determine Python executable for Kokoro
    if platform.system() == "Windows":
        kokoro_python = os.path.join(
            "Kokoro-TTS-Local-master", "venv", "Scripts", "python.exe"
        )
        if not os.path.exists(kokoro_python):
            kokoro_python = ensure_kokoro_venv()
    else:
        kokoro_python = ensure_kokoro_venv()

    # 1. Start Kokoro English TTS (Port 8003)
    print(f"\n[1/3] Starting Kokoro English TTS (Port 8003) with {kokoro_python}...")
    kokoro_proc = subprocess.Popen(
        [kokoro_python, os.path.join("Kokoro-TTS-Local-master", "example.py")], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT
    )
    time.sleep(5)

    # 2. Start Vosk Russian TTS (Port 8002)
    print("[2/3] Starting Vosk Russian TTS (Port 8002)...")
    vosk_proc = subprocess.Popen(
        [sys.executable, os.path.join("services", "tts_vosk.py")],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    time.sleep(10)  # Vosk model takes a long time to load

    # 3. Start Radio Front + Dashboard + Orchestrator (Port 8000 & 8001)
    print(
        "[3/3] Starting Radio Front-end + Dashboard + Orchestrator (Port 8000 & 8001)..."
    )
    # Pass the kokoro python path to the orchestrator so it knows where to run it
    env = os.environ.copy()
    env["KOKORO_PYTHON"] = kokoro_python
    front_proc = subprocess.Popen(
        [sys.executable, "-u", os.path.join("services", "web_front", "app.py")],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )

    # Check if SSL is enabled
    protocol = "http"
    print("\n=== SYSTEM IS UP AND RUNNING ===")
    print(f"Radio URL:     {protocol}://localhost:8000")
    print(f"Dashboard URL: {protocol}://localhost:8001")
    print(f"\nPress Ctrl+C to stop all services.")

    def read_logs(proc, prefix):
        import select
        # Non-blocking read where possible
        if platform.system() != "Windows":
             import fcntl
             fd = proc.stdout.fileno()
             fl = fcntl.fcntl(fd, fcntl.F_GETFL)
             fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        while True:
            line = proc.stdout.readline()
            if line:
                print(f"[{prefix}] {line.decode('utf-8', errors='replace').strip()}")
            else:
                if proc.poll() is not None:
                    break
                time.sleep(0.1)

    # Use threads to read from all 3 processes
    import threading
    threads = [
        threading.Thread(target=read_logs, args=(kokoro_proc, "Kokoro"), daemon=True),
        threading.Thread(target=read_logs, args=(vosk_proc, "Vosk"), daemon=True),
        threading.Thread(target=read_logs, args=(front_proc, "Radio"), daemon=True),
    ]
    for t in threads: t.start()

    try:
        while True:
            if any(t.is_alive() for t in threads):
                time.sleep(1)
            else:
                break
    except KeyboardInterrupt:
        print("\nStopping all services...")
        kokoro_proc.terminate()
        vosk_proc.terminate()
        front_proc.terminate()
        print("All processes stopped.")


if __name__ == "__main__":
    start_radio()
