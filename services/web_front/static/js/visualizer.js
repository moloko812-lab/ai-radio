/**
 * AI Radio - Enhanced Audio Visualizer
 * Features: Multiple modes (Bars, Waveform, Circle), Neon glows, Responsive, HLS support
 */

class Visualizer {
    constructor(canvasId) {
        this.canvas = document.getElementById(canvasId);
        if (!this.canvas) return;
        this.ctx = this.canvas.getContext('2d');
        this.audioCtx = null;
        this.analyser = null;
        this.source = null;
        this.dataArray = null;
        this.bufferLength = 0;
        this.animationId = null;
        this.modes = ['bars', 'wave', 'neon', 'particles'];
        this.currentModeIndex = 0;
        
        // Colors from CSS root
        this.colors = {
            cyan: '#00f5ff',
            purple: '#bf00ff',
            pink: '#ff0080',
            bg: '#0a0a0f'
        };

        this.init();
        this.resize();
        window.addEventListener('resize', () => this.resize());
        
        // Mode controls
        document.getElementById('visNextBtn')?.addEventListener('click', () => this.nextMode());
        document.getElementById('visPrevBtn')?.addEventListener('click', () => this.prevMode());
    }

    init() {
        this.resize();
    }

    resize() {
        const container = this.canvas.parentElement;
        this.canvas.width = container.clientWidth;
        this.canvas.height = container.clientHeight || 300;
    }

    async connectAudio(audioElement) {
        if (this.audioCtx) return; // Already connected

        try {
            this.audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            this.analyser = this.audioCtx.createAnalyser();
            
            // Connect element to analyser
            this.source = this.audioCtx.createMediaElementSource(audioElement);
            this.source.connect(this.analyser);
            this.analyser.connect(this.audioCtx.destination);

            this.analyser.fftSize = 256;
            this.bufferLength = this.analyser.frequencyBinCount;
            this.dataArray = new Uint8Array(this.bufferLength);

            this.animate();
            console.log("[Visualizer] Connected to audio stream");
        } catch (e) {
            console.error("[Visualizer] AudioContext init failed:", e);
        }
    }

    nextMode() {
        this.currentModeIndex = (this.currentModeIndex + 1) % this.modes.length;
    }

    prevMode() {
        this.currentModeIndex = (this.currentModeIndex - 1 + this.modes.length) % this.modes.length;
    }

    animate() {
        this.animationId = requestAnimationFrame(() => this.animate());
        this.analyser.getByteFrequencyData(this.dataArray);

        this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);

        const mode = this.modes[this.currentModeIndex];
        if (mode === 'bars') this.drawBars();
        else if (mode === 'wave') this.drawWave();
        else if (mode === 'neon') this.drawNeon();
        else if (mode === 'particles') this.drawParticles();
    }

    drawBars() {
        const barWidth = (this.canvas.width / this.bufferLength) * 2.5;
        let barHeight;
        let x = 0;

        for (let i = 0; i < this.bufferLength; i++) {
            barHeight = (this.dataArray[i] / 255) * this.canvas.height * 0.8;

            const gradient = this.ctx.createLinearGradient(0, this.canvas.height, 0, 0);
            gradient.addColorStop(0, this.colors.purple);
            gradient.addColorStop(1, this.colors.cyan);

            this.ctx.fillStyle = gradient;
            this.ctx.fillRect(x, this.canvas.height - barHeight, barWidth - 1, barHeight);

            x += barWidth;
        }
    }

    drawWave() {
        this.ctx.beginPath();
        this.ctx.lineWidth = 3;
        this.ctx.strokeStyle = this.colors.cyan;
        this.ctx.shadowBlur = 15;
        this.ctx.shadowColor = this.colors.cyan;

        const sliceWidth = this.canvas.width / this.bufferLength;
        let x = 0;

        for (let i = 0; i < this.bufferLength; i++) {
            const v = this.dataArray[i] / 128.0;
            const y = (v * this.canvas.height) / 2;

            if (i === 0) this.ctx.moveTo(x, y);
            else this.ctx.lineTo(x, y);

            x += sliceWidth;
        }

        this.ctx.lineTo(this.canvas.width, this.canvas.height / 2);
        this.ctx.stroke();
        this.ctx.shadowBlur = 0;
    }

    drawNeon() {
        const centerY = this.canvas.height / 2;
        const centerX = this.canvas.width / 2;
        
        for (let i = 0; i < this.bufferLength; i += 2) {
            const val = this.dataArray[i];
            const percent = val / 255;
            const height = percent * this.canvas.height * 0.5;
            
            this.ctx.strokeStyle = i % 4 === 0 ? this.colors.pink : this.colors.cyan;
            this.ctx.lineWidth = 2;
            this.ctx.shadowBlur = 10;
            this.ctx.shadowColor = this.ctx.strokeStyle;
            
            this.ctx.beginPath();
            this.ctx.moveTo(centerX + (i * 4) - (this.bufferLength * 2), centerY - height);
            this.ctx.lineTo(centerX + (i * 4) - (this.bufferLength * 2), centerY + height);
            this.ctx.stroke();
        }
        this.ctx.shadowBlur = 0;
    }

    drawParticles() {
        // Simple reactive circles
        const centerX = this.canvas.width / 2;
        const centerY = this.canvas.height / 2;
        const avg = Array.from(this.dataArray).reduce((a, b) => a + b, 0) / this.bufferLength;
        const radius = (avg / 255) * Math.min(centerX, centerY) * 1.5;

        const grad = this.ctx.createRadialGradient(centerX, centerY, 0, centerX, centerY, radius);
        grad.addColorStop(0, this.colors.cyan);
        grad.addColorStop(0.5, this.colors.purple + '88');
        grad.addColorStop(1, 'transparent');

        this.ctx.fillStyle = grad;
        this.ctx.beginPath();
        this.ctx.arc(centerX, centerY, radius, 0, Math.PI * 2);
        this.ctx.fill();
    }
}

// Global hook for the radio player
window.initVisualizer = function(audioElement) {
    if (!window.radioVisualizer) {
        window.radioVisualizer = new Visualizer('visualizer');
    }
    window.radioVisualizer.connectAudio(audioElement);
};

// Auto-trigger when radio starts playing in index.html
// The index.html should call window.initVisualizer(audioElement) inside togglePlay or when HLS starts.
