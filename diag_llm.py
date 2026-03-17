import httpx
import time
import json
import sys
import os

# Put the key here or use environment variable
API_KEY = "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"
ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "arcee-ai/trinity-large-preview:free"

def test_llm():
    print(f"Testing LLM connectivity from Ubuntu...")
    print(f"Python version: {sys.version}")
    print(f"Target URL: {ENDPOINT}")
    print(f"Model: {MODEL}")
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/alexey-pelykh/ai-radio",
        "X-Title": "AI Radio Diagnostic"
    }
    
    payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": "Say 'Diagnostic OK'"}],
        "temperature": 0.7,
        "max_tokens": 50
    }
    
    t_start = time.time()
    try:
        print(f"[{time.strftime('%H:%M:%S')}] Starting request...")
        # Force no proxies to avoid environment issues
        with httpx.Client(timeout=60, trust_env=False) as client:
            print(f"[{time.strftime('%H:%M:%S')}] Sending POST...")
            resp = client.post(ENDPOINT, json=payload, headers=headers)
            
            t_total = time.time() - t_start
            print(f"[{time.strftime('%H:%M:%S')}] Response received in {t_total:.2f}s")
            print(f"Status Code: {resp.status_code}")
            
            if resp.status_code == 200:
                data = resp.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                print(f"LLM Response: {content}")
                print("\nDIAGNOSTIC SUCCESS!")
            else:
                print(f"Error Response: {resp.text}")
                print("\nDIAGNOSTIC FAILED (API ERROR)")
                
    except Exception as e:
        t_total = time.time() - t_start
        print(f"\n[{time.strftime('%H:%M:%S')}] FATAL ERROR after {t_total:.2f}s:")
        import traceback
        traceback.print_exc()
        print("\nDIAGNOSTIC FAILED (NETWORK/PYTHON ERROR)")

if __name__ == "__main__":
    test_llm()
