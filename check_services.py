import requests
import json

def check_local_services():
    services = {
        "Front API": "http://localhost:8000/api/status",
        "Dashboard API": "http://localhost:8001/api/status",
        "Config": "http://localhost:8001/api/config",
        "Voices": "http://localhost:8001/api/voices"
    }
    
    for name, url in services.items():
        try:
            r = requests.get(url, timeout=2)
            print(f"{name}: {r.status_code}")
            if r.status_code == 200:
                data = r.json()
                if name == "Config":
                    djs = data.get('djs', {}).get('list', [])
                    print(f"  DJs in Config: {len(djs)}")
                elif name == "Dashboard API":
                    print(f"  System State: {data.get('state')}")
        except Exception as e:
            print(f"{name}: ERROR - {e}")

if __name__ == "__main__":
    check_local_services()
