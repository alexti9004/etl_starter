import requests
import json
from pathlib import Path

def fetch_weather(city="Vancouver"):
    url = f"https://wttr.in/{city}?format=j1"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()

    out_dir = Path("data/bronze")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "weather.json"
    with open(out_file, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Saved to {out_file}")

if __name__ == "__main__":
    fetch_weather()
