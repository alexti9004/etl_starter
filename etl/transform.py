import json
from pathlib import Path
import pandas as pd

BRONZE = Path('data/bronze/weather.json')
SILVER_DIR = Path('data/silver')
SILVER_DIR.mkdir(parents=True, exist_ok=True)

def transform():
    if not BRONZE.exists():
        raise FileNotFoundError(f"Не найден {BRONZE}. Сначала запусти extract.py")

    with open(BRONZE, 'r') as f:
        raw = json.load(f)

    # wttr.in -> daily weather в разделе 'weather'
    days = raw.get('weather', [])
    rows = []
    for d in days:
        date = d.get('date')
        # берём дневной блок (hourly с индексом 4 ~ около полудня)
        hourly = d.get('hourly', [])
        h = hourly[4] if len(hourly) > 4 else (hourly[0] if hourly else {})
        row = {
            'date': date,
            'tempC': float(h.get('tempC', 'nan')),
            'feelsLikeC': float(h.get('FeelsLikeC', 'nan')),
            'humidity': float(h.get('humidity', 'nan')),
            'windspeedKmph': float(h.get('windspeedKmph', 'nan')),
            'weatherDesc': (h.get('weatherDesc', [{}])[0].get('value') if h.get('weatherDesc') else None),
        }
        rows.append(row)

    df = pd.DataFrame(rows).dropna(how='all')
    # Сохраняем в silver
    df.to_parquet(SILVER_DIR / 'weather.parquet', index=False)
    df.to_csv(SILVER_DIR / 'weather.csv', index=False)
    print(f"✅ Saved: {SILVER_DIR / 'weather.parquet'} and weather.csv")
    print(df)

if __name__ == '__main__':
    transform()

