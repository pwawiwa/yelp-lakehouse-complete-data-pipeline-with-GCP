import os
import json
import time
import requests
import random
from datetime import datetime, timezone
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook

# ── Configuration ───────────────
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "yelp-production")
TOPIC_ID = os.getenv("WEATHER_TOPIC", "weather-stream")
API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Top Yelp Cities to monitor
YELP_CITIES = [
    "Philadelphia", "Tampa", "Indianapolis", "Nashville", "Tucson", 
    "New Orleans", "Edmonton", "Santa Barbara", "St. Louis", "Boise"
]

def fetch_weather(city):
    """Fetch current weather for a city from OpenWeatherMap."""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Error fetching weather for {city}: {e}")
        return None

def publish_batch():
    """
    Fetches and publishes weather data for all cities in a single shot.
    Designed for Airflow PythonOperator.
    """
    if not API_KEY or API_KEY == "your_api_key_here":
        print("⚠️  Warning: OPENWEATHER_API_KEY is not set. Using mock data.")
        use_mock = True
    else:
        use_mock = False

    hook = PubSubHook(gcp_conn_id="google_cloud_default")
    
    print(f"📡 Batch Ingest: Processing {len(YELP_CITIES)} cities...")
    
    messages = []
    for city in YELP_CITIES:
        if use_mock:
            weather_data = {
                "name": city,
                "main": {"temp": round(random.uniform(10, 35), 1), "humidity": random.randint(30, 80)},
                "weather": [{"main": random.choice(["Clear", "Clouds", "Rain", "Thunderstorm"])}],
                "wind": {"speed": round(random.uniform(0, 15), 1)},
                "dt": int(time.time())
            }
        else:
            weather_data = fetch_weather(city)

        if weather_data:
            enriched = {
                "city": weather_data.get("name"),
                "temp_c": weather_data.get("main", {}).get("temp"),
                "condition": weather_data.get("weather", [{}])[0].get("main"),
                "humidity": weather_data.get("main", {}).get("humidity"),
                "wind_speed": weather_data.get("wind", {}).get("speed"),
                "api_timestamp": weather_data.get("dt"),
                "ingest_timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "OpenWeatherMap" if not use_mock else "MockData"
            }
            
            message_json = json.dumps(enriched)
            message_bytes = message_json.encode("utf-8")
            messages.append({"data": message_bytes})

    if messages:
        try:
            hook.publish(project_id=PROJECT_ID, topic=TOPIC_ID, messages=messages)
            print(f"✅ Successfully published {len(messages)} messages to {TOPIC_ID}")
        except Exception as e:
            print(f"❌ Batch publish failed: {e}")

def main():
    """Fallback for running locally/cli with a loop."""
    print("🚀 Starting Weather Producer (Loop Mode)...")
    while True:
        publish_batch()
        print("💤 Sleeping for 60s...")
        time.sleep(60)

if __name__ == "__main__":
    main()
