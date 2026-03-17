import asyncio
import httpx
import logging
import json
import os
from datetime import datetime, timedelta
from typing import Dict

logger = logging.getLogger(__name__)

# Constants for logging
LOG_DIR = "logs"
API_LOG_FILE = os.path.join(LOG_DIR, "news_api.log")

def log_api_call(source, url, status, response_data=None):
    """Logs API calls and cleans up old entries (> 7 days)."""
    if not os.path.exists(LOG_DIR):
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
        except: pass
    
    timestamp = datetime.now().isoformat()
    entry = {
        "timestamp": timestamp,
        "source": source,
        "url": url,
        "status": status,
        "response": response_data
    }
    
    try:
        with open(API_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except: pass
    
    # Cleanup rotation (keep 1 week)
    cleanup_old_logs()

def cleanup_old_logs():
    """Removes entries older than 7 days from the API log."""
    if not os.path.exists(API_LOG_FILE):
         return
         
    limit = datetime.now() - timedelta(days=7)
    valid_lines = []
    
    try:
        with open(API_LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    ts = datetime.fromisoformat(data["timestamp"])
                    if ts > limit:
                        valid_lines.append(line)
                except:
                    continue
                    
        with open(API_LOG_FILE, "w", encoding="utf-8") as f:
            f.writelines(valid_lines)
    except Exception as e:
        logger.error(f"Cleanup logs failed: {e}")

async def fetch_weather(api_key: str, city: str) -> str:
    import urllib.parse
    safe_city = urllib.parse.quote(city)
    
    if not api_key or api_key.startswith("YOUR_"):
        # Fallback to free open-meteo which requires no API key!
        try:
            # First get coordinates for the city (geocoding)
            geo_url = f"https://geocoding-api.open-meteo.com/v1/search?name={safe_city}&count=1&language=en&format=json"
            async with httpx.AsyncClient() as client:
                geo_resp = await client.get(geo_url, timeout=10)
                geo_data = geo_resp.json()
                if "results" not in geo_data:
                    return f"[SYSTEM NOTE: Weather data missing. DO NOT MENTION WEATHER AT ALL.]"
                
                lat = geo_data["results"][0]["latitude"]
                lon = geo_data["results"][0]["longitude"]
                resolved_city = geo_data["results"][0].get("name", city)
                
                # Now fetch weather
                weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,cloud_cover,precipitation,wind_speed_10m&wind_speed_unit=ms"
                w_resp = await client.get(weather_url, timeout=10)
                w_data = w_resp.json()
                current = w_data["current"]
                
                temp = round(current["temperature_2m"])
                wind = current["wind_speed_10m"]
                cloud = current["cloud_cover"]
                
                conditions = "Clear skies"
                if cloud > 70:
                    conditions = "Cloudy"
                elif cloud > 30:
                    conditions = "Partly cloudy"
                    
                precip = current.get("precipitation", 0)
                if precip > 0:
                    conditions = "Rainy"
                    
                return f"In {resolved_city}, it is currently {temp} degrees Celsius, {conditions}, with wind speeds around {wind} m/s."
        except Exception as e:
            logger.error(f"Failed to fetch open-meteo: {e}")
            return f"[SYSTEM NOTE: Weather data missing. DO NOT MENTION WEATHER AT ALL.]"
    
    # Determine units
    units = "imperial" if city.lower() in ["new york", "ny", "usa", "us"] or city.endswith(", us") else "metric"
    
    # Otherwise use OpenWeatherMap
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={safe_city}&appid={api_key}&units={units}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10)
            data = resp.json()
            log_api_call("OpenWeatherMap", url, resp.status_code, data)
            
            if data.get("cod") != 200:
                raise Exception(f"OpenWeather API Error: {data.get('message')}")
            
            temp = round(data["main"]["temp"])
            desc = data["weather"][0]["description"]
            wind = data.get("wind", {}).get("speed", 0)
            
            if units == "imperial":
                celsius = round((temp - 32) * 5/9)
                return f"In {city}, it is currently {temp} degrees Fahrenheit ({celsius} Celsius) with {desc}. Winds are at {wind} mph."
            else:
                return f"The current weather in {city} is {temp} degrees Celsius with {desc}. Wind speed is {wind} m/s."
    except Exception as e:
        logger.error(f"Failed to fetch weather: {e}")
        return f"[SYSTEM NOTE: Weather data missing. DO NOT MENTION WEATHER AT ALL.]"

async def fetch_news(api_key: str, country: str = "us") -> str:
    if not api_key or api_key.startswith("YOUR_"):
        return "[SYSTEM NOTE: No global news available right now. Do not mention news at all, or just briefly mention that the latest global wire is down.]"
        
    try:
        # Broadening search by removing category=top which can sometimes be empty for certain countries/keys
        url = f"https://newsdata.io/api/1/news?apikey={api_key}&country={country}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=10)
            data = resp.json()
            log_api_call("NewsData", url, resp.status_code, data)
            
            if data.get("status") != "success":
                raise Exception(f"News API error: {data.get('message', 'status not success')}")
                
            articles = data.get("results", [])[:5] # Get up to 5 for more choice
            if not articles:
                 return "There are no major headlines reported for this region at the moment."
                 
            headlines = []
            for a in articles:
                title = a.get("title", "")
                if title:
                    headlines.append(f"- {title}")
                
            return "Latest global headlines:\n" + "\n".join(headlines)
    except Exception as e:
        logger.error(f"Failed to fetch news: {e}")
        return "[SYSTEM NOTE: Global news feed is currently off-grid. Acknowledge briefly if necessary.]"

async def fetch_sports(api_key: str) -> str:
    """Fetch latest sports news using TheSportsDB.
    Tries highlights first, then falls back to recent scores from major leagues.
    """
    if not api_key or api_key.startswith("YOUR_"):
        api_key = "3" # Use the standard developer key as suggested
        
    try:
        async with httpx.AsyncClient() as client:
            # Define major leagues to follow
            leagues = [
                {"id": "4328", "name": "Premier League"},
                {"id": "4346", "name": "MLS"},
                {"id": "4391", "name": "NFL"},
                {"id": "4387", "name": "NBA"},
                {"id": "4380", "name": "NHL"},
                {"id": "4424", "name": "MLB"},
                {"id": "4440", "name": "Tennis ATP"}
            ]
            
            sports_news = []
            
            # Step 1: Try highlights (briefly)
            try:
                highlights_url = f"https://www.thesportsdb.com/api/v1/json/{api_key}/eventshighlights.php"
                resp = await client.get(highlights_url, timeout=5)
                h_data_json = resp.json()
                log_api_call("SportsDB_Highlights", highlights_url, resp.status_code, h_data_json)
                
                h_data = h_data_json.get("tvhighlights", [])
                if h_data:
                    for e in h_data[:3]:
                        sports_news.append(f"Highlight: {e.get('strEvent')}")
            except: pass

            # Step 2: Fetch data for each league in parallel
            async def fetch_league_data(league):
                l_id = league["id"]
                l_name = league["name"]
                local_results = []
                try:
                    # Results
                    res_url = f"https://www.thesportsdb.com/api/v1/json/{api_key}/eventslast.php?id={l_id}"
                    res = await client.get(res_url, timeout=3)
                    if res.status_code == 200:
                        res_json = res.json()
                        log_api_call(f"SportsDB_Last_{l_name}", res_url, res.status_code, res_json)
                        results = res_json.get("results") or []
                        for r in results[:2]:
                            home = r.get("strHomeTeam")
                            away = r.get("strAwayTeam")
                            h_score = r.get("intHomeScore")
                            a_score = r.get("intAwayScore")
                            if home and away and h_score is not None:
                                local_results.append(f"{l_name}: {home} {h_score} - {away} {a_score}")
                    
                    # Standing (Top 3) for major leagues
                    if l_id in ["4328", "4346", "4387", "4380", "4424"]:
                        tab_url = f"https://www.thesportsdb.com/api/v1/json/{api_key}/lookuptable.php?l={l_id}"
                        tab = await client.get(tab_url, timeout=3)
                        if tab.status_code == 200:
                            tab_json = tab.json()
                            log_api_call(f"SportsDB_Table_{l_name}", tab_url, tab.status_code, tab_json)
                            table = tab_json.get("table") or []
                            if table:
                                top_teams = [t.get("strTeam") for t in table[:3]]
                                local_results.append(f"{l_name} top 3: " + ", ".join(top_teams))
                except Exception as e:
                    logger.warning(f"Failed sports league {l_name}: {e}")
                return local_results

            league_tasks = [fetch_league_data(l) for l in leagues]
            results_nested = await asyncio.gather(*league_tasks)
            for sublist in results_nested:
                sports_news.extend(sublist)

            if sports_news:
                # Deduplicate
                unique_news = []
                for n in sports_news:
                    if n not in unique_news:
                        unique_news.append(n)
                
                # Limit to 12 items for a rich bulletin
                summary = " | ".join(unique_news[:12])
                return f"In the world of sports: {summary}."
            
            return "Major global leagues are currently between match days, preparing for upcoming fixtures."
    except Exception as e:
        logger.error(f"Failed to fetch sports: {e}")
        return "[SYSTEM NOTE: Sports feed is currently unavailable.]"

async def fetch_all_hourly_news(config_data: Dict, target_time: datetime = None) -> str:
    # Helper to get nested keys from the dict
    def get_nested(d, path, default=None):
        keys = path.split('.')
        for k in keys:
            if isinstance(d, dict):
                d = d.get(k)
            else:
                return default
        return d if d is not None else default

    weather_city = get_nested(config_data, 'hourly_news.openweathermap.city', 'London')
    w_key = get_nested(config_data, 'hourly_news.openweathermap.api_key', '')
    
    n_country = get_nested(config_data, 'hourly_news.newsdata.country', 'us')
    n_key = get_nested(config_data, 'hourly_news.newsdata.api_key', '')
    
    s_key = get_nested(config_data, 'hourly_news.thesportsdb.api_key', '123')

    logger.info(f"Fetching hourly news for {weather_city}...")
    # Fetch in parallel
    weather, news, sports = await asyncio.gather(
        fetch_weather(w_key, weather_city),
        fetch_news(n_key, n_country),
        fetch_sports(s_key),
        return_exceptions=True
    )
    
    # Handle exceptions if any
    if isinstance(weather, Exception): weather = f"Weather unavailable ({weather})"
    if isinstance(news, Exception): news = "News unavailable."
    if isinstance(sports, Exception): sports = "Sports missing."

    dt = target_time or datetime.now()
    if dt.minute > 45:
        dt = (dt + timedelta(hours=1)).replace(minute=0, second=0)

    date_str = dt.strftime("%B %d, %Y, %I %p")

    combined_prompt = f"""
It is {date_str}. You are reading the top-of-the-hour news, weather, and sports digest for our radio station.

RAW DATA PROVIDED:
[WEATHER]
{weather}

[NEWS]
{news}

[SPORTS]
{sports}

YOUR TASK:
Write a quick, professional, yet engaging 1-minute radio news bulletin integrating the above data.
Do not invent facts not provided. 
CRITICAL RULE: If a specific section (weather, news, or sports) has a [SYSTEM NOTE] or is unavailable/missing data, DO NOT apologize, DO NOT mention the outage, and DO NOT talk about that topic at all. Simply skip it smoothly as if it wasn't on the agenda for today.
Make transitions smooth. At the end, sign off with "That's the latest, now back to the music." 
Output ONLY the text meant to be read on air.
"""
    return combined_prompt
