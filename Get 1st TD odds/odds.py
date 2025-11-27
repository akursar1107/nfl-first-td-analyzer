import requests
import os
import json
from datetime import datetime, timedelta
import pytz
import polars as pl
from config import BASE_URL, SPORT, REGION, API_TIMEOUT, MARKET_1ST_TD, ODDS_CACHE_DIR, ODDS_CACHE_EXPIRY, NFL_TEAM_MAP

def get_odds_api_event_ids_for_season(schedule_df: pl.DataFrame, api_key: str) -> dict:
    """
    Fetches all upcoming NFL events from the Odds API once and maps them to nflreadpy game_ids.
    """
    odds_api_event_map = {}
    
    url = f'{BASE_URL}/{SPORT}/events'
    params = {
        'apiKey': api_key,
        'regions': REGION,
        'dateFormat': 'iso',
        'upcoming': 'true'
    }
    
    try:
        resp = requests.get(url, params=params, timeout=API_TIMEOUT)
        resp.raise_for_status()
        odds_events = resp.json()
    except Exception as e:
        print(f"Error fetching Odds API events: {e}")
        return odds_api_event_map

    # Prepare nflreadpy schedule for matching
    nfl_games_for_matching = schedule_df.select([
        pl.col("game_id"),
        pl.col("gameday").str.replace("Z", "+00:00").str.to_datetime(format="%Y-%m-%d", strict=False).alias("nfl_date_dt"),
        pl.col("away_team").replace(NFL_TEAM_MAP, default=pl.col("away_team")).alias("away_team_full"),
        pl.col("home_team").replace(NFL_TEAM_MAP, default=pl.col("home_team")).alias("home_team_full")
    ]).to_dicts()

    for nfl_game in nfl_games_for_matching:
        nfl_game_id = nfl_game['game_id']
        nfl_date = nfl_game['nfl_date_dt']
        nfl_away_full = nfl_game['away_team_full']
        nfl_home_full = nfl_game['home_team_full']

        if not nfl_date:
            continue

        for odds_event in odds_events:
            odds_event_id = odds_event['id']
            odds_home_team = odds_event['home_team']
            odds_away_team = odds_event['away_team']
            odds_commence_time_str = odds_event['commence_time']

            try:
                odds_commence_dt = datetime.fromisoformat(odds_commence_time_str.replace('Z', '+00:00')).astimezone(pytz.utc)
                nfl_date_utc = nfl_date.replace(tzinfo=pytz.utc)
                
                # Check if dates match (allow for 1 day difference due to timezone/late games)
                nfl_date_date = nfl_date_utc.date()
                odds_date_date = odds_commence_dt.date()
                date_match = (nfl_date_date == odds_date_date) or \
                             (odds_date_date == nfl_date_date + timedelta(days=1))
                
                home_match = (nfl_home_full.lower() in odds_home_team.lower()) or \
                             (odds_home_team.lower() in nfl_home_full.lower())
                away_match = (nfl_away_full.lower() in odds_away_team.lower()) or \
                             (odds_away_team.lower() in nfl_away_full.lower())

                if date_match and home_match and away_match:
                    odds_api_event_map[nfl_game_id] = odds_event_id
                    break
            except ValueError:
                continue
    
    return odds_api_event_map

def fetch_odds_data(api_key: str, sport: str, event_id: str):
    """
    Fetches odds data for a given event_id, with caching.
    """
    # Ensure cache directory exists
    os.makedirs(ODDS_CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(ODDS_CACHE_DIR, f"{event_id}.json")
    
    # Check cache
    if os.path.exists(cache_file):
        file_age = datetime.now().timestamp() - os.path.getmtime(cache_file)
        if file_age < ODDS_CACHE_EXPIRY:
            print(f"Loading odds from cache (age: {int(file_age)}s)...")
            try:
                with open(cache_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print("Cache file corrupted, fetching fresh data...")

    url = f"{BASE_URL}/{sport}/events/{event_id}/odds"
    params = {
        "apiKey": api_key,
        "markets": MARKET_1ST_TD,
        "regions": REGION,
        "oddsFormat": "american"
    }
    try:
        response = requests.get(url, params=params, timeout=API_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        # Save to cache
        with open(cache_file, 'w') as f:
            json.dump(data, f)
            
        return data
    except Exception as e:
        print(f"Error fetching odds for event ID {event_id}: {e}")
        return None
