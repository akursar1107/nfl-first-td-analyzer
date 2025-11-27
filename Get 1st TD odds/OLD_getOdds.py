import requests
from datetime import datetime, time, timedelta
import nflreadpy as nfl
from dateutil.parser import isoparse
import polars as pl

# An api key is emailed to you when you sign up to a plan
# Get a free API key at https://api.the-odds-api.com/
api_key = '11d265853d712ded110d5e0a5ff82c5b'

SPORT = 'upcoming' # use the sport_key from the /sports endpoint below, or use 'upcoming' to see the next 8 games across all sports
REGIONS = 'us' # uk | us | eu | au. Multiple can be specified if comma delimited
MARKETS = 'h2h,spreads,player_1st_td' # h2h | spreads | totals. Multiple can be specified if comma delimited
ODDS_FORMAT = 'american' # decimal | american
DATE_FORMAT = 'iso' # iso | unix

sport_key = "americanfootball_nfl"
event_id = "fe2eb07797580740af78037ab498198e"  # replace with the specific game ID
url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events"

def normalize_team(name):
    return name.lower().replace(".", "").replace(" ", "")

def linkGames(odds_events_df: pl.DataFrame, schedule_df: pl.DataFrame):
    """
    Links the nflreadpy schedule to The Odds API events by team and time.

    Parameters:
    - odds_events_df: Polars DataFrame of events from The Odds API.
    - schedule_df: Polars DataFrame of the NFL schedule from nflreadpy.
    
    Returns:
    - A Polars DataFrame with the schedule linked to the event IDs.
    """
    # Prepare the schedule DataFrame by normalizing team names
    schedule_df_normalized = schedule_df.with_columns(
        pl.col("home_team").str.to_lowercase().str.replace_all('.','',literal=True).str.replace_all(' ','',literal=True).alias("norm_home_team"),
        pl.col("away_team").str.to_lowercase().str.replace_all('.','',literal=True).str.replace_all(' ','',literal=True).alias("norm_away_team")
    )

    # Prepare the odds events DataFrame
    odds_events_df_normalized = odds_events_df.with_columns(
        pl.col("home_team").str.to_lowercase().str.replace_all(".", "", literal=True).str.replace_all(" ", "", literal=True).alias("norm_home_team"),
        pl.col("away_team").str.to_lowercase().str.replace_all(".", "", literal=True).str.replace_all(" ", "", literal=True).alias("norm_away_team"),
        pl.col("commence_time").str.to_datetime("%Y-%m-%dT%H:%M:%SZ").alias("event_time")
    ).sort(["norm_home_team", "norm_away_team", "event_time"])

    # Sort schedule by the grouping columns and time for proper asof join
    schedule_df_normalized = schedule_df_normalized.sort(["norm_home_team", "norm_away_team", "game_date"])

    # Perform an asof join to link the two DataFrames
    linked_df = schedule_df_normalized.join_asof(
        odds_events_df_normalized,
        left_on="game_date",
        right_on="event_time",
        by=["norm_home_team", "norm_away_team"],
        strategy="nearest",
        tolerance="1h" # 1 hour tolerance to handle potential time differences
    )
    
    return linked_df.rename({"id": "event_id"}).drop(["norm_home_team", "norm_away_team", "home_team", "away_team", "commence_time", "event_time"])

def get_week_events(api_key: str, sport_key: str, url: str):
    # Example usage
    # api_key = "YOUR_API_KEY"
    # sport_key = "americanfootball_nfl"
    # url = "https://api.the-odds-api.com/v4/sports/{sport}/events"
    # games = get_week_events(api_key, sport_key, url)
    # for game in games:
    #     print(f"{game['commence_time']} - {game['away_team']} at {game['home_team']}")

    # Calculate current week's Monday 00:00:00 and Sunday 23:59:59 (UTC)
    today = datetime.utcnow()
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6, hours=23, minutes=59, seconds=59)

    params = {
        "apiKey": api_key,
        "dateFormat": "iso",
        "commenceTimeFrom": monday.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "commenceTimeTo": sunday.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    response = requests.get(url.format(sport=sport_key), params=params)
    response.raise_for_status()

    #games = response.json()

    #print(f"Upcoming NFL games this week ({params['commenceTimeFrom']} to {params['commenceTimeTo']}):\n")
    #for game in games:
    #    print(f"{game['id']} - {game['commence_time']} - {game['away_team']} at {game['home_team']}")

    return response.json()

def get_event_details(api_key: str, sport_key: str, event_id: str):
    """
    Fetches event details (home_team, away_team, commence_time) from The Odds API.
    
    Parameters:
    - api_key: The Odds API key
    - sport_key: Sport key (e.g., "americanfootball_nfl")
    - event_id: The event ID to fetch
    
    Returns:
    - A dictionary with event details or None if not found
    """
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds"
    
    params = {
        "apiKey": api_key,
        "markets": "player_1st_td",
        "regions": "us",
        "oddsFormat": "american"
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    return {
        "home_team": data.get("home_team"),
        "away_team": data.get("away_team"),
        "commence_time": data.get("commence_time")
    }

def match_event_to_schedule(api_key: str, sport_key: str, event_id: str, schedule_df: pl.DataFrame):
    """
    Matches an event_id from The Odds API to a game in the nflreadpy schedule.
    
    Parameters:
    - api_key: The Odds API key
    - sport_key: Sport key
    - event_id: The event ID to match
    - schedule_df: Polars DataFrame of the NFL schedule from nflreadpy
    
    Returns:
    - A Polars DataFrame row with the matched game, or None if no match found
    """
    # Get event details from API
    event = get_event_details(api_key, sport_key, event_id)
    
    # Normalize team names
    event_home = event["home_team"].lower().replace(".", "").replace(" ", "")
    event_away = event["away_team"].lower().replace(".", "").replace(" ", "")
    event_time = isoparse(event["commence_time"])
    
    # Search for matching game in schedule
    schedule_normalized = schedule_df.with_columns(
        pl.col("home_team").str.to_lowercase().str.replace_all('.','',literal=True).str.replace_all(' ','',literal=True).alias("norm_home_team"),
        pl.col("away_team").str.to_lowercase().str.replace_all('.','',literal=True).str.replace_all(' ','',literal=True).alias("norm_away_team")
    )
    
    # Find the game
    matched = schedule_normalized.filter(
        (pl.col("norm_home_team") == event_home) & 
        (pl.col("norm_away_team") == event_away)
    )
    
    if matched.height == 0:
        return None
    
    # Return the matched game with event_id
    return matched.drop(["norm_home_team", "norm_away_team"]).with_columns(
        pl.lit(event_id).alias("event_id")
    )

def get_player_1st_td_odds(api_key: str, sport_key: str, event_id: str):
    # Example usage:
    # api_key = "YOUR_API_KEY"
    # sport_key = "americanfootball_nfl"
    # event_id = "a_specific_event_id_here"
    # get_player_1st_td_odds(api_key, sport_key, event_id)

    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds"
    
    params = {
        "apiKey": api_key,
        "markets": "player_1st_td",
        "regions": "us",  # US region bookmakers
        "oddsFormat": "american"
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    
    for bookmaker in data.get("bookmakers", []):
        if bookmaker["title"].lower() == "fanduel":
            print(f"FanDuel Odds for First Touchdown Scorer:")
            for market in bookmaker.get("markets", []):
                if market["key"] == "player_1st_td":
                    for outcome in market.get("outcomes", []):
                        player = outcome.get("description", outcome["name"]) # Use description if available
                        price = outcome["price"]
                        print(f"Player: {player}, Odds: {price}")
            print()
            break
    else:
        print("FanDuel odds for player_1st_td not found in this event.")

def main():
    
    # 3. Get the odds for this specific event
    print("\nFirst Touchdown Scorer Odds:")
    get_player_1st_td_odds(api_key, sport_key, event_id)

if __name__ == "__main__":
    main()