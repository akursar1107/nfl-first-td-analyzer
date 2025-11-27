import requests
from datetime import datetime, timedelta
import nflreadpy as nfl
import polars as pl
import json

# API Configuration
API_KEY = '11d265853d712ded110d5e0a5ff82c5b'
SPORT = "americanfootball_nfl"
REGION = 'us' # uk | us | eu | au
event_id = "fe2eb07797580740af78037ab498198e"

# Constants
BASE_URL = "https://api.the-odds-api.com/v4/sports"
API_TIMEOUT = 10
MARKET_1ST_TD = "player_1st_td"

def is_standalone_game(gameday, gametime):
    """
    Returns True for any game that is NOT part of the Sunday main slate.
    Arguments:
      gameday (str): Date string (YYYY-MM-DD)
      gametime (str): Time string (HH:MM)
    """
    if not gameday or not gametime:
        return False
        
    try:
        # 1. Parse Date
        # Handle date string, removing 'Z' if present
        dt = datetime.strptime(str(gameday).replace('Z', ''), "%Y-%m-%d")
        
        # 2. Check if it's Sunday (weekday 6)
        if dt.weekday() == 6:
            # Parse hour to check time
            hour = int(str(gametime).split(':')[0])
            
            # If Sunday AND before 8 PM (20:00), it's Main Slate (Not Standalone)
            if hour < 20:
                return False
                
        # If it's not a Sunday Day Game, it is Standalone.
        return True
        
    except (ValueError, TypeError):
        return False

def get_standalone_games(season: int, schedule_df: pl.DataFrame | None = None) -> list:
    """
    Fetches all standalone games for a given NFL season.
    
    Parameters:
    - season: NFL season year (e.g., 2024)
    - schedule_df: Optional Polars DataFrame from nfl.load_schedules()
    
    Returns:
    - List of standalone game dictionaries with game info
    """
    if schedule_df is None:
        try:
            schedule_df = nfl.load_schedules()
        except Exception as e:
            print(f"Error loading NFL schedule: {e}")
            return []
    
    # Filter for the specified season
    season_games = schedule_df.filter(
        pl.col("season").cast(pl.Int64) == season
    )
    
    if season_games.height == 0:
        print(f"No games found for season {season}")
        return []
    
    # Convert to list of dicts for easier processing
    games_list = season_games.to_dicts()
    
    # Filter for standalone games
    standalone_games = []
    for game in games_list:
        game_date = game.get("gameday")
        game_time = game.get("gametime")
        
        if is_standalone_game(game_date, game_time):
            standalone_games.append(game)
    
    return standalone_games

def print_standalone_games(standalone_games: list, first_td_map: dict | None = None) -> None:
    """
    Prints standalone games in a formatted table.
    
    Parameters:
    - standalone_games: List of standalone game dictionaries
    """
    if not standalone_games:
        print("No standalone games found.")
        return
    
    # Increased width from 80 to 105 to fit the new column
    width = 105
    print("\n" + "="*width)
    print(f"Standalone Games ({len(standalone_games)} total)")
    print("="*width)
    
    # Added 'Game ID' column
    print(f"{'Week':<6} {'Date':<12} {'Time':<8} {'Away':<6} {'Home':<6} {'Game Type':<15} {'Game ID':<22} {'1st TD':<20}")
    print("-" * width)
    
    for game in standalone_games:
        week = game.get("week", "?")
        gameday = game.get("gameday", "Unknown")
        gametime = game.get("gametime", "TBD")
        away_team = game.get("away_team", "?")
        home_team = game.get("home_team", "?")
        game_id = str(game.get("game_id", "Unknown"))
        
        # Determine game type
        try:
            game_date = datetime.fromisoformat(str(gameday).replace('Z', '+00:00'))
            day_of_week = game_date.weekday()
            day_names = ["Mon Night", "Tue (Holiday)", "Wed (Holiday)", "Thu Night", 
                        "Friday", "Saturday", "Sun Night"]
            game_type = day_names[day_of_week] if day_of_week < 7 else "Unknown"
        except Exception:
            game_type = "Unknown"
        
        # Format date for display
        try:
            game_date = datetime.fromisoformat(str(gameday).replace('Z', '+00:00'))
            date_str = game_date.strftime("%m-%d")
        except Exception:
            date_str = str(gameday)[:10]
        
        first_td = None
        if first_td_map and game.get('game_id'):
            first_td = first_td_map.get(game.get('game_id'))
        first_td_str = first_td if first_td else "-"
        print(f"{week:<6} {date_str:<12} {str(gametime):<8} {away_team:<6} {home_team:<6} {game_type:<15} {game_id:<22} {first_td_str:<20}")

    print("=" * width)

def get_first_td_for_game(game_id: str, season: int | None = None) -> str | None:
    """
    Return the player name who scored the first TD for the given game_id.
    Returns None if no touchdown data is available.
    """
    try:
        # Determine season if not provided (game_id format: YYYY_WW_AWAY_HOME)
        if season is None:
            try:
                season = int(str(game_id).split('_')[0])
            except Exception:
                season = None

        seasons = [season] if season else None
        pbp = nfl.load_pbp(seasons=seasons)
        if pbp is None or len(pbp) == 0:
            return None

        # Filter to this game
        if 'game_id' not in pbp.columns:
            return None
        game_plays = pbp.filter(pl.col('game_id') == game_id)
        if game_plays.height == 0:
            return None

        # Filter touchdown plays
        td_plays = None
        if 'touchdown' in game_plays.columns:
            td_plays = game_plays.filter(pl.col('touchdown') == 1)
        else:
            for col in ['td_player_name', 'td_player', 'player_name', 'td_team']:
                if col in game_plays.columns:
                    td_plays = game_plays.filter(pl.col(col).is_not_null())
                    break

        if td_plays is None or td_plays.height == 0:
            return None

        # Assume pbp is chronological; take first touchdown row
        first_td = td_plays.head(1).to_dicts()[0]

        # Extract player name from available columns
        for key in ['td_player_name', 'td_player', 'player_name', 'description']:
            if key in first_td and first_td.get(key):
                return str(first_td.get(key))

        # Fallback: try parsing description
        desc = first_td.get('desc') or first_td.get('description')
        if desc:
            # crude parse: look for 'for' or 'by'
            return desc.split('for')[0].split('by')[-1].strip()

        return None
    except Exception:
        return None

def parse_gameday(gameday_val):
    """Parse gameday value into a datetime or return None."""
    try:
        return datetime.fromisoformat(str(gameday_val).replace('Z', '+00:00'))
    except Exception:
        return None

def get_event_id_for_game(api_key, home_team, away_team):
    """Finds the API Event ID for a specific game."""
    url = 'https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events'
    try:
        resp = requests.get(url, params={'apiKey': api_key, 'regions': 'us'})
        resp.raise_for_status()
        events = resp.json()
        
        for event in events:
            # Fuzzy match team names
            api_home = event['home_team'].lower()
            api_away = event['away_team'].lower()
            if (home_team.lower() in api_home or api_home in home_team.lower()) and \
               (away_team.lower() in api_away or api_away in away_team.lower()):
                return event['id']
    except Exception:
        return None
    return None

def get_player_1st_td_odds(API_KEY: str, SPORT: str, event_id: str):
    # Example usage:
    # api_key = "YOUR_API_KEY"
    # sport_key = "americanfootball_nfl"
    # event_id = "a_specific_event_id_here"
    # get_player_1st_td_odds(api_key, sport_key, event_id)

    url = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events/{event_id}/odds"
    
    params = {
        "apiKey": API_KEY,
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

def get_odds_api_event_id(nfl_game_row, api_key):
    """
    Matches an nflreadr game row to an Odds API Event ID.
    
    Args:
        nfl_game_row (dict): Row from nflreadr (must have 'home_team', 'away_team', 'gameday')
        api_key (str): Your Odds API Key
        
    Returns:
        str: The Odds API Event ID (e.g., "fe2eb0...") or None if not found.
    """
    
    # 1. Fetch ALL upcoming NFL games from Odds API
    # (In a real app, fetch this ONCE and cache it to save API credits)
    url = 'https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events'
    params = {
        'apiKey': api_key,
        'regions': 'us',
        'dateFormat': 'iso'
    }
    
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        odds_events = resp.json()
    except Exception as e:
        print(f"Error fetching API events: {e}")
        return None

    # 2. Normalize nflreadr Data
    # nflreadr uses abbreviations (KC, BAL, DET)
    # Odds API uses full names (Kansas City Chiefs, Baltimore Ravens)
    # We also need to match the Date to avoid preseason/postseason confusion.
    
    nfl_home = nfl_game_row.get('home_team', '').upper()
    nfl_away = nfl_game_row.get('away_team', '').upper()
    nfl_date = nfl_game_row.get('gameday', '') # Format: YYYY-MM-DD
    
    # Dictionary to map Abbreviations to Full Names (or substrings)
    # This helps match "KC" -> "Chiefs"
    team_map = {
        'ARI': 'Cardinals', 'ATL': 'Falcons', 'BAL': 'Ravens', 'BUF': 'Bills',
        'CAR': 'Panthers', 'CHI': 'Bears', 'CIN': 'Bengals', 'CLE': 'Browns',
        'DAL': 'Cowboys', 'DEN': 'Broncos', 'DET': 'Lions', 'GB': 'Packers',
        'HOU': 'Texans', 'IND': 'Colts', 'JAX': 'Jaguars', 'KC': 'Chiefs',
        'LV': 'Raiders', 'LAC': 'Chargers', 'LAR': 'Rams', 'MIA': 'Dolphins',
        'MIN': 'Vikings', 'NE': 'Patriots', 'NO': 'Saints', 'NYG': 'Giants',
        'NYJ': 'Jets', 'PHI': 'Eagles', 'PIT': 'Steelers', 'SF': '49ers',
        'SEA': 'Seahawks', 'TB': 'Buccaneers', 'TEN': 'Titans', 'WAS': 'Commanders'
    }

    search_home = team_map.get(nfl_home, nfl_home)
    search_away = team_map.get(nfl_away, nfl_away)

    # 3. Find the Match
    for event in odds_events:
        api_home_full = event['home_team']
        api_away_full = event['away_team']
        api_start_time = event['commence_time'] # ISO format: 2024-09-05T20:20:00Z
        
        # Date Check (Simple string match on YYYY-MM-DD)
        # Odds API times are UTC, so we grab just the date part. 
        # NOTE: This can be risky for late night games shifting dates, 
        # but usually works for finding the "Game Week".
        api_date_str = api_start_time[:10] 
        
        # Loose Date Check: Allow match if dates are same OR off by 1 day (timezone)
        # But for simplicity, let's rely heavily on TEAM NAME matching first.
        
        # Team Name Check (Is "Chiefs" inside "Kansas City Chiefs"?)
        match_home = search_home in api_home_full
        match_away = search_away in api_away_full
        
        if match_home and match_away:
            return event['id']

    return None

def main():
    try:
        # Load the NFL schedule
        schedule_df = nfl.load_schedules()
    except Exception as e:
        print(f"Error loading schedule: {e}")
        return
    
    print("\n" + "="*60)
    print("NFL Standalone Games Finder")
    print("="*60)
    
    # Get user input for season
    while True:
        try:
            season_input = input("\nEnter NFL season (e.g., 2024): ").strip()
            season = int(season_input)
            break
        except ValueError:
            print("Invalid input. Please enter a valid season year.")
    
    # Get standalone games for the season
    print(f"\nFetching standalone games for season {season}...")
    standalone_games = get_standalone_games(season, schedule_df)

    if not standalone_games:
        print("No standalone games found for this season.")
        return

    # Ask user which week to view (0 = all weeks)
    while True:
        try:
            week_input = input("\nEnter week to view (1-18) or 0 to view all weeks: ").strip()
            week = int(week_input)
            if 0 <= week <= 18:
                break
            else:
                print("Week must be between 0 and 18.")
        except ValueError:
            print("Invalid input. Please enter a valid week number or 0.")

    # Filter standalone games by week if requested
    if week == 0:
        games_to_show = standalone_games
    else:
        games_to_show = [g for g in standalone_games if int(g.get("week", -1)) == week]

    # For played games in the selection, fetch first TD scorers
    first_td_map = {}
    now = datetime.now()
    for g in games_to_show:
        gid = g.get('game_id')
        gd = parse_gameday(g.get('gameday'))
        if not gid or gd is None:
            continue
        now_comp = datetime.now(gd.tzinfo) if gd.tzinfo else now
        if gd < now_comp:
            scorer = get_first_td_for_game(gid, season)
            if scorer:
                first_td_map[gid] = scorer

    # Display selected games (with first TD where available)
    print_standalone_games(games_to_show, first_td_map)

    # Compute summary counts
    total_standalone = len(standalone_games)
    played_to_date = 0
    for g in standalone_games:
        gd = parse_gameday(g.get("gameday"))
        if gd is None:
            continue
        # compare in same timezone context
        now_comp = datetime.now(gd.tzinfo) if gd.tzinfo else now
        if gd < now_comp:
            played_to_date += 1

    current_week_count = len(games_to_show)
    remaining = total_standalone - played_to_date

    # Print summary
    print(f"\nSummary:")
    print(f"  Total Standalone Games (season): {total_standalone}")
    print(f"  Standalone Games Played To Date: {played_to_date}")
    print(f"  Standalone Games In Selected Week: {current_week_count}")
    print(f"  Standalone Games Remaining This Season: {remaining}")

    # for game in games_to_show:
    #     # 2. Get the Linked ID
    #     odds_api_id = get_odds_api_event_id(game, API_KEY)
        
    #     if odds_api_id:
    #         print(f"Found Odds API ID: {odds_api_id}")
    #         # 3. Call your odds function using this ID
    #         get_player_1st_td_odds(API_KEY, "americanfootball_nfl", odds_api_id)
    #     else:
    #         print("Could not link to Odds API (Game not posted yet?)")

if __name__ == "__main__":
    main()