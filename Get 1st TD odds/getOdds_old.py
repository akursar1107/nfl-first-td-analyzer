import requests
from datetime import datetime, timedelta
import nflreadpy as nfl
import polars as pl
import json
import pytz
import os
import io

# API Configuration
API_KEY = '11d265853d712ded110d5e0a5ff82c5b'
SPORT = "americanfootball_nfl"
REGION = 'us'
BASE_URL = "https://api.the-odds-api.com/v4/sports"
API_TIMEOUT = 10
MARKET_1ST_TD = "player_1st_td"
ODDS_CACHE_DIR = "cache/odds"
ODDS_CACHE_EXPIRY = 3600  # 1 hour in seconds

# Mapping for nflreadpy abbreviations to common full team names for Odds API matching
NFL_TEAM_MAP = {
    'ARI': 'Arizona Cardinals', 'ATL': 'Atlanta Falcons', 'BAL': 'Baltimore Ravens',
    'BUF': 'Buffalo Bills', 'CAR': 'Carolina Panthers', 'CHI': 'Chicago Bears',
    'CIN': 'Cincinnati Bengals', 'CLE': 'Cleveland Browns', 'DAL': 'Dallas Cowboys',
    'DEN': 'Denver Broncos', 'DET': 'Detroit Lions', 'GB': 'Green Bay Packers',
    'HOU': 'Houston Texans', 'IND': 'Indianapolis Colts', 'JAX': 'Jacksonville Jaguars',
    'KC': 'Kansas City Chiefs', 'LV': 'Las Vegas Raiders', 'LAC': 'Los Angeles Chargers',
    'LAR': 'Los Angeles Rams', 'MIA': 'Miami Dolphins', 'MIN': 'Minnesota Vikings',
    'NE': 'New England Patriots', 'NO': 'New Orleans Saints', 'NYG': 'New York Giants',
    'NYJ': 'New York Jets', 'PHI': 'Philadelphia Eagles', 'PIT': 'Pittsburgh Steelers',
    'SF': 'San Francisco 49ers', 'SEA': 'Seattle Seahawks', 'TB': 'Tampa Bay Buccaneers',
    'TEN': 'Tennessee Titans', 'WAS': 'Washington Commanders'
}

def is_standalone_game(gameday_expr: pl.Expr, gametime_expr: pl.Expr) -> pl.Expr:
    """
    Returns a Polars Expression that evaluates to True for any game that is NOT part of the Sunday main slate.
    """
    # Convert gameday to datetime objects
    gameday_dt = gameday_expr.str.replace("Z", "").str.to_datetime(format="%Y-%m-%d", strict=False)
    
    # Extract weekday (Sunday is 7 in Polars)
    is_sunday = gameday_dt.dt.weekday() == 7
    
    # Extract hour from gametime
    game_hour = gametime_expr.str.split(':').list.get(0).cast(pl.Int64)
    
    # A game is NOT standalone if it's a Sunday AND starts before 8 PM (20:00)
    is_not_sunday = ~is_sunday
    is_sunday_late = is_sunday & (game_hour >= 20)
    
    return is_not_sunday | is_sunday_late

def get_season_games(season: int, schedule_df: pl.DataFrame) -> pl.DataFrame:
    """
    Fetches all games for a given NFL season and adds 'is_standalone' column.
    """
    # Filter for the specified season
    season_games = schedule_df.filter(
        pl.col("season").cast(pl.Int64) == season
    )
    
    if season_games.height == 0:
        print(f"No games found for season {season}.")
        return pl.DataFrame()

    # Add a column indicating if the game is standalone
    season_games = season_games.with_columns(
        is_standalone_game(pl.col("gameday"), pl.col("gametime")).alias("is_standalone")
    )
    
    return season_games

def get_first_td_scorers(pbp_df: pl.DataFrame, target_game_ids: list[str] | None = None, roster_df: pl.DataFrame | None = None) -> dict:
    """
    Processes play-by-play data to find the first TD scorer for specified games.
    """
    first_td_map = {}
    
    if pbp_df.height == 0:
        return first_td_map

    # Optimization: Filter for only the games we care about
    if target_game_ids:
        pbp_df = pbp_df.filter(pl.col("game_id").is_in(target_game_ids))
        if pbp_df.height == 0:
            return first_td_map

    # Filter for touchdown plays
    td_plays = pbp_df.filter(
        (pl.col('touchdown') == 1) | 
        (pl.col('td_player_name').is_not_null())
    )

    if td_plays.height == 0:
        return first_td_map

    # Sort by game_id and then by play_id/time
    if 'play_id' in td_plays.columns:
        td_plays = td_plays.sort(['game_id', 'play_id'])
    else:
        td_plays = td_plays.sort(['game_id', 'qtr', 'time']) 

    # Get the first touchdown for each game_id
    first_td_per_game = td_plays.group_by('game_id').first()

    # Create a mapping from ID to Full Name if roster is provided
    id_to_name = {}
    if roster_df is not None and "gsis_id" in roster_df.columns and "full_name" in roster_df.columns:
        # Create a dictionary for fast lookup
        temp_df = roster_df.select(["gsis_id", "full_name"]).unique()
        for r in temp_df.to_dicts():
            if r["gsis_id"] and r["full_name"]:
                id_to_name[r["gsis_id"]] = r["full_name"]

    for row in first_td_per_game.to_dicts():
        game_id = row.get('game_id')
        if not game_id:
            continue
            
        scorer = None
        player_id = row.get('td_player_id')

        # 1. Try Roster Lookup
        if player_id and player_id in id_to_name:
            scorer = id_to_name[player_id]

        # 2. Try PBP columns if no roster match
        if not scorer:
            for key in ['fantasy_player_name', 'player_name', 'td_player_name', 'desc', 'description']:
                if key in row and row.get(key):
                    scorer = str(row.get(key))
                    if key in ['desc', 'description'] and ' for ' in scorer:
                        scorer = scorer.split(' for ')[0].strip()
                    break
        
        # Get team
        team = row.get('td_team') or row.get('posteam') or "UNK"
        
        if scorer:
            first_td_map[game_id] = {'player': scorer, 'team': team, 'player_id': player_id}
            
    return first_td_map

def print_games(games_df: pl.DataFrame, title: str = "Games", first_td_map: dict | None = None) -> None:
    """
    Prints games in a formatted table.
    """
    if games_df.height == 0:
        print(f"No games found to display for: {title}.")
        return

    # Prepare data for display
    display_df = games_df.clone()

    # Add 'Game Type' column
    display_df = display_df.with_columns([
        pl.col("gameday").str.replace("Z", "+00:00").str.to_datetime(format="%Y-%m-%d", strict=False).dt.weekday().alias("weekday"),
        pl.col("gametime").str.split(':').list.get(0).cast(pl.Int64, strict=False).fill_null(0).alias("game_hour")
    ]).with_columns(
        pl.when(pl.col("weekday") == 1).then(pl.lit("Mon Night"))
        .when(pl.col("weekday") == 4).then(pl.lit("Thu Night"))
        .when(pl.col("weekday") == 5).then(pl.lit("Friday"))
        .when(pl.col("weekday") == 6).then(pl.lit("Saturday"))
        .when((pl.col("weekday") == 7) & (pl.col("game_hour") >= 20)).then(pl.lit("Sun Night"))
        .when((pl.col("weekday") == 7) & (pl.col("game_hour") < 20)).then(pl.lit("Sun Afternoon"))
        .otherwise(pl.lit("Other")).alias("game_type")
    ).with_columns(
        pl.col("gameday").str.slice(0, 10).alias("display_date")
    )

    # Add '1st TD' and 'TD Team' columns from map
    if first_td_map:
        def get_scorer(gid):
            val = first_td_map.get(gid)
            if isinstance(val, dict):
                return val.get('player', "-")
            return val if val else "-"
            
        def get_td_team(gid):
            val = first_td_map.get(gid)
            if isinstance(val, dict):
                return val.get('team', "-")
            return "-"

        display_df = display_df.with_columns([
            pl.col("game_id").map_elements(get_scorer, return_dtype=pl.Utf8).alias("1st TD"),
            pl.col("game_id").map_elements(get_td_team, return_dtype=pl.Utf8).alias("TD Team")
        ])
    else:
        display_df = display_df.with_columns([
            pl.lit("-").alias("1st TD"),
            pl.lit("-").alias("TD Team")
        ])

    # Select and reorder columns for printing
    columns_to_print = ["week", "display_date", "gametime", "away_team", "home_team", "game_type", "game_id", "1st TD", "TD Team"]
    
    # Ensure all columns exist
    for col in columns_to_print:
        if col not in display_df.columns:
            display_df = display_df.with_columns(pl.lit("?").alias(col))

    games_list = display_df.select(columns_to_print).to_dicts()

    width = 130
    print("\n" + "="*width)
    print(f"{title} ({len(games_list)} total)")
    print("="*width)
    print(f"{'Week':<6} {'Date':<10} {'Time':<8} {'Away':<6} {'Home':<6} {'Game Type':<15} {'Game ID':<25} {'1st TD':<20} {'Team':<6}")
    print("-" * width)
    
    for game in games_list:
        print(f"{game['week']:<6} {game['display_date']:<10} {game['gametime']:<8} {game['away_team']:<6} {game['home_team']:<6} {game['game_type']:<15} {game['game_id']:<25} {game['1st TD']:<20} {game['TD Team']:<6}")
    print("=" * width)

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
        pl.col("away_team").replace_strict(NFL_TEAM_MAP, default=pl.col("away_team")).alias("away_team_full"),
        pl.col("home_team").replace_strict(NFL_TEAM_MAP, default=pl.col("home_team")).alias("home_team_full")
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

def get_player_season_stats(schedule_df: pl.DataFrame, first_td_map: dict, last_n_games: int | None = None) -> dict:
    """
    Calculates season stats (games played by team, 1st TDs by player) to determine probabilities.
    Optionally filters to the last N games for each team.
    Returns: {player_name: {'team': str, 'first_tds': int, 'team_games': int, 'prob': float, 'player_id': str}}
    """
    # 1. Get completed games sorted by date
    completed_game_ids = list(first_td_map.keys())
    if not completed_game_ids:
        return {}
        
    relevant_games = schedule_df.filter(pl.col("game_id").is_in(completed_game_ids))
    # Sort by gameday, gametime to ensure "last N" is accurate
    relevant_games = relevant_games.sort(["gameday", "gametime"])
    
    # 2. Determine valid games per team
    team_valid_games = {} # team -> set(game_ids)
    
    # Get all teams involved in these games
    all_teams = set(relevant_games["home_team"].unique().to_list() + relevant_games["away_team"].unique().to_list())
    
    for team in all_teams:
        # Find games where this team played
        team_games_df = relevant_games.filter(
            (pl.col("home_team") == team) | (pl.col("away_team") == team)
        )
        
        # Get IDs
        all_ids = team_games_df["game_id"].to_list()
        
        if last_n_games:
            # Take last N
            valid_ids = set(all_ids[-last_n_games:])
        else:
            valid_ids = set(all_ids)
            
        team_valid_games[team] = valid_ids

    # 3. Count 1st TDs per Player (filtering by valid games)
    player_stats = {}
    
    for game_id, data in first_td_map.items():
        p_name = data['player']
        p_team = data['team']
        p_id = data.get('player_id')
        
        # Check if this game is in the player's team's valid list
        # (It should be, unless we are filtering)
        if p_team in team_valid_games and game_id in team_valid_games[p_team]:
             if p_name not in player_stats:
                player_stats[p_name] = {'team': p_team, 'first_tds': 0, 'player_id': p_id}
             player_stats[p_name]['first_tds'] += 1
             # Update team/id to most recent
             player_stats[p_name]['team'] = p_team
             if p_id: player_stats[p_name]['player_id'] = p_id
        
    # 4. Calculate Probabilities
    final_stats = {}
    for p_name, stats in player_stats.items():
        team = stats['team']
        # Denominator is the number of valid games for that team
        valid_ids = team_valid_games.get(team, set())
        games_count = len(valid_ids)
        
        first_tds = stats['first_tds']
        
        if games_count > 0:
            prob = first_tds / games_count
            final_stats[p_name] = {
                'team': team,
                'first_tds': first_tds,
                'team_games': games_count,
                'prob': prob,
                'player_id': stats.get('player_id')
            }
            
    return final_stats

def calculate_defense_rankings(schedule_df: pl.DataFrame, first_td_map: dict, roster_df: pl.DataFrame) -> dict:
    """
    Calculates defense rankings vs positions based on First TDs allowed.
    Rank 1 = Fewest Allowed (Best Defense), Rank 32 = Most Allowed (Worst Defense).
    Returns: {team: {pos: rank}}
    """
    # Defense Team -> {WR: 0, RB: 0, TE: 0, QB: 0, Other: 0}
    defense_stats = {}
    all_teams = set(schedule_df["home_team"].unique().to_list() + schedule_df["away_team"].unique().to_list())
    for t in all_teams:
        defense_stats[t] = {'WR': 0, 'RB': 0, 'TE': 0, 'QB': 0, 'Other': 0, 'Total': 0}

    # Get game details
    games_df = schedule_df.filter(pl.col("game_id").is_in(list(first_td_map.keys())))
    game_map = {row['game_id']: {'home': row['home_team'], 'away': row['away_team']} for row in games_df.select(['game_id', 'home_team', 'away_team']).to_dicts()}
    
    for game_id, data in first_td_map.items():
        if game_id not in game_map: continue
        
        scorer_team = data['team']
        player_name = data['player']
        player_id = data.get('player_id')
        
        game_info = game_map[game_id]
        if scorer_team == game_info['home']: defense_team = game_info['away']
        elif scorer_team == game_info['away']: defense_team = game_info['home']
        else: continue
            
        pos = get_player_position(player_id, player_name, roster_df)
        if pos in ['WR', 'RB', 'TE', 'QB']: group_pos = pos
        else: group_pos = 'Other'
            
        if defense_team in defense_stats:
            defense_stats[defense_team][group_pos] += 1
            defense_stats[defense_team]['Total'] += 1

    # Convert counts to ranks
    # We want to rank for each position.
    rankings = {} # team -> {pos: rank}
    for t in all_teams: rankings[t] = {}
    
    for pos in ['WR', 'RB', 'TE', 'QB', 'Total']:
        # Sort teams by count allowed (ascending)
        # Fewest allowed = Rank 1 (Best Defense)
        # Most allowed = Rank 32 (Worst Defense)
        sorted_teams = sorted(defense_stats.items(), key=lambda x: x[1].get(pos, 0))
        
        for rank, (team, stats) in enumerate(sorted_teams, 1):
            rankings[team][pos] = rank
            
    return rankings

def calculate_fair_odds(prob: float) -> int:
    """
    Converts a probability (0-1) to American Odds.
    """
    if prob <= 0:
        return 0 # No chance
    if prob >= 1:
        return -10000 # Certainty
        
    if prob > 0.5:
        return int((prob / (1 - prob)) * -100)
    else:
        return int(((1 - prob) / prob) * 100)

def display_odds(odds_data: dict | None, interactive: bool = False, default_mode: str = 'best', 
                 player_stats: dict | None = None, 
                 defense_rankings: dict | None = None,
                 roster_df: pl.DataFrame | None = None,
                 home_team: str | None = None,
                 away_team: str | None = None):
    """
    Displays odds data based on user preference or default mode.
    Modes: 'best', 'all', 'specific'
    Includes EV calculation if player_stats is provided.
    Includes Matchup analysis if defense_rankings is provided.
    """
    if not odds_data:
        return

    bookmakers = odds_data.get("bookmakers", [])
    if not bookmakers:
        print("No bookmakers found with odds for this event.")
        return

    bm_names = [b['title'] for b in bookmakers]
    mode = default_mode
    selected_bm = None

    if interactive:
        print(f"\nAvailable Bookmakers: {', '.join(bm_names)}")
        print("1. Best Odds (Compare all)")
        print("2. All Bookmakers")
        print("3. Select Specific Bookmaker")
        
        choice = input("Enter choice (1-3): ").strip()
        if choice == '2':
            mode = 'all'
        elif choice == '3':
            print("\nSelect Bookmaker:")
            for i, name in enumerate(bm_names, 1):
                print(f"{i}. {name}")
            try:
                idx = int(input("Enter number: ")) - 1
                if 0 <= idx < len(bm_names):
                    mode = 'specific'
                    selected_bm = bm_names[idx]
                else:
                    print("Invalid selection. Defaulting to Best Odds.")
            except ValueError:
                print("Invalid input. Defaulting to Best Odds.")
        else:
            mode = 'best'

    print(f"\n--- Odds Display Mode: {mode.upper()}{f' ({selected_bm})' if selected_bm else ''} ---")

    # Helper to find stats for a player name (fuzzy match)
    def get_stats_for_player(name):
        if not player_stats:
            return None
        # Direct match
        if name in player_stats:
            return player_stats[name]
        # Case insensitive
        for k, v in player_stats.items():
            if k.lower() == name.lower():
                return v
            # Partial match (e.g. "T.Kelce" vs "Travis Kelce")
            # If one is a substring of the other and length is reasonable
            if (k.lower() in name.lower() or name.lower() in k.lower()) and len(k) > 3 and len(name) > 3:
                return v
        return None

    if mode == 'best':
        # Find best odds per player
        best_odds = {} # player -> {price: -inf, bookmaker: ''}
        for bm in bookmakers:
            bm_title = bm['title']
            for market in bm.get('markets', []):
                if market['key'] == MARKET_1ST_TD:
                    for outcome in market['outcomes']:
                        player = outcome.get('description', outcome['name'])
                        price = outcome['price']
                        if player not in best_odds or price > best_odds[player]['price']:
                            best_odds[player] = {'price': price, 'bookmaker': bm_title}
        
        sorted_players = sorted(best_odds.items(), key=lambda x: x[1]['price'])
        
        print(f"\n{'Player':<30} {'Odds':<8} {'Book':<15} {'Fair':<8} {'EV%':<6} {'Stats (TD/G)':<12} {'Matchup'}")
        print("-" * 100)
        
        for player, data in sorted_players:
            price = data['price']
            price_str = f"+{price}" if price > 0 else str(price)
            
            # EV Calculation
            stats = get_stats_for_player(player)
            fair_str = "-"
            ev_str = "-"
            stats_str = "-"
            matchup_str = "-"
            
            if stats:
                prob = stats['prob']
                fair_odds = calculate_fair_odds(prob)
                fair_str = f"+{fair_odds}" if fair_odds > 0 else str(fair_odds)
                
                # Calculate EV
                # Convert American to Decimal
                decimal_odds = (price / 100) + 1 if price > 0 else (100 / abs(price)) + 1
                ev = (prob * decimal_odds) - 1
                ev_pct = ev * 100
                ev_str = f"{ev_pct:+.1f}%"
                
                stats_str = f"{stats['first_tds']}/{stats['team_games']}"
                
                # Highlight positive EV
                if ev > 0:
                    ev_str = f"*{ev_str}*"
                    
                # Matchup Analysis
                if defense_rankings and roster_df is not None and home_team and away_team:
                    p_team = stats['team']
                    opponent = None
                    if p_team == home_team: opponent = away_team
                    elif p_team == away_team: opponent = home_team
                    
                    if opponent and opponent in defense_rankings:
                        p_id = stats.get('player_id')
                        # Try to get position
                        pos = get_player_position(p_id, player, roster_df)
                        if pos in ['WR', 'RB', 'TE', 'QB']:
                            search_pos = pos
                        else:
                            search_pos = 'Other'
                            
                        rank = defense_rankings[opponent].get(search_pos, '-')
                        matchup_str = f"vs #{rank} {search_pos}"

            print(f"{player:<30} {price_str:<8} {data['bookmaker']:<15} {fair_str:<8} {ev_str:<6} {stats_str:<12} {matchup_str}")

    elif mode == 'specific':
        for bm in bookmakers:
            if bm['title'] == selected_bm:
                print(f"\n[{bm['title']}]")
                print(f"{'Player':<30} {'Odds':<8} {'Fair':<8} {'EV%':<6} {'Stats':<12} {'Matchup'}")
                print("-" * 85)
                
                for market in bm.get('markets', []):
                    if market['key'] == MARKET_1ST_TD:
                        outcomes = sorted(market.get("outcomes", []), key=lambda x: x["price"])
                        for outcome in outcomes:
                            player = outcome.get("description", outcome["name"])
                            price = outcome["price"]
                            price_str = f"+{price}" if price > 0 else str(price)
                            
                            stats = get_stats_for_player(player)
                            fair_str = "-"
                            ev_str = "-"
                            stats_str = "-"
                            matchup_str = "-"
                            
                            if stats:
                                prob = stats['prob']
                                fair_odds = calculate_fair_odds(prob)
                                fair_str = f"+{fair_odds}" if fair_odds > 0 else str(fair_odds)
                                decimal_odds = (price / 100) + 1 if price > 0 else (100 / abs(price)) + 1
                                ev = (prob * decimal_odds) - 1
                                ev_str = f"{ev * 100:+.1f}%"
                                stats_str = f"{stats['first_tds']}/{stats['team_games']}"
                                if ev > 0: ev_str = f"*{ev_str}*"
                                
                                # Matchup Analysis
                                if defense_rankings and roster_df is not None and home_team and away_team:
                                    p_team = stats['team']
                                    opponent = None
                                    if p_team == home_team: opponent = away_team
                                    elif p_team == away_team: opponent = home_team
                                    
                                    if opponent and opponent in defense_rankings:
                                        p_id = stats.get('player_id')
                                        pos = get_player_position(p_id, player, roster_df)
                                        if pos in ['WR', 'RB', 'TE', 'QB']: search_pos = pos
                                        else: search_pos = 'Other'
                                        rank = defense_rankings[opponent].get(search_pos, '-')
                                        matchup_str = f"vs #{rank} {search_pos}"

                            print(f"{player:<30} {price_str:<8} {fair_str:<8} {ev_str:<6} {stats_str:<12} {matchup_str}")

    else: # mode == 'all'
        for bm in bookmakers:
            print(f"\n[{bm['title']}]")
            # ... similar logic for 'all' ...
            # For brevity, just printing basic info, but could add EV here too
            for market in bm.get('markets', []):
                if market['key'] == MARKET_1ST_TD:
                    outcomes = sorted(market.get("outcomes", []), key=lambda x: x["price"])
                    for outcome in outcomes:
                        player = outcome.get("description", outcome["name"])
                        price = outcome["price"]
                        price_str = f"+{price}" if price > 0 else str(price)
                        print(f"  {player:<30} {price_str}")

def load_data_with_cache(season: int) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Loads schedule, pbp, and roster data, using local parquet cache if available.
    """
    cache_dir = "cache"
    os.makedirs(cache_dir, exist_ok=True)
    
    schedule_path = os.path.join(cache_dir, f"season_{season}_schedule.parquet")
    pbp_path = os.path.join(cache_dir, f"season_{season}_pbp.parquet")
    roster_path = os.path.join(cache_dir, f"season_{season}_roster.parquet")
    
    if os.path.exists(schedule_path) and os.path.exists(pbp_path) and os.path.exists(roster_path):
        use_cache = input("\nFound cached data. Load from cache? (y/n): ").strip().lower()
        if use_cache == 'y':
            print("Loading from cache...")
            try:
                schedule_df = pl.read_parquet(schedule_path)
                pbp_df = pl.read_parquet(pbp_path)
                roster_df = pl.read_parquet(roster_path)
                return schedule_df, pbp_df, roster_df
            except Exception as e:
                print(f"Error loading cache: {e}. Downloading fresh data.")
    
    print("Downloading data (this may take a moment)...")
    
    # Schedule
    try:
        schedule_df = nfl.load_schedules(seasons=season)
        if not isinstance(schedule_df, pl.DataFrame):
            schedule_df = pl.from_pandas(schedule_df)
    except Exception as e:
        print(f"nflreadpy schedule load failed ({e}), trying manual download...")
        url = "https://github.com/nflverse/nfldata/raw/master/data/games.csv"
        r = requests.get(url)
        schedule_df = pl.read_csv(io.BytesIO(r.content))

    # PBP
    try:
        pbp_df = nfl.load_pbp(seasons=season)
        if not isinstance(pbp_df, pl.DataFrame):
            pbp_df = pl.from_pandas(pbp_df)
    except Exception as e:
        print(f"nflreadpy pbp load failed ({e}), trying manual download...")
        url = f"https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{season}.parquet"
        r = requests.get(url)
        pbp_df = pl.read_parquet(io.BytesIO(r.content))

    # Roster
    try:
        roster_df = nfl.load_rosters(seasons=season)
        if not isinstance(roster_df, pl.DataFrame):
            roster_df = pl.from_pandas(roster_df)
    except Exception as e:
        print(f"nflreadpy roster load failed ({e}), trying manual download...")
        url = f"https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_{season}.parquet"
        r = requests.get(url)
        roster_df = pl.read_parquet(io.BytesIO(r.content))

    print("Saving to cache...")
    schedule_df.write_parquet(schedule_path)
    pbp_df.write_parquet(pbp_path)
    roster_df.write_parquet(roster_path)
    
    return schedule_df, pbp_df, roster_df

def view_weekly_schedule(season: int, schedule_df: pl.DataFrame, first_td_map: dict, roster_df: pl.DataFrame):
    """
    Handles the existing functionality: View schedule by week and optionally fetch odds.
    """
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

    while True:
        filter_choice = input("\nView (1) Standalone Games or (2) All Games? (Enter 1 or 2): ").strip()
        if filter_choice in ['1', '2']:
            break
        print("Invalid input.")

    # Filter by week
    if week == 0:
        games_to_show_df = schedule_df
    else:
        games_to_show_df = schedule_df.filter(pl.col("week").cast(pl.Int64) == week)

    # Filter by type
    if filter_choice == '1':
        games_to_show_df = games_to_show_df.filter(pl.col("is_standalone"))
        title_text = "Standalone Games"
    else:
        title_text = "All Games"

    print_games(games_to_show_df, title_text, first_td_map)

    # --- Odds Fetching Logic ---
    # Create datetime column for comparison
    games_to_show_df = games_to_show_df.with_columns(
        pl.concat_str([
            pl.col("gameday").str.slice(0, 10),
            pl.col("gametime")
        ], separator=" ").str.to_datetime(format="%Y-%m-%d %H:%M", strict=False)
        .dt.replace_time_zone("US/Eastern")
        .dt.convert_time_zone("UTC")
        .alias("gameday_dt")
    )
    
    now_utc = datetime.now(pytz.utc)
    
    # Only ask to fetch odds if current/future season
    current_year = datetime.now().year
    if season < current_year:
        return

    # Determine current week based on schedule
    today_str = datetime.now().strftime("%Y-%m-%d")
    future_games = schedule_df.filter(pl.col("gameday") >= today_str)
    
    current_week = None
    if future_games.height > 0:
        current_week = future_games["week"].cast(pl.Int64).min()
        
    # Only ask if the user selected the current week
    if current_week is None or week != current_week:
        return

    fetch_odds_choice = input("\nDo you want to fetch 1st TD odds for upcoming games in this view? (yes/no): ").strip().lower()
    if fetch_odds_choice in ['yes', 'y']:
        upcoming_games_df = games_to_show_df.filter(pl.col("gameday_dt") >= now_utc)
        
        if upcoming_games_df.height == 0:
            print("No upcoming games in this view to fetch odds for.")
            return

        # Ask for display preference once
        print("\nHow do you want to view the odds?")
        print("1. Best Odds (Compare all bookmakers)")
        print("2. All Bookmakers (Detailed view)")
        mode_input = input("Enter choice (1 or 2): ").strip()
        default_mode = 'all' if mode_input == '2' else 'best'

        print("\nFetching Odds API event IDs...")
        odds_api_event_ids = get_odds_api_event_ids_for_season(schedule_df, API_KEY)
        
        # Calculate player stats for EV
        print("\nEV Analysis Settings:")
        print("1. Full Season Stats")
        print("2. Last 5 Games (Recent Form)")
        ev_choice = input("Enter choice (1 or 2): ").strip()
        
        last_n = 5 if ev_choice == '2' else None
        print(f"Calculating player stats ({'Last 5 Games' if last_n else 'Full Season'})...")
        
        player_stats = get_player_season_stats(schedule_df, first_td_map, last_n_games=last_n)
        
        # Calculate Defense Rankings
        print("Calculating Defense Rankings...")
        defense_rankings = calculate_defense_rankings(schedule_df, first_td_map, roster_df)
        
        for game_row in upcoming_games_df.to_dicts():
            nfl_game_id = game_row['game_id']
            odds_event_id = odds_api_event_ids.get(nfl_game_id)
            
            if odds_event_id:
                print(f"\n--- Fetching odds for {game_row['away_team']} @ {game_row['home_team']} ---")
                data = fetch_odds_data(API_KEY, SPORT, odds_event_id)
                display_odds(data, interactive=False, default_mode=default_mode, 
                             player_stats=player_stats, 
                             defense_rankings=defense_rankings,
                             roster_df=roster_df,
                             home_team=game_row['home_team'],
                             away_team=game_row['away_team'])
            else:
                print(f"Could not find odds for {game_row['away_team']} @ {game_row['home_team']}.")

def view_team_history(schedule_df: pl.DataFrame, first_td_map: dict):
    """
    Filters schedule for a specific team and shows their 1st TD history.
    """
    team = input("\nEnter Team Abbreviation (e.g., KC, BUF, PHI): ").strip().upper()
    
    # Filter for games where team is home or away
    team_games = schedule_df.filter(
        (pl.col("home_team") == team) | (pl.col("away_team") == team)
    )
    
    if team_games.height == 0:
        print(f"No games found for team '{team}'. Check the abbreviation.")
        return

    print_games(team_games, f"History for {team}", first_td_map)

def view_player_stats(schedule_df: pl.DataFrame, first_td_map: dict):
    """
    Searches for a player's 1st TD scores across the entire season.
    """
    player_query = input("\nEnter Player Name (e.g., Travis Kelce): ").strip().lower()
    
    matches = []
    # first_td_map is now {game_id: {'player': name, 'team': team}}
    for game_id, data in first_td_map.items():
        if isinstance(data, dict):
            scorer_name = data.get('player', '')
            scorer_team = data.get('team', 'UNK')
        else:
            scorer_name = str(data)
            scorer_team = "UNK"
            
        if player_query in scorer_name.lower():
            matches.append({
                'game_id': game_id,
                'player': scorer_name,
                'team': scorer_team
            })
            
    if not matches:
        print(f"\nNo First TDs found for '{player_query}'.")
        return

    # Group by (Player Name, Team) to handle multiple players with similar names
    # or players changing teams (though rare for 1st TD context in one season)
    grouped_matches = {}
    for m in matches:
        key = (m['player'], m['team'])
        if key not in grouped_matches:
            grouped_matches[key] = []
        grouped_matches[key].append(m['game_id'])
        
    print(f"\nFound {len(matches)} First TDs matching '{player_query}':")
    
    for (p_name, p_team), game_ids in grouped_matches.items():
        print(f"\nPlayer: {p_name} ({p_team}) - {len(game_ids)} First TDs")
        
        # Filter schedule to just these games to show details
        matched_games = schedule_df.filter(pl.col("game_id").is_in(game_ids))
        print_games(matched_games, f"Games with 1st TD by {p_name}", first_td_map)

def view_team_stats_analysis(schedule_df: pl.DataFrame, first_td_map: dict):
    """
    Displays a leaderboard of teams ranked by First TD frequency.
    """
    if not first_td_map:
        print("No First TD data available.")
        return

    print("\nCalculating Team Stats...")
    
    # Filter schedule for games that have a recorded First TD
    completed_game_ids = list(first_td_map.keys())
    relevant_games = schedule_df.filter(pl.col("game_id").is_in(completed_game_ids))
    
    stats = {} # team -> {games: 0, first_tds: 0}
    
    # Count Games Played
    for row in relevant_games.select(['game_id', 'home_team', 'away_team']).to_dicts():
        gid = row['game_id']
        home = row['home_team']
        away = row['away_team']
        
        if home not in stats: stats[home] = {'games': 0, 'first_tds': 0}
        if away not in stats: stats[away] = {'games': 0, 'first_tds': 0}
        
        stats[home]['games'] += 1
        stats[away]['games'] += 1
        
        # Count 1st TDs
        td_data = first_td_map.get(gid)
        if td_data:
            td_team = td_data['team']
            if td_team in stats:
                stats[td_team]['first_tds'] += 1
            else:
                # If team not in stats (maybe a weird abbreviation), ignore or add
                pass

    # Create DataFrame for display
    data = []
    for team, s in stats.items():
        pct = (s['first_tds'] / s['games'] * 100) if s['games'] > 0 else 0.0
        data.append({
            "Team": team,
            "Games": s['games'],
            "1st TDs": s['first_tds'],
            "First TD %": pct
        })
        
    if not data:
        print("No team stats could be calculated.")
        return

    stats_df = pl.DataFrame(data).sort("First TD %", descending=True)
    
    # Add Rank
    stats_df = stats_df.with_columns(
        pl.arange(1, stats_df.height + 1).alias("Rank")
    )
    
    # Print Table
    print("\n" + "="*60)
    print("Team First TD Leaderboard")
    print("="*60)
    print(f"{'Rank':<6} {'Team':<6} {'Games':<8} {'1st TDs':<10} {'First TD %':<10}")
    print("-" * 60)
    
    for row in stats_df.to_dicts():
        print(f"{row['Rank']:<6} {row['Team']:<6} {row['Games']:<8} {row['1st TDs']:<10} {row['First TD %']:<10.1f}")
    print("=" * 60)

def get_player_position(player_id: str, player_name: str, roster_df: pl.DataFrame) -> str:
    """
    Helper to find a player's position from the roster DataFrame.
    """
    # Try by ID first (gsis_id is standard in nflreadpy rosters)
    if player_id:
        # Check if gsis_id column exists
        if "gsis_id" in roster_df.columns:
            res = roster_df.filter(pl.col("gsis_id") == player_id)
            if res.height > 0:
                return res["position"][0]
    
    # Try by name
    if player_name:
        # Check if full_name column exists
        if "full_name" in roster_df.columns:
            res = roster_df.filter(pl.col("full_name").str.to_lowercase() == player_name.lower())
            if res.height > 0:
                return res["position"][0]
            
    return "UNK"

def view_defense_vs_position(schedule_df: pl.DataFrame, first_td_map: dict, roster_df: pl.DataFrame):
    """
    Analyzes which positions score the first TD against each defense.
    """
    if not first_td_map:
        print("No First TD data available.")
        return

    print("\nCalculating Defense vs Position Stats...")
    
    # Defense Team -> {WR: 0, RB: 0, TE: 0, QB: 0, Other: 0}
    defense_stats = {}
    
    # Get game details to find the opponent (defense)
    games_df = schedule_df.filter(pl.col("game_id").is_in(list(first_td_map.keys())))
    game_map = {row['game_id']: {'home': row['home_team'], 'away': row['away_team']} for row in games_df.select(['game_id', 'home_team', 'away_team']).to_dicts()}
    
    for game_id, data in first_td_map.items():
        if game_id not in game_map:
            continue
            
        scorer_team = data['team']
        player_name = data['player']
        player_id = data.get('player_id')
        
        # Determine Defense Team (Opponent of Scorer)
        game_info = game_map[game_id]
        if scorer_team == game_info['home']:
            defense_team = game_info['away']
        elif scorer_team == game_info['away']:
            defense_team = game_info['home']
        else:
            continue # Unknown team mapping
            
        # Get Position
        pos = get_player_position(player_id, player_name, roster_df)
        
        # Group positions
        if pos in ['WR', 'RB', 'TE', 'QB']:
            group_pos = pos
        else:
            group_pos = 'Other'
            
        if defense_team not in defense_stats:
            defense_stats[defense_team] = {'WR': 0, 'RB': 0, 'TE': 0, 'QB': 0, 'Other': 0, 'Total': 0}
            
        defense_stats[defense_team][group_pos] += 1
        defense_stats[defense_team]['Total'] += 1

    # Display Table
    print("\n" + "="*80)
    print("Defense vs Position (First TDs Allowed)")
    print("="*80)
    print(f"{'Defense':<8} {'Total':<6} {'WR':<6} {'RB':<6} {'TE':<6} {'QB':<6} {'Other':<6}")
    print("-" * 80)
    
    sorted_defenses = sorted(defense_stats.items(), key=lambda x: x[1]['Total'], reverse=True)
    
    for team, stats in sorted_defenses:
        print(f"{team:<8} {stats['Total']:<6} {stats['WR']:<6} {stats['RB']:<6} {stats['TE']:<6} {stats['QB']:<6} {stats['Other']:<6}")
    print("=" * 80)

def view_opening_drive_stats(pbp_df: pl.DataFrame):
    """
    Analyzes opening drive success rates for each team.
    """
    print("\nCalculating Opening Drive Stats (this may take a moment)...")
    
    # Filter for the first drive of each game for both teams
    # We need to identify the first drive for the home team and the first drive for the away team.
    # pbp_df has 'game_id', 'drive', 'posteam', 'fixed_drive_result'
    
    # Get unique drives per game/team
    # We want the minimum drive number for each posteam in each game
    
    if "drive" not in pbp_df.columns or "fixed_drive_result" not in pbp_df.columns:
        print("Necessary columns missing from PBP data.")
        return

    # Group by game_id and posteam, find min drive
    first_drives = pbp_df.filter(
        pl.col("drive").is_not_null() & pl.col("posteam").is_not_null()
    ).group_by(["game_id", "posteam"]).agg([
        pl.col("drive").min().alias("first_drive_number")
    ])
    
    # Now join back to get the result of that drive
    # We need to be careful because a drive has multiple plays. We just need the result.
    # Usually 'fixed_drive_result' is the same for all plays in the drive.
    
    # Let's get one row per drive
    drives_df = pbp_df.select(["game_id", "posteam", "drive", "fixed_drive_result"]).unique()
    
    # Join
    opening_drives = first_drives.join(
        drives_df, 
        left_on=["game_id", "posteam", "first_drive_number"], 
        right_on=["game_id", "posteam", "drive"]
    )
    
    stats = {} # team -> {drives: 0, tds: 0}
    
    for row in opening_drives.to_dicts():
        team = row['posteam']
        result = row['fixed_drive_result']
        
        if team not in stats:
            stats[team] = {'drives': 0, 'tds': 0}
            
        stats[team]['drives'] += 1
        if result == 'Touchdown':
            stats[team]['tds'] += 1
            
    # Display
    data = []
    for team, s in stats.items():
        pct = (s['tds'] / s['drives'] * 100) if s['drives'] > 0 else 0.0
        data.append({
            "Team": team,
            "Drives": s['drives'],
            "TDs": s['tds'],
            "Success %": pct
        })
        
    stats_df = pl.DataFrame(data).sort("Success %", descending=True)
    
    print("\n" + "="*60)
    print("Opening Drive Touchdown Rate")
    print("="*60)
    print(f"{'Rank':<6} {'Team':<6} {'Drives':<8} {'TDs':<6} {'Success %':<10}")
    print("-" * 60)
    
    for i, row in enumerate(stats_df.to_dicts(), 1):
        print(f"{i:<6} {row['Team']:<6} {row['Drives']:<8} {row['TDs']:<6} {row['Success %']:<10.1f}")
    print("=" * 60)

def view_home_away_splits(schedule_df: pl.DataFrame, first_td_map: dict):
    """
    Analyzes First TD rates for Home vs Away teams.
    """
    if not first_td_map:
        print("No First TD data available.")
        return

    print("\nCalculating Home/Away Splits...")
    
    games_df = schedule_df.filter(pl.col("game_id").is_in(list(first_td_map.keys())))
    
    home_tds = 0
    away_tds = 0
    total_games = 0
    
    team_stats = {} # team -> {home_games: 0, home_tds: 0, away_games: 0, away_tds: 0}
    
    for row in games_df.select(['game_id', 'home_team', 'away_team']).to_dicts():
        gid = row['game_id']
        home = row['home_team']
        away = row['away_team']
        
        if home not in team_stats: team_stats[home] = {'home_games': 0, 'home_tds': 0, 'away_games': 0, 'away_tds': 0}
        if away not in team_stats: team_stats[away] = {'home_games': 0, 'home_tds': 0, 'away_games': 0, 'away_tds': 0}
        
        team_stats[home]['home_games'] += 1
        team_stats[away]['away_games'] += 1
        total_games += 1
        
        td_data = first_td_map.get(gid)
        if td_data:
            scorer_team = td_data['team']
            if scorer_team == home:
                home_tds += 1
                team_stats[home]['home_tds'] += 1
            elif scorer_team == away:
                away_tds += 1
                team_stats[away]['away_tds'] += 1
                
    # Overall Stats
    print("\n" + "="*40)
    print("League-Wide Home/Away Split")
    print("="*40)
    print(f"Total Games: {total_games}")
    print(f"Home Team 1st TD: {home_tds} ({home_tds/total_games*100:.1f}%)")
    print(f"Away Team 1st TD: {away_tds} ({away_tds/total_games*100:.1f}%)")
    print("="*40)
    
    # Team Specific Table
    print("\n" + "="*80)
    print("Team Home/Away Splits (1st TD %)")
    print("="*80)
    print(f"{'Team':<6} {'Home %':<10} {'(G/TD)':<10} {'Away %':<10} {'(G/TD)':<10} {'Diff':<6}")
    print("-" * 80)
    
    data = []
    for team, s in team_stats.items():
        home_pct = (s['home_tds'] / s['home_games'] * 100) if s['home_games'] > 0 else 0.0
        away_pct = (s['away_tds'] / s['away_games'] * 100) if s['away_games'] > 0 else 0.0
        diff = home_pct - away_pct
        data.append({
            'Team': team,
            'Home %': home_pct,
            'Home G/TD': f"{s['home_games']}/{s['home_tds']}",
            'Away %': away_pct,
            'Away G/TD': f"{s['away_games']}/{s['away_tds']}",
            'Diff': diff
        })
        
    stats_df = pl.DataFrame(data).sort("Diff", descending=True)
    
    for row in stats_df.to_dicts():
        print(f"{row['Team']:<6} {row['Home %']:<10.1f} {row['Home G/TD']:<10} {row['Away %']:<10.1f} {row['Away G/TD']:<10} {row['Diff']:<+6.1f}")
    print("=" * 80)

def view_current_week_odds(schedule_df: pl.DataFrame, first_td_map: dict, roster_df: pl.DataFrame):
    """
    Identifies current week, lists games, allows user to pick one for odds.
    """
    # Determine current week based on schedule and today's date
    today_str = datetime.now().strftime("%Y-%m-%d")
    # Filter for games today or in future
    future_games = schedule_df.filter(pl.col("gameday") >= today_str)
    
    if future_games.height == 0:
        print("\nNo upcoming games found in schedule.")
        return

    current_week = future_games["week"].cast(pl.Int64).min()
    
    # Filter for current week games
    week_games = schedule_df.filter(pl.col("week").cast(pl.Int64) == current_week)
    
    if week_games.height == 0:
        print(f"\nNo games found for Week {current_week}.")
        return

    # Sort by time
    week_games = week_games.sort(["gameday", "gametime"])
    
    # Display games with index
    games_list = week_games.select(["game_id", "away_team", "home_team", "gameday", "gametime", "week"]).to_dicts()
    
    print(f"\n--- Week {current_week} Schedule ---")
    print(f"{'No.':<4} {'Matchup':<25} {'Date':<12} {'Time':<8}")
    print("-" * 55)
    
    for i, game in enumerate(games_list, 1):
        matchup = f"{game['away_team']} @ {game['home_team']}"
        print(f"{i:<4} {matchup:<25} {game['gameday']:<12} {game['gametime']:<8}")
        
    # User selection
    while True:
        try:
            choice_input = input("\nEnter game number to view odds (0 to cancel): ").strip()
            choice = int(choice_input)
            if choice == 0:
                return
            if 1 <= choice <= len(games_list):
                selected_game = games_list[choice - 1]
                break
            print("Invalid number.")
        except ValueError:
            print("Invalid input.")

    # Fetch odds
    print(f"\nFetching odds for {selected_game['away_team']} @ {selected_game['home_team']}...")
    
    # Fetch mapping
    odds_api_event_ids = get_odds_api_event_ids_for_season(schedule_df, API_KEY)
    
    nfl_game_id = selected_game['game_id']
    odds_event_id = odds_api_event_ids.get(nfl_game_id)
    
    if odds_event_id:
        data = fetch_odds_data(API_KEY, SPORT, odds_event_id)
        
        # Calculate stats for EV
        print("\nEV Analysis Settings:")
        print("1. Full Season Stats")
        print("2. Last 5 Games (Recent Form)")
        ev_choice = input("Enter choice (1 or 2): ").strip()
        last_n = 5 if ev_choice == '2' else None
        
        player_stats = get_player_season_stats(schedule_df, first_td_map, last_n_games=last_n)
        
        # Calculate Defense Rankings
        print("Calculating Defense Rankings...")
        defense_rankings = calculate_defense_rankings(schedule_df, first_td_map, roster_df)
        
        display_odds(data, interactive=True, 
                     player_stats=player_stats,
                     defense_rankings=defense_rankings,
                     roster_df=roster_df,
                     home_team=selected_game['home_team'],
                     away_team=selected_game['away_team'])
    else:
        print(f"Odds not found for {selected_game['away_team']} @ {selected_game['home_team']}.")

def main():
    print("\n" + "="*60)
    print("NFL First TD Tracker & Odds")
    print("="*60)
    
    # --- 1. Initial Setup (Run Once) ---
    while True:
        try:
            season_input = input("\nEnter NFL season (e.g., 2024): ").strip()
            season = int(season_input)
            break
        except ValueError:
            print("Invalid input.")
    
    print(f"\nLoading data for {season}...")
    try:
        # Use cached loading function
        schedule_df, pbp_df, roster_df = load_data_with_cache(season)
        
        if schedule_df.height == 0:
            print("No schedule data found.")
            return
        
        # Pre-process schedule
        schedule_df = get_season_games(season, schedule_df)
        
        print("Calculating First TD scorers for the entire season...")
        # Pass None to process ALL games in the dataframe
        first_td_map = get_first_td_scorers(pbp_df, target_game_ids=None, roster_df=roster_df)
        print(f"Data loaded! Found {len(first_td_map)} first TDs so far.")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # --- 2. Main Menu Loop ---
    while True:
        print("\n" + "-"*30)
        print("MAIN MENU")
        print("-"*-30)
        print("1. Weekly Schedule ( & Odds)")
        print("2. Check Current Week Odds")
        print("3. Team History")
        print("4. Player Stats")
        print("5. Team Stats Analysis")
        print("6. Defense vs Position")
        print("7. Opening Drive Stats")
        print("8. Home/Away Splits")
        print("9. Exit")
        
        choice = input("\nEnter choice (1-9): ").strip()
        
        if choice == '1':
            view_weekly_schedule(season, schedule_df, first_td_map, roster_df)
        elif choice == '2':
            view_current_week_odds(schedule_df, first_td_map, roster_df)
        elif choice == '3':
            view_team_history(schedule_df, first_td_map)
        elif choice == '4':
            view_player_stats(schedule_df, first_td_map)
        elif choice == '5':
            view_team_stats_analysis(schedule_df, first_td_map)
        elif choice == '6':
            view_defense_vs_position(schedule_df, first_td_map, roster_df)
        elif choice == '7':
            view_opening_drive_stats(pbp_df)
        elif choice == '8':
            view_home_away_splits(schedule_df, first_td_map)
        elif choice == '9':
            print("Exiting.")
            break
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()