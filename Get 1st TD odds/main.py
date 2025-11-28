import polars as pl
from datetime import datetime
import pytz
from config import API_KEY, SPORT
from data import load_data_with_cache, get_season_games
from stats import (
    get_first_td_scorers, 
    get_player_season_stats, 
    calculate_defense_rankings, 
    get_player_position,
    get_red_zone_stats,
    get_opening_drive_stats,
    calculate_kelly_criterion,
    get_team_red_zone_splits,
    identify_funnel_defenses
)
from odds import get_odds_api_event_ids_for_season, fetch_odds_data
from ui import print_games, display_odds, display_best_bets

def view_weekly_schedule(season: int, schedule_df: pl.DataFrame, first_td_map: dict, roster_df: pl.DataFrame, pbp_df: pl.DataFrame):
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
        if not API_KEY:
            print("\nError: ODDS_API_KEY not found in environment variables.")
            print("Please set the ODDS_API_KEY environment variable.")
            return

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
        funnel_defenses = identify_funnel_defenses(defense_rankings)
        
        # Calculate RZ and OD Stats
        print("Calculating Red Zone & Opening Drive Stats...")
        rz_stats = get_red_zone_stats(pbp_df, roster_df)
        od_stats = get_opening_drive_stats(pbp_df, roster_df)
        team_rz_splits = get_team_red_zone_splits(pbp_df)
        
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
                             away_team=game_row['away_team'],
                             red_zone_stats=rz_stats,
                             opening_drive_stats=od_stats,
                             team_rz_splits=team_rz_splits,
                             funnel_defenses=funnel_defenses)
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

def view_current_week_odds(schedule_df: pl.DataFrame, first_td_map: dict, roster_df: pl.DataFrame, pbp_df: pl.DataFrame):
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
    
    if not API_KEY:
        print("\nError: ODDS_API_KEY not found in environment variables.")
        return

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
        funnel_defenses = identify_funnel_defenses(defense_rankings)
        
        # Calculate RZ and OD Stats
        print("Calculating Red Zone & Opening Drive Stats...")
        rz_stats = get_red_zone_stats(pbp_df, roster_df)
        od_stats = get_opening_drive_stats(pbp_df, roster_df)
        team_rz_splits = get_team_red_zone_splits(pbp_df)
        
        display_odds(data, interactive=True, 
                     player_stats=player_stats,
                     defense_rankings=defense_rankings,
                     roster_df=roster_df,
                     home_team=selected_game['home_team'],
                     away_team=selected_game['away_team'],
                     red_zone_stats=rz_stats,
                     opening_drive_stats=od_stats,
                     team_rz_splits=team_rz_splits,
                     funnel_defenses=funnel_defenses)
    else:
        print(f"Odds not found for {selected_game['away_team']} @ {selected_game['home_team']}.")

def view_best_bets_scanner(schedule_df: pl.DataFrame, first_td_map: dict, roster_df: pl.DataFrame, pbp_df: pl.DataFrame):
    """
    Scans all upcoming games in the current week for positive EV bets.
    """
    print("\n--- Best Bets Scanner ---")
    
    # 1. Identify Current Week Games
    today_str = datetime.now().strftime("%Y-%m-%d")
    future_games = schedule_df.filter(pl.col("gameday") >= today_str)
    
    if future_games.height == 0:
        print("No upcoming games found.")
        return

    current_week = future_games["week"].cast(pl.Int64).min()
    week_games = schedule_df.filter(pl.col("week").cast(pl.Int64) == current_week)
    
    if week_games.height == 0:
        print(f"No games found for Week {current_week}.")
        return

    print(f"Scanning {week_games.height} games for Week {current_week}...")

    # 2. Calculate Stats (Once for all)
    print("Calculating Season Stats...")
    player_stats = get_player_season_stats(schedule_df, first_td_map, last_n_games=5) # Default to last 5 for scanner
    
    print("Calculating Red Zone & Opening Drive Stats...")
    rz_stats = get_red_zone_stats(pbp_df, roster_df)
    od_stats = get_opening_drive_stats(pbp_df, roster_df)
    team_rz_splits = get_team_red_zone_splits(pbp_df)
    
    print("Calculating Defense Rankings...")
    defense_rankings = calculate_defense_rankings(schedule_df, first_td_map, roster_df)
    funnel_defenses = identify_funnel_defenses(defense_rankings)

    # 3. Fetch Odds and Find Bets
    if not API_KEY:
        print("\nError: ODDS_API_KEY not found in environment variables.")
        return

    odds_api_event_ids = get_odds_api_event_ids_for_season(schedule_df, API_KEY)
    
    best_bets = []
    
    # Helper for fuzzy match (reused logic)
    def get_stats(name):
        if name in player_stats: return player_stats[name]
        for k, v in player_stats.items():
            if (k.lower() in name.lower() or name.lower() in k.lower()) and len(k) > 3 and len(name) > 3:
                return v
        return None

    for game in week_games.to_dicts():
        nfl_game_id = game['game_id']
        home_team = game['home_team']
        away_team = game['away_team']
        
        odds_event_id = odds_api_event_ids.get(nfl_game_id)
        if not odds_event_id:
            continue
            
        print(f"Fetching odds for {away_team} @ {home_team}...")
        data = fetch_odds_data(API_KEY, SPORT, odds_event_id)
        
        if not data or "bookmakers" not in data:
            continue
            
        # Process odds
        # We want the BEST price for each player across all bookmakers
        game_best_prices = {} # player -> {price, bookmaker}
        
        for bm in data.get("bookmakers", []):
            bm_title = bm['title']
            for market in bm.get('markets', []):
                if market['key'] == 'player_first_td':
                    for outcome in market['outcomes']:
                        player = outcome.get('description', outcome['name'])
                        price = outcome['price']
                        
                        if player not in game_best_prices or price > game_best_prices[player]['price']:
                            game_best_prices[player] = {'price': price, 'bookmaker': bm_title}
                            
        # Calculate EV for each player
        for player, price_data in game_best_prices.items():
            stats = get_stats(player)
            if stats:
                prob = stats['prob']
                price = price_data['price']
                
                # EV Calc
                decimal_odds = (price / 100) + 1 if price > 0 else (100 / abs(price)) + 1
                ev = (prob * decimal_odds) - 1
                
                if ev > 0:
                    # Positive EV! Add to list
                    from stats import calculate_fair_odds # Import here or ensure available
                    fair_odds = calculate_fair_odds(prob)
                    
                    # Kelly
                    kelly = calculate_kelly_criterion(prob, decimal_odds, bankroll=1000.0)
                    
                    # Matchup
                    matchup_str = "-"
                    p_team = stats['team']
                    opponent = away_team if p_team == home_team else home_team
                    if opponent in defense_rankings:
                        p_id = stats.get('player_id')
                        pos = get_player_position(p_id, player, roster_df)
                        search_pos = pos if pos in ['WR', 'RB', 'TE', 'QB'] else 'Other'
                        rank = defense_rankings[opponent].get(search_pos, '-')
                        matchup_str = f"vs #{rank} {search_pos}"
                        
                        # Funnel Logic
                        if opponent in funnel_defenses:
                            funnel_type = funnel_defenses[opponent]
                            if funnel_type:
                                is_funnel_match = False
                                if funnel_type == "Pass Funnel" and search_pos in ['WR', 'TE', 'QB']:
                                    is_funnel_match = True
                                elif funnel_type == "Run Funnel" and search_pos == 'RB':
                                    is_funnel_match = True
                                    
                                if is_funnel_match:
                                    matchup_str += f" ({funnel_type})"
                        
                    # RZ/OD Stats
                    # ... (existing comments) ...
                    
                    # For now, simple lookup
                    rz = rz_stats.get(player) # Exact match
                    od = od_stats.get(player)
                    
                    # Team RZ Split
                    team_split = team_rz_splits.get(p_team) if p_team else None
                    
                    best_bets.append({
                        'game_id': nfl_game_id,
                        'home': home_team,
                        'away': away_team,
                        'player': player,
                        'price': price,
                        'bookmaker': price_data['bookmaker'],
                        'ev': ev,
                        'prob': prob,
                        'fair_odds': fair_odds,
                        'kelly': kelly,
                        'stats': stats,
                        'rz_stats': rz,
                        'od_stats': od,
                        'matchup': matchup_str,
                        'team_rz_split': team_split
                    })

    display_best_bets(best_bets)

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
        print("9. Best Bets Scanner")
        print("10. Exit")
        
        choice = input("\nEnter choice (1-10): ").strip()
        
        if choice == '1':
            view_weekly_schedule(season, schedule_df, first_td_map, roster_df, pbp_df)
        elif choice == '2':
            view_current_week_odds(schedule_df, first_td_map, roster_df, pbp_df)
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
            view_best_bets_scanner(schedule_df, first_td_map, roster_df, pbp_df)
        elif choice == '10':
            print("Exiting.")
            break
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()
