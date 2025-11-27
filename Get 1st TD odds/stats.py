import polars as pl

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

def get_red_zone_stats(pbp_df: pl.DataFrame, roster_df: pl.DataFrame) -> dict:
    """
    Calculates Red Zone (<= 20 yards) stats for players.
    Returns: {player_name: {'rz_opps': int, 'rz_tds': int}}
    """
    if pbp_df.height == 0:
        return {}

    # Filter for Red Zone plays
    rz_plays = pbp_df.filter(pl.col("yardline_100") <= 20)
    
    if rz_plays.height == 0:
        return {}

    # 1. Rushing Opps (rusher_player_id)
    rush_opps = rz_plays.filter(pl.col("rusher_player_id").is_not_null()) \
        .group_by("rusher_player_id").count().rename({"count": "rushes"})
        
    # 2. Receiving Targets (receiver_player_id)
    rec_opps = rz_plays.filter(pl.col("receiver_player_id").is_not_null()) \
        .group_by("receiver_player_id").count().rename({"count": "targets"})
        
    # 3. Touchdowns (td_player_id)
    tds = rz_plays.filter((pl.col("touchdown") == 1) & (pl.col("td_player_id").is_not_null())) \
        .group_by("td_player_id").count().rename({"count": "tds"})

    # Merge these
    all_ids = set()
    if "rusher_player_id" in rush_opps.columns:
        all_ids.update(rush_opps["rusher_player_id"].to_list())
    if "receiver_player_id" in rec_opps.columns:
        all_ids.update(rec_opps["receiver_player_id"].to_list())
    if "td_player_id" in tds.columns:
        all_ids.update(tds["td_player_id"].to_list())
        
    # Create ID to Name map
    id_to_name = {}
    if roster_df is not None and "gsis_id" in roster_df.columns and "full_name" in roster_df.columns:
        temp_df = roster_df.select(["gsis_id", "full_name"]).unique()
        for r in temp_df.to_dicts():
            if r["gsis_id"] and r["full_name"]:
                id_to_name[r["gsis_id"]] = r["full_name"]
                
    # Build result dict
    final_stats = {}
    
    rush_map = {row['rusher_player_id']: row['rushes'] for row in rush_opps.to_dicts()} if rush_opps.height > 0 else {}
    rec_map = {row['receiver_player_id']: row['targets'] for row in rec_opps.to_dicts()} if rec_opps.height > 0 else {}
    td_map = {row['td_player_id']: row['tds'] for row in tds.to_dicts()} if tds.height > 0 else {}
    
    for pid in all_ids:
        if not pid: continue
        
        name = id_to_name.get(pid, pid) # Fallback to ID if name not found
        
        opps = rush_map.get(pid, 0) + rec_map.get(pid, 0)
        td_count = td_map.get(pid, 0)
        
        if opps > 0 or td_count > 0:
            final_stats[name] = {'rz_opps': opps, 'rz_tds': td_count}
            
    return final_stats

def get_opening_drive_stats(pbp_df: pl.DataFrame, roster_df: pl.DataFrame) -> dict:
    """
    Calculates stats for the opening drive of each team in each game.
    Returns: {player_name: {'od_opps': int, 'od_tds': int}}
    """
    if pbp_df.height == 0:
        return {}

    # Identify opening drives: Min drive number per game per posteam
    valid_drives = pbp_df.filter(pl.col("drive").is_not_null())
    
    if valid_drives.height == 0:
        return {}
        
    opening_drives = valid_drives.group_by(["game_id", "posteam"]).agg(pl.col("drive").min().alias("min_drive"))
    
    # Join back to filter pbp
    od_plays = valid_drives.join(opening_drives, left_on=["game_id", "posteam"], right_on=["game_id", "posteam"])
    od_plays = od_plays.filter(pl.col("drive") == pl.col("min_drive"))
    
    if od_plays.height == 0:
        return {}

    # 1. Rushing Opps
    rush_opps = od_plays.filter(pl.col("rusher_player_id").is_not_null()) \
        .group_by("rusher_player_id").count().rename({"count": "rushes"})
        
    # 2. Receiving Targets
    rec_opps = od_plays.filter(pl.col("receiver_player_id").is_not_null()) \
        .group_by("receiver_player_id").count().rename({"count": "targets"})
        
    # 3. Touchdowns
    tds = od_plays.filter((pl.col("touchdown") == 1) & (pl.col("td_player_id").is_not_null())) \
        .group_by("td_player_id").count().rename({"count": "tds"})

    # Merge
    all_ids = set()
    if "rusher_player_id" in rush_opps.columns:
        all_ids.update(rush_opps["rusher_player_id"].to_list())
    if "receiver_player_id" in rec_opps.columns:
        all_ids.update(rec_opps["receiver_player_id"].to_list())
    if "td_player_id" in tds.columns:
        all_ids.update(tds["td_player_id"].to_list())
        
    # Create ID to Name map
    id_to_name = {}
    if roster_df is not None and "gsis_id" in roster_df.columns and "full_name" in roster_df.columns:
        temp_df = roster_df.select(["gsis_id", "full_name"]).unique()
        for r in temp_df.to_dicts():
            if r["gsis_id"] and r["full_name"]:
                id_to_name[r["gsis_id"]] = r["full_name"]
                
    final_stats = {}
    
    rush_map = {row['rusher_player_id']: row['rushes'] for row in rush_opps.to_dicts()} if rush_opps.height > 0 else {}
    rec_map = {row['receiver_player_id']: row['targets'] for row in rec_opps.to_dicts()} if rec_opps.height > 0 else {}
    td_map = {row['td_player_id']: row['tds'] for row in tds.to_dicts()} if tds.height > 0 else {}
    
    for pid in all_ids:
        if not pid: continue
        
        name = id_to_name.get(pid, pid)
        
        opps = rush_map.get(pid, 0) + rec_map.get(pid, 0)
        td_count = td_map.get(pid, 0)
        
        if opps > 0 or td_count > 0:
            final_stats[name] = {'od_opps': opps, 'od_tds': td_count}
            
    return final_stats

def calculate_kelly_criterion(prob: float, decimal_odds: float, bankroll: float = 1000.0, fractional: float = 0.25) -> float:
    """
    Calculates the Kelly Criterion bet size.
    Formula: f* = (bp - q) / b
    Where:
        b = decimal_odds - 1
        p = probability of winning
        q = probability of losing (1 - p)
    Returns the bet amount (bankroll * f* * fractional).
    """
    if prob <= 0 or decimal_odds <= 1:
        return 0.0
        
    b = decimal_odds - 1
    p = prob
    q = 1 - p
    
    f_star = (b * p - q) / b
    
    if f_star <= 0:
        return 0.0
        
    return bankroll * f_star * fractional

def get_team_red_zone_splits(pbp_df: pl.DataFrame) -> dict:
    """
    Calculates Run/Pass splits for each team in the Red Zone (<= 20 yards).
    Returns: {team: {'pass_pct': float, 'run_pct': float, 'total_plays': int}}
    """
    if pbp_df.height == 0:
        return {}

    # Filter for Red Zone plays
    # We only care about plays that are actually runs or passes (exclude FGs, punts, etc if any)
    # play_type is usually 'pass' or 'run' in nflverse data
    rz_plays = pbp_df.filter(
        (pl.col("yardline_100") <= 20) &
        (pl.col("play_type").is_in(["pass", "run"]))
    )
    
    if rz_plays.height == 0:
        return {}

    # Group by team and play_type
    splits = rz_plays.group_by(["posteam", "play_type"]).count()
    
    # Get total plays per team
    total_plays = rz_plays.group_by("posteam").count().rename({"count": "total"})
    
    # Join back
    splits = splits.join(total_plays, on="posteam")
    
    team_stats = {}
    
    for row in splits.to_dicts():
        team = row['posteam']
        ptype = row['play_type']
        count = row['count']
        total = row['total']
        
        if team not in team_stats:
            team_stats[team] = {'pass_pct': 0.0, 'run_pct': 0.0, 'total_plays': total}
            
        if ptype == 'pass':
            team_stats[team]['pass_pct'] = (count / total) * 100
        elif ptype == 'run':
            team_stats[team]['run_pct'] = (count / total) * 100
            
    return team_stats

def identify_funnel_defenses(defense_rankings: dict) -> dict:
    """
    Identifies if a defense is a 'Pass Funnel' (Good Run Def, Bad Pass Def)
    or 'Run Funnel' (Good Pass Def, Bad Run Def).
    Rank 1 = Best Defense (Fewest TDs allowed).
    Rank 32 = Worst Defense (Most TDs allowed).
    
    Pass Funnel: Strong Run Def (Rank <= 12) & Weak Pass Def (Rank >= 20)
    Run Funnel: Strong Pass Def (Rank <= 12) & Weak Run Def (Rank >= 20)
    
    Returns: {team: 'Pass Funnel' | 'Run Funnel' | None}
    """
    funnels = {}
    for team, ranks in defense_rankings.items():
        rb_rank = ranks.get('RB', 16)
        wr_rank = ranks.get('WR', 16)
        te_rank = ranks.get('TE', 16)
        
        # Pass Defense Rank (Average of WR and TE)
        pass_rank = (wr_rank + te_rank) / 2
        
        # Pass Funnel: Elite Run Def (Rank <= 12) & Weak Pass Def (Rank >= 20)
        # Forces teams to pass
        if rb_rank <= 12 and pass_rank >= 20:
            funnels[team] = "Pass Funnel"
            
        # Run Funnel: Elite Pass Def (Rank <= 12) & Weak Run Def (Rank >= 20)
        # Forces teams to run
        elif pass_rank <= 12 and rb_rank >= 20:
            funnels[team] = "Run Funnel"
            
        else:
            funnels[team] = None
            
    return funnels
