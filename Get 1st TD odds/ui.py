import polars as pl
from config import MARKET_1ST_TD
from stats import calculate_fair_odds, get_player_position, calculate_kelly_criterion

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

def display_odds(odds_data: dict | None, interactive: bool = False, default_mode: str = 'best', 
                 player_stats: dict | None = None, 
                 defense_rankings: dict | None = None,
                 roster_df: pl.DataFrame | None = None,
                 home_team: str | None = None,
                 away_team: str | None = None,
                 red_zone_stats: dict | None = None,
                 opening_drive_stats: dict | None = None,
                 bankroll: float = 1000.0,
                 team_rz_splits: dict | None = None,
                 funnel_defenses: dict | None = None):
    """
    Displays odds data based on user preference or default mode.
    Modes: 'best', 'all', 'specific'
    Includes EV calculation if player_stats is provided.
    Includes Matchup analysis if defense_rankings is provided.
    Includes Red Zone and Opening Drive stats if provided.
    Includes Kelly Criterion calculation for positive EV bets.
    Includes Team Red Zone Splits if provided.
    Includes Funnel Defense identification if provided.
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
    def get_stats_for_player(name, stats_dict):
        if not stats_dict:
            return None
        # Direct match
        if name in stats_dict:
            return stats_dict[name]
        # Case insensitive
        for k, v in stats_dict.items():
            if k.lower() == name.lower():
                return v
            # Partial match (e.g. "T.Kelce" vs "Travis Kelce")
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
        
        print(f"\n{'Player':<25} {'Odds':<6} {'Book':<12} {'Fair':<6} {'EV%':<6} {'Kelly':<6} {'Stats':<8} {'RZ(Op/TD)':<10} {'OD(Op/TD)':<10} {'Team RZ':<12} {'Matchup'}")
        print("-" * 130)
        
        for player, data in sorted_players:
            price = data['price']
            price_str = f"+{price}" if price > 0 else str(price)
            
            # EV Calculation
            stats = get_stats_for_player(player, player_stats)
            fair_str = "-"
            ev_str = "-"
            stats_str = "-"
            matchup_str = "-"
            kelly_str = "-"
            rz_str = "-"
            od_str = "-"
            team_rz_str = "-"
            
            if stats:
                prob = stats['prob']
                p_team = stats['team']
                fair_odds = calculate_fair_odds(prob)
                fair_str = f"+{fair_odds}" if fair_odds > 0 else str(fair_odds)
                
                # Calculate EV
                # Convert American to Decimal
                decimal_odds = (price / 100) + 1 if price > 0 else (100 / abs(price)) + 1
                ev = (prob * decimal_odds) - 1
                ev_pct = ev * 100
                ev_str = f"{ev_pct:+.1f}%"
                
                stats_str = f"{stats['first_tds']}/{stats['team_games']}"
                
                # Highlight positive EV and Calc Kelly
                if ev > 0:
                    ev_str = f"*{ev_str}*"
                    kelly_bet = calculate_kelly_criterion(prob, decimal_odds, bankroll)
                    if kelly_bet > 0:
                        kelly_str = f"${kelly_bet:.0f}"
                    
                # Matchup Analysis
                if defense_rankings and roster_df is not None and home_team and away_team:
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
                        
                        # Funnel Defense Check
                        if funnel_defenses and opponent in funnel_defenses:
                            funnel_type = funnel_defenses[opponent]
                            if funnel_type:
                                is_funnel_match = False
                                if funnel_type == "Pass Funnel" and search_pos in ['WR', 'TE', 'QB']:
                                    is_funnel_match = True
                                elif funnel_type == "Run Funnel" and search_pos == 'RB':
                                    is_funnel_match = True
                                    
                                if is_funnel_match:
                                    matchup_str += f" ({funnel_type})"
                    
                # Team RZ Splits
                if team_rz_splits and p_team in team_rz_splits:
                    splits = team_rz_splits[p_team]
                    pass_pct = splits.get('pass_pct', 0)
                    run_pct = splits.get('run_pct', 0)
                    if pass_pct > run_pct:
                        team_rz_str = f"{pass_pct:.0f}% Pass"
                    else:
                        team_rz_str = f"{run_pct:.0f}% Run"

                # Red Zone Stats
                rz_data = get_stats_for_player(player, red_zone_stats)
                if rz_data:
                    rz_str = f"{rz_data['rz_opps']}/{rz_data['rz_tds']}"

                # Opening Drive Stats
                od_data = get_stats_for_player(player, opening_drive_stats)
                if od_data:
                    od_str = f"{od_data['od_opps']}/{od_data['od_tds']}"

            print(f"{player:<25} {price_str:<6} {data['bookmaker']:<12} {fair_str:<6} {ev_str:<6} {kelly_str:<6} {stats_str:<8} {rz_str:<10} {od_str:<10} {team_rz_str:<12} {matchup_str}")

    elif mode == 'specific':
        for bm in bookmakers:
            if bm['title'] == selected_bm:
                print(f"\n[{bm['title']}]")
                print(f"{'Player':<25} {'Odds':<6} {'Fair':<6} {'EV%':<6} {'Kelly':<6} {'Stats':<8} {'RZ':<8} {'OD':<8} {'Team RZ':<12} {'Matchup'}")
                print("-" * 115)
                
                for market in bm.get('markets', []):
                    if market['key'] == MARKET_1ST_TD:
                        outcomes = sorted(market.get("outcomes", []), key=lambda x: x["price"])
                        for outcome in outcomes:
                            player = outcome.get("description", outcome["name"])
                            price = outcome["price"]
                            price_str = f"+{price}" if price > 0 else str(price)
                            
                            stats = get_stats_for_player(player, player_stats)
                            fair_str = "-"
                            ev_str = "-"
                            stats_str = "-"
                            matchup_str = "-"
                            kelly_str = "-"
                            rz_str = "-"
                            od_str = "-"
                            team_rz_str = "-"
                            
                            if stats:
                                prob = stats['prob']
                                p_team = stats['team']
                                fair_odds = calculate_fair_odds(prob)
                                fair_str = f"+{fair_odds}" if fair_odds > 0 else str(fair_odds)
                                decimal_odds = (price / 100) + 1 if price > 0 else (100 / abs(price)) + 1
                                ev = (prob * decimal_odds) - 1
                                ev_str = f"{ev * 100:+.1f}%"
                                stats_str = f"{stats['first_tds']}/{stats['team_games']}"
                                if ev > 0: 
                                    ev_str = f"*{ev_str}*"
                                    kelly_bet = calculate_kelly_criterion(prob, decimal_odds, bankroll)
                                    if kelly_bet > 0:
                                        kelly_str = f"${kelly_bet:.0f}"
                                
                                # Matchup Analysis
                                if defense_rankings and roster_df is not None and home_team and away_team:
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
                                        
                                        # Funnel Defense Check
                                        if funnel_defenses and opponent in funnel_defenses:
                                            funnel_type = funnel_defenses[opponent]
                                            if funnel_type:
                                                is_funnel_match = False
                                                if funnel_type == "Pass Funnel" and search_pos in ['WR', 'TE', 'QB']:
                                                    is_funnel_match = True
                                                elif funnel_type == "Run Funnel" and search_pos == 'RB':
                                                    is_funnel_match = True
                                                    
                                                if is_funnel_match:
                                                    matchup_str += f" ({funnel_type})"
                                    
                                # Team RZ Splits
                                if team_rz_splits and p_team in team_rz_splits:
                                    splits = team_rz_splits[p_team]
                                    pass_pct = splits.get('pass_pct', 0)
                                    run_pct = splits.get('run_pct', 0)
                                    if pass_pct > run_pct:
                                        team_rz_str = f"{pass_pct:.0f}% Pass"
                                    else:
                                        team_rz_str = f"{run_pct:.0f}% Run"
                                
                                # Red Zone Stats
                                rz_data = get_stats_for_player(player, red_zone_stats)
                                if rz_data:
                                    rz_str = f"{rz_data['rz_opps']}/{rz_data['rz_tds']}"

                                # Opening Drive Stats
                                od_data = get_stats_for_player(player, opening_drive_stats)
                                if od_data:
                                    od_str = f"{od_data['od_opps']}/{od_data['od_tds']}"

                            print(f"{player:<25} {price_str:<6} {fair_str:<6} {ev_str:<6} {kelly_str:<6} {stats_str:<8} {rz_str:<8} {od_str:<8} {team_rz_str:<12} {matchup_str}")

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

def display_best_bets(best_bets: list, bankroll: float = 1000.0):
    """
    Displays a consolidated list of the best bets (Positive EV) across all games.
    """
    if not best_bets:
        print("\nNo positive EV bets found.")
        return

    print(f"\n" + "="*135)
    print(f"BEST BETS SCANNER (Bankroll: ${bankroll})")
    print("="*135)
    print(f"{'Game':<20} {'Player':<20} {'Odds':<6} {'Book':<12} {'Fair':<6} {'EV%':<6} {'Kelly':<6} {'Stats':<8} {'RZ':<8} {'OD':<8} {'Team RZ':<12} {'Matchup'}")
    print("-" * 135)

    # Sort by EV descending
    sorted_bets = sorted(best_bets, key=lambda x: x['ev'], reverse=True)

    for bet in sorted_bets:
        game_str = f"{bet['away']} @ {bet['home']}"
        price_str = f"+{bet['price']}" if bet['price'] > 0 else str(bet['price'])
        fair_str = f"+{bet['fair_odds']}" if bet['fair_odds'] > 0 else str(bet['fair_odds'])
        ev_str = f"{bet['ev']*100:+.1f}%"
        kelly_str = f"${bet['kelly']:.0f}" if bet['kelly'] > 0 else "-"
        stats_str = f"{bet['stats']['first_tds']}/{bet['stats']['team_games']}"
        
        rz_str = "-"
        if bet.get('rz_stats'):
            rz_str = f"{bet['rz_stats']['rz_opps']}/{bet['rz_stats']['rz_tds']}"
            
        od_str = "-"
        if bet.get('od_stats'):
            od_str = f"{bet['od_stats']['od_opps']}/{bet['od_stats']['od_tds']}"
            
        matchup_str = bet.get('matchup', '-')
        
        team_rz_str = "-"
        if bet.get('team_rz_split'):
            splits = bet['team_rz_split']
            pass_pct = splits.get('pass_pct', 0)
            run_pct = splits.get('run_pct', 0)
            if pass_pct > run_pct:
                team_rz_str = f"{pass_pct:.0f}% Pass"
            else:
                team_rz_str = f"{run_pct:.0f}% Run"

        print(f"{game_str:<20} {bet['player']:<20} {price_str:<6} {bet['bookmaker']:<12} {fair_str:<6} {ev_str:<6} {kelly_str:<6} {stats_str:<8} {rz_str:<8} {od_str:<8} {team_rz_str:<12} {matchup_str}")
    print("=" * 135)
