import nflreadpy as nfl
import polars as pl
import os
import requests
import io

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
