import polars as pl
from datetime import datetime
from config import API_KEY
from data import load_data_with_cache, get_season_games
from stats import get_first_td_scorers
from main import view_best_bets_scanner
import sys

def test():
    # Try 2025 first as it matches the current date context
    season = 2025
    print(f"Loading data for {season}...")
    try:
        schedule_df, pbp_df, roster_df = load_data_with_cache(season)
        
        if schedule_df.height == 0:
            print("No schedule data found for 2025. Trying 2024...")
            season = 2024
            schedule_df, pbp_df, roster_df = load_data_with_cache(season)

        if schedule_df.height == 0:
            print("No schedule data found.")
            return

        schedule_df = get_season_games(season, schedule_df)
        print("Calculating First TD scorers...")
        first_td_map = get_first_td_scorers(pbp_df, target_game_ids=None, roster_df=roster_df)
        
        print("Data loaded. Running scanner...")
        view_best_bets_scanner(schedule_df, first_td_map, roster_df, pbp_df)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test()
