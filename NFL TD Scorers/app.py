from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect, text, func
from datetime import datetime, date
import os
import nflreadpy as nfl
import polars as pl
import pandas as pd
import requests
from collections import defaultdict

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///nfl_td_scorers.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production-2025')
db = SQLAlchemy(app)

# Database Models
class Player(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    position = db.Column(db.String(20))
    team = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    touchdowns = db.relationship('Touchdown', backref='player', lazy=True, cascade='all, delete-orphan')
    
    @property
    def total_tds(self):
        return db.session.query(Touchdown).filter_by(player_id=self.id).count()
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'position': self.position,
            'team': self.team,
            'total_tds': db.session.query(Touchdown).filter_by(player_id=self.id).count()
        }

class Touchdown(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    player_id = db.Column(db.Integer, db.ForeignKey('player.id'), nullable=False)
    game_date = db.Column(db.Date, nullable=False)
    opponent = db.Column(db.String(50))
    quarter = db.Column(db.Integer)
    game_type = db.Column(db.String(20), default='Regular')  # Regular, Playoff, Super Bowl
    notes = db.Column(db.Text)
    game_id = db.Column(db.String(50))  # Store NFL game ID to avoid duplicates
    play_id = db.Column(db.String(50))  # Store play ID for uniqueness
    game_time = db.Column(db.String(20))  # Store game time (e.g., "19:00")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def to_dict(self):
        # Type ignore for backref relationship
        player = getattr(self, 'player', None)
        player_name = player.name if player else 'Unknown'
        return {
            'id': self.id,
            'player_id': self.player_id,
            'player_name': player_name,
            'game_date': self.game_date.strftime('%Y-%m-%d'),
            'opponent': self.opponent,
            'quarter': self.quarter,
            'game_type': self.game_type,
            'notes': self.notes
        }

# Team name mapping from nflreadpy abbreviations to full names
TEAM_NAMES = {
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

# Odds API Configuration
ODDS_API_KEY = os.environ.get('ODDS_API_KEY', '11d265853d712ded110d5e0a5ff82c5b')
ODDS_BASE_URL = "https://api.the-odds-api.com/v4/sports"
ODDS_SPORT_KEY = "americanfootball_nfl"
ODDS_MARKET = "player_1st_td"
ODDS_TIMEOUT = 10

def get_team_full_name(abbrev):
    """Convert team abbreviation to full name"""
    return TEAM_NAMES.get(abbrev, abbrev)

def normalize_team_name(name: str) -> str:
    """Normalize team names by lowercasing and removing special characters."""
    return name.lower().replace(".", "").replace(" ", "")

def get_player_1st_td_odds(event_id: str) -> dict:
    """
    Fetch first touchdown scorer odds from The Odds API for a specific game.
    
    Returns:
    - Dictionary with player odds: {player_name: odds_value}
    """
    try:
        url = f"{ODDS_BASE_URL}/{ODDS_SPORT_KEY}/events/{event_id}/odds"
        params = {
            "apiKey": ODDS_API_KEY,
            "markets": ODDS_MARKET,
            "regions": "us",
            "oddsFormat": "american"
        }
        
        response = requests.get(url, params=params, timeout=ODDS_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        player_odds = {}
        # Extract odds from all bookmakers
        for bookmaker in data.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                if market["key"] == ODDS_MARKET:
                    for outcome in market.get("outcomes", []):
                        player = outcome.get("description", outcome.get("name", "Unknown"))
                        price = outcome.get("price", None)
                        # Only add if we have a valid price and haven't seen this player yet
                        if price and player not in player_odds:
                            player_odds[player] = price
        
        return player_odds
    except Exception as e:
        print(f"Error fetching odds for event {event_id}: {e}")
        return {}

def determine_game_type(week, season_type=None):
    """Determine if game is Regular, Playoff, or Super Bowl"""
    if season_type and 'POST' in str(season_type):
        # Check if it's Super Bowl (usually week 5 in playoffs)
        if week == 5:
            return 'Super Bowl'
        return 'Playoff'
    return 'Regular'

def fetch_nfl_touchdowns(seasons=None):
    """Fetch touchdown data from nflreadpy and return processed data"""
    try:
        if seasons is None:
            # Default to current year and previous year
            current_year = datetime.now().year
            seasons = [current_year - 1, current_year]
        
        print(f"Loading play-by-play data for seasons: {seasons}")
        
        # Load play-by-play data
        pbp_data = nfl.load_pbp(seasons=seasons)
        
        if pbp_data is None or len(pbp_data) == 0:
            print("No play-by-play data loaded")
            return None
        
        print(f"Loaded {len(pbp_data)} plays")
        
        # Filter for touchdown plays - check if touchdown column exists
        if 'touchdown' not in pbp_data.columns:
            print("Warning: 'touchdown' column not found. Available columns:", list(pbp_data.columns)[:20])
            # Try alternative column names
            if 'td_team' in pbp_data.columns:
                td_plays = pbp_data.filter(pl.col('td_team').is_not_null())
            else:
                print("No touchdown-related columns found")
                return None
        else:
            # Filter for touchdown plays using polars syntax
            td_plays = pbp_data.filter(pl.col('touchdown') == 1)
        
        if len(td_plays) == 0:
            print("No touchdown plays found after filtering")
            return None
        
        print(f"Found {len(td_plays)} touchdown plays")
        
        # Get relevant columns - check which ones exist
        available_cols = td_plays.columns
        cols_to_select = []
        for col in ['game_id', 'play_id', 'season', 'week', 'season_type',
                    'posteam', 'defteam', 'qtr', 'desc', 
                    'td_player_name', 'td_player_id', 'posteam_type']:
            if col in available_cols:
                cols_to_select.append(col)
        
        if not cols_to_select:
            print("No expected columns found. Available columns:", available_cols)
            return None
        
        td_data = td_plays.select(cols_to_select)
        
        # Convert to pandas for easier processing
        td_df = td_data.to_pandas()
        
        if td_df.empty:
            print("DataFrame is empty after conversion")
            return None
        
        print(f"Converted to pandas: {len(td_df)} rows")
        
        # Also load games data to get game dates
        print("Loading schedule data...")
        games_data = nfl.load_schedules(seasons=seasons)
        games_df = games_data.to_pandas()
        
        if games_df.empty:
            print("No schedule data loaded")
            return None
        
        print(f"Loaded {len(games_df)} games from schedule")
        
        # Merge to get game dates
        if 'game_id' not in td_df.columns or 'game_id' not in games_df.columns:
            print("game_id column missing for merge")
            return None
        
        # Get available schedule columns
        schedule_cols = ['game_id', 'gameday', 'home_team', 'away_team', 'home_score', 'away_score']
        # Add game time columns if available
        for col in ['gametime', 'start_time', 'time']:
            if col in games_df.columns:
                schedule_cols.append(col)
                break
        
        merged_df = td_df.merge(
            games_df[schedule_cols],
            on='game_id',
            how='left'
        )
        
        print(f"Merge complete: {len(merged_df)} rows")
        return merged_df
        
    except Exception as e:
        import traceback
        error_msg = f"Error fetching NFL data: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return None

def sync_nfl_data(seasons=None, update_existing=False):
    """Sync touchdown data from nflreadpy to database"""
    try:
        td_data = fetch_nfl_touchdowns(seasons)
        if td_data is None:
            return {'success': False, 'message': 'No data fetched - check console for details', 'added': 0}
        if hasattr(td_data, 'empty') and td_data.empty:
            return {'success': False, 'message': 'Data fetched but DataFrame is empty', 'added': 0}
        if len(td_data) == 0:
            return {'success': False, 'message': 'Data fetched but has 0 rows', 'added': 0}
        
        added_players = 0
        added_touchdowns = 0
        skipped_touchdowns = 0
        
        # Group by player name for batch processing
        for _, row in td_data.iterrows():
            player_name = row.get('td_player_name')
            if player_name is None or (isinstance(player_name, str) and not player_name.strip()):
                continue
            if hasattr(pd, 'isna') and pd.isna(player_name):
                continue
            
            # Get or create player
            player = Player.query.filter_by(name=player_name).first()
            if not player:
                # Try to get team info
                team_abbrev = row.get('posteam')
                team_name = get_team_full_name(team_abbrev) if team_abbrev else None
                
                player = Player()
                player.name = player_name
                player.team = team_name
                db.session.add(player)
                db.session.flush()  # Get the ID
                added_players += 1
            
            # Check if touchdown already exists
            game_id = str(row.get('game_id', ''))
            play_id = str(row.get('play_id', ''))
            
            existing_td = Touchdown.query.filter_by(
                player_id=player.id,
                game_id=game_id,
                play_id=play_id
            ).first()
            
            if existing_td and not update_existing:
                skipped_touchdowns += 1
                continue
            
            # Parse game date
            game_date_str = row.get('gameday')
            if game_date_str is None:
                continue
            if hasattr(pd, 'isna') and pd.isna(game_date_str):
                continue
            
            try:
                game_date = datetime.strptime(str(game_date_str), '%Y-%m-%d').date()
            except:
                continue
            
            # Get game time if available
            game_time = None
            for time_col in ['gametime', 'start_time', 'time']:
                if time_col in row and row.get(time_col) is not None:
                    time_val = row.get(time_col)
                    if not (hasattr(pd, 'isna') and pd.isna(time_val)):
                        game_time = str(time_val)
                        break
            
            # Get opponent
            posteam = row.get('posteam')
            defteam = row.get('defteam')
            opponent_abbrev = defteam if posteam else None
            opponent = get_team_full_name(opponent_abbrev) if opponent_abbrev else None
            
            # Get quarter
            qtr_val = row.get('qtr')
            quarter = None
            if qtr_val is not None:
                try:
                    if not (hasattr(pd, 'isna') and pd.isna(qtr_val)):
                        quarter = int(qtr_val)
                except (ValueError, TypeError):
                    pass
            
            # Determine game type
            week_val = row.get('week')
            week = 0
            if week_val is not None:
                try:
                    if not (hasattr(pd, 'isna') and pd.isna(week_val)):
                        week = int(week_val)
                except (ValueError, TypeError):
                    pass
            season_type = row.get('season_type', '')
            game_type = determine_game_type(week, season_type)
            
            # Create touchdown record
            if existing_td and update_existing:
                # Update existing
                existing_td.game_date = game_date
                existing_td.opponent = opponent
                existing_td.quarter = quarter
                existing_td.game_type = game_type
                existing_td.game_time = game_time
            else:
                # Create new
                touchdown = Touchdown()
                touchdown.player_id = player.id
                touchdown.game_date = game_date
                touchdown.opponent = opponent
                touchdown.quarter = quarter
                touchdown.game_type = game_type
                touchdown.game_id = game_id
                touchdown.play_id = play_id
                touchdown.game_time = game_time
                touchdown.notes = "Auto-synced from nflreadpy"
                db.session.add(touchdown)
                added_touchdowns += 1
        
        db.session.commit()
        
        return {
            'success': True,
            'message': f'Successfully synced data',
            'added_players': added_players,
            'added_touchdowns': added_touchdowns,
            'skipped_touchdowns': skipped_touchdowns
        }
    except Exception as e:
        db.session.rollback()
        return {'success': False, 'message': f'Error syncing data: {str(e)}', 'added': 0}

# Routes
@app.route('/')
def index():
    # Use aggregation to avoid N+1 queries
    from sqlalchemy.orm import joinedload
    players_with_counts = db.session.query(
        Player,
        func.count(Touchdown.id).filter(Touchdown.game_type == 'Regular').label('regular_tds'),
        func.count(Touchdown.id).filter(Touchdown.game_type == 'Playoff').label('playoff_tds'),
        func.count(Touchdown.id).filter(Touchdown.game_type == 'Super Bowl').label('superbowl_tds'),
        func.count(Touchdown.id).label('total_tds')
    ).outerjoin(Touchdown).group_by(Player.id).all()
    
    player_stats = [
        {
            'player': player.to_dict(),
            'regular_season_tds': regular_tds or 0,
            'playoff_tds': playoff_tds or 0,
            'super_bowl_tds': superbowl_tds or 0,
            'total_tds': total_tds or 0
        }
        for player, regular_tds, playoff_tds, superbowl_tds, total_tds in players_with_counts
    ]
    
    player_stats.sort(key=lambda x: x['total_tds'], reverse=True)
    return render_template('index.html', player_stats=player_stats)

@app.route('/players')
def players():
    players = Player.query.order_by(Player.name).all()
    return render_template('players.html', players=players)

@app.route('/player/<int:player_id>')
def player_detail(player_id):
    player = Player.query.get_or_404(player_id)
    touchdowns = Touchdown.query.filter_by(player_id=player_id).order_by(Touchdown.game_date.desc()).all()
    return render_template('player_detail.html', player=player, touchdowns=touchdowns)

@app.route('/api/stats')
def api_stats():
    # Use aggregation for efficient stats calculation
    total_touchdowns = db.session.query(func.count(Touchdown.id)).scalar() or 0
    
    # Get top 10 scorers with aggregation
    top_scorers_query = db.session.query(
        Player.name,
        Player.team,
        Player.position,
        func.count(Touchdown.id).label('total_tds')
    ).join(Touchdown).group_by(Player.id).order_by(func.count(Touchdown.id).desc()).limit(10).all()
    
    top_scorers = [
        {
            'name': name,
            'team': team,
            'position': position,
            'total_tds': total_tds
        }
        for name, team, position, total_tds in top_scorers_query
    ]
    
    stats = {
        'total_players': db.session.query(func.count(Player.id)).scalar() or 0,
        'total_touchdowns': total_touchdowns,
        'top_scorers': top_scorers
    }
    
    return jsonify(stats)

@app.route('/delete_touchdown/<int:td_id>', methods=['POST'])
def delete_touchdown(td_id):
    touchdown = Touchdown.query.get_or_404(td_id)
    player_id = touchdown.player_id
    db.session.delete(touchdown)
    db.session.commit()
    return redirect(url_for('player_detail', player_id=player_id))

@app.route('/delete_player/<int:player_id>', methods=['POST'])
def delete_player(player_id):
    player = Player.query.get_or_404(player_id)
    db.session.delete(player)
    db.session.commit()
    return redirect(url_for('players'))

@app.route('/sync', methods=['GET', 'POST'])
def sync_data():
    """Sync data from nflreadpy"""
    if request.method == 'POST':
        seasons_str = request.form.get('seasons', '')
        update_existing = request.form.get('update_existing') == 'on'
        
        # Parse seasons
        if seasons_str:
            try:
                seasons = [int(s.strip()) for s in seasons_str.split(',')]
            except:
                seasons = None
        else:
            seasons = None
        
        result = sync_nfl_data(seasons=seasons, update_existing=update_existing)
        
        if result['success']:
            flash(f"Sync completed! Added {result['added_players']} players and {result['added_touchdowns']} touchdowns. Skipped {result['skipped_touchdowns']} duplicates.", 'success')
        else:
            flash(f"Sync failed: {result['message']}", 'error')
        
        return redirect(url_for('sync_data'))
    
    # Get current year for default
    current_year = datetime.now().year
    return render_template('sync.html', current_year=current_year)

@app.route('/api/sync', methods=['POST'])
def api_sync():
    """API endpoint for syncing data"""
    data = request.get_json()
    seasons = data.get('seasons')
    update_existing = data.get('update_existing', False)
    
    result = sync_nfl_data(seasons=seasons, update_existing=update_existing)
    return jsonify(result)

def is_holiday(date):
    """Check if a date falls on a holiday when NFL games might be played"""
    month = date.month
    day = date.day
    
    # Common holidays when NFL might schedule games on unusual days
    holidays = [
        (12, 25),  # Christmas
        (12, 26),  # Day after Christmas
        (12, 31),  # New Year's Eve
        (1, 1),    # New Year's Day
        (1, 2),    # Day after New Year's
    ]
    
    return (month, day) in holidays

def is_standalone_game(game_date, game_id=None, game_time=None):
    """Determine if a game is a standalone game (primetime or non-Sunday)"""
    # Get day of week (0 = Monday, 6 = Sunday)
    day_of_week = game_date.weekday()
    
    # Standalone games are:
    # - Thursday (3) - Thursday Night Football
    # - Friday (4) - occasional games
    # - Saturday (5) - usually late season
    # - Monday (0) - Monday Night Football
    # - Tuesday (1) - holiday games
    # - Wednesday (2) - holiday games
    # - Sunday (6) after 19:00 EST - Sunday Night Football
    
    if day_of_week in [0, 3, 4, 5]:  # Monday, Thursday, Friday, Saturday
        return True
    
    # Tuesday or Wednesday games on holidays
    if day_of_week in [1, 2] and is_holiday(game_date):  # Tuesday, Wednesday
        return True
    
    # Check for Sunday Night Football (after 19:00 EST)
    if day_of_week == 6 and game_time:  # Sunday
        try:
            # Parse time string (format could be "19:00", "7:00 PM", etc.)
            time_str = str(game_time).strip().upper()
            
            # Handle 24-hour format (e.g., "19:00", "20:30")
            if ':' in time_str and len(time_str) <= 5:
                hour, minute = time_str.split(':')
                hour = int(hour)
                if hour >= 19:  # 7:00 PM or later
                    return True
            
            # Handle 12-hour format (e.g., "7:00 PM", "8:30 PM")
            elif 'PM' in time_str:
                time_part = time_str.replace('PM', '').strip()
                if ':' in time_part:
                    hour, minute = time_part.split(':')
                    hour = int(hour)
                    if hour >= 7:  # 7:00 PM or later (7 PM = 19:00)
                        return True
                else:
                    hour = int(time_part)
                    if hour >= 7:
                        return True
        except (ValueError, AttributeError):
            # If we can't parse the time, skip this check
            pass
    
    return False

@app.route('/first_td_scorers')
def first_td_scorers():
    """Display players who scored the first TD of each game"""
    
    # Get all touchdowns with game_id, ordered efficiently
    touchdowns = Touchdown.query.filter(
        Touchdown.game_id.isnot(None),
        Touchdown.game_id != ''
    ).all()
    
    # Group by game_id and find first TD for each game
    first_tds_dict = {}
    for td in touchdowns:
        game_id = td.game_id
        if game_id not in first_tds_dict:
            first_tds_dict[game_id] = td
        else:
            # Compare to find earliest TD (prioritize by quarter, then date)
            existing = first_tds_dict[game_id]
            td_priority = (td.quarter or 99, td.game_date)
            existing_priority = (existing.quarter or 99, existing.game_date)
            if td_priority < existing_priority:
                first_tds_dict[game_id] = td
    
    first_td_list = sorted(first_tds_dict.values(), key=lambda x: (x.game_date, x.quarter or 99), reverse=True)
    
    # Filter standalone games once
    standalone_first_tds = [
        td for td in first_td_list 
        if is_standalone_game(td.game_date, td.game_id, td.game_time)
    ]
    
    # Count first TDs per player (process all and standalone in one pass)
    player_first_td_count = {}
    standalone_player_count = {}
    
    for td in first_td_list:
        player_id = td.player_id
        if player_id not in player_first_td_count:
            player_first_td_count[player_id] = {'count': 0, 'touchdowns': []}
        player_first_td_count[player_id]['count'] += 1
        player_first_td_count[player_id]['touchdowns'].append(td)
        
        if td in standalone_first_tds:
            if player_id not in standalone_player_count:
                standalone_player_count[player_id] = {'count': 0, 'touchdowns': []}
            standalone_player_count[player_id]['count'] += 1
            standalone_player_count[player_id]['touchdowns'].append(td)
    
    # Add player objects and sort
    player_stats = []
    for player_id, td_info in player_first_td_count.items():
        # Get odds for each touchdown's game if available
        odds_data = []
        for td in td_info['touchdowns']:
            if td.game_id:
                odds = get_player_1st_td_odds(td.game_id)
                if odds:
                    odds_data.append({
                        'game_date': td.game_date,
                        'opponent': td.opponent,
                        'odds': odds
                    })
        
        player_stats.append({
            'player': td_info['touchdowns'][0].player,
            'count': td_info['count'],
            'touchdowns': td_info['touchdowns'],
            'odds_data': odds_data
        })
    player_stats.sort(key=lambda x: x['count'], reverse=True)
    
    standalone_player_stats = []
    for player_id, td_info in standalone_player_count.items():
        standalone_player_stats.append({
            'player': td_info['touchdowns'][0].player,
            'count': td_info['count'],
            'touchdowns': td_info['touchdowns']
        })
    standalone_player_stats.sort(key=lambda x: x['count'], reverse=True)
    
    return render_template('first_td_scorers.html', 
                         first_tds=first_td_list,
                         player_stats=player_stats,
                         standalone_first_tds=standalone_first_tds,
                         standalone_player_stats=standalone_player_stats)

def init_db():
    """Initialize database and add missing columns if needed"""
    with app.app_context():
        # Create all tables
        db.create_all()
        
        # Check if touchdown table needs migration for game_id and play_id
        inspector = inspect(db.engine)
        
        if 'touchdown' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('touchdown')]
            
            # Add game_id column if it doesn't exist
            if 'game_id' not in columns:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text("ALTER TABLE touchdown ADD COLUMN game_id VARCHAR(50)"))
                    print("Added game_id column to touchdown table")
                except Exception as e:
                    print(f"Error adding game_id column: {e}")
            
            # Add play_id column if it doesn't exist
            if 'play_id' not in columns:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text("ALTER TABLE touchdown ADD COLUMN play_id VARCHAR(50)"))
                    print("Added play_id column to touchdown table")
                except Exception as e:
                    print(f"Error adding play_id column: {e}")
            
            # Add game_time column if it doesn't exist
            if 'game_time' not in columns:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text("ALTER TABLE touchdown ADD COLUMN game_time VARCHAR(20)"))
                    print("Added game_time column to touchdown table")
                except Exception as e:
                    print(f"Error adding game_time column: {e}")

if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='0.0.0.0', port=5000)

