# NFL TD Scorers

A web application to track and analyze NFL touchdown scorers. Keep track of your favorite players' touchdowns throughout the season, including regular season, playoffs, and Super Bowl performances.

## Features

- 📊 **Dashboard**: View touchdown leaderboard with total statistics
- 👥 **Player Management**: Add, view, and manage players
- 🏈 **Touchdown Tracking**: Record touchdowns with game details (date, opponent, quarter, game type)
- 📈 **Statistics**: Track touchdowns by game type (Regular Season, Playoffs, Super Bowl)
- 🎨 **Modern UI**: Clean and responsive design

## Installation

1. **Clone or navigate to the project directory:**
   ```bash
   cd "NFL TD Scorers"
   ```

2. **Create a virtual environment (recommended):**
   ```bash
   python -m venv venv
   ```

3. **Activate the virtual environment:**
   - On Windows:
     ```bash
     venv\Scripts\activate
     ```
   - On macOS/Linux:
     ```bash
     source venv/bin/activate
     ```

4. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Running the Application

1. **Start the Flask server:**
   ```bash
   python app.py
   ```

2. **Open your browser and navigate to:**
   ```
   http://localhost:5000
   ```

## Usage

### Adding a Player
1. Click on "Add Player" in the navigation
2. Fill in the player's name (required)
3. Optionally add position and team
4. Click "Add Player"

### Adding a Touchdown
1. Click on "Add Touchdown" in the navigation
2. Select the player who scored
3. Enter the game date (required)
4. Optionally add opponent, quarter, game type, and notes
5. Click "Add Touchdown"

### Viewing Statistics
- **Dashboard**: See the leaderboard with all players ranked by total touchdowns
- **Player Detail**: Click on any player to see their complete touchdown history
- **Players Page**: View all players in a card layout

## Database

The application uses SQLite database (`nfl_td_scorers.db`) which is automatically created on first run. The database includes:

- **Players**: Name, position, team
- **Touchdowns**: Player, game date, opponent, quarter, game type, notes

## Project Structure

```
NFL TD Scorers/
├── app.py                 # Main Flask application
├── requirements.txt       # Python dependencies
├── README.md             # This file
├── templates/            # HTML templates
│   ├── index.html        # Dashboard
│   ├── players.html      # Players list
│   ├── player_detail.html # Individual player page
│   ├── add_player.html   # Add player form
│   └── add_touchdown.html # Add touchdown form
└── static/
    └── css/
        └── style.css     # Stylesheet
```

## Technologies Used

- **Flask**: Web framework
- **SQLAlchemy**: Database ORM
- **SQLite**: Database
- **HTML/CSS**: Frontend

## Future Enhancements

Potential features to add:
- Import/export data
- Advanced statistics and charts
- Player comparison
- Season/year filtering
- API endpoints for data access
- User authentication
- Multiple seasons tracking

## License

This project is open source and available for personal use.

