# NFL First TD Tracker & Odds Analyzer

This tool analyzes NFL First Touchdown scorer data and compares it with real-time odds to find positive Expected Value (EV) bets.

## Features

*   **Historical Analysis**: Tracks First TD scorers for every game in the season.
*   **Funnel Defense Identification**: Spots defenses that are weak against specific play types (Run vs Pass).
*   **EV Scanner**: Calculates Expected Value for bets based on historical hit rates vs bookmaker odds.
*   **Red Zone & Opening Drive Stats**: Deep dive into player usage in critical situations.

## Setup

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Set API Key**:
    You need an API key from [The Odds API](https://the-odds-api.com/).
    Set it as an environment variable:

    *   **Windows (PowerShell)**:
        ```powershell
        $env:ODDS_API_KEY="your_api_key_here"
        ```
    *   **Mac/Linux**:
        ```bash
        export ODDS_API_KEY="your_api_key_here"
        ```

3.  **Run the Tool**:
    ```bash
    python main.py
    ```

## Usage

Follow the on-screen menu to view schedules, check odds, or run the "Best Bets Scanner".
