# Fichier: database.py
# Version validée et stable.

import sqlite3
import time
from typing import List, Dict, Any, Optional

DB_FILE = 'darwin_bot.db'

def get_db_connection():
    """Crée et retourne une connexion à la base de données."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def setup_database():
    """Initialise la DB et crée les tables si elles n'existent pas."""
    print("Initialisation de la base de données SQLite...")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL, side TEXT NOT NULL, regime TEXT, status TEXT NOT NULL,
            entry_price REAL NOT NULL, sl_price REAL NOT NULL, tp_price REAL NOT NULL,
            quantity REAL NOT NULL, risk_percent REAL,
            management_strategy TEXT DEFAULT 'NORMAL', breakeven_status TEXT DEFAULT 'PENDING',
            pnl REAL DEFAULT 0.0, pnl_percent REAL DEFAULT 0.0,
            open_timestamp INTEGER NOT NULL, close_timestamp INTEGER,
            duration_minutes INTEGER, max_drawdown REAL,
            entry_atr REAL, entry_rsi REAL
        );
    ''')
    cursor.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);')
    conn.commit()
    conn.close()
    print("Base de données prête.")

def create_trade(symbol: str, side: str, regime: str, entry_price: float, sl_price: float, tp_price: float, quantity: float, risk_percent: float, management_strategy: str, entry_atr: float, entry_rsi: float):
    """Enregistre un nouveau trade dans la DB."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = """
        INSERT INTO trades (
            symbol, side, regime, status, entry_price, sl_price, tp_price,
            quantity, risk_percent, open_timestamp, management_strategy,
            entry_atr, entry_rsi
        ) VALUES (?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(sql, (
        symbol, side, regime, entry_price, sl_price, tp_price,
        quantity, risk_percent, int(time.time()), management_strategy,
        entry_atr, entry_rsi
    ))
    conn.commit()
    conn.close()

def close_trade(trade_id: int, status: str, exit_price: float):
    """Met à jour un trade comme étant fermé et calcule le PNL."""
    conn = get_db_connection()
    trade = get_trade_by_id(trade_id)
    if not trade: return

    pnl = (exit_price - trade['entry_price']) * trade['quantity'] if trade['side'] == 'buy' else (trade['entry_price'] - exit_price) * trade['quantity']
    position_value = trade['entry_price'] * trade['quantity']
    pnl_percent = (pnl / position_value) * 100 if position_value > 0 else 0
    
    close_ts = int(time.time())
    duration = (close_ts - trade['open_timestamp']) // 60
    
    cursor = conn.cursor()
    cursor.execute("UPDATE trades SET status = ?, pnl = ?, pnl_percent = ?, close_timestamp = ?, duration_minutes = ? WHERE id = ?", (status, pnl, pnl_percent, close_ts, duration, trade_id))
    conn.commit()
    conn.close()

# ... (les autres fonctions get_last_closed_trade, get_all_closed_trades, etc. restent ici et sont correctes)
