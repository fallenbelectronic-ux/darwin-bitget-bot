# Fichier: database.py
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
            quantity REAL NOT NULL, risk_percent REAL, pnl REAL DEFAULT 0.0,
            open_timestamp INTEGER NOT NULL, close_timestamp INTEGER, bb20_mid_at_entry REAL,
            management_strategy TEXT DEFAULT 'NORMAL', breakeven_status TEXT DEFAULT 'PENDING' 
        );
    ''')
    cursor.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);')
    conn.commit()
    conn.close()
    print("Base de données prête.")

def create_trade(symbol: str, side: str, regime: str, entry_price: float, sl_price: float, tp_price: float, quantity: float, risk_percent: float, open_timestamp: int, bb20_mid_at_entry: Optional[float], management_strategy: str):
    """Enregistre un nouveau trade dans la DB."""
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = """
        INSERT INTO trades (
            symbol, side, regime, status, entry_price, sl_price, tp_price,
            quantity, risk_percent, open_timestamp, bb20_mid_at_entry, management_strategy
        ) VALUES (?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(sql, (
        symbol, side, regime, entry_price, sl_price, tp_price,
        quantity, risk_percent, open_timestamp, bb20_mid_at_entry, management_strategy
    ))
    conn.commit()
    conn.close()
    print(f"DB: Trade pour {symbol} enregistré (Stratégie: {management_strategy}).")

def update_trade_to_breakeven(trade_id: int, remaining_quantity: float, new_sl: float):
    """Met à jour un trade après sa mise à breakeven."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE trades SET breakeven_status = 'ACTIVATED', quantity = ?, sl_price = ? WHERE id = ?",
        (remaining_quantity, new_sl, trade_id)
    )
    conn.commit()
    conn.close()
    print(f"DB: Trade #{trade_id} mis à breakeven. Quantité restante: {remaining_quantity}")

def get_setting(key: str, default: Any = None) -> Any:
    """Récupère un paramètre depuis la DB."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row['value'] if row else default

def set_setting(key: str, value: Any):
    """Met à jour ou insère un paramètre dans la DB."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()
    print(f"DB: Paramètre '{key}' mis à jour.")

def is_position_open(symbol: str) -> bool:
    """Vérifie si une position est déjà ouverte pour un symbole."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM trades WHERE symbol = ? AND status = 'OPEN' LIMIT 1", (symbol,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def get_open_positions() -> List[Dict[str, Any]]:
    """Récupère toutes les positions ouvertes."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM trades WHERE status = 'OPEN'")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_trade_by_id(trade_id: int) -> Optional[Dict[str, Any]]:
    """Récupère un trade par son ID."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM trades WHERE id = ?", (trade_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def close_trade(trade_id: int, status: str, pnl: float):
    """Met à jour un trade comme étant fermé."""
    conn = get_db_connection()
    cursor = conn.cursor()
    close_ts = int(time.time())
    cursor.execute(
        "UPDATE trades SET status = ?, pnl = ?, close_timestamp = ? WHERE id = ?",
        (status, pnl, close_ts, trade_id)
    )
    conn.commit()
    conn.close()
    print(f"DB: Trade #{trade_id} fermé avec le statut '{status}'.")

def update_trade_tp(trade_id: int, new_tp_price: float):
    """Met à jour le prix du TP pour un trade donné."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE trades SET tp_price = ? WHERE id = ?", (new_tp_price, trade_id))
    conn.commit()
    conn.close()
    print(f"DB: TP pour le trade #{trade_id} mis à jour à {new_tp_price}.")

def get_closed_trades_since(timestamp: int) -> List[Dict[str, Any]]:
    """Récupère les trades clôturés depuis un timestamp."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM trades WHERE status != 'OPEN' AND close_timestamp >= ?", (timestamp,))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]
