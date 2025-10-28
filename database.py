# Fichier: database.py
import sqlite3
import time
from typing import List, Dict, Any, Optional, Tuple

DB_FILE = 'darwin_bot.db'

# -------- Connexion + pragmas sécu/perf --------
def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    with conn:  # appliquer des pragmas sûrs
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
    return conn

# -------- Création / Migrations idempotentes --------
def setup_database():
    print("Initialisation de la base de données SQLite...")
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY,
                symbol TEXT,
                side TEXT,
                regime TEXT,
                status TEXT,
                entry_price REAL,
                sl_price REAL,
                tp_price REAL,
                quantity REAL,
                risk_percent REAL,
                management_strategy TEXT DEFAULT 'NORMAL',
                breakeven_status TEXT DEFAULT 'PENDING',
                pnl REAL DEFAULT 0,
                open_timestamp INTEGER,
                close_timestamp INTEGER
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        # Migrations idempotentes (ignore si déjà présentes)
        _ensure_column(conn, "trades", "pnl_percent", "REAL", "0")
        _ensure_column(conn, "trades", "entry_atr", "REAL", "0")
        _ensure_column(conn, "trades", "entry_rsi", "REAL", "0")
        _ensure_column(conn, "trades", "management_strategy", "TEXT", "'NORMAL'")
        _ensure_column(conn, "trades", "breakeven_status", "TEXT", "'PENDING'")

        # Index utiles
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol_status ON trades(symbol, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_close_ts ON trades(close_timestamp)")
        conn.commit()

def _ensure_column(conn: sqlite3.Connection, table: str, col: str, coltype: str, default_sql_literal: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r["name"] for r in cur.fetchall()]
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype} DEFAULT {default_sql_literal}")
        conn.commit()

# -------- CRUD Trades --------
def create_trade(
    symbol: str,
    side: str,
    regime: str,
    entry_price: float,
    sl_price: float,
    tp_price: float,
    quantity: float,
    risk_percent: float,
    management_strategy: str,
    entry_atr: float = 0.0,
    entry_rsi: float = 0.0
) -> int:
    """Crée un trade OPEN (breakeven_status PENDING par défaut) et retourne l'ID."""
    open_ts = int(time.time())
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO trades (
                symbol, side, regime, status,
                entry_price, sl_price, tp_price, quantity, risk_percent,
                management_strategy, breakeven_status,
                open_timestamp, pnl, close_timestamp,
                entry_atr, entry_rsi
            ) VALUES (
                ?, ?, ?, 'OPEN',
                ?, ?, ?, ?, ?,
                ?, 'PENDING',
                ?, 0, NULL,
                ?, ?
            )
        """, (
            symbol, side, regime,
            entry_price, sl_price, tp_price, quantity, risk_percent,
            management_strategy,
            open_ts,
            entry_atr, entry_rsi
        ))
        conn.commit()
        return cur.lastrowid

def update_trade_to_breakeven(trade_id: int, remaining_quantity: float, new_sl: float):
    """Met à jour après mise à breakeven (valeur 'ACTIVATED' conservée pour compat)."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE trades
               SET breakeven_status = 'ACTIVATED',
                   quantity = ?,
                   sl_price = ?
             WHERE id = ?
        """, (remaining_quantity, new_sl, trade_id))
        conn.commit()
    print(f"DB: Trade #{trade_id} mis à breakeven. Quantité restante: {remaining_quantity}")

def close_trade(trade_id: int, status: str, pnl: float):
    """Ferme un trade (status: CLOSED / CLOSED_MANUAL / ERROR...)."""
    close_ts = int(time.time())
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE trades
               SET status = ?, pnl = ?, close_timestamp = ?
             WHERE id = ?
        """, (status, pnl, close_ts, trade_id))
        conn.commit()
    print(f"DB: Trade #{trade_id} fermé avec le statut '{status}'.")

def update_trade_tp(trade_id: int, new_tp_price: float):
    """Met à jour le TP."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE trades SET tp_price = ? WHERE id = ?", (new_tp_price, trade_id))
        conn.commit()
    print(f"DB: TP pour le trade #{trade_id} mis à jour à {new_tp_price}.")

def update_trade_sl(trade_id: int, new_sl_price: float):
    """Met à jour le SL."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE trades SET sl_price = ? WHERE id = ?", (new_sl_price, trade_id))
        conn.commit()
    print(f"DB: SL pour le trade #{trade_id} mis à jour à {new_sl_price}.")

def is_position_open(symbol: str) -> bool:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM trades WHERE symbol = ? AND status = 'OPEN' LIMIT 1", (symbol,))
        return cur.fetchone() is not None

def get_open_positions() -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM trades WHERE status = 'OPEN' ORDER BY open_timestamp DESC")
        return [dict(r) for r in cur.fetchall()]

def get_trade_by_id(trade_id: int) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM trades WHERE id = ?", (trade_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def get_closed_trades_since(timestamp: int) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM trades
             WHERE status != 'OPEN'
               AND close_timestamp IS NOT NULL
               AND close_timestamp >= ?
             ORDER BY close_timestamp DESC
        """, (timestamp,))
        return [dict(r) for r in cur.fetchall()]

# -------- Settings (key/value) --------
def get_setting(key: str, default: Any = None) -> Any:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cur.fetchone()
        return row["value"] if row else default

def set_setting(key: str, value: Any):
    with get_db_connection() as conn:
        cur = conn.cursor()
        # Upsert propre (évite INSERT OR REPLACE qui supprime la ligne puis recrée)
        cur.execute("""
            INSERT INTO settings(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, str(value)))
        conn.commit()
    print(f"DB: Paramètre '{key}' mis à jour.")

def toggle_setting_bool(key: str, default_true: bool = False) -> bool:
    curr = str(get_setting(key, 'true' if default_true else 'false')).lower() in ("1","true","yes","on")
    new_val = not curr
    set_setting(key, str(new_val).lower())
    return new_val
