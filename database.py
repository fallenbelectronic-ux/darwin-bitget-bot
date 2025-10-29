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

def _load_json_setting(key: str, default):
    """Charge un setting JSON (dict/list) en sûreté."""
    import json
    raw = get_setting(key, None)
    if raw is None:
        return default
    try:
        val = json.loads(raw)
        return val if isinstance(val, (dict, list)) else default
    except Exception:
        return default

def _save_json_setting(key: str, value) -> None:
    """Sauvegarde un setting JSON (dict/list) en sûreté."""
    import json
    set_setting(key, json.dumps(value, ensure_ascii=False))

def _now_ms() -> int:
    import time
    return int(time.time() * 1000)

def _purge_by_age_and_size(items: list, max_days: int, max_items: int, ts_field: str = "created_at") -> list:
    """Purge liste par âge (jours) et taille max, tri desc par ts_field."""
    import time
    now_ms = _now_ms()
    min_ts = now_ms - max(1, int(max_days)) * 24 * 3600 * 1000
    kept = [x for x in items if int(x.get(ts_field, 0)) >= min_ts]
    kept.sort(key=lambda x: int(x.get(ts_field, 0)), reverse=True)
    return kept[:max(1, int(max_items))]

def save_execution_open(exec_data: Dict[str, Any]) -> str:
    """
    Ajoute une exécution (position ouverte) dans EXECUTIONS_LOG (JSON list).
    Retourne exec_id (créé si absent).
    Champs conseillés: exec_id, exchange, account_mode, symbol, side, qty, leverage, avg_entry,
    sl, tp1, tp2, opened_at (ms), status='open', link_signal_id.
    """
    import hashlib, json
    executions = _load_json_setting('EXECUTIONS_LOG', [])
    payload = dict(exec_data or {})
    if not payload.get('exec_id'):
        base = json.dumps({
            'exchange': payload.get('exchange'),
            'account_mode': payload.get('account_mode'),
            'symbol': payload.get('symbol'),
            'side': payload.get('side'),
            'avg_entry': payload.get('avg_entry'),
            'opened_at': payload.get('opened_at') or _now_ms()
        }, sort_keys=True, ensure_ascii=False)
        payload['exec_id'] = hashlib.sha1(base.encode('utf-8')).hexdigest()
    payload['opened_at'] = int(payload.get('opened_at') or _now_ms())
    payload['status'] = payload.get('status', 'open')
    payload['created_at'] = payload.get('created_at', payload['opened_at'])
    payload['updated_at'] = _now_ms()
    # upsert par exec_id
    idx = next((i for i, x in enumerate(executions) if x.get('exec_id') == payload['exec_id']), None)
    if idx is None:
        executions.append(payload)
    else:
        executions[idx].update(payload)
        executions[idx]['updated_at'] = _now_ms()
    _save_json_setting('EXECUTIONS_LOG', executions)
    return payload['exec_id']

def close_execution(exec_id: str, close_price: float, closed_at_ms: Optional[int] = None,
                    pnl_abs: Optional[float] = None, pnl_pct: Optional[float] = None,
                    fees: Optional[float] = None, status: str = 'closed') -> None:
    """
    Met à jour une exécution à la fermeture (status closed/cancelled).
    """
    if not exec_id: return
    executions = _load_json_setting('EXECUTIONS_LOG', [])
    for x in executions:
        if x.get('exec_id') == exec_id:
            x['close_price'] = float(close_price)
            x['closed_at'] = int(closed_at_ms or _now_ms())
            if pnl_abs is not None: x['pnl_abs'] = float(pnl_abs)
            if pnl_pct is not None: x['pnl_pct'] = float(pnl_pct)
            if fees is not None: x['fees'] = float(fees)
            x['status'] = status
            x['updated_at'] = _now_ms()
            break
    _save_json_setting('EXECUTIONS_LOG', executions)

def fetch_open_executions(limit: int = 100) -> List[Dict[str, Any]]:
    """Retourne les exécutions avec status == 'open' triées par opened_at desc."""
    executions = _load_json_setting('EXECUTIONS_LOG', [])
    items = [x for x in executions if str(x.get('status', '')).lower() == 'open']
    items.sort(key=lambda x: int(x.get('opened_at', 0)), reverse=True)
    return items[:max(1, int(limit))]

def fetch_recent_executions(hours: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """Retourne les exécutions récentes (fermé ou ouvert), filtre sur fenêtre glissante en heures."""
    import time
    executions = _load_json_setting('EXECUTIONS_LOG', [])
    items = list(executions)
    if hours is not None:
        min_ts = _now_ms() - int(hours) * 3600 * 1000
        items = [x for x in items if int(x.get('opened_at', 0)) >= min_ts]
    items.sort(key=lambda x: int(x.get('opened_at', 0)), reverse=True)
    return items[:max(1, int(limit))]

def save_order_record(order_data: Dict[str, Any]) -> str:
    """
    Ajoute un ordre dans ORDERS_LOG (JSON list).
    Champs conseillés: order_id, exchange, symbol, side, type, price, qty, status, placed_at, updated_at, link_exec_id.
    """
    import json
    orders = _load_json_setting('ORDERS_LOG', [])
    payload = dict(order_data or {})
    if not payload.get('order_id'):
        # si l'ID exchange n'existe pas encore, on génère un identifiant local
        payload['order_id'] = f"local_{len(orders)+1}_{_now_ms()}"
    payload['placed_at'] = int(payload.get('placed_at') or _now_ms())
    payload['updated_at'] = _now_ms()
    idx = next((i for i, x in enumerate(orders) if x.get('order_id') == payload['order_id']), None)
    if idx is None:
        orders.append(payload)
    else:
        orders[idx].update(payload)
        orders[idx]['updated_at'] = _now_ms()
    _save_json_setting('ORDERS_LOG', orders)
    return payload['order_id']

def fetch_recent_orders(hours: Optional[int] = None, limit: int = 200) -> List[Dict[str, Any]]:
    """Retourne les ordres récents, optionnellement filtrés par fenêtre horaire."""
    orders = _load_json_setting('ORDERS_LOG', [])
    items = list(orders)
    if hours is not None:
        min_ts = _now_ms() - int(hours) * 3600 * 1000
        items = [x for x in items if int(x.get('placed_at', 0)) >= min_ts]
    items.sort(key=lambda x: int(x.get('placed_at', 0)), reverse=True)
    return items[:max(1, int(limit))]

def save_stats_snapshot(horizon: str, snapshot: Dict[str, Any]) -> None:
    """
    Sauvegarde un snapshot de stats dans STATS_CACHE (dict de horizons).
    Horizons attendus: '7d', '30d'. (all-time non requis selon ton choix)
    """
    stats = _load_json_setting('STATS_CACHE', {})
    if not isinstance(stats, dict): stats = {}
    h = str(horizon).lower()
    stats[h] = dict(snapshot or {})
    stats[h]['ts'] = _now_ms()
    _save_json_setting('STATS_CACHE', stats)

def fetch_latest_stats(horizon: str) -> Dict[str, Any]:
    """Retourne le dernier snapshot pour un horizon ('7d' ou '30d'), ou {}."""
    stats = _load_json_setting('STATS_CACHE', {})
    return stats.get(str(horizon).lower(), {}) if isinstance(stats, dict) else {}

def recompute_stats_from_executions(horizon: str) -> Dict[str, Any]:
    """
    Recalcule basiquement un snapshot de stats à partir d'EXECUTIONS_LOG.
    Utilise les exécutions fermées dans la fenêtre demandée.
    Renvoie et ne sauvegarde PAS (la sauvegarde se fait via save_stats_snapshot si voulu).
    """
    import math
    horizon = str(horizon).lower()
    hours = 7*24 if horizon == '7d' else 30*24 if horizon == '30d' else None
    execs = _load_json_setting('EXECUTIONS_LOG', [])
    now_ms = _now_ms()
    if hours is not None:
        min_ts = now_ms - hours * 3600 * 1000
        execs = [e for e in execs if int(e.get('closed_at', 0)) >= min_ts]
    closed = [e for e in execs if str(e.get('status','')).lower() == 'closed']
    n = len(closed)
    if n == 0:
        return {"trades_count": 0, "win_rate": 0.0, "avg_pnl_pct": 0.0, "profit_factor": 0.0, "max_drawdown_pct": 0.0}
    wins = [e for e in closed if float(e.get('pnl_pct', 0)) > 0]
    losses = [e for e in closed if float(e.get('pnl_pct', 0)) <= 0]
    avg_pnl_pct = sum(float(e.get('pnl_pct', 0)) for e in closed) / n
    gross_profit = sum(max(0.0, float(e.get('pnl_abs', 0))) for e in closed)
    gross_loss = abs(sum(min(0.0, float(e.get('pnl_abs', 0))) for e in closed))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float('inf')
    # max drawdown approximé par cumul des pnl_abs
    cum, peak, mdd = 0.0, 0.0, 0.0
    for e in sorted(closed, key=lambda x: int(x.get('closed_at', 0))):
        cum += float(e.get('pnl_abs', 0))
        peak = max(peak, cum)
        mdd = min(mdd, cum - peak)
    max_drawdown_pct = 0.0  # pct indisponible sans equity de référence; on laisse 0.0
    return {
        "trades_count": n,
        "win_rate": len(wins) / n if n else 0.0,
        "avg_pnl_pct": avg_pnl_pct,
        "profit_factor": profit_factor if math.isfinite(profit_factor) else 0.0,
        "max_drawdown_pct": max_drawdown_pct,
    }

def purge_persistence(retention_days: int = 180, max_execs: int = 10000, max_orders: int = 20000) -> None:
    """
    Purge EXECUTIONS_LOG & ORDERS_LOG selon la rétention choisie.
    """
    executions = _load_json_setting('EXECUTIONS_LOG', [])
    executions = _purge_by_age_and_size(executions, retention_days, max_execs, ts_field="opened_at")
    _save_json_setting('EXECUTIONS_LOG', executions)

    orders = _load_json_setting('ORDERS_LOG', [])
    orders = _purge_by_age_and_size(orders, retention_days, max_orders, ts_field="placed_at")
    _save_json_setting('ORDERS_LOG', orders)
