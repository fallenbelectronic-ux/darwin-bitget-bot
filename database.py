# Fichier: database.py
import os
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
            /* NEW: table des signaux utilisée par notifier.tg_show_signals_* */
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT CHECK(side IN ('buy','sell')) NOT NULL,
                timeframe TEXT,
                regime TEXT,
                entry REAL,
                sl REAL,
                tp REAL,
                rr REAL,
                ts INTEGER NOT NULL,
                state TEXT
            );
        """)

        # Dédup avant contrainte d'unicité (garde le dernier enregistrement)
        _dedup_signals(conn)

        # Unicité logique d’un signal : même symbol/side/timeframe/ts
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_signals_uni
            ON signals(symbol, side, timeframe, ts)
        """)

        # Migrations idempotentes
        _ensure_column(conn, "trades", "pnl_percent", "REAL", "0")
        _ensure_column(conn, "trades", "entry_atr", "REAL", "0")
        _ensure_column(conn, "trades", "entry_rsi", "REAL", "0")
        _ensure_column(conn, "trades", "management_strategy", "TEXT", "'NORMAL'")
        _ensure_column(conn, "trades", "breakeven_status", "TEXT", "'PENDING'")

        # Index utiles
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol_status ON trades(symbol, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_close_ts ON trades(close_timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_state_ts ON signals(state, ts DESC)")
        conn.commit()
        
def _store():
    """Singleton mémoire (léger) pour éviter toute dépendance externe."""
    st = getattr(_store, "_st", None)
    if st is None:
        st = {"settings": {}, "trades": [], "signals": [], "next_trade_id": 1}
        _store._st = st
    return st
    
def _dedup_signals(conn: sqlite3.Connection) -> None:
    """Supprime les doublons (symbol, side, timeframe, ts) en gardant le plus récent."""
    try:
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM signals
             WHERE rowid NOT IN (
                   SELECT MAX(rowid)
                     FROM signals
                    GROUP BY symbol, side, timeframe, ts
             )
        """)
        conn.commit()
    except sqlite3.OperationalError:
        # table peut ne pas exister lors du tout premier run
        pass

def get_signals(state: Optional[str] = None, since_minutes: Optional[int] = None, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Retourne une liste de signaux (dict) depuis la table 'signals'.
    - state: filtre optionnel (ex: 'PENDING', 'VALID', 'SKIPPED', ou 'VALID_SKIPPED' pour combiner VALID et SKIPPED)
    - since_minutes: fenêtre glissante en minutes, basée sur 'ts' (accepte ts en sec ou ms)
    - limit: max éléments retournés après filtres (ordre anté-chronologique)
    Champs utilisés par notifier: symbol, timeframe, side, entry, sl, tp, rr, ts.
    """
    with get_db_connection() as conn:
        cur = conn.cursor()
        try:
            rows = cur.execute("SELECT * FROM signals ORDER BY ts DESC LIMIT 1000").fetchall()
        except sqlite3.OperationalError:
            return []

    import time as _t
    now_sec = _t.time()
    min_ts_sec = None
    if since_minutes is not None:
        try:
            min_ts_sec = now_sec - (int(since_minutes) * 60)
        except Exception:
            min_ts_sec = None

    want_valid_skipped = (str(state).upper() == "VALID_SKIPPED") if state is not None else False
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)

        # Filtre state (inclut cas spécial VALID_SKIPPED)
        if state is not None:
            st = str(d.get("state", "")).upper()
            if want_valid_skipped:
                if st not in ("VALID", "SKIPPED"):
                    continue
            else:
                if st != str(state).upper():
                    continue

        # Filtre fenêtre temporelle (support ts en sec ou ms)
        if min_ts_sec is not None:
            try:
                ts_raw = float(d.get("ts", 0))
            except Exception:
                ts_raw = 0.0
            ts_sec = ts_raw / 1000.0 if ts_raw > 10_000_000_000 else ts_raw
            if ts_sec < min_ts_sec:
                continue

        out.append(d)
        if len(out) >= int(limit):
            break

    return out

def upsert_open_position(rec: Dict[str, Any]) -> bool:
    """Compatibilité: pas de table 'open_positions' dédiée — no-op pour rester compatible avec l'appelant."""
    return True

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

def update_signal_state(symbol: str, timeframe: str, ts: int, new_state: str, meta: Optional[Dict[str, Any]] = None) -> bool:
    """
    Alias compat : met à jour l'état d'un signal sans exiger 'side'.
    Met à jour toutes les lignes correspondant à (symbol, timeframe, ts).
    'meta' est accepté pour compat mais ignoré (pas de colonne dédiée).
    Retourne True si au moins une ligne a été modifiée.
    """
    with get_db_connection() as conn:
        cur = conn.cursor()
        res = cur.execute("""
            UPDATE signals
               SET state = ?
             WHERE symbol = ?
               AND timeframe = ?
               AND ts = ?
        """, (str(new_state), str(symbol), str(timeframe), int(ts)))
        conn.commit()
        return res.rowcount > 0


def update_trade_to_breakeven(trade_id: int, remaining_quantity: float, new_sl: float):
    """Mise à breakeven : breakeven_status='ACTIVE', maj quantité et SL."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE trades
               SET breakeven_status = 'ACTIVE',
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
    """
    Lecture robuste d’un paramètre. Retourne `default` si la clé est absente
    ou en cas d’erreur DB (table/connexion/etc.).
    """
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cur.fetchone()
            if not row:
                return default
            # Supporte row_mapping (dict-like) ou tuple selon row_factory
            try:
                val = row["value"]  # type: ignore[index]
            except Exception:
                val = row[0] if isinstance(row, (list, tuple)) and len(row) > 0 else None
            return default if (val is None or val == "") else val
    except Exception:
        return default


def set_setting(key: str, value: Any) -> None:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO settings(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (str(key), str(value)))
        conn.commit()
        

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

def get_stats_24h():
    import time
    return get_closed_trades_since(int(time.time()) - 24 * 60 * 60)

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

def remove_open_position(symbol: str) -> int:
    """
    Alias compat pour trader.sync_positions_with_exchange():
    ferme en DB toutes les positions OPEN du symbole donné
    avec le statut 'CLOSED_BY_EXCHANGE' (PnL=0.0).
    Retourne le nombre de trades affectés.
    """
    # On récupère les IDs sous une seule connexion, puis on ferme proprement.
    with get_db_connection() as conn:
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT id FROM trades WHERE symbol = ? AND status = 'OPEN'",
            (str(symbol),)
        ).fetchall()
        ids = [int(r["id"]) for r in rows]

    # On ferme chaque trade via l’API existante (ouvre sa propre connexion).
    for tid in ids:
        try:
            close_trade(tid, status='CLOSED_BY_EXCHANGE', pnl=0.0)
        except Exception:
            # on continue même si un trade pose problème
            pass

    return len(ids)

def upsert_signal(sig: Dict[str, Any], state: str = "PENDING") -> int:
    """
    Insère ou met à jour un signal par (symbol, side, timeframe, ts).
    Champs pris en compte: symbol, side, timeframe, regime, entry, sl, tp, rr, ts, state.
    Retourne l'id du signal.
    """
    required = ("symbol", "side", "timeframe", "ts")
    for k in required:
        if k not in sig:
            raise ValueError(f"upsert_signal: champ manquant '{k}'")

    payload = {
        "symbol": str(sig["symbol"]),
        "side": str(sig["side"]).lower(),
        "timeframe": str(sig["timeframe"]),
        "regime": str(sig.get("regime") or ""),
        "entry": float(sig.get("entry") or 0.0),
        "sl": float(sig.get("sl") or 0.0),
        "tp": float(sig.get("tp") or 0.0),
        "rr": float(sig.get("rr") or 0.0),
        "ts": int(sig["ts"]),
        "state": str(state),
    }

    with get_db_connection() as conn:
        cur = conn.cursor()
        # Existe déjà ?
        row = cur.execute(
            "SELECT id FROM signals WHERE symbol=? AND side=? AND timeframe=? AND ts=?",
            (payload["symbol"], payload["side"], payload["timeframe"], payload["ts"])
        ).fetchone()

        if row:
            cur.execute("""
                UPDATE signals
                   SET regime = ?, entry = ?, sl = ?, tp = ?, rr = ?, state = ?
                 WHERE id = ?
            """, (payload["regime"], payload["entry"], payload["sl"], payload["tp"],
                  payload["rr"], payload["state"], row["id"]))
            conn.commit()
            return int(row["id"])
        else:
            cur.execute("""
                INSERT INTO signals(symbol, side, timeframe, regime, entry, sl, tp, rr, ts, state)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (payload["symbol"], payload["side"], payload["timeframe"], payload["regime"],
                  payload["entry"], payload["sl"], payload["tp"], payload["rr"],
                  payload["ts"], payload["state"]))
            conn.commit()
            return int(cur.lastrowid)

def set_signal_state(symbol: str, side: str, timeframe: str, ts: int, new_state: str) -> bool:
    """Change l'état d’un signal identifié par (symbol, side, timeframe, ts). Retourne True si modifié."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        res = cur.execute("""
            UPDATE signals SET state=? WHERE symbol=? AND side=? AND timeframe=? AND ts=?
        """, (str(new_state), str(symbol), str(side).lower(), str(timeframe), int(ts)))
        conn.commit()
        return res.rowcount > 0

def insert_signal(**payload):
    """Alias compat: insert → upsert_signal, avec normalisation légère des champs."""
    sig = dict(payload.pop('sig', {})) if isinstance(payload.get('sig'), dict) else dict(payload)
    # Normalisations
    if 'side' in sig: sig['side'] = str(sig['side']).lower()
    if 'ts' in sig:
        try:
            ts_raw = float(sig['ts'])
            # Standardise en millisecondes si fourni en secondes
            sig['ts'] = int(ts_raw if ts_raw > 10_000_000_000 else ts_raw * 1000.0)
        except Exception:
            sig['ts'] = int(time.time() * 1000)
    state = str(sig.pop('state', 'PENDING'))
    return upsert_signal(sig, state=state)

def save_signal(**payload):
    """Alias compat: save → upsert_signal, même normalisation que insert_signal."""
    sig = dict(payload.pop('sig', {})) if isinstance(payload.get('sig'), dict) else dict(payload)
    if 'side' in sig: sig['side'] = str(sig['side']).lower()
    if 'ts' in sig:
        try:
            ts_raw = float(sig['ts'])
            sig['ts'] = int(ts_raw if ts_raw > 10_000_000_000 else ts_raw * 1000.0)
        except Exception:
            sig['ts'] = int(time.time() * 1000)
    state = str(sig.pop('state', 'PENDING'))
    return upsert_signal(sig, state=state)



