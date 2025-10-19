# main.py
import os, time, csv, math, traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np
import ccxt
from ta.volatility import BollingerBands, AverageTrueRange
from dotenv import load_dotenv

from notifier import (
    tg_send, tg_get_updates,
    tg_send_start_banner, tg_send_signal_card, tg_send_trade_exec,
    remember_signal_message, signals_last_hour_text
)

load_dotenv()

# ======================= ENV =======================
BITGET_TESTNET   = os.getenv("BITGET_TESTNET","true").lower() in ("1","true","yes")
API_KEY          = os.getenv("BITGET_API_KEY","")
API_SECRET       = os.getenv("BITGET_API_SECRET","")
PASSPHRASE       = os.getenv("BITGET_API_PASSWORD", os.getenv("BITGET_PASSPHRASE",""))

TF               = os.getenv("TIMEFRAME","1h")
RISK_PER_TRADE   = float(os.getenv("RISK_PER_TRADE","0.01"))
MIN_RR           = float(os.getenv("MIN_RR","3.0"))
MAX_OPEN_TRADES  = int(os.getenv("MAX_OPEN_TRADES","4"))
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE","30"))
LOOP_DELAY       = int(os.getenv("LOOP_DELAY","5"))
TZ               = os.getenv("TIMEZONE","Europe/Lisbon")
DRY_RUN          = os.getenv("DRY_RUN","true").lower() in ("1","true","yes")

TRADES_CSV       = os.getenv("TRADES_CSV","/app/trades.csv")

MAX_LEVERAGE     = int(os.getenv("MAX_LEVERAGE","2"))
POSITION_MODE    = os.getenv("POSITION_MODE","cross")

ATR_WINDOW       = 14
SL_ATR_CUSHION   = 0.25

PROLONG_LOOKBACK = 6
PROLONG_MIN      = 4
VALIDATION_WINDOW = 2   # garde la logique pour compatibilité

FALLBACK_TESTNET = ["BTC/USDT:USDT","ETH/USDT:USDT","XRP/USDT:USDT"]

# ======================= EXCHANGE =======================
def create_exchange():
    ex = ccxt.bitget({
        "apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}
    })
    if BITGET_TESTNET:
        try: ex.set_sandbox_mode(True)
        except Exception: pass
    return ex

def try_set_leverage(ex, symbol, lev=2, mode="cross"):
    try:
        ex.set_leverage(lev, symbol, params={"marginMode": mode})
    except Exception:
        pass

# ======================= DATA =======================
def fetch_ohlcv_df(ex, symbol, tf, limit=200):
    raw = ex.fetch_ohlcv(symbol, timeframe=tf, limit=limit)
    if not raw or len(raw) < 85:
        raise ValueError(f"OHLCV vide/insuffisant pour {symbol}")
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)

    bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb20_mid"] = bb20.bollinger_mavg()
    df["bb20_up"]  = bb20.bollinger_hband()
    df["bb20_lo"]  = bb20.bollinger_lband()

    bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb80_mid"] = bb80.bollinger_mavg()
    df["bb80_up"]  = bb80.bollinger_hband()
    df["bb80_lo"]  = bb80.bollinger_lband()

    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()

    return df

def _safe_items(obj):
    """Renvoie un itérable de (k,v) si obj est dict-like, sinon []"""
    return obj.items() if isinstance(obj, dict) else []

def load_universe(ex):
    """Top-N USDT-perp linéaires par volume. Ultra blindé."""
    try:
        ex.load_markets()
        candidates = []
        for m in ex.markets.values():
            if (m.get("type")=="swap" or m.get("swap") is True) and m.get("linear") \
               and m.get("quote")=="USDT" and m.get("settle")=="USDT" and m.get("symbol"):
                candidates.append(m["symbol"])
    except Exception:
        candidates = []

    rows = []
    try:
        tickers = ex.fetch_tickers(candidates if candidates else None)
        for s, tk in _safe_items(tickers):
            # filtrage USDT
            if ("/USDT" not in s) and (":USDT" not in s): 
                continue
            vol = tk.get("quoteVolume") or tk.get("baseVolume") or 0
            try:
                vol = float(vol)
            except Exception:
                vol = 0.0
            rows.append((s, vol))
    except Exception:
        rows = []

    if rows:
        df = pd.DataFrame(rows, columns=["symbol","volume"]).sort_values("volume", ascending=False)
        return df.head(UNIVERSE_SIZE)["symbol"].tolist()

    # Testnet / fallback : ne garder que ceux qui répondent au fetch_ohlcv
    valids=[]
    for s in FALLBACK_TESTNET:
        try:
            ex.fetch_ohlcv(s, timeframe=TF, limit=2)
            valids.append(s)
        except Exception:
            pass
    return valids or FALLBACK_TESTNET[:2]

# ======================= RULES =======================
def candle_inside_bb20(c):
    return (c["close"]<=c["bb20_up"]) and (c["close"]>=c["bb20_lo"])

def touched_upper_both(c):
    return (c["high"]>=c["bb20_up"]) and (c["high"]>=c["bb80_up"])

def touched_lower_both(c):
    return (c["low"]<=c["bb20_lo"]) and (c["low"]<=c["bb80_lo"])

def protruded_both_series(df, lookback=PROLONG_LOOKBACK):
    cnt_up=cnt_dn=0
    # inspecte les N barres avant la bougie signal (prev)
    for i in range(-lookback-2, -2):
        r = df.iloc[i]
        if touched_upper_both(r):
            cnt_up += 1; cnt_dn = 0
        elif touched_lower_both(r):
            cnt_dn += 1; cnt_up = 0
        else:
            cnt_up=cnt_dn=0
    if cnt_up>=PROLONG_MIN: return "up"
    if cnt_dn>=PROLONG_MIN: return "down"
    return None

def deep_and_bb20_outside_bb80(df):
    last = df.iloc[-2]
    return (last["bb20_up"]>last["bb80_up"]) or (last["bb20_lo"]<last["bb80_lo"])

def detect_signal(df, state, sym):
    if len(df)<85: return None
    last = df.iloc[-1]   # en formation
    prev = df.iloc[-2]   # close H1

    above80 = prev["close"] >= prev["bb80_mid"]

    # contacts sur la bougie de signal uniquement
    c20_up = prev["high"]>=prev["bb20_up"]
    c20_lo = prev["low"] <=prev["bb20_lo"]
    c80_up = prev["high"]>=prev["bb80_up"]
    c80_lo = prev["low"] <=prev["bb80_lo"]

    close_inside_20 = candle_inside_bb20(prev)

    # sortie prolongée -> on saute le premier trade
    protruded = protruded_both_series(df)
    if protruded:
        st = state.setdefault(sym,{})
        if st.get("cooldown", False):
            st["cooldown"]=False
            return None
        else:
            st["cooldown"]=True
            return None

    # classification
    side = regime = None
    if (c20_up and c80_up and not above80):
        side, regime = "sell", "counter"
    elif (c20_lo and c80_lo and above80):
        side, regime = "buy", "counter"
    elif above80 and c20_lo:
        side, regime = "buy", "trend"
    elif (not above80) and c20_up:
        side, regime = "sell", "trend"
    else:
        return None

    # contre-tendance : cas “deep” → close dans les 2 bandes
    if regime=="counter" and deep_and_bb20_outside_bb80(df):
        if not (close_inside_20 and (prev["close"]<=prev["bb80_up"]) and (prev["close"]>=prev["bb80_lo"])):
            return None

    # réaction : close doit être DANS BB20 (sinon on considérera non valide)
    if not close_inside_20:
        return None

    entry = float(prev["close"]); atr=float(prev["atr"])
    if side=="buy":
        sl = float(prev["low"]) - SL_ATR_CUSHION*atr
        tp = float(prev["bb80_up"]) if regime=="trend" else float(prev["bb20_up"])
    else:
        sl = float(prev["high"]) + SL_ATR_CUSHION*atr
        tp = float(prev["bb80_lo"]) if regime=="trend" else float(prev["bb20_lo"])

    rr = 0.0 if entry==sl else abs((tp-entry)/(entry-sl))
    if rr < MIN_RR: return None

    notes=[]
    if regime=="counter":
        notes += ["Contact BB20+BB80", "Clôture dans BB20"]
    else:
        notes += ["Contact extrême BB20"]
    notes += [f"RR x{rr:.2f} (≥ {MIN_RR})"]

    return {"side":side,"regime":regime,"entry":entry,"sl":sl,"tp":tp,"rr":rr,"notes":notes}

# ======================= RISK =======================
def ensure_trades_csv():
    if not os.path.exists(TRADES_CSV):
        with open(TRADES_CSV,"w",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow(["ts","symbol","side","regime","entry","exit","pnl_pct","rr","result","mode"])

def compute_qty(entry, sl, risk_amt):
    diff = abs(entry-sl)
    return 0.0 if diff<=0 else risk_amt/diff

# ======================= TG COMMANDS =======================
_last_update_id=None
_paused=False
_version="Darwin-Bitget v1.14"

def poll_commands(ex):
    global _last_update_id, _paused
    try:
        ups = tg_get_updates(_last_update_id+1 if _last_update_id is not None else None)
        if not isinstance(ups, list):
            return
        for upd in ups:
            _last_update_id = upd.get("update_id", _last_update_id)
            msg = upd.get("message") or upd.get("edited_message")
            if not msg: continue
            text = (msg.get("text") or "").strip().lower()
            if not text: continue

            if text.startswith("/start"):
                tg_send_start_banner("PAPER" if DRY_RUN else ("TESTNET" if BITGET_TESTNET else "LIVE"),
                                     TF, int(RISK_PER_TRADE*100), MIN_RR)
            elif text.startswith("/config"):
                tg_send(
                    "<b>Config</b>\n"
                    f"Mode: {'PAPER' if DRY_RUN else ('TESTNET' if BITGET_TESTNET else 'LIVE')}\n"
                    f"TF: <code>{TF}</code> | Risk: <code>{int(RISK_PER_TRADE*100)}%</code> | RR≥<code>{MIN_RR}</code>\n"
                    f"Top: <code>{UNIVERSE_SIZE}</code> | Picks/h: <code>{MAX_OPEN_TRADES}</code>\n"
                    f"CSV: <code>{TRADES_CSV}</code>"
                )
            elif text.startswith("/mode"):
                tg_send(f"Mode actuel: <b>{'PAPER' if DRY_RUN else ('TESTNET' if BITGET_TESTNET else 'LIVE')}</b>")
            elif text.startswith("/stats") or text.startswith("/report"):
                tg_send("🧾 Rapport (compact) — le chat conserve <b>trades</b> et <b>signaux</b> du jour.")
            elif text.startswith("/signals"):
                tg_send(signals_last_hour_text())
            elif text.startswith("/pause"):
                _paused=True; tg_send("⏸️ Bot en pause.")
            elif text.startswith("/resume"):
                _paused=False; tg_send("▶️ Bot relancé.")
            elif text.startswith("/ping"):
                tg_send("🔔 Ping ok.")
            elif text.startswith("/version"):
                tg_send(f"🧩 Version: <code>{_version}</code>")
    except Exception as e:
        tg_send(f"[CMD ERR] <code>{e}</code>")

# ======================= MAIN =======================
def main():
    ex = create_exchange()
    tg_send_start_banner("PAPER" if DRY_RUN else ("TESTNET" if BITGET_TESTNET else "LIVE"),
                         TF, int(RISK_PER_TRADE*100), MIN_RR)

    universe = load_universe(ex)
    state={}
    last_bar_seen={}
    active_this_hour=set()

    while True:
        try:
            poll_commands(ex)
            if _paused:
                time.sleep(LOOP_DELAY); continue

            # reset picks par heure
            utcnow = datetime.utcnow()
            hour_key = utcnow.replace(minute=0, second=0, microsecond=0)
            # garde seulement les symboles dont la dernière close est dans l'heure
            active_this_hour = {s for s in active_this_hour if last_bar_seen.get(s, None) and
                                last_bar_seen[s] >= hour_key}

            candidates=[]

            for sym in universe:
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, limit=120)
                except Exception:
                    continue

                last_ts = df.index[-1]    # open bar
                close_ts= df.index[-2]    # CLOSED bar

                # n’agir qu’à la clôture H1
                if last_bar_seen.get(sym) == close_ts:
                    continue
                last_bar_seen[sym] = close_ts

                sig = detect_signal(df, state, sym)
                if sig:
                    candidates.append((sym, sig))

            # tri par RR, sélection max 4
            if candidates:
                try:
                    candidates.sort(key=lambda x: (x[1] or {}).get("rr", 0.0), reverse=True)
                except Exception:
                    # sécurité ultime si un item mal formé se glisse
                    candidates = [(s,sg) for (s,sg) in candidates if isinstance(sg, dict) and "rr" in sg]

            picks=[]
            for item in candidates:
                if not isinstance(item, (list, tuple)) or len(item)!=2:
                    continue
                sym, sig = item
                if len(picks) >= MAX_OPEN_TRADES: break
                if sym in active_this_hour: continue
                picks.append((sym, sig))
                active_this_hour.add(sym)

            # Exécution (papier) + notifications
            for sym, sig in picks:
                try_set_leverage(ex, sym, MAX_LEVERAGE, POSITION_MODE)

                try:
                    bal = ex.fetch_balance()
                    usdt = 1000.0 if DRY_RUN else float(bal.get("USDT",{}).get("free",0) or 0)
                except Exception:
                    usdt = 1000.0
                qty = compute_qty(sig["entry"], sig["sl"], max(1.0, usdt*RISK_PER_TRADE))
                if qty <= 0: 
                    continue

                bullets = sig.get("notes",[])
                tg_send_signal_card(sym, sig["side"], sig["entry"], sig["sl"], sig["tp"], sig["rr"],
                                    bullets=bullets, regime=sig["regime"], paper=DRY_RUN)
                tg_send_trade_exec(sym, sig["side"], sig["entry"], sig["rr"], paper=DRY_RUN)

            time.sleep(LOOP_DELAY)

        except Exception as e:
            # log concis mais utile
            tg_send(f"[LOOP ERR] <code>{e}</code>")
            # dé-commenter si besoin de trace:
            # tg_send(f"<code>{traceback.format_exc()[-800:]}</code>")
            time.sleep(5)

if __name__ == "__main__":
    main()
