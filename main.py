# === main.py — Darwin-Bitget (strict close-only logic) ========================
import os, time, math, csv, requests
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands, AverageTrueRange
from datetime import datetime
from notifier import tg_send

# ─────────────────────────── ENV ───────────────────────────
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1","true","yes")
API_KEY          = os.getenv("BITGET_API_KEY", "")
API_SECRET       = os.getenv("BITGET_API_SECRET", "")
PASSPHRASE       = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF               = os.getenv("TIMEFRAME","1h")
RISK_PER_TRADE   = float(os.getenv("RISK_PER_TRADE","0.01"))
MIN_RR           = float(os.getenv("MIN_RR","3.0"))
MAX_OPEN_TRADES  = int(os.getenv("MAX_OPEN_TRADES","4"))
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE","100"))
LOOP_DELAY       = int(os.getenv("LOOP_DELAY","5"))
DRY_RUN          = os.getenv("DRY_RUN","true").lower() in ("1","true","yes")

ATR_WINDOW       = 14
SL_ATR_CUSHION   = 0.25
TP_TICKS         = 2
TICK_PCT         = 0.0001

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN","")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID","")

FALLBACK_TESTNET = ["BTC/USDT:USDT","ETH/USDT:USDT","XRP/USDT:USDT","LTC/USDT:USDT","BCH/USDT:USDT"]

# ─────────────────────────── EXCHANGE ───────────────────────────
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

# ─────────────────────────── DATA ───────────────────────────
def fetch_ohlcv_df(ex, symbol, timeframe="1h", limit=300):
    o = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(o, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
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

def tick_of(price: float) -> float:
    return max(price * TICK_PCT, 0.01)

def touches(value, target, side, tol=0.0):
    if pd.isna(value) or pd.isna(target): return False
    if side == "up":
        return value >= (target - tol)
    return value <= (target + tol)

# ─────────────────────────── UNIVERS ───────────────────────────
def filter_working_symbols(ex, symbols):
    ok=[]
    for s in symbols:
        try: ex.fetch_ohlcv(s, timeframe=TF, limit=2); ok.append(s)
        except Exception: pass
    return ok

def build_universe(ex):
    try:
        ex.load_markets()
        candidates = [
            m["symbol"] for m in ex.markets.values()
            if (m.get("type")=="swap" or m.get("swap")) and m.get("linear")
               and m.get("settle")=="USDT" and m.get("quote")=="USDT"
        ]
    except Exception:
        candidates = []
    rows=[]
    try:
        t = ex.fetch_tickers(candidates if candidates else None)
        for s, tk in t.items():
            if "/USDT" not in s and ":USDT" not in s: continue
            vol = tk.get("quoteVolume") or tk.get("baseVolume") or 0
            try: vol=float(vol)
            except: vol=0.0
            rows.append((s, vol))
    except Exception:
        pass
    if rows:
        df = pd.DataFrame(rows, columns=["symbol","vol"]).sort_values("vol", ascending=False)
        uni = df.head(UNIVERSE_SIZE)["symbol"].tolist()
        if BITGET_TESTNET:
            uni = filter_working_symbols(ex, uni[:30]) or FALLBACK_TESTNET
        return uni
    return filter_working_symbols(ex, FALLBACK_TESTNET)

# ─────────────────────────── PATTERN FORT ───────────────────────────
def reaction_strong(prev, last, side):
    body = abs(last["close"] - last["open"])
    rng  = last["high"] - last["low"]
    if rng <= 0: return False
    body_ratio = body / rng
    upper_wick = last["high"] - max(last["close"], last["open"])
    lower_wick = min(last["close"], last["open"]) - last["low"]
    wr_up   = upper_wick / rng
    wr_down = lower_wick / rng
    gap_up   = last["open"] > prev["close"] and last["close"] > last["open"]
    gap_down = last["open"] < prev["close"] and last["close"] < last["open"]
    if side == "buy":
        if wr_down >= 0.30: return True
        if body_ratio >= 0.60 and last["close"] > last["open"]: return True
        if gap_up: return True
    else:
        if wr_up >= 0.30: return True
        if body_ratio >= 0.60 and last["close"] < last["open"]: return True
        if gap_down: return True
    return False

# ─────────────────────────── DÉTECTION SUR BOUGIE CLOSE ───────────────────────────
def detect_signal_strict(df):
    """
    Utilise la dernière bougie **close** (index -2).
    """
    if len(df) < 5: return None
    last  = df.iloc[-2]   # BOUGIE FERMÉE
    prev  = df.iloc[-3]
    prev2 = df.iloc[-4]

    # close DANS/SUR BB20
    in_bb20 = (last["close"] <= last["bb20_up"] + 1e-12) and (last["close"] >= last["bb20_lo"] - 1e-12)
    if not in_bb20: return None

    t = tick_of(float(last["close"]))

    long_trend  = (last["close"] >= last["bb80_mid"])
    short_trend = (last["close"] <= last["bb80_mid"])

    contacted_lo_20 = touches(prev["low"], prev["bb20_lo"], "lo", t) or touches(prev2["low"], prev2["bb20_lo"], "lo", t)
    contacted_up_20 = touches(prev["high"], prev["bb20_up"], "up", t) or touches(prev2["high"], prev2["bb20_up"], "up", t)

    prev_out_up   = touches(prev["high"], max(prev["bb80_up"], prev["bb20_up"]), "up", t)
    prev_out_down = touches(prev["low"],  min(prev["bb80_lo"], prev["bb20_lo"]), "lo", t)
    double_ext_up   = prev_out_up   and (prev["close"] >  prev["bb20_up"])
    double_ext_down = prev_out_down and (prev["close"] <  prev["bb20_lo"])

    side=None; regime=None; notes=[]

    if long_trend and contacted_lo_20:
        side, regime = "buy","trend"; notes += ["Contact bande basse BB20","Clôture dans/sur BB20","Tendance"]
    elif short_trend and contacted_up_20:
        side, regime = "sell","trend"; notes += ["Contact bande haute BB20","Clôture dans/sur BB20","Tendance"]
    elif double_ext_down:
        side, regime = "buy","counter"; notes += ["Double extrême bas (BB80 & BB20)","Réintégration + close dans/sur BB20","Contre-tendance"]
    elif double_ext_up:
        side, regime = "sell","counter"; notes += ["Double extrême haut (BB80 & BB20)","Réintégration + close dans/sur BB20","Contre-tendance"]
    else:
        return None

    if not reaction_strong(prev, last, side): return None

    entry = float(last["close"])
    atr   = float(last["atr"])
    if side=="buy":
        sl = min(float(prev["low"]), float(last["low"])) - SL_ATR_CUSHION*atr
        tp = (float(last["bb80_up"]) if regime=="trend" else float(last["bb20_up"])) - TP_TICKS*t
    else:
        sl = max(float(prev["high"]), float(last["high"])) + SL_ATR_CUSHION*atr
        tp = (float(last["bb80_lo"]) if regime=="trend" else float(last["bb20_lo"])) + TP_TICKS*t

    denom = abs(entry - sl)
    rr = abs((tp - entry)/denom) if denom>0 else 0.0
    if rr < MIN_RR: return None

    return {"side":side,"regime":regime,"entry":entry,"sl":sl,"tp":tp,"rr":rr,"notes":notes}

# ─────────────────────────── ORDRES ───────────────────────────
def has_open_position(ex, symbol):
    try:
        pos = ex.fetch_positions([symbol])
        for p in pos:
            size = float(p.get("contracts") or p.get("size") or 0)
            if abs(size)>0: return True
        return False
    except Exception:
        return False

def count_open_positions(ex):
    try:
        pos = ex.fetch_positions()
        return sum(1 for p in pos if abs(float(p.get("contracts") or p.get("size") or 0))>0)
    except Exception:
        return 0

def compute_qty(entry, sl, risk_amount):
    d = abs(entry - sl)
    return 0 if d<=0 else risk_amount / d

# ─────────────────────────── NOTIFS ───────────────────────────
def notify_signal(sym, sig):
    if not TG_TOKEN or not TG_CHAT_ID: return
    side = "LONG" if sig["side"]=="buy" else "SHORT"
    regime = "Tendance" if sig["regime"]=="trend" else "Contre-tendance"
    bullets = "\n".join([f"• {n}" for n in sig.get("notes",[])])
    tg_send(
        f"📈 *Signal {'[PAPER]' if DRY_RUN else ''}* `{sym}` {side}\n"
        f"Entrée `{sig['entry']:.6f}` | SL `{sig['sl']:.6f}` | TP `{sig['tp']:.6f}`\n"
        f"RR x{sig['rr']:.2f}\n{bullets}"
    )

def notify_exec(sym, side, qty):
    if not TG_TOKEN or not TG_CHAT_ID: return
    tg_send(f"🎯 {'PAPER ' if DRY_RUN else ''}{sym} {side.upper()} qty `{qty:.6f}`")

# ─────────────────────────── TELEGRAM (minimal) ───────────────────────────
_last_update_id = None
def poll_tg(ex):
    global _last_update_id
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
        if _last_update_id is not None: url += f"?offset={_last_update_id+1}"
        data = requests.get(url, timeout=6).json()
        if not data.get("ok"): return
        for upd in data.get("result", []):
            _last_update_id = upd["update_id"]
            msg = upd.get("message") or upd.get("edited_message")
            if not msg or str(msg["chat"]["id"])!=str(TG_CHAT_ID): continue
            text = (msg.get("text") or "").strip().lower()
            if text.startswith("/start"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(f"🤖 Bot — Mode {mode} — TF {TF} — Risk {int(RISK_PER_TRADE*100)}% — RR≥{MIN_RR}")
            elif text.startswith("/config"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(f"*Config*\nMode: {mode}\nTF:{TF}\nRisk:{int(RISK_PER_TRADE*100)}%\nRR≥{MIN_RR}\nMax:{MAX_OPEN_TRADES}")
    except Exception:
        pass

# ─────────────────────────── MAIN LOOP ───────────────────────────
def main():
    ex = create_exchange()
    universe = build_universe(ex)
    if not universe:
        tg_send("⚠️ Univers vide, arrêt."); return

    # suivi du dernier **close** traité par symbole
    last_closed_ts = {}
    # signaux en attente d'exécution à l'OPEN suivant (on tolère encore 1 bougie si la 1ère open n'est pas “dans BB20”)
    pending = {}  # sym -> {"sig":sig, "remaining_checks":2, "last_closed":ts_closed}

    if TG_TOKEN and TG_CHAT_ID:
        mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
        tg_send(f"🚀 Démarrage — {mode} — {len(universe)} marchés")

    while True:
        try:
            poll_tg(ex)
            open_cnt = 0 if DRY_RUN else count_open_positions(ex)
            slots = max(0, MAX_OPEN_TRADES - open_cnt)

            for sym in universe:
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, limit=220)
                except Exception:
                    continue

                # ts_closed = index de la DERNIÈRE bougie **fermée**
                if len(df.index) < 3: 
                    continue
                ts_closed = df.index[-2]

                # Évaluer un NOUVEAU signal uniquement quand une nouvelle bougie s’est fermée
                if last_closed_ts.get(sym) != ts_closed:
                    last_closed_ts[sym] = ts_closed
                    sig = detect_signal_strict(df)
                    if sig:
                        notify_signal(sym, sig)
                        pending[sym] = {"sig":sig, "remaining_checks":2, "last_closed":ts_closed}

                # Tentative d’exécution à l’OPEN suivant : la bougie -2 a changé au cycle précédent,
                # maintenant la bougie -1 est la bougie en formation (open courant).
                if sym in pending and slots>0:
                    p = pending[sym]
                    # on veut vérifier la bougie COURANTE (en formation) est bien “dans/sur BB20”
                    cur = df.iloc[-1]
                    in_bb20_now = (cur["close"] <= cur["bb20_up"] + 1e-12) and (cur["close"] >= cur["bb20_lo"] - 1e-12)
                    if in_bb20_now and (not has_open_position(ex, sym)):
                        try:
                            bal = ex.fetch_balance()
                            usdt = float(bal.get("USDT",{}).get("free",0)) if not DRY_RUN else 1000.0
                        except Exception:
                            usdt = 1000.0 if DRY_RUN else 0.0
                        risk = max(1.0, usdt*RISK_PER_TRADE)
                        qty = compute_qty(p["sig"]["entry"], p["sig"]["sl"], risk)
                        if qty>0:
                            if not DRY_RUN:
                                ex.create_order(sym, "market", p["sig"]["side"], qty)
                            notify_exec(sym, p["sig"]["side"], qty)
                            slots -= 1
                            del pending[sym]
                            continue
                    # sinon on retente encore 1 bougie
                    p["remaining_checks"] -= 1
                    if p["remaining_checks"] <= 0:
                        del pending[sym]

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            tg_send("⛔ Arrêt manuel."); break
        except Exception as e:
            try: tg_send(f"⚠️ Loop error: {e}")
            except: pass
            time.sleep(5)

if __name__ == "__main__":
    main()
