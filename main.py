import os
import time
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands, AverageTrueRange
from dotenv import load_dotenv
from notifier import tg_send

load_dotenv()

# =======================
# ENV / PARAMÈTRES GÉNÉRAUX
# =======================
BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY       = os.getenv("BITGET_API_KEY")
API_SECRET    = os.getenv("BITGET_API_SECRET")
PASSPHRASE    = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF                 = os.getenv("TIMEFRAME", "1h")            # 1H unique
RISK_PER_TRADE     = float(os.getenv("RISK_PER_TRADE", "0.01"))  # 1%
MIN_RR             = float(os.getenv("MIN_RR", "3"))         # RR mini 1:3
MAX_OPEN_TRADES    = int(os.getenv("MAX_OPEN_TRADES", "4"))
LOOP_DELAY         = int(os.getenv("LOOP_DELAY", "5"))

UNIVERSE_SIZE      = int(os.getenv("UNIVERSE_SIZE", "100"))

ATR_WINDOW         = 14
SL_ATR_CUSHION     = 0.25

QUICK_BARS         = 3
QUICK_PROGRESS     = 0.30

# =========== TESTNET SAFE ===========
FALLBACK_TESTNET = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "XRP/USDT:USDT",
]
# ====================================

# =======================
# EXCHANGE
# =======================
def create_exchange():
    ex = ccxt.bitget({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}
    })
    if BITGET_TESTNET:
        try:
            ex.set_sandbox_mode(True)
            print("[INFO] Bitget sandbox mode ON (testnet)")
        except Exception as e:
            print("[WARN] set_sandbox_mode:", e)
    else:
        print("[INFO] Bitget LIVE mode")
    return ex

# =======================
# UTILITAIRES
# =======================
def fetch_ohlcv_df(ex, symbol, timeframe, limit=300):
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)

    bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb20_mid"] = bb20.bollinger_mavg()
    df["bb20_up"] = bb20.bollinger_hband()
    df["bb20_lo"] = bb20.bollinger_lband()

    bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb80_mid"] = bb80.bollinger_mavg()
    df["bb80_up"] = bb80.bollinger_hband()
    df["bb80_lo"] = bb80.bollinger_lband()

    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()
    return df

# =======================
# UNIVERS
# =======================
def filter_working_symbols(ex, symbols, timeframe="1h"):
    ok = []
    for s in symbols:
        try:
            ex.fetch_ohlcv(s, timeframe=timeframe, limit=2)
            ok.append(s)
        except Exception:
            pass
    return ok

def build_universe(ex):
    print("[UNIVERSE] building top by 24h volume...")
    try:
        ex.load_markets()
        candidates = [m["symbol"] for m in ex.markets.values()
                      if (m.get("type") == "swap" or m.get("swap")) and m.get("linear")
                      and m.get("settle") == "USDT" and m.get("quote") == "USDT"]
    except Exception:
        candidates = []

    rows = []
    try:
        tickers = ex.fetch_tickers(candidates if candidates else None)
        for s, t in tickers.items():
            if "/USDT" not in s and ":USDT" not in s:
                continue
            vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try:
                vol = float(vol)
            except:
                vol = 0.0
            rows.append((s, vol))
    except Exception:
        pass

    if rows:
        df = pd.DataFrame(rows, columns=["symbol", "volume"]).sort_values("volume", ascending=False)
        universe = df.head(UNIVERSE_SIZE)["symbol"].tolist()
        head10 = df.head(10)
        preview = ", ".join([f"{r.symbol}:{int(r.volume)}" for r in head10.itertuples(index=False)])
        print(f"[UNIVERSE] size={len(universe)} (ranked by 24h volume)")
        print(f"[UNIVERSE] top10: {preview}")
        tg_send(f"📊 Univers LIVE top10: {preview}")

        if BITGET_TESTNET:
            universe = filter_working_symbols(ex, universe[:20], timeframe=TF) or FALLBACK_TESTNET
            print(f"[UNIVERSE] testnet filtered: {universe}")
            tg_send(f"🧪 Testnet marchés OK: {', '.join(universe)}")
        return universe

    print("[UNIVERSE] empty after volume filter, using fallback list (testnet)")
    universe = filter_working_symbols(ex, FALLBACK_TESTNET, timeframe=TF)
    print(f"[UNIVERSE] size={len(universe)} (fallback)")
    print(f"[UNIVERSE] list: {', '.join(universe)}")
    tg_send(f"🧪 Univers TESTNET: {', '.join(universe)}")
    return universe

# =======================
# DÉTECTION
# =======================
def detect_signal(df):
    if len(df) < 3:
        return None
    last, prev, prev2 = df.iloc[-1], df.iloc[-2], df.iloc[-3]

    above80 = last["close"] >= last["bb80_mid"]

    reinteg_long = (prev["low"] <= min(prev["bb20_lo"], prev["bb80_lo"])) and (last["close"] > last["bb20_lo"])
    reinteg_short = (prev["high"] >= max(prev["bb20_up"], prev["bb80_up"])) and (last["close"] < last["bb20_up"])

    long_trend = above80 and reinteg_long
    short_trend = (not above80) and reinteg_short

    if long_trend:
        side, regime = "buy", "trend"
    elif short_trend:
        side, regime = "sell", "trend"
    elif reinteg_long:
        side, regime = "buy", "counter"
    elif reinteg_short:
        side, regime = "sell", "counter"
    else:
        return None

    entry = float(last["close"])
    atr = float(last["atr"])
    tick = max(entry * 0.0001, 0.01)
    if side == "buy":
        sl = float(prev["low"]) - SL_ATR_CUSHION * atr
        tp = float(last["bb80_up"]) if regime == "trend" else float(last["bb20_up"])
    else:
        sl = float(prev["high"]) + SL_ATR_CUSHION * atr
        tp = float(last["bb80_lo"]) if regime == "trend" else float(last["bb20_lo"])

    rr = abs((tp - entry) / (entry - sl)) if (entry != sl) else 0
    if rr < MIN_RR:
        return None

    return {"side": side, "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}

# =======================
# POSITIONS
# =======================
def has_open_position(ex, symbol):
    try:
        pos = ex.fetch_positions([symbol])
        for p in pos:
            if abs(float(p.get("contracts") or 0)) > 0:
                return True
        return False
    except:
        return False

def count_open_positions(ex):
    try:
        pos = ex.fetch_positions()
        return sum(1 for p in pos if abs(float(p.get("contracts") or 0)) > 0)
    except:
        return 0

def compute_qty(entry, sl, risk_amount):
    diff = abs(entry - sl)
    return risk_amount / diff if diff > 0 else 0

# =======================
# MAIN LOOP
# =======================
def main():
    ex = create_exchange()
    tg_send(f"🤖 Darwin H1 — Risk {int(RISK_PER_TRADE*100)}% — RR≥{MIN_RR}")
    universe = build_universe(ex)
    valid_set = set(universe)

    last_ts_seen = {}
    while True:
        try:
            for sym in list(universe):
                if sym not in valid_set:
                    continue
                df = fetch_ohlcv_df(ex, sym, TF, 300)
                last_ts = df.index[-1]
                if last_ts_seen.get(sym) == last_ts:
                    continue
                last_ts_seen[sym] = last_ts
                sig = detect_signal(df)
                if not sig:
                    continue
                if has_open_position(ex, sym):
                    continue

                print(f"[SIGNAL] {sym} {sig['side']} {sig['regime']} RR={sig['rr']:.2f}")
                bal = ex.fetch_balance()
                usdt = float(bal.get("USDT", {}).get("free", 0))
                risk_amt = usdt * RISK_PER_TRADE
                qty = compute_qty(sig["entry"], sig["sl"], risk_amt)
                if qty <= 0:
                    continue
                try:
                    ex.create_order(sym, "market", sig["side"], qty)
                    tg_send(f"✅ {sym} {sig['side'].upper()} RR={sig['rr']:.2f} ({sig['regime']})")
                except Exception as e:
                    print("[ERROR] order:", e)
            time.sleep(LOOP_DELAY)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print("[FATAL]", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
