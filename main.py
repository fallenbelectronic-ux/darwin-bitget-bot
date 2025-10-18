import os
import time
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands
from dotenv import load_dotenv
from notifier import tg_send

load_dotenv()

# ====== ENV ======
BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY = os.getenv("BITGET_API_KEY")
API_SECRET = os.getenv("BITGET_API_SECRET")
PASSPHRASE = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF = os.getenv("TIMEFRAME", "1h")
HIGHER_TF = os.getenv("HIGHER_TF", "4h")
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.01"))
MIN_RR = float(os.getenv("MIN_RR", "3"))
MAX_LEVERAGE = int(os.getenv("MAX_LEVERAGE", "2"))
LOOP_DELAY = int(os.getenv("LOOP_DELAY", "5"))
POSITION_MODE = os.getenv("POSITION_MODE", "cross")

UNIVERSE_SIZE = int(os.getenv("UNIVERSE_SIZE", "100"))
PICKS = int(os.getenv("PICKS", "4"))
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "4"))
MIN_VOLUME_USDT = float(os.getenv("MIN_VOLUME_USDT", "0"))

# ====== EXCHANGE ======
def create_exchange():
    exchange = ccxt.bitget({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"}
    })
    if BITGET_TESTNET:
        try:
            exchange.set_sandbox_mode(True)
            print("[INFO] Bitget sandbox mode ON (testnet)")
        except Exception:
            print("[WARN] Sandbox non disponible via cette version de ccxt")
    return exchange

# ====== DATA HELPERS ======
def fetch_ohlcv_df(exchange, symbol, timeframe, limit=300):
    raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)
    return df

def add_bbands(df, period, std, prefix):
    bb = BollingerBands(close=df["close"], window=period, window_dev=std)
    df[f"{prefix}_mid"] = bb.bollinger_mavg()
    df[f"{prefix}_upper"] = bb.bollinger_hband()
    df[f"{prefix}_lower"] = bb.bollinger_lband()
    return df

def touches_band(candle, band_price, side="lower", tol_pct=0.0006):
    if band_price is None or np.isnan(band_price):
        return False
    tol = band_price * tol_pct
    if side == "lower":
        return candle["low"] <= (band_price + tol)
    return candle["high"] >= (band_price - tol)

def compute_qty(entry_price, stop_price, risk_amount):
    diff = abs(entry_price - stop_price)
    if diff <= 0:
        return 0.0
    return float(risk_amount / diff)

# ====== UNIVERSE (Top 100 par volume USDT) ======
def build_universe(exchange):
    print("[UNIVERSE] building top by 24h volume...")
    markets = exchange.load_markets()
    symbols = [
        s for s, m in markets.items()
        if (m.get("type") == "swap" or m.get("swap")) and m.get("linear") and m.get("quote") == "USDT"
    ]

    volumes = []
    try:
        tickers = exchange.fetch_tickers(symbols)
    except Exception as e:
        print("[WARN] fetch_tickers failed:", e)
        tickers = {}

    for s in symbols:
        t = tickers.get(s, {})
        vol = t.get("quoteVolume") or t.get("baseVolume") or 0
        try:
            vol = float(vol or 0)
        except Exception:
            vol = 0.0
        if MIN_VOLUME_USDT <= 0 or vol >= MIN_VOLUME_USDT:
            volumes.append((s, vol))

    volumes.sort(key=lambda x: x[1], reverse=True)
    uni = [s for s, _ in volumes[:UNIVERSE_SIZE]] or symbols[:UNIVERSE_SIZE]
    print(f"[UNIVERSE] size={len(uni)}")
    return uni

# ====== POSITIONS ======
def count_open_positions(exchange):
    try:
        pos = exchange.fetch_positions()
        n = 0
        for p in pos:
            amt = p.get("contracts") or p.get("contractSize") or p.get("size") or 0
            try:
                amt = float(amt)
            except Exception:
                amt = 0
            if abs(amt) > 0:
                n += 1
        return n
    except Exception as e:
        print("[WARN] fetch_positions failed:", e)
        return 0

def has_open_position(exchange, symbol):
    try:
        pos = exchange.fetch_positions([symbol])
        for p in pos:
            amt = p.get("contracts") or p.get("contractSize") or p.get("size") or 0
            try:
                amt = float(amt)
            except Exception:
                amt = 0
            if abs(amt) > 0:
                return True
        return False
    except Exception:
        return False

# ====== SIGNALS ======
def find_signal_for_symbol(exchange, symbol):
    df_low = fetch_ohlcv_df(exchange, symbol, TF, limit=300)
    df_high = fetch_ohlcv_df(exchange, symbol, HIGHER_TF, limit=300)

    # BB(20,2) sur 1h et 4h
    df_low = add_bbands(df_low, period=20, std=2, prefix="bb_white")
    df_high = add_bbands(df_high, period=20, std=2, prefix="bb_yellow")

    last_low = df_low.iloc[-1]
    high_row = df_high.loc[df_high.index.asof(df_low.index[-1])]

    long_cond = touches_band(last_low, last_low.get("bb_white_lower"), "lower") and \
                touches_band(high_row, high_row.get("bb_yellow_lower"), "lower")
    short_cond = touches_band(last_low, last_low.get("bb_white_upper"), "upper") and \
                 touches_band(high_row, high_row.get("bb_yellow_upper"), "upper")

    if not (long_cond or short_cond):
        return None

    side = "buy" if long_cond else "sell"
    entry = float(last_low["close"])
    tick = max(entry * 0.0001, 0.01)

    if side == "buy":
        stop = float(last_low["low"]) - 2 * tick
        tp = float(last_low["bb_white_mid"]) if not np.isnan(last_low["bb_white_mid"]) else entry * 1.01
    else:
        stop = float(last_low["high"]) + 2 * tick
        tp = float(last_low["bb_white_mid"]) if not np.isnan(last_low["bb_white_mid"]) else entry * 0.99

    rr = abs((tp - entry) / (entry - stop)) if (entry - stop) != 0 else 0
    if rr < MIN_RR:
        return None

    # score simple = RR (tu pourras raffiner)
    score = rr
    return {"symbol": symbol, "side": side, "entry": entry, "stop": stop, "tp": tp, "rr": rr, "score": score}

# ====== ORDERS ======
def place_futures_order(exchange, symbol, side, qty, leverage, stop_price, tp_price):
    try:
        try:
            if hasattr(exchange, "set_leverage"):
                exchange.set_leverage(leverage, symbol, params={"marginMode": POSITION_MODE})
        except Exception as e:
            print("[WARN] set_leverage:", e)

        print(f"[ORDER] {symbol} {side} qty={qty:.6f} lev={leverage}")
        if side == "buy":
            order = exchange.create_market_buy_order(symbol, qty)
        else:
            order = exchange.create_market_sell_order(symbol, qty)
        print("[OK] market order:", order)
        tg_send(f"Trade executed\n{symbol} {side.upper()}\nQty {qty:.4f}")
    except Exception as e:
        print("[ERROR] create_market:", e)
        tg_send(f"Order error {symbol} {side}: {e}")
        return False

    try:
        ro = {"reduceOnly": True}
        if side == "buy":
            exchange.create_order(symbol, "limit", "sell", qty, tp_price, ro)
            try:
                exchange.create_order(symbol, "stop_market", "sell", qty, None, {"stopPrice": stop_price, "reduceOnly": True})
            except Exception:
                exchange.create_order(symbol, "stop_market", "sell", qty, None, {"triggerPrice": stop_price, "reduceOnly": True})
        else:
            exchange.create_order(symbol, "limit", "buy", qty, tp_price, ro)
            try:
                exchange.create_order(symbol, "stop_market", "buy", qty, None, {"stopPrice": stop_price, "reduceOnly": True})
            except Exception:
                exchange.create_order(symbol, "stop_market", "buy", qty, None, {"triggerPrice": stop_price, "reduceOnly": True})
        print("[OK] TP/SL placed")
    except Exception as e:
        print("[WARN] TP/SL issue:", e)
        tg_send(f"TP/SL issue {symbol}: {e}")
    return True

# ====== MAIN LOOP ======
def main():
    ex = create_exchange()
    tg_send(f"Bot started (Bitget) TF={TF} HTF={HIGHER_TF} picks={PICKS} max_open={MAX_OPEN_TRADES}")

    if not API_KEY or not API_SECRET or not PASSPHRASE:
        print("[FATAL] Missing API keys")
        tg_send("Missing BITGET_API_KEY/SECRET/PASSWORD")
        return

    universe = build_universe(ex)

    while True:
        try:
            try:
                bal = ex.fetch_balance()
                usdt_free = 0.0
                if isinstance(bal, dict):
                    if "USDT" in bal:
                        usdt_free = float(bal["USDT"].get("free", 0) or bal["USDT"].get("available", 0))
                    elif "free" in bal and isinstance(bal["free"], dict):
                        usdt_free = float(bal["free"].get("USDT", 0))
            except Exception as e:
                print("[WARN] fetch_balance:", e)
                usdt_free = 0.0

            open_cnt = count_open_positions(ex)
            slots = max(0, MAX_OPEN_TRADES - open_cnt)
            if slots == 0:
                print("[INFO] Max open reached, scanning only")

            signals = []
            for sym in universe:
                try:
                    sig = find_signal_for_symbol(ex, sym)
                    if sig is not None:
                        signals.append(sig)
                except Exception as e:
                    print(f"[ERROR] scan {sym}:", e)

            if not signals:
                print("[SCAN] no signals")
                time.sleep(LOOP_DELAY)
                continue

            signals.sort(key=lambda x: x["score"], reverse=True)
            top = signals[:PICKS]

            executed = 0
            for s in top:
                if slots <= 0:
                    break
                if has_open_position(ex, s["symbol"]):
                    print(f"[SKIP] already open on {s['symbol']}")
                    continue

                risk_amount = max(1.0, usdt_free * RISK_PER_TRADE)
                qty = round(compute_qty(s["entry"], s["stop"], risk_amount), 6)
                if qty <= 0:
                    print(f"[SKIP] qty<=0 {s['symbol']}")
                    continue

                lev = min(MAX_LEVERAGE, 2)
                ok = place_futures_order(ex, s["symbol"], s["side"], qty, lev, s["stop"], s["tp"])
                if ok:
                    executed += 1
                    slots -= 1
                    time.sleep(0.5)

            print(f"[CYCLE] executed {executed}, open <= {MAX_OPEN_TRADES}")
            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            print("Stopped by user")
            tg_send("Bot stopped manually")
            break
        except Exception as e:
            print("[FATAL LOOP ERROR]", e)
            tg_send(f"Fatal: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
