import os
import time
from typing import List, Dict, Any, Optional
import ccxt
import pandas as pd
from ta.volatility import BollingerBands

import database
import trader
import notifier

# ... (toute la section ENV/PARAMS reste identique) ...
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY          = os.getenv("BITGET_API_KEY", "")
API_SECRET       = os.getenv("BITGET_API_SECRET", "")
PASSPHRASSE      = os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASE", "")
TIMEFRAME        = os.getenv("TIMEFRAME", "1h")
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE", "30"))
MIN_RR           = float(os.getenv("MIN_RR", "3.0"))
MM80_DEAD_ZONE_PERCENT = float(os.getenv("MM80_DEAD_ZONE_PERCENT", "0.1"))
LOOP_DELAY       = int(os.getenv("LOOP_DELAY", "5"))
TICK_RATIO       = 0.0005
FALLBACK_TESTNET = ["BTC/USDT:USDT", "ETH/USDT:USDT", "XRP/USDT:USDT"]


def create_exchange():
    ex = ccxt.bitget({
        "apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASE,
        "enableRateLimit": True, "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}
    })
    if BITGET_TESTNET: ex.set_sandbox_mode(True)
    return ex

def fetch_ohlcv_df(ex: ccxt.Exchange, symbol: str, timeframe: str, limit: int = 300) -> Optional[pd.DataFrame]:
    try:
        raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not raw or len(raw) < 100: return None
        df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "vol"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms")
        df.set_index("ts", inplace=True)
        bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
        df["bb20_mid"], df["bb20_up"], df["bb20_lo"] = bb20.bollinger_mavg(), bb20.bollinger_hband(), bb20.bollinger_lband()
        bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
        df["bb80_mid"], df["bb80_up"], df["bb80_lo"] = bb80.bollinger_mavg(), bb80.bollinger_hband(), bb80.bollinger_lband()
        return df
    except Exception:
        return None

def build_universe(ex) -> List[str]:
    try:
        ex.load_markets()
        candidates = [m['symbol'] for m in ex.markets.values() if m.get('swap') and m.get('linear') and m.get('settle') == 'USDT' and m.get('quote') == 'USDT']
        tickers = ex.fetch_tickers(candidates if candidates else None)
        rows = [(s, t.get('quoteVolume', 0.0)) for s, t in tickers.items()]
        if rows:
            df = pd.DataFrame(rows, columns=["symbol", "volume"]).sort_values("volume", ascending=False)
            return df.head(UNIVERSE_SIZE)["symbol"].tolist()
    except Exception: pass
    return FALLBACK_TESTNET

def tick_from_price(price: float) -> float: return max(price * TICK_RATIO, 0.01)
def touched_or_crossed(low, high, band, side): return (low <= band) if side == "buy" else (high >= band)
def close_inside_bb20(close, lo, up): return lo <= close <= up

def detect_signal(df: pd.DataFrame, state: Dict, sym: str) -> Optional[Dict]:
    # (Logique de détection inchangée)
    if len(df) < 5: return None
    last, prev = df.iloc[-1], df.iloc[-2]
    entry = float(last["close"])
    side = regime = None
    if (last["close"] >= last["bb80_mid"]) and touched_or_crossed(prev["low"], prev["high"], prev["bb20_lo"], "buy"): side, regime = "buy", "trend"
    elif (last["close"] < last["bb80_mid"]) and touched_or_crossed(prev["low"], prev["high"], prev["bb20_up"], "sell"): side, regime = "sell", "trend"
    elif (prev["low"] <= min(prev["bb20_lo"], prev["bb80_lo"])): side, regime = "buy", "counter"
    elif (prev["high"] >= max(prev["bb20_up"], prev["bb80_up"])): side, regime = "sell", "counter"
    if not side or not close_inside_bb20(entry, last["bb20_lo"], last["bb20_up"]): return None
    if MM80_DEAD_ZONE_PERCENT > 0:
        dead_zone = last["bb80_mid"] * (MM80_DEAD_ZONE_PERCENT / 100.0)
        if (last["bb80_mid"] - dead_zone) <= entry <= (last["bb80_mid"] + dead_zone): return None
    tck = tick_from_price(entry)
    if side == "buy":
        sl, tp = float(prev["low"]) - (2 * tck), float(last["bb80_up"] if regime == "trend" else last["bb20_up"])
    else:
        sl, tp = float(prev["high"]) + (2 * tck), float(last["bb80_lo"] if regime == "trend" else last["bb20_lo"])
    if abs(entry - sl) == 0: return None
    rr = abs(tp - entry) / abs(entry - sl)
    if rr < MIN_RR: return None
    return {"side": side, "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr, "bb20_mid": float(last["bb20_mid"])}

_paused = False

def main():
    ex = create_exchange()
    database.setup_database()

    platform_mode = "TESTNET" if BITGET_TESTNET else "LIVE (Bitget)"
    trading_mode = "PAPIER (Simulation)" if trader.PAPER_TRADING_MODE else "RÉEL (Argent Live)"
    
    # Appel à la nouvelle bannière
    notifier.tg_send_start_banner(platform_mode, trading_mode, trader.RISK_PER_TRADE_PERCENT)

    universe = build_universe(ex)
    last_ts_seen: Dict[str, pd.Timestamp] = {}
    state: Dict[str, Any] = {}
    print(f"Univers de {len(universe)} symboles chargé.")

    while True:
        try:
            if _paused:
                time.sleep(LOOP_DELAY)
                continue
            trader.manage_open_positions(ex)
            for sym in universe:
                df = fetch_ohlcv_df(ex, sym, TIMEFRAME, 300)
                if df is None or last_ts_seen.get(sym) == df.index[-1]:
                    continue
                last_ts_seen[sym] = df.index[-1]
                sig = detect_signal(df, state, sym)
                if sig:
                    trader.execute_trade(ex, sym, sig)
            time.sleep(LOOP_DELAY)
        except KeyboardInterrupt:
            notifier.tg_send("⛔ Arrêt manuel du bot.")
            break
        except Exception as e:
            notifier.tg_send_error("Erreur critique (boucle principale)", e)
            time.sleep(15)

if __name__ == "__main__":
    main()
