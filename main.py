import os
import time
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands
from dotenv import load_dotenv
from notifier import tg_send

load_dotenv()

# --- Config depuis les variables d'environnement ---
BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY = os.getenv("BITGET_API_KEY")
API_SECRET = os.getenv("BITGET_API_SECRET")
SYMBOLS_ENV = os.getenv("SYMBOLS", "")
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

# --- Initialisation de Bitget ---
def create_exchange():
    params = {
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"}
    }

    exchange = ccxt.bitget(params)
    if BITGET_TESTNET:
        exchange.urls["api"] = exchange.urls["test"]
        print("[INFO] Bitget sandbox mode ON (testnet)")
    else:
        print("[INFO] Bitget mode LIVE")

    return exchange


# --- Téléchargement des données OHLCV ---
def fetch_ohlcv_df(exchange, symbol, timeframe, limit=200):
    raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)
    return df


# --- Calcul des bandes de Bollinger ---
def add_bbands(df, period=20, std=2, prefix='bb'):
    bb = BollingerBands(close=df['close'], window=period, window_dev=std)
    df[f'{prefix}_mid'] = bb.bollinger_mavg()
    df[f'{prefix}_upper'] = bb.bollinger_hband()
    df[f'{prefix}_lower'] = bb.bollinger_lband()
    return df


# --- Filtrage de l’univers ---
def build_universe(exchange, limit=100, min_vol=0):
    try:
        tickers = exchange.fetch_tickers()
        symbols = []
        for s, info in tickers.items():
            if s and isinstance(s, str) and ('/USDT' in s or ':USDT' in s):
                vol = info.get("quoteVolume", 0) or 0
                if vol >= min_vol:
                    symbols.append((s, vol))

        df = pd.DataFrame(symbols, columns=["symbol", "volume"])
        df = df.sort_values("volume", ascending=False).head(limit)
        print(f"[UNIVERSE] built top {len(df)} by 24h volume")
        return df["symbol"].tolist()

    except Exception as e:
        print("[WARN] fetch_tickers failed:", e)
        return []


# --- Calcul de la taille de position ---
def compute_qty(entry_price, stop_price, risk_amount):
    price_diff = abs(entry_price - stop_price)
    if price_diff <= 0:
        return 0
    qty = risk_amount / price_diff
    return float(qty)


# --- Placement des ordres ---
def place_futures_order(exchange, symbol, side, qty, leverage, stop_price, tp_price):
    try:
        print(f"[ORDER] {symbol} {side} qty={qty:.6f} lev={leverage}")
        order = exchange.create_market_order(symbol, side, qty)
        tg_send(f"✅ *Trade exécuté*\n{symbol} {side.upper()} qty={qty:.4f}")
    except Exception as e:
        print("[ERROR] Order failed:", e)
        tg_send(f"⚠️ *Erreur d'ordre* {symbol}: {e}")
        return False
    return True


# --- Boucle principale ---
def main():
    exchange = create_exchange()
    tg_send(f"🤖 Darwin Bot démarré (testnet={BITGET_TESTNET})")

    if not API_KEY or not API_SECRET:
        print("[FATAL] Missing API keys")
        tg_send("❌ Clés API manquantes (BITGET_API_KEY / BITGET_API_SECRET)")
        return

    open_trades = 0

    while True:
        try:
            # --- Construction de l'univers dynamique ---
            symbols = build_universe(exchange, UNIVERSE_SIZE, MIN_VOLUME_USDT)
            if not symbols:
                print("[UNIVERSE] size=0")
                time.sleep(10)
                continue

            # --- Scan de signaux ---
            print(f"[SCAN] scanning {len(symbols)} symbols...")
            signals = []

            for symbol in symbols:
                try:
                    df_low = fetch_ohlcv_df(exchange, symbol, TF)
                    df_high = fetch_ohlcv_df(exchange, symbol, HIGHER_TF)

                    df_low = add_bbands(df_low, prefix='bb_white')
                    df_high = add_bbands(df_high, period=80, prefix='bb_yellow')

                    last_low = df_low.iloc[-1]
                    high_row = df_high.loc[df_high.index.asof(df_low.index[-1])]

                    long_cond = last_low['low'] <= last_low['bb_white_lower'] and high_row['low'] <= high_row['bb_yellow_lower']
                    short_cond = last_low['high'] >= last_low['bb_white_upper'] and high_row['high'] >= high_row['bb_yellow_upper']

                    if long_cond or short_cond:
                        signals.append((symbol, "buy" if long_cond else "sell"))
                except Exception as e:
                    print(f"[SCAN ERROR] {symbol} :", e)
                    continue

            # --- Exécution des meilleurs signaux ---
            if not signals:
                print("[SCAN] no signals")
                time.sleep(LOOP_DELAY)
                continue

            signals = signals[:PICKS]
            print(f"[EXEC] {len(signals)} signals found")

            for symbol, side in signals:
                if open_trades >= MAX_OPEN_TRADES:
                    print("[LIMIT] max open trades reached")
                    break

                entry_price = exchange.fetch_ticker(symbol)["last"]
                stop_price = entry_price * (0.99 if side == "buy" else 1.01)
                tp_price = entry_price * (1.03 if side == "buy" else 0.97)

                risk_amount = 10  # Ajustable
                qty = compute_qty(entry_price, stop_price, risk_amount)
                leverage = MAX_LEVERAGE

                ok = place_futures_order(exchange, symbol, side, qty, leverage, stop_price, tp_price)
                if ok:
                    open_trades += 1

            print(f"[CYCLE DONE] open_trades={open_trades}")
            time.sleep(LOOP_DELAY)

        except Exception as e:
            print("[FATAL LOOP ERROR]", e)
            tg_send(f"🚨 Erreur fatale : {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
