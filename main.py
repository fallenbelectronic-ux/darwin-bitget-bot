import os
import time
import math
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands
from dotenv import load_dotenv
from notifier import tg_send

load_dotenv()

# ------------ ENV ------------
# Bitget API
BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY = os.getenv("BITGET_API_KEY")
API_SECRET = os.getenv("BITGET_API_SECRET")
PASSPHRASE = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

# Trading core
TF = os.getenv("TIMEFRAME", "1h")
HIGHER_TF = os.getenv("HIGHER_TF", "4h")       # utilisé pour la BB "jaune"
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.01"))
MIN_RR = float(os.getenv("MIN_RR", "3"))
MAX_LEVERAGE = int(os.getenv("MAX_LEVERAGE", "2"))
LOOP_DELAY = int(os.getenv("LOOP_DELAY", "5"))
POSITION_MODE = os.getenv("POSITION_MODE", "cross")

# Scanner / portefeuille
UNIVERSE_SIZE = int(os.getenv("UNIVERSE_SIZE", "100"))
PICKS = int(os.getenv("PICKS", "4"))
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "4"))
MIN_VOLUME_USDT = float(os.getenv("MIN_VOLUME_USDT", "0"))

# ------------ EXCHANGE ------------
def create_exchange():
    exchange = ccxt.bitget({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap"  # Perp USDT
        }
    })
    if BITGET_TESTNET:
        try:
            exchange.set_sandbox_mode(True)
            print("[INFO] Bitget sandbox mode ON (testnet)")
        except Exception:
            print("[WARN] Sandbox non exposé par cette version CCXT")
    return exchange

# ------------ DATA HELPERS ------------
def fetch_ohlcv_df(exchange, symbol, timeframe, limit=300):
    raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)
    return df

def add_bbands(df, period, std, prefix):
    bb = BollingerBands(close=df["close"], window=period, window_dev=std)
    df[f"{prefix}_mid"]   = bb.bollinger_mavg()
    df[f"{prefix}_upper"] = bb.bollinger_hband()
    df[f"{prefix}_lower"] = bb.bollinger_lband()
    return df

def touches_band(candle, band_price, side='lower', tol_pct=0.0006):
    if band_price is None or np.isnan(band_price):
        return False
    tol = band_price * tol_pct
    if side == 'lower':
        return candle["low"] <= (band_price + tol)
    else:
        return candle["high"] >= (band_price - tol)

def compute_qty(entry_price, stop_price, risk_amount):
    diff = abs(entry_price - stop_price)
    if diff <= 0:
        return 0.0
    return float(risk_amount / diff)

# ------------ UNIVERSE (Top 100 volume USDT) ------------
def build_universe(exchange):
    print("[UNIVERSE] building top by 24h volume...")
    markets = exchange.load_markets()
    # garder seulement les swaps USDT linéaires
    symbols = [s for s, m in markets.items()
               if (m.get("type") == "swap" or m.get("swap")) and m.get("linear") and m.get("quote") == "USDT"]

    volumes = []
    try:
        tickers = exchange.fetch_tickers(symbols)
    except Exception as e:
        print("[WARN] fetch_tickers failed:", e)
        tickers = {}

    for s in symbols:
        t = tickers.get(s, {})
        # essayer différents champs selon l'implémentation ccxt
        vol = t.get("quoteVolume") or t.get("baseVolume") or 0
        # parfois ccxt renvoie None
        try:
            vol = float(vol or 0)
        except Exception:
            vol = 0.0
        volumes.append((s, vol))

    # filtre volume mini si voulu
    if MIN_VOLUME_USDT > 0:
        volumes = [x for x in volumes if x[1] >= MIN_VOLUME_USDT]

    # tri desc par volume et tronque
    volumes.sort(key=lambda x: x[1], reverse=True)
    uni = [s for s, _ in volumes[:UNIVERSE_SIZE]]

    # fallback si tout vide
    if not uni:
        uni = symbols[:UNIVERSE_SIZE]
    print(f"[UNIVERSE] size={len(uni)}")
    return uni

# ------------ POSITIONS ------------
def count_open_positions(exchange):
    try:
        pos = exchange.fetch_positions()
        n = 0
        for p in pos:
            # plusieurs implémentations -> on check la taille
            amt = p.get("contracts") or p.get("contractSize") or p.get("size") or 0
            try:
                amt = float(amt)
            except Exception:
                amt = 0
            if amt and abs(amt) > 0:
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
            if amt and abs(amt) > 0:
                return True
        return False
    except Exception:
        return False

# ------------ SIGNALS & RANKING ------------
def score_signal(side, entry, stop, tp, last, bb_white_mid, band_distance_scale=0.001):
    # RR
    rr = abs((tp - entry) / (entry - stop)) if (entry - stop) != 0 else 0
    if rr <= 0:
        return 0
    # Intensité de contact: plus proche de la bande, mieux c'est
    price = float(last["close"])
    if side == "buy":
        band = float(last.get("bb_white_lower", np.nan))
        dist = abs(price - band) if not np.isnan(band) else price * band_distance_scale
    else:
        band = float(last.get("bb_white_upper", np.nan))
        dist = abs(price - band) if not np.isnan(band) else price * band_distance_scale
    strength = max(0.0, 1.0 - dist / (price * band_distance_scale))
    return rr * (1.0 + max(0.0, strength))

def find_signal_for_symbol(exchange, symbol):
    # Data 1h (TF) + 4h (HIGHER_TF)
    df_low = fetch_ohlcv_df(exchange, symbol, TF, limit=300)
    df_high = fetch_ohlcv_df(exchange, symbol, HIGHER_TF, limit=300)

    # BB(20,2) pour les deux séries
    df_low = add_bbands(df_low, period=20, std=2, prefix="bb_white")
    df_high = add_bbands(df_high, period=20, std=2, prefix="bb_yellow")

    last_low = df_low.iloc[-1]
    # alignement de la bougie 4h la plus récente <= 1h actuelle
    high_row = df_high.loc[df_high.index.asof(df_low.index[-1])]

    long_cond = touches_band(last_low, last_low.get("bb_white_lower"), "lower") and \
                touches_band(high_row, high_row.get("bb_yellow_lower"), "lower")
    short_cond = touches_band(last_low, last_low.get("bb_white_upper"), "upper") and \
                 touches_band(high_row, high_row.get("bb_yellow_upper"), "upper")

    if not (long_cond or short_cond):
        return None

    side = "buy" if long_cond else "sell"
    entry = float(last_low["close"])
    tick  = max(entry * 0.0001, 0.01)
    if side == "buy":
        stop = float(last_low["low"]) - 2 * tick
        tp   = float(last_low["bb_white_mid"]) if not np.isnan(last_low["bb_white_mid"]) else entry * 1.01
    else:
        stop = float(last_low["high"]) + 2 * tick
        tp   = float(last_low["bb_white_mid"]) if not np.isnan(last_low["bb_white_mid"]) else entry * 0.99

    rr = abs((tp - entry) / (entry - stop)) if (entry - stop) != 0 else 0
    if rr < MIN_RR:
        return None

    score = score_signal(side, entry, stop, tp, last_low, last_low.get("bb_white_mid"))
    return {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "stop": stop,
        "tp": tp,
        "rr": rr,
        "score": score
    }

# ------------ ORDERS ------------
def place_futures_order(exchange, symbol, side, qty, leverage, stop_price, tp_price):
    try:
        # Levier
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
        tg_send(f"✅ *Trade exécuté*\n{symbol} {side.upper()}\nQty {qty:.4f}\nPrix ~ {order.get('price','?')}")
    except Exception as e:
        print("[ERROR] create_market:", e)
        tg_send(f"⚠️ Erreur ordre {symbol} {side}: {e}")
        return False

    # TP/SL reduce-only (paramètres varient parfois selon CCXT/Bitget)
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

        print("[OK] TP/SL placés")
        tg_send(f"🎯 TP/SL placés {symbol}\nTP {tp_price}\nSL {stop_price}")
    except Exception as e:
        print("[WARN] TP/SL issue:", e)
        tg_send(f"⚠️ TP/SL issue {symbol}: {e}")
    return True

# ------------ MAIN LOOP ------------
def main():
    ex = create_exchange()
    tg_send(f"🤖 Darwin Bot (Bitget) — TF={TF} / HTF={HIGHER_TF} — picks={PICKS} — max_open={MAX_OPEN_TRADES}")

    # Sanity check API
    if not API_KEY or not API_SECRET or not PASSPHRASE:
        print("[FATAL] Clés API manquantes")
        tg_send("❌ Manque BITGET_API_KEY/SECRET/PASSWORD")
        return

    universe = build_universe(ex)

    while True:
        try:
            # Solde USDT dispo
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

            # Respect plafond positions
            open_cnt = count_open_positions(ex)
            if open_cnt >= MAX_OPEN_TRADES:
                print(f"[INFO] Max open trades reached ({open_cnt}/{MAX_OPEN_TRADES}) — scan only.")
            slots = max(0, MAX_OPEN_TRADES - open_cnt)

            # Scan + scoring
            signals = []
            for sym in universe:
                try:
                    sig = find_signal_for_symbol(ex, sym)
                    if sig is not None:
                        signals.append(sig)
                except Exception as e:
                    print(f"[ERROR] scan {sym}:", e)
                    continue

            if not signals:
                print("[SCAN] no signals")
                time.sleep(LOOP_DELAY)
                continue

            # Rank by score desc, take top PICKS
            signals.sort(key=lambda x: x["score"], reverse=True)
            top = signals[:PICKS]

            # Envoi résumé Telegram
            try:
                lines = [f"#{i+1} {s['symbol']} {s['side'].upper()} RR={s['rr']:.2f}" for i, s in enumerate(top)]
                tg_send("📊 *Top picks*\n" + "\n".join(lines))
            except Exception:
                pass

            # Exécuter jusqu'à 'slots' trades max et éviter symbol déjà ouvert
            executed = 0
            for s in top:
                if slots <= 0:
                    break
                if has_open_position(ex, s["symbol"]):
                    print(f"[SKIP] already open on {s['symbol']}")
                    continue

                # Position sizing (risque % du cash)
                risk_amount = max(1.0, usdt_free * RISK_PER_TRADE)
                qty = round(compute_qty(s["entry"], s["stop"], risk_amount), 6)
                if qty <= 0:
                    print(f"[SKIP] qty<=0 {s['symbol']}")
                    continue

                lev = min(MAX_LEVERAGE, 2)  # plafonné à 2 par ta conf
                ok = place_futures_order(ex, s["symbol"], s["side"], qty, lev, s["stop"], s["tp"])
                if ok:
                    executed += 1
                    slots -= 1
                    time.sleep(0.5)

            print(f"[CYCLE] executed {executed}, open now ~ {count_open_positions(ex)}")
            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            print("Arrêt manuel.")
            tg_send("⛔ Bot arrêté manuellement.")
            break
        except Exception as e:
            print("[FATAL LOOP ERROR]", e)
            tg_send(f"🚨 Fatal: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
