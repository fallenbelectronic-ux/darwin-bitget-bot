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
BYBIT_TESTNET = os.getenv("BYBIT_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT").split(",") if s.strip()]
TF = os.getenv("TIMEFRAME", "1m")
HIGHER_TF = os.getenv("HIGHER_TF", "5m")
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.002"))
MIN_RR = float(os.getenv("MIN_RR", "3"))
MAX_LEVERAGE = int(os.getenv("MAX_LEVERAGE", "10"))
LOOP_DELAY = int(os.getenv("LOOP_DELAY", "5"))
POSITION_MODE = os.getenv("POSITION_MODE", "isolated")


# --- Initialisation de l’exchange Bybit ---
def create_exchange():
    exchange = ccxt.bybit({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "enableRateLimit": True,
        "options": {"defaultType": "future"}  # important : futures USDT
    })
    if BYBIT_TESTNET:
        try:
            exchange.set_sandbox_mode(True)
            print("[INFO] Bybit sandbox mode activé (testnet)")
        except Exception:
            print("[WARN] sandbox mode non supporté (ccxt)")
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


# --- Détection de contact avec les bandes ---
def touches_band(candle, band_price, side='lower', tol_pct=0.0006):
    if band_price is None or np.isnan(band_price):
        return False
    tol = band_price * tol_pct
    if side == 'lower':
        return candle['low'] <= (band_price + tol)
    else:
        return candle['high'] >= (band_price - tol)


# --- Calcul du volume (quantité) ---
def compute_qty(entry_price, stop_price, risk_amount):
    price_diff = abs(entry_price - stop_price)
    if price_diff <= 0:
        return 0
    qty = risk_amount / price_diff
    return float(qty)


# --- Placement des ordres ---
def place_futures_order(exchange, symbol, side, qty, leverage, stop_price, tp_price):
    try:
        # Définir le levier
        try:
            if hasattr(exchange, 'set_leverage'):
                exchange.set_leverage(leverage, symbol)
        except Exception as e:
            print("[WARN] set_leverage échoué :", e)

        # Créer un ordre au marché
        print(f"[ORDER] {symbol} {side} qty={qty:.6f} lev={leverage}")
        if side.lower() == 'buy':
            order = exchange.create_market_buy_order(symbol, qty)
        else:
            order = exchange.create_market_sell_order(symbol, qty)
        print("[OK] Ordre exécuté :", order)
        tg_send(f"✅ *Trade exécuté*\n{symbol} {side.upper()}\nTaille : {qty:.4f}\nPrix ~ {order.get('price', '?')}")
    except Exception as e:
        print("[ERROR] Erreur exécution :", e)
        tg_send(f"⚠️ *Erreur d'ordre* {symbol} {side}\n{e}")
        return False

    # Placer Take Profit / Stop Loss
    try:
        if side.lower() == 'buy':
            exchange.create_order(symbol, 'limit', 'sell', qty, tp_price, {'reduce_only': True})
            try:
                exchange.create_order(symbol, 'stop_market', 'sell', qty, None,
                                      {'stop_px': stop_price, 'reduce_only': True})
            except Exception:
                exchange.create_order(symbol, 'stop_market', 'sell', qty, None,
                                      {'stopPrice': stop_price, 'reduceOnly': True})
        else:
            exchange.create_order(symbol, 'limit', 'buy', qty, tp_price, {'reduce_only': True})
            try:
                exchange.create_order(symbol, 'stop_market', 'buy', qty, None,
                                      {'stop_px': stop_price, 'reduce_only': True})
            except Exception:
                exchange.create_order(symbol, 'stop_market', 'buy', qty, None,
                                      {'stopPrice': stop_price, 'reduceOnly': True})

        print("[OK] TP/SL placés (à vérifier sur Bybit)")
        tg_send(f"🎯 TP/SL placés pour {symbol}\nTP : {tp_price}\nSL : {stop_price}")
    except Exception as e:
        print("[WARN] TP/SL échec partiel :", e)
        tg_send(f"⚠️ Problème TP/SL pour {symbol} : {e}")
    return True


# --- Boucle principale ---
def main():
    exchange = create_exchange()
    tg_send(f"🤖 Darwin Bot démarré (testnet={BYBIT_TESTNET})")

    if not API_KEY or not API_SECRET:
        print("[FATAL] Clés API manquantes")
        tg_send("❌ Clés API manquantes (BYBIT_API_KEY / BYBIT_API_SECRET)")
        return

    while True:
        try:
            try:
                bal = exchange.fetch_balance()
                usdt_free = 0
                if isinstance(bal, dict) and 'USDT' in bal:
                    usdt_free = float(bal['USDT'].get('free', 0))
            except Exception as e:
                print("[WARN] fetch_balance échoué :", e)
                usdt_free = 0

            for symbol in SYMBOLS:
                try:
                    df_low = fetch_ohlcv_df(exchange, symbol, TF, limit=200)
                    df_high = fetch_ohlcv_df(exchange, symbol, HIGHER_TF, limit=200)

                    df_low = add_bbands(df_low, prefix='bb_white')
                    df_high = add_bbands(df_high, prefix='bb_yellow')

                    last_low = df_low.iloc[-1]
                    high_row = df_high.loc[df_high.index.asof(df_low.index[-1])]

                    long_cond = touches_band(last_low, last_low.get('bb_white_lower'), 'lower') and \
                                touches_band(high_row, high_row.get('bb_yellow_lower'), 'lower')
                    short_cond = touches_band(last_low, last_low.get('bb_white_upper'), 'upper') and \
                                 touches_band(high_row, high_row.get('bb_yellow_upper'), 'upper')

                    if not (long_cond or short_cond):
                        continue

                    side = 'buy' if long_cond else 'sell'
                    entry_price = float(last_low['close'])
                    tick = max(entry_price * 0.0001, 0.01)

                    if side == 'buy':
                        stop_price = float(last_low['low']) - 2 * tick
                        tp_price = float(last_low['bb_white_mid']) if not np.isnan(last_low['bb_white_mid']) else entry_price * 1.01
                    else:
                        stop_price = float(last_low['high']) + 2 * tick
                        tp_price = float(last_low['bb_white_mid']) if not np.isnan(last_low['bb_white_mid']) else entry_price * 0.99

                    rr = abs((tp_price - entry_price) / (entry_price - stop_price)) if (entry_price - stop_price) != 0 else 0
                    if rr < MIN_RR:
                        print(f"[SKIP] {symbol} RR trop faible ({rr:.2f})")
                        continue

                    risk_amount = max(1.0, usdt_free * RISK_PER_TRADE)
                    qty = compute_qty(entry_price, stop_price, risk_amount)
                    qty = round(qty, 6)
                    leverage = min(MAX_LEVERAGE, 10)

                    tg_send(f"🔔 *Signal détecté*\n{symbol} {side.upper()} @ {entry_price:.2f}\nSL {stop_price:.2f} | TP {tp_price:.2f}\nRR {rr:.2f} | Qty {qty:.6f}")

                    ok = place_futures_order(exchange, symbol, side, qty, leverage, stop_price, tp_price)
                    if ok:
                        print(f"[EXECUTED] {symbol} {side.upper()} trade envoyé.")
                    time.sleep(0.5)

                except Exception as e:
                    print(f"[ERROR] boucle symbol {symbol} :", e)
                    tg_send(f"⚠️ Erreur {symbol} : {e}")
                    continue

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            print("Arrêt manuel du bot.")
            tg_send("⛔ Darwin Bot arrêté manuellement.")
            break

        except Exception as e:
            print("[FATAL LOOP ERROR]", e)
            tg_send(f"🚨 Erreur fatale : {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
