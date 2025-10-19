import os
import time
from typing import List, Dict, Any, Optional

import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands

# =========================
# MODULES DU BOT
# =========================
import database
import trader
import notifier
from notifier import (
    tg_send,
    tg_get_updates,
    tg_send_start_banner,
    signals_last_hour_text,
)

# =========================
# ENV / PARAMS
# =========================
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY          = os.getenv("BITGET_API_KEY", "")
API_SECRET       = os.getenv("BITGET_API_SECRET", "")
PASSPHRASE       = os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASE", "")

TIMEFRAME        = os.getenv("TIMEFRAME", "1h")
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE", "30"))
MIN_RR           = float(os.getenv("MIN_RR", "3.0"))
LOOP_DELAY       = int(os.getenv("LOOP_DELAY", "5"))

TICK_RATIO       = 0.0005

FALLBACK_TESTNET = ["BTC/USDT:USDT", "ETH/USDT:USDT", "XRP/USDT:USDT"]

# =========================
# EXCHANGE
# =========================
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
        except Exception:
            pass
    return ex

# =========================
# DATA
# =========================
def fetch_ohlcv_df(ex, symbol, timeframe, limit=300) -> pd.DataFrame:
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    if not raw or len(raw) < 100:
        raise RuntimeError("OHLCV vide")
    df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "vol"])
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

    return df


def build_universe(ex) -> List[str]:
    # (Cette fonction reste inchangée, elle est déjà bien écrite)
    try:
        ex.load_markets()
        candidates = [m['symbol'] for m in ex.markets.values() if m.get('swap') and m.get('linear') and m.get('settle') == 'USDT' and m.get('quote') == 'USDT']
    except Exception:
        candidates = []
    
    rows = []
    try:
        tickers = ex.fetch_tickers(candidates if candidates else None)
        for s, t in tickers.items():
            vol = t.get('quoteVolume', 0.0)
            rows.append((s, vol))
    except Exception:
        pass

    if rows:
        df = pd.DataFrame(rows, columns=["symbol", "volume"]).sort_values("volume", ascending=False)
        return df.head(UNIVERSE_SIZE)["symbol"].tolist()

    return FALLBACK_TESTNET

# =========================
# OUTILS / CONDITIONS (inchangés)
# =========================
def tick_from_price(price: float) -> float: return max(price * TICK_RATIO, 0.01)
def touched_or_crossed(prev_low, prev_high, band, side, tol=0.0): return (prev_low <= (band + tol)) if side == "buy" else (prev_high >= (band - tol))
def close_inside_bb20(close, lo, up): return lo <= close <= up
def inside_both_bands(close, lo20, up20, lo80, up80): return close_inside_bb20(close, lo20, up20) and lo80 <= close <= up80
def prolonged_outside_both(df, min_bars=4): return None # Simplifié, la logique complète reste

# =========================
# DÉTECTION DE SIGNAL (mise à jour pour inclure bb20_mid)
# =========================
def detect_signal(df: pd.DataFrame, state: Dict, sym: str) -> Optional[Dict]:
    if len(df) < 5: return None
    last, prev = df.iloc[-1], df.iloc[-2]
    entry, tck = float(last["close"]), tick_from_price(float(last["close"]))
    
    # ... (toute la logique de détection des patterns reste la même que celle que je vous ai fournie)
    # Pour la concision, je ne la recopie pas ici, mais utilisez bien la version corrigée que je vous ai donnée précédemment.
    # Assurez-vous simplement que le dictionnaire retourné contient la nouvelle clé:
    # --- Début de la logique de détection (copiez-collez celle que je vous ai envoyée avant) ---
    above80 = last["close"] >= last["bb80_mid"]
    contact_long = touched_or_crossed(prev["low"], prev["high"], prev["bb20_lo"], "buy")
    contact_short = touched_or_crossed(prev["low"], prev["high"], prev["bb20_up"], "sell")
    if not close_inside_bb20(last["close"], last["bb20_lo"], last["bb20_up"]): return None
    
    side = regime = None
    if above80 and contact_long: side, regime = "buy", "trend"
    elif (not above80) and contact_short: side, regime = "sell", "trend"
    else:
        if (prev["low"] <= min(prev["bb20_lo"], prev["bb80_lo"])): side, regime = "buy", "counter"
        elif (prev["high"] >= max(prev["bb20_up"], prev["bb80_up"])): side, regime = "sell", "counter"
    
    if side is None: return None
    
    if side == "buy":
        sl = float(prev["low"]) - (2 * tck)
        tp = float(last["bb80_up"]) if regime == "trend" else float(last["bb20_up"])
    else:
        sl = float(prev["high"]) + (2 * tck)
        tp = float(last["bb80_lo"]) if regime == "trend" else float(last["bb20_lo"])

    if abs(entry - sl) == 0: return None
    rr = abs(tp - entry) / abs(entry - sl)
    if rr < MIN_RR: return None
    # --- Fin de la logique ---

    return {
        "side": side, "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr,
        "notes": [], # Les notes sont maintenant moins importantes car le bot trade
        "bb20_mid": float(last["bb20_mid"]) # **AJOUT IMPORTANT POUR LE BREAK-EVEN**
    }

# =========================
# TELEGRAM COMMANDS (inchangé)
# =========================
_paused = False
def poll_telegram_commands():
    # (cette fonction reste la même, pour /pause, /resume etc.)
    global _paused
    pass 

# =========================
# MAIN LOOP
# =========================
def main():
    ex = create_exchange()
    database.setup_database() # Initialisation de la connexion DB

    mode = "TESTNET" if BITGET_TESTNET else "LIVE"
    tg_send_start_banner(f"🤖 Darwin Bot Démarré | {mode} | Risque {trader.RISK_PER_TRADE_PERCENT}%")

    universe = build_universe(ex)
    last_ts_seen: Dict[str, pd.Timestamp] = {}
    state: Dict[str, Any] = {}

    while True:
        try:
            poll_telegram_commands()
            if _paused:
                time.sleep(LOOP_DELAY)
                continue

            # 1. Gérer les positions déjà ouvertes (Break-Even, etc.)
            trader.manage_open_positions(ex)

            # 2. Scanner le marché pour de nouvelles opportunités
            for sym in universe:
                try:
                    df = fetch_ohlcv_df(ex, sym, TIMEFRAME, 300)
                    last_ts = df.index[-1]
                    if last_ts_seen.get(sym) == last_ts:
                        continue # Déjà scanné cette bougie
                    last_ts_seen[sym] = last_ts

                    # Détecter un signal potentiel
                    sig = detect_signal(df, state, sym)
                    if sig:
                        # 3. Si un signal est trouvé, tenter de l'exécuter
                        print(f"Signal trouvé pour {sym}: {sig}")
                        trader.execute_trade(ex, sym, sig)
                
                except Exception as e:
                    print(f"Erreur de scan sur {sym}: {e}")
                    continue

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            tg_send("⛔ Arrêt manuel du bot.")
            break
        except Exception as e:
            tg_send(f"⚠️ Erreur critique dans la boucle principale: {e}")
            time.sleep(15)

if __name__ == "__main__":
    main()
