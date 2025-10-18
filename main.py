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
# ENV / PARAMÈTRES
# =======================
BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY       = os.getenv("BITGET_API_KEY")
API_SECRET    = os.getenv("BITGET_API_SECRET")
PASSPHRASE    = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF                 = os.getenv("TIMEFRAME", "1h")        # H1 unique
RISK_PER_TRADE     = float(os.getenv("RISK_PER_TRADE", "0.01"))  # 1%
MIN_RR             = float(os.getenv("MIN_RR", "3"))     # RR mini 1:3
MAX_LEVERAGE       = int(os.getenv("MAX_LEVERAGE", "2"))
LOOP_DELAY         = int(os.getenv("LOOP_DELAY", "5"))
POSITION_MODE      = os.getenv("POSITION_MODE", "cross")

UNIVERSE_SIZE      = int(os.getenv("UNIVERSE_SIZE", "100"))
PICKS              = int(os.getenv("PICKS", "4"))
MAX_OPEN_TRADES    = int(os.getenv("MAX_OPEN_TRADES", "4"))
MIN_VOLUME_USDT    = float(os.getenv("MIN_VOLUME_USDT", "0"))

# SL “pro”
ATR_WINDOW         = 14
SL_ATR_CUSHION     = 0.25     # 0.25 * ATR au-delà de la mèche

# “Réaction rapide” en tendance
QUICK_BARS         = 3        # doit avancer vite en <= 3 barres
QUICK_PROGRESS     = 0.30     # >= 30% du chemin vers TP

# Pyramide
PYRAMID_MAX        = 1

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
            print("[WARN] set_sandbox_mode not available:", e)
    else:
        print("[INFO] Bitget LIVE mode")
    return ex

# =======================
# DATA / INDICATEURS
# =======================
def fetch_ohlcv_df(ex, symbol, timeframe, limit=500):
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)

    # BB 20x2 (blanche)
    bb_fast = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb_fast_mid"]   = bb_fast.bollinger_mavg()
    df["bb_fast_upper"] = bb_fast.bollinger_hband()
    df["bb_fast_lower"] = bb_fast.bollinger_lband()

    # BB 80x2 (jaune)
    bb_slow = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb_slow_mid"]   = bb_slow.bollinger_mavg()
    df["bb_slow_upper"] = bb_slow.bollinger_hband()
    df["bb_slow_lower"] = bb_slow.bollinger_lband()

    # ATR
    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()
    return df

def touches_band(c, band_price, side="lower", tol_pct=0.0006):
    if band_price is None or np.isnan(band_price):
        return False
    tol = band_price * tol_pct
    if side == "lower":
        return c["low"] <= (band_price + tol)
    return c["high"] >= (band_price - tol)

# =======================
# UNIVERS
# =======================
def build_universe(ex):
    print("[UNIVERSE] building top by 24h volume...")
    try:
        tickers = ex.fetch_tickers()
    except Exception as e:
        print("[WARN] fetch_tickers failed:", e)
        return []
    rows = []
    for s, t in tickers.items():
        if not s or not isinstance(s, str):
            continue
        if "/USDT" in s or ":USDT" in s:
            vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try:
                vol = float(vol or 0)
            except Exception:
                vol = 0.0
            if MIN_VOLUME_USDT <= 0 or vol >= MIN_VOLUME_USDT:
                rows.append((s, vol))
    if not rows:
        print("[UNIVERSE] empty after volume filter")
        return []
    df = pd.DataFrame(rows, columns=["symbol","volume"]).sort_values("volume", ascending=False)
    uni = df.head(UNIVERSE_SIZE)["symbol"].tolist()
    print(f"[UNIVERSE] size={len(uni)}")
    return uni

# =======================
# DÉTECTION (fenêtre 1–2 barres) + traversée prolongée
# =======================
def count_prolonged_extreme(df, side):
    """Compte le nb de barres consécutives où le prix est au-delà des 2 bandes (BB20 & BB80)"""
    cnt = 0
    idx = -2  # on regarde l’historique avant la bougie de signal (qui clôture)
    while abs(idx) <= len(df):
        r = df.iloc[idx]
        if side == "buy":
            outside = (r["low"] <= min(r["bb_fast_lower"], r["bb_slow_lower"]))
        else:
            outside = (r["high"] >= max(r["bb_fast_upper"], r["bb_slow_upper"]))
        if outside:
            cnt += 1
            idx -= 1
        else:
            break
    return cnt

def detect_signal(df):
    """
    Retourne None ou dict {side, regime, entry, stop, tp, rr, atr, entry_delay_bars}
    - fenêtre 1–2 barres pour la réintégration
    - entry à l’ouverture de la bougie suivante
    - si traversée prolongée (>=2 barres) -> entry_delay_bars=1 (on saute la 1ère opportunité)
    """
    if len(df) < 3:
        return None
    last  = df.iloc[-1]   # bougie qui vient de CLÔTURER
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]

    above_slow_mid = last["close"] >= last["bb_slow_mid"]

    # Réintégration 1-bougie (prev = extrême, last = réintégration)
    reinteg_long_1  = (prev["low"]  <= min(prev["bb_fast_lower"], prev["bb_slow_lower"])) and (last["close"] > last["bb_fast_lower"])
    reinteg_short_1 = (prev["high"] >= max(prev["bb_fast_upper"], prev["bb_slow_upper"])) and (last["close"] < last["bb_fast_upper"])

    # Réintégration 2-bougies (prev2 = extrême, prev = “transition”, last = réintégration)
    reinteg_long_2  = (prev2["low"]  <= min(prev2["bb_fast_lower"], prev2["bb_slow_lower"])) and (last["close"] > last["bb_fast_lower"])
    reinteg_short_2 = (prev2["high"] >= max(prev2["bb_fast_upper"], prev2["bb_slow_upper"])) and (last["close"] < last["bb_fast_upper"])

    # TENDANCE (fenêtre 1–2 barres)
    long_trend  = (above_slow_mid and ((prev["low"] <= prev["bb_fast_lower"]) or (prev2["low"] <= prev2["bb_fast_lower"])) and (last["close"] > last["bb_fast_lower"]))
    short_trend = ((not above_slow_mid) and ((prev["high"] >= prev["bb_fast_upper"]) or (prev2["high"] >= prev2["bb_fast_upper"])) and (last["close"] < last["bb_fast_upper"]))

    # CONTRE-TENDANCE = double extrême + réintégration (fenêtre 1–2 barres)
    long_ct  = reinteg_long_1 or reinteg_long_2
    short_ct = reinteg_short_1 or reinteg_short_2

    side = regime = None
    if long_trend:
        side, regime = "buy", "trend"
    elif short_trend:
        side, regime = "sell", "trend"
    elif long_ct:
        side, regime = "buy", "counter"
    elif short_ct:
        side, regime = "sell", "counter"
    else:
        return None

    entry  = float(last["close"])  # référence; l’entrée réelle se fera à l’ouverture de la suivante
    atr    = float(last["atr"])
    tick   = max(entry * 0.0001, 0.01)

    # SL mèche +/- cushion ATR
    if side == "buy":
        raw_sl = float(last["low"]) - 2 * tick
        sl = min(raw_sl, float(prev["low"])) - SL_ATR_CUSHION * atr
    else:
        raw_sl = float(last["high"]) + 2 * tick
        sl = max(raw_sl, float(prev["high"])) + SL_ATR_CUSHION * atr

    # TP théorique (sera ajusté dynamiquement avec offset ticks)
    if regime == "trend":
        tp = float(last["bb_slow_upper"] if side == "buy" else last["bb_slow_lower"])
    else:
        tp = float(last["bb_fast_upper"] if side == "buy" else last["bb_fast_lower"])

    denom = abs(entry - sl)
    rr = abs((tp - entry) / denom) if denom > 0 else 0.0
    if rr < MIN_RR:
        return None

    # traversée prolongée -> retarder l’entrée d’1 barre (on saute la 1ère opportunité)
    ext_cnt = count_prolonged_extreme(df, side)
    entry_delay_bars = 1 if ext_cnt >= 2 else 0

    return {
        "side": side, "regime": regime,
        "entry": entry, "stop": sl, "tp": tp, "rr": rr, "atr": atr,
        "entry_delay_bars": entry_delay_bars
    }

# =======================
# POSITION / ORDRES
# =======================
def count_open_positions(ex):
    try:
        pos = ex.fetch_positions()
        n = 0
        for p in pos:
            size = float(p.get("contracts") or p.get("size") or 0)
            if abs(size) > 0:
                n += 1
        return n
    except Exception as e:
        print("[WARN] fetch_positions:", e)
        return 0

def has_open_position(ex, symbol):
    try:
        pos = ex.fetch_positions([symbol])
        for p in pos:
            size = float(p.get("contracts") or p.get("size") or 0)
            if abs(size) > 0:
                return True
        return False
    except Exception:
        return False

def compute_qty(entry, sl, risk_amount):
    diff = abs(entry - sl)
    if diff <= 0:
        return 0.0
    return float(risk_amount / diff)

def place_market(ex, symbol, side, qty):
    return ex.create_order(symbol, "market", side, qty)

# =======================
# GESTION POST-ENTRÉE (règles spécifiques)
# =======================
def get_tick(price):
    return max(price * 0.0001, 0.01)

def latest_bands(ex, symbol):
    df = fetch_ohlcv_df(ex, symbol, TF, limit=120)
    last = df.iloc[-1]
    return {
        "fast_mid":  float(last["bb_fast_mid"]),
        "fast_up":   float(last["bb_fast_upper"]),
        "fast_lo":   float(last["bb_fast_lower"]),
        "slow_mid":  float(last["bb_slow_mid"]),
        "slow_up":   float(last["bb_slow_upper"]),
        "slow_lo":   float(last["bb_slow_lower"]),
    }

def manage_trend(ex, symbol, side, entry, sl, tp_init, qty):
    """
    Tendance :
    - Si réaction RAPIDE (>=30% du chemin en <=3 barres) -> pas de BE, on laisse courir
    - Si réaction LENTE -> prendre 50% sur MM(BB20) (sans BE), reste sur BB80 opposée
    - TP dynamiques avec offset ticks
    """
    try:
        px = ex.fetch_ticker(symbol)["last"]
    except Exception:
        px = entry
    tick = get_tick(px)

    # placer TP principal (BB80 opposée ± offset)
    b = latest_bands(ex, symbol)
    if side == "buy":
        tp80 = b["slow_up"] - 2 * tick
    else:
        tp80 = b["slow_lo"] + 2 * tick

    # place TP principal (100% pour l’instant)
    try:
        ex.create_order(symbol, "limit", "sell" if side == "buy" else "buy", qty, tp80, {"reduceOnly": True})
    except Exception as e:
        print("[WARN] place TP80:", e)

    # SL initial
    try:
        ex.create_order(symbol, "stop_market", "sell" if side == "buy" else "buy", qty, None,
                        {"stopPrice": sl, "reduceOnly": True})
    except Exception as e:
        print("[WARN] place SL:", e)

    bars = 0
    progressed = 0.0
    dist_total = abs(tp80 - entry) if tp80 and entry else 0.0
    took_half = False

    while True:
        time.sleep(LOOP_DELAY)
        try:
            px = ex.fetch_ticker(symbol)["last"]
        except Exception:
            continue

        # progression
        prog = abs(px - entry)
        if dist_total > 0:
            progressed = max(progressed, prog / dist_total)

        bars += 1

        # Réaction rapide ?
        if bars <= QUICK_BARS and progressed >= QUICK_PROGRESS:
            # Rien à faire (pas de BE, pas de partial), on laisse courir
            pass
        # Réaction lente : après QUICK_BARS si progression insuffisante -> 50% sur MM blanche
        if (not took_half) and (bars > QUICK_BARS) and (progressed < QUICK_PROGRESS):
            b = latest_bands(ex, symbol)
            tp_mid = (b["fast_mid"] - 1 * tick) if side == "buy" else (b["fast_mid"] + 1 * tick)
            try:
                ex.create_order(symbol, "limit", "sell" if side == "buy" else "buy", max(qty * 0.5, 0.000001),
                                tp_mid, {"reduceOnly": True})
                tg_send(f"🟨 {symbol} tendance: 50% au TP sur MM(BB20)")
                took_half = True
            except Exception as e:
                print("[WARN] place 50% mid:", e)

        # Ajuste TP80 dynamiquement (option simple)
        if bars % 6 == 0:  # toutes les ~30s si LOOP_DELAY=5
            b = latest_bands(ex, symbol)
            tp80_new = (b["slow_up"] - 2 * tick) if side == "buy" else (b["slow_lo"] + 2 * tick)
            tp80 = tp80_new  # on pourrait gérer l’update fine (cancel/remettre) selon ton souhait

        # sortie ?
        if not has_open_position(ex, symbol):
            tg_send(f"✅ Position clôturée {symbol} (tendance)")
            break

def manage_counter(ex, symbol, side, entry, sl, qty):
    """
    Contre-tendance :
    - BE = stop sur MM(BB20) (stop “suiveur” sur la moyenne blanche)
    - TP = bande blanche opposée (± offset)
    - TP dynamique avec réactualisation simple
    """
    tick = get_tick(entry)

    # place SL initial (sera déplacé vers la MM blanche)
    try:
        ex.create_order(symbol, "stop_market", "sell" if side == "buy" else "buy", qty, None,
                        {"stopPrice": sl, "reduceOnly": True})
    except Exception as e:
        print("[WARN] place SL:", e)

    bars = 0
    while True:
        time.sleep(LOOP_DELAY)
        b = latest_bands(ex, symbol)

        # BE = stop sur MM(BB20)
        be_lvl = (b["fast_mid"])
        be_stop = (be_lvl if side == "buy" else be_lvl)
        try:
            ex.create_order(symbol, "stop_market", "sell" if side == "buy" else "buy", qty, None,
                            {"stopPrice": be_stop, "reduceOnly": True})
        except Exception as e:
            print("[WARN] move BE to fast_mid:", e)

        # TP = bande blanche opposée ± ticks
        if side == "buy":
            tp_fast = b["fast_up"] - 2 * tick
        else:
            tp_fast = b["fast_lo"] + 2 * tick

        try:
            ex.create_order(symbol, "limit", "sell" if side == "buy" else "buy", qty, tp_fast, {"reduceOnly": True})
        except Exception as e:
            print("[WARN] place TP fast:", e)

        bars += 1
        if not has_open_position(ex, symbol):
            tg_send(f"✅ Position clôturée {symbol} (contre-tendance)")
            break

# =======================
# MAIN LOOP (entrées à l’ouverture)
# =======================
def main():
    ex = create_exchange()
    tg_send(f"🤖 Darwin H1 — BB20/2 & BB80/2 — Risk {int(RISK_PER_TRADE*100)}% — RR min {MIN_RR}")

    if not API_KEY or not API_SECRET or not PASSPHRASE:
        print("[FATAL] Missing API keys")
        tg_send("❌ BITGET_API_* manquantes")
        return

    universe = build_universe(ex)
    last_bar_time = {}       # ts de dernière bougie vue par symbole
    pending = {}             # signaux en attente d’entrée (par symbole)
    pyramided = {}

    while True:
        try:
            # solde USDT
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
            if not universe:
                universe = build_universe(ex)

            # Détection: uniquement à la CLÔTURE d’une nouvelle bougie H1
            for sym in universe:
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, limit=300)
                    last_ts = df.index[-1]
                    if last_bar_time.get(sym) == last_ts:
                        continue  # rien de nouveau
                    # nouvelle bougie clôturée -> détecter signaux
                    last_bar_time[sym] = last_ts
                    sig = detect_signal(df)
                    if sig:
                        # enregistrer pour entrée à l’OUVERTURE de la prochaine bougie (+ éventuel délai)
                        enter_after = 1 + sig["entry_delay_bars"]
                        pending[sym] = {"wait": enter_after, "sig": sig}
                        print(f"[SIGNAL] {sym} {sig['side']} ({sig['regime']}) RR={sig['rr']:.2f} wait={enter_after}")
                except Exception as e:
                    print(f"[ERROR] scan {sym}:", e)

            # Gestion du compteur d’attente (entrées à l’ouverture)
            to_delete = []
            for sym, p in pending.items():
                # chaque fois qu’une nouvelle bougie se ferme, on décrémente "wait"
                if last_bar_time.get(sym) is None:
                    continue
                p["wait"] -= 1
                if p["wait"] > 0:
                    continue

                # prêt à entrer (ouverture de la bougie courante)
                if slots <= 0:
                    continue
                if has_open_position(ex, sym):
                    continue

                s = p["sig"]
                risk_amount = max(1.0, usdt_free * RISK_PER_TRADE)
                qty = round(compute_qty(s["entry"], s["stop"], risk_amount), 6)
                if qty <= 0:
                    to_delete.append(sym)
                    continue

                try:
                    place_market(ex, sym, s["side"], qty)
                    tg_send(f"✅ {sym} {s['side'].upper()} @{s['entry']:.4f} RR={s['rr']:.2f} ({s['regime']})")
                    slots -= 1
                except Exception as e:
                    print("[ERROR] market:", e)
                    tg_send(f"⚠️ Ordre market échoué {sym}: {e}")
                    to_delete.append(sym)
                    continue

                # Gestion post-entrée selon régime
                if s["regime"] == "trend":
                    manage_trend(ex, sym, s["side"], s["entry"], s["stop"], s["tp"], qty)
                else:
                    manage_counter(ex, sym, s["side"], s["entry"], s["stop"], qty)

                to_delete.append(sym)

            # nettoyer les pending traités
            for sym in to_delete:
                pending.pop(sym, None)

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            print("Stopped by user")
            tg_send("⛔ Arrêt manuel du bot")
            break
        except Exception as e:
            print("[FATAL LOOP ERROR]", e)
            tg_send(f"🚨 Fatal: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
