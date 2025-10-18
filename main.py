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
# ENV
# =======================
BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY       = os.getenv("BITGET_API_KEY")
API_SECRET    = os.getenv("BITGET_API_SECRET")
PASSPHRASE    = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF                 = os.getenv("TIMEFRAME", "1h")
RISK_PER_TRADE     = float(os.getenv("RISK_PER_TRADE", "0.01"))   # 1 %
MIN_RR             = float(os.getenv("MIN_RR", "3"))
MAX_OPEN_TRADES    = int(os.getenv("MAX_OPEN_TRADES", "4"))
LOOP_DELAY         = int(os.getenv("LOOP_DELAY", "5"))
UNIVERSE_SIZE      = int(os.getenv("UNIVERSE_SIZE", "100"))

ATR_WINDOW         = 14
SL_ATR_CUSHION     = 0.25

# vitesse (réaction rapide en tendance)
QUICK_BARS         = 3
QUICK_PROGRESS     = 0.30  # +30% de la distance TP en <= QUICK_BARS bougies

# Testnet de secours
FALLBACK_TESTNET = ["BTC/USDT:USDT", "ETH/USDT:USDT", "XRP/USDT:USDT"]

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
# OUTILS
# =======================
def fetch_ohlcv_df(ex, symbol, timeframe, limit=300):
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)

    # BB blanche: 20 / 2
    bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb20_mid"] = bb20.bollinger_mavg()
    df["bb20_up"]  = bb20.bollinger_hband()
    df["bb20_lo"]  = bb20.bollinger_lband()

    # BB jaune: 80 / 2
    bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb80_mid"] = bb80.bollinger_mavg()
    df["bb80_up"]  = bb80.bollinger_hband()
    df["bb80_lo"]  = bb80.bollinger_lband()

    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()
    return df

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
                      if (m.get("type") == "swap" or m.get("swap"))
                      and m.get("linear") and m.get("settle") == "USDT"
                      and m.get("quote") == "USDT"]
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
        top10 = df.head(10)
        preview = ", ".join([f"{r.symbol}:{int(r.volume)}" for r in top10.itertuples(index=False)])
        print(f"[UNIVERSE] size={len(universe)} (ranked by 24h volume)")
        print(f"[UNIVERSE] top10: {preview}")
        tg_send(f"📊 *Univers LIVE (Top10)*\n{preview}")

        if BITGET_TESTNET:
            universe = filter_working_symbols(ex, universe[:20], timeframe=TF) or FALLBACK_TESTNET
            print(f"[UNIVERSE] testnet filtered: {universe}")
            tg_send("🧪 *Testnet actifs* : " + ", ".join(universe))
        return universe

    # Fallback testnet
    print("[UNIVERSE] empty after volume filter, using fallback list (testnet)")
    universe = filter_working_symbols(ex, FALLBACK_TESTNET, timeframe=TF)
    print(f"[UNIVERSE] size={len(universe)} (fallback)")
    print(f"[UNIVERSE] list: {', '.join(universe)}")
    tg_send("🧪 *Univers TESTNET* : " + ", ".join(universe))
    return universe

# =======================
# DÉTECTION — règles Darwin H1
# =======================
def prolonged_double_exit(df, lookback=6):
    """
    Vrai s’il y a eu >= 3 bougies consécutives *à l’extérieur des 2 bornes en même temps*,
    i.e. high>bb20_up & >bb80_up OU low<bb20_lo & <bb80_lo.
    Sert à sauter le *premier* signal après une longue sortie (consigne PDF).
    """
    streak = 0
    out_side = None
    for i in range(-lookback-3, -1):  # parcourt un peu avant
        row = df.iloc[i]
        up_both = (row["high"] >= row["bb20_up"]) and (row["high"] >= row["bb80_up"])
        lo_both = (row["low"]  <= row["bb20_lo"]) and (row["low"]  <= row["bb80_lo"])
        if up_both:
            if out_side in (None, "up"):
                streak += 1; out_side = "up"
            else:
                streak = 1; out_side = "up"
        elif lo_both:
            if out_side in (None, "down"):
                streak += 1; out_side = "down"
            else:
                streak = 1; out_side = "down"
        else:
            streak = 0; out_side = None
    return streak >= 3

def detect_signal(df, skip_first_after_prolonged=True, state=None, sym=None):
    """
    Retourne un dict {side, regime, entry, sl, tp, rr, notes[]} ou None
    """
    if len(df) < 3:
        return None
    last, prev = df.iloc[-1], df.iloc[-2]

    notes = []
    above80 = last["close"] >= last["bb80_mid"]
    # règles “contact/travers + réintégration” sur BB blanche (1H)
    reinteg_long  = (prev["low"]  <= min(prev["bb20_lo"], prev["bb80_lo"])) and (last["close"] > last["bb20_lo"])
    reinteg_short = (prev["high"] >= max(prev["bb20_up"], prev["bb80_up"])) and (last["close"] < last["bb20_up"])

    long_trend  =  above80 and reinteg_long
    short_trend = (not above80) and reinteg_short

    # gestion “ne pas prendre le premier trade après sortie prolongée”
    if skip_first_after_prolonged and state is not None and sym is not None:
        if state.get(sym, {}).get("cooldown", False) is True:
            notes.append("⏳ 1er signal après *sortie prolongée* — *skippé*")
            state[sym]["cooldown"] = False  # ne skippe qu'une fois
            return None
        # détecte une sortie prolongée en cours → armer le cooldown
        if prolonged_double_exit(df):
            st = state.setdefault(sym, {})
            st["cooldown"] = True
            notes.append("⚠️ *Sortie prolongée* détectée — prochain signal sera ignoré")
            # on ne déclenche pas un trade *sur* la barre de sortie prolongée
            return None

    if long_trend:
        side, regime = "buy", "trend"
        notes.append("Au-dessus MM *BB80* + réintégration *BB20 basse*")
    elif short_trend:
        side, regime = "sell", "trend"
        notes.append("Sous MM *BB80* + réintégration *BB20 haute*")
    elif reinteg_long:
        side, regime = "buy", "counter"; notes.append("Contre-tendance : réintégration *BB20 basse*")
    elif reinteg_short:
        side, regime = "sell", "counter"; notes.append("Contre-tendance : réintégration *BB20 haute*")
    else:
        return None

    entry = float(last["close"])
    atr = float(last["atr"])
    if side == "buy":
        sl = float(prev["low"])  - SL_ATR_CUSHION * atr
        tp = float(last["bb80_up"]) if regime == "trend" else float(last["bb20_up"])
    else:
        sl = float(prev["high"]) + SL_ATR_CUSHION * atr
        tp = float(last["bb80_lo"]) if regime == "trend" else float(last["bb20_lo"])

    rr = abs((tp - entry) / (entry - sl)) if (entry != sl) else 0
    if rr < MIN_RR:
        return None

    return {"side": side, "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr, "notes": notes}

# =======================
# POSITIONS & MONEY MGMT
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
# NOTIFICATIONS
# =======================
def notify_signal(symbol, sig):
    regime_emoji = "📈" if sig["regime"] == "trend" else "🔄"
    side_txt = "LONG" if sig["side"] == "buy" else "SHORT"
    bullet = "\n".join([f"• {n}" for n in sig.get("notes", [])])
    msg = (
        f"{regime_emoji} *Signal* `{symbol}` {side_txt}\n"
        f"Entrée ~ `{sig['entry']:.4f}`  |  SL ~ `{sig['sl']:.4f}`  |  TP ~ `{sig['tp']:.4f}`\n"
        f"RR `x{sig['rr']:.2f}`\n{bullet}"
    )
    tg_send(msg)

def notify_order_ok(symbol, side, qty, be_rule=None, tp_rule=None):
    side_txt = "LONG" if side == "buy" else "SHORT"
    lines = [f"🎯 *Trade exécuté* `{symbol}` {side_txt}\nTaille : `{qty:.6f}`"]
    if be_rule: lines.append(f"• BE : {be_rule}")
    if tp_rule: lines.append(f"• TP : {tp_rule}")
    tg_send("\n".join(lines))

def notify_error(context, err):
    tg_send(f"⚠️ *Erreur* `{context}`\n{err}")

# =======================
# MAIN
# =======================
def main():
    ex = create_exchange()
    tg_send(f"🔔 Bot démarré — H1 — Risk {int(RISK_PER_TRADE*100)}% — RR≥{MIN_RR}")
    universe = build_universe(ex)
    valid = set(universe)

    # état par symbole (cooldown sortie prolongée)
    state = {}

    last_ts_seen = {}
    while True:
        try:
            for sym in list(universe):
                if sym not in valid:
                    continue

                # OHLCV
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, 300)
                except Exception as e:
                    print("[WARN] fetch_ohlcv:", e)
                    continue

                # anti-double lecture même bougie
                last_ts = df.index[-1]
                if last_ts_seen.get(sym) == last_ts:
                    continue
                last_ts_seen[sym] = last_ts

                # signal
                sig = detect_signal(df, skip_first_after_prolonged=True, state=state, sym=sym)
                if not sig:
                    continue

                if count_open_positions(ex) >= MAX_OPEN_TRADES:
                    continue
                if has_open_position(ex, sym):
                    continue

                print(f"[SIGNAL] {sym} {sig['side']} {sig['regime']} RR={sig['rr']:.2f}")
                notify_signal(sym, sig)

                # Taille
                try:
                    bal = ex.fetch_balance()
                    usdt = float(bal.get("USDT", {}).get("free", 0))
                except Exception:
                    usdt = 0.0
                risk_amt = max(1.0, usdt * RISK_PER_TRADE)
                qty = compute_qty(sig["entry"], sig["sl"], risk_amt)
                if qty <= 0:
                    continue

                # Règles MM -> info Telegram
                be_rule = None; tp_rule = None
                if sig["regime"] == "trend":
                    be_rule = "Pas de BE si réaction rapide ; sinon *50%* pris à la *MM BB20* (pas de BE)."
                    tp_rule = "TP dynamique sur *BB80 opposée* (ajusté quelques ticks)."
                else:
                    be_rule = "BE à la *MM BB20* après entrée."
                    tp_rule = "TP sur *borne BB20 opposée* (ajusté quelques ticks)."

                # Ordre au marché (testnet recommandé)
                try:
                    ex.create_order(sym, "market", sig["side"], qty)
                    notify_order_ok(sym, sig["side"], qty, be_rule=be_rule, tp_rule=tp_rule)
                except Exception as e:
                    print("[ERROR] order:", e)
                    notify_error("order", e)

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            tg_send("⛔ Arrêt manuel du bot.")
            break
        except Exception as e:
            print("[FATAL]", e)
            notify_error("loop", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
