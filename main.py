import os
import time
from typing import List, Dict, Any, Optional

import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands, AverageTrueRange

from notifier import (
    tg_send,
    tg_get_updates,
    tg_send_start_banner,
    remember_signal_message,
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
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE", "30"))   # top 30
PICKS_PER_HOUR   = int(os.getenv("PICKS_PER_HOUR", "4"))   # 4 meilleurs signaux
MIN_RR           = float(os.getenv("MIN_RR", "3.0"))       # RR ≥ 3
LOOP_DELAY       = int(os.getenv("LOOP_DELAY", "5"))       # 5s de polling

ATR_WINDOW       = 14
SL_ATR_CUSHION   = 0.25
TICK_RATIO       = 0.0005  # offset TP par défaut (~0.05%)

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

    # BB 20 (blanche)
    bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb20_mid"] = bb20.bollinger_mavg()
    df["bb20_up"]  = bb20.bollinger_hband()
    df["bb20_lo"]  = bb20.bollinger_lband()

    # BB 80 (jaune) – approx H4 sur H1
    bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb80_mid"] = bb80.bollinger_mavg()
    df["bb80_up"]  = bb80.bollinger_hband()
    df["bb80_lo"]  = bb80.bollinger_lband()

    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()
    return df


def filter_working_symbols(ex, symbols, timeframe="1h") -> List[str]:
    ok = []
    for s in symbols:
        try:
            ex.fetch_ohlcv(s, timeframe=timeframe, limit=2)
            ok.append(s)
        except Exception:
            pass
    return ok


def build_universe(ex) -> List[str]:
    """Top par volume USDT. Testnet → fallback filtré."""
    try:
        ex.load_markets()
        candidates = []
        for m in ex.markets.values():
            if (
                (m.get("type") == "swap" or m.get("swap")) and
                m.get("linear") and
                m.get("settle") == "USDT" and
                m.get("quote") == "USDT" and
                m.get("symbol")
            ):
                candidates.append(m["symbol"])
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
            except Exception:
                vol = 0.0
            rows.append((s, vol))
    except Exception:
        pass

    if rows:
        df = pd.DataFrame(rows, columns=["symbol", "volume"]).sort_values("volume", ascending=False)
        uni = df.head(UNIVERSE_SIZE)["symbol"].tolist()
        if BITGET_TESTNET:
            uni = filter_working_symbols(ex, uni[:20], timeframe=TIMEFRAME) or FALLBACK_TESTNET
        return uni

    # Fallback testnet
    uni = filter_working_symbols(ex, FALLBACK_TESTNET, timeframe=TIMEFRAME)
    return uni or FALLBACK_TESTNET

# =========================
# OUTILS / CONDITIONS
# =========================
def tick_from_price(price: float) -> float:
    return max(price * TICK_RATIO, 0.01)


def touched_or_crossed(prev_low: float, prev_high: float, band_price: float, side: str, tol: float = 0.0) -> bool:
    """Contact / traversée sur la bougie précédente."""
    if np.isnan(band_price):
        return False
    if side == "buy":
        return prev_low <= (band_price + tol)
    else:
        return prev_high >= (band_price - tol)


def close_inside_bb20(last_close: float, last_lo20: float, last_up20: float) -> bool:
    return (last_close <= last_up20) and (last_close >= last_lo20)


def inside_both_bands(last_close: float, lo20: float, up20: float, lo80: float, up80: float) -> bool:
    return close_inside_bb20(last_close, lo20, up20) and (last_close >= lo80) and (last_close <= up80)


def prolonged_outside_both(df: pd.DataFrame, min_bars: int = 4) -> Optional[str]:
    """
    Vérifie s'il y a eu ≥ min_bars bougies consécutives hors des 2 bandes.
    Renvoie "up" (au-dessus), "down" (en-dessous) ou None.
    La bougie de réintégration (close dedans) ne compte pas comme la min_bars-ième.
    """
    cnt_up = cnt_down = 0
    i = -2  # on part de l'avant-dernière (la dernière vient de clôturer)
    while abs(i) <= len(df):
        r = df.iloc[i]
        up_both   = (r["high"] >= max(r["bb20_up"], r["bb80_up"]))
        down_both = (r["low"]  <= min(r["bb20_lo"], r["bb80_lo"]))

        # si réintégration (close inside au moins une BB), on s'arrête
        inside_any = (r["close"] <= r["bb20_up"] and r["close"] >= r["bb20_lo"]) or \
                     (r["close"] <= r["bb80_up"] and r["close"] >= r["bb80_lo"])
        if inside_any:
            break

        if up_both:
            cnt_up += 1
            i -= 1
            continue
        if down_both:
            cnt_down += 1
            i -= 1
            continue
        break

    if cnt_up >= min_bars:
        return "up"
    if cnt_down >= min_bars:
        return "down"
    return None

# =========================
# DÉTECTION DE SIGNAL
# =========================
def detect_signal(df: pd.DataFrame, state: Dict[str, Any], sym: str) -> Optional[Dict[str, Any]]:
    """
    Retourne un dict signal {side, regime, entry, sl, tp, rr, notes} ou None.
    Règles:
      - Entrées à l'ouverture suivante → on ne fait que détecter à la clôture
      - 3 conditions obligatoires: contact/traversée (bougie -1), réaction (clôture DANS BB20),
        RR ≥ MIN_RR
      - Contre-tendance: si BB20 est à l’extérieur de BB80 sur le côté considéré,
        réintégration des DEUX BB obligatoire (close dans 20 ET dans 80).
      - Sortie prolongée: sauter le PREMIER trade après ≥4 bougies hors des 2 BB.
    """
    if len(df) < 5:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]

    entry = float(last["close"])
    atr   = float(last["atr"])
    tck   = tick_from_price(entry)

    notes = []

    # Filtre "en tendance": position par rapport à BB80_mid
    above80 = last["close"] >= last["bb80_mid"]

    # --- Contact / traversée (bougie précédente)
    contact_long  = touched_or_crossed(prev_low=prev["low"],  prev_high=prev["high"], band_price=prev["bb20_lo"], side="buy")
    contact_short = touched_or_crossed(prev_low=prev["low"],  prev_high=prev["high"], band_price=prev["bb20_up"], side="sell")

    # --- Réaction : close DANS BB20 (obligatoire)
    inside20 = close_inside_bb20(last_close=last["close"], last_lo20=last["bb20_lo"], last_up20=last["bb20_up"])
    if not inside20:
        return None

    # --- Type de régime
    side = regime = None

    # Tendance (contact BB20 + filtre BB80_mid)
    if above80 and contact_long:
        side, regime = "buy", "trend"
        notes.append("Contact bande basse BB20")
        notes.append("Clôture dans BB20")
        notes.append("Tendance (au-dessus de BB80_mid)")
    elif (not above80) and contact_short:
        side, regime = "sell", "trend"
        notes.append("Contact bande haute BB20")
        notes.append("Clôture dans BB20")
        notes.append("Tendance (au-dessous de BB80_mid)")

    # Contre-tendance (double extrême & réintégration stricte)
    if side is None:
        # Bougie précédente en dehors des deux bandes
        long_ct  = (prev["low"]  <= min(prev["bb20_lo"], prev["bb80_lo"])) and contact_long
        short_ct = (prev["high"] >= max(prev["bb20_up"], prev["bb80_up"])) and contact_short

        # Si BB20 est "à l’extérieur" de BB80 (écart), forcer l’intérieur des DEUX
        must_inside_both_long  = last["bb20_lo"] < last["bb80_lo"]
        must_inside_both_short = last["bb20_up"] > last["bb80_up"]

        if long_ct:
            if must_inside_both_long and not inside_both_bands(last["close"], last["bb20_lo"], last["bb20_up"], last["bb80_lo"], last["bb80_up"]):
                return None
            side, regime = "buy", "counter"
            notes.append("Contre-tendance : double extrême bas & réintégration")
        elif short_ct:
            if must_inside_both_short and not inside_both_bands(last["close"], last["bb20_lo"], last["bb20_up"], last["bb80_lo"], last["bb80_up"]):
                return None
            side, regime = "sell", "counter"
            notes.append("Contre-tendance : double extrême haut & réintégration")

    if side is None:
        return None

    # --- Sortie prolongée : sauter le premier trade
    long_or_short_out = prolonged_outside_both(df, min_bars=4)
    st = state.setdefault(sym, {"skip_once": False})
    if long_or_short_out is not None:
        st["skip_once"] = True
    else:
        # Pas de nouvelle prolongation : si on avait un "skip_once" et qu’on a un signal → on saute 1x
        if st.get("skip_once", False):
            st["skip_once"] = False  # on consomme le skip
            return None

    # --- SL / TP
    if side == "buy":
        sl = float(prev["low"]) - SL_ATR_CUSHION * atr
        tp = float(last["bb80_up"] - tck) if regime == "trend" else float(last["bb20_up"] - tck)
    else:
        sl = float(prev["high"]) + SL_ATR_CUSHION * atr
        tp = float(last["bb80_lo"] + tck) if regime == "trend" else float(last["bb20_lo"] + tck)

    denom = abs(entry - sl)
    rr = abs((tp - entry) / denom) if denom > 0 else 0.0
    if rr < MIN_RR:
        return None

    notes.append(f"RR x{rr:.2f} (≥ {MIN_RR:.1f})")
    notes.append("Tendance" if regime == "trend" else "Contre-tendance")

    return {
        "side": side,
        "regime": regime,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "notes": notes,
    }

# =========================
# TELEGRAM COMMANDS (minimal, robustes)
# =========================
_last_update_id: Optional[int] = None
_paused = False

def poll_telegram_commands() -> None:
    global _last_update_id, _paused
    data = tg_get_updates(_last_update_id + 1 if _last_update_id is not None else None)
    if not data.get("ok"):
        return
    for upd in data.get("result", []):
        _last_update_id = upd.get("update_id", _last_update_id)
        msg = upd.get("message") or upd.get("edited_message")
        if not msg:
            continue
        text = (msg.get("text") or "").strip().lower()
        if not text.startswith("/"):
            continue

        if text.startswith("/start"):
            mode = "TESTNET" if BITGET_TESTNET else "LIVE"
            tg_send_start_banner(f"{mode} • TF {TIMEFRAME} • Top {UNIVERSE_SIZE} • Picks/h {PICKS_PER_HOUR} • RR≥{MIN_RR}")
        elif text.startswith("/config"):
            mode = "TESTNET" if BITGET_TESTNET else "LIVE"
            tg_send(
                f"⚙️ <b>Config</b>\n"
                f"Mode: {mode}\n"
                f"TF: {TIMEFRAME}\n"
                f"Top: {UNIVERSE_SIZE} | Picks/h: {PICKS_PER_HOUR}\n"
                f"RR min: {MIN_RR}\n"
                f"Loop: {LOOP_DELAY}s"
            )
        elif text.startswith("/signaux") or text.startswith("/signals"):
            tg_send(signals_last_hour_text())
        elif text.startswith("/pause"):
            _paused = True
            tg_send("⏸️ Bot en pause (scan arrêté).")
        elif text.startswith("/resume"):
            _paused = False
            tg_send("▶️ Bot relancé.")
        elif text.startswith("/ping"):
            tg_send("📶 Ping ok.")

# =========================
# FORMATAGE
# =========================
def fmt_signal(sym: str, sig: Dict[str, Any]) -> str:
    side = "LONG" if sig["side"] == "buy" else "SHORT"
    notes = "\n".join([f"• {n}" for n in sig["notes"]])
    return (
        f"📈 <b>Signal</b> <code>{sym}</code> {side}\n"
        f"Entrée <code>{sig['entry']:.6f}</code> | SL <code>{sig['sl']:.6f}</code> | TP <code>{sig['tp']:.6f}</code>\n"
        f"{notes}"
    )

# =========================
# MAIN LOOP
# =========================
def main():
    ex = create_exchange()
    mode = "TESTNET" if BITGET_TESTNET else "LIVE"
    tg_send_start_banner(f"{mode} • TF {TIMEFRAME} • Top {UNIVERSE_SIZE} • Picks/h {PICKS_PER_HOUR} • RR≥{MIN_RR}")

    universe = build_universe(ex)
    last_ts_seen: Dict[str, pd.Timestamp] = {}
    state: Dict[str, Any] = {}

    # Gestion “scan une fois à la clôture” + agrégation des meilleurs signaux par heure
    last_hour_bucket: Optional[pd.Timestamp] = None

    while True:
        try:
            poll_telegram_commands()
            if _paused:
                time.sleep(LOOP_DELAY)
                continue

            # Heure courante “bucket” alignée sur les barres H1
            now_bucket = pd.Timestamp.utcnow().floor("H")

            # Si on change d’heure, on remet l’agrégateur (on signale en direct quand on trouve)
            if last_hour_bucket is None:
                last_hour_bucket = now_bucket

            signals_found: List[Dict[str, Any]] = []

            for sym in universe:
                try:
                    df = fetch_ohlcv_df(ex, sym, TIMEFRAME, 300)
                except Exception:
                    continue

                # Ne déclencher QUE à la clôture (pas d’intra-heure)
                last_ts = df.index[-1]
                if last_ts_seen.get(sym) == last_ts:
                    # déjà scanné cette bougie
                    continue
                last_ts_seen[sym] = last_ts

                # On est à la clôture → tenter un signal
                sig = detect_signal(df, state, sym)
                if sig:
                    signals_found.append({"symbol": sym, "sig": sig})

            # Si on a des signaux sur la bougie closée, choisir les 4 meilleurs (RR décroissant) et notifier
            if signals_found:
                signals_found.sort(key=lambda x: x["sig"]["rr"], reverse=True)
                picks = signals_found[:max(1, PICKS_PER_HOUR)]
                for p in picks:
                    sym = p["symbol"]
                    sig = p["sig"]
                    msg = fmt_signal(sym, sig)
                    tg_send(msg)
                    remember_signal_message(sym, sig["side"], sig["rr"], msg)

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            tg_send("⛔ Arrêt manuel.")
            break
        except Exception as e:
            # On ne spam pas : message simple, sans HTML agressif
            tg_send(f"⚠️ Loop error: {str(e)}")
            time.sleep(5)


if __name__ == "__main__":
    main()
