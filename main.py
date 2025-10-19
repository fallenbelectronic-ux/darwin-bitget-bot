# main.py
import os, time, csv, math
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands, AverageTrueRange
from datetime import datetime, timedelta
from dotenv import load_dotenv

from notifier import (
    tg_send, tg_get_updates, remember_signal_message, signals_last_hour_text,
    tg_send_start_banner, tg_send_signal_card, tg_send_trade_exec
)

load_dotenv()

# ========= ENV =========
BITGET_TESTNET  = os.getenv("BITGET_TESTNET", "true").lower() in ("1","true","yes")
API_KEY         = os.getenv("BITGET_API_KEY", "")
API_SECRET      = os.getenv("BITGET_API_SECRET", "")
PASSPHRASE      = os.getenv("BITGET_API_PASSWORD", os.getenv("BITGET_PASSPHRASE",""))

TF              = os.getenv("TIMEFRAME", "1h")
RISK_PER_TRADE  = float(os.getenv("RISK_PER_TRADE", "0.01"))  # 1%
MIN_RR          = float(os.getenv("MIN_RR", "3.0"))
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "4"))
UNIVERSE_SIZE   = int(os.getenv("UNIVERSE_SIZE", "30"))       # top 30 volume
PICKS_PER_HOUR  = int(os.getenv("PICKS_PER_HOUR", "4"))       # max 4 signaux/heure
LOOP_DELAY      = int(os.getenv("LOOP_DELAY", "5"))
DRY_RUN         = os.getenv("DRY_RUN","true").lower() in ("1","true","yes")

TRADES_CSV      = os.getenv("TRADES_CSV", "/app/trades.csv")

ATR_WINDOW      = 14
SL_ATR_CUSHION  = 0.25

# ========= EXCHANGE =========
def create_exchange():
    ex = ccxt.bitget({
        "apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {"defaultType":"swap","testnet":BITGET_TESTNET}
    })
    if BITGET_TESTNET:
        try: ex.set_sandbox_mode(True)
        except: pass
    return ex

def try_set_leverage(ex, symbol, lev=2, mode="cross"):
    """Applique levier x2 cross quand c'est supporté. Sans spam de logs."""
    try:
        ex.set_leverage(lev, symbol, params={"marginMode": mode})
    except Exception:
        pass  # silencieux

# ========= DATA / INDICATEURS =========
def fetch_ohlcv_df(ex, symbol, timeframe="1h", limit=300):
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms"); df.set_index("ts", inplace=True)

    bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb20_mid"], df["bb20_up"], df["bb20_lo"] = bb20.bollinger_mavg(), bb20.bollinger_hband(), bb20.bollinger_lband()

    # “BB80/2 sur H1” = proxy du 4h
    bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb80_mid"], df["bb80_up"], df["bb80_lo"] = bb80.bollinger_mavg(), bb80.bollinger_hband(), bb80.bollinger_lband()

    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()
    return df

# ========= UNIVERS =========
def build_universe(ex):
    try:
        ex.load_markets()
        candidates = [
            m["symbol"] for m in ex.markets.values()
            if (m.get("type")=="swap" or m.get("swap")) and m.get("linear")
            and m.get("settle")=="USDT" and m.get("quote")=="USDT"
        ]
    except Exception:
        candidates = []
    rows=[]
    try:
        tickers = ex.fetch_tickers(candidates if candidates else None)
        for s,t in tickers.items():
            if ":USDT" not in s and "/USDT" not in s: continue
            vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try: vol=float(vol)
            except: vol=0.0
            rows.append((s,vol))
    except Exception:
        pass
    if not rows:
        # petit fallback minimal si testnet vide
        return ["BTC/USDT:USDT","ETH/USDT:USDT","XRP/USDT:USDT"]
    df = pd.DataFrame(rows, columns=["symbol","volume"]).sort_values("volume", ascending=False)
    uni = df.head(UNIVERSE_SIZE)["symbol"].tolist()
    return uni

# ========= RÈGLES STRATÉGIE =========
def strong_reaction(candle) -> bool:
    """
    Approximation simple des patterns: pinbar/méchée, marubozu, impulsion.
    On exige au moins une des caractéristiques + close dans/sur BB20.
    """
    # sera utilisé en combinaison avec "close dans BB20"
    body = abs(candle["close"] - candle["open"])
    rng  = candle["high"] - candle["low"]
    if rng <= 0: return False
    # Pinbar / méchée: longue mèche > 30% range
    upper_wick = candle["high"] - max(candle["close"], candle["open"])
    lower_wick = min(candle["close"], candle["open"]) - candle["low"]
    wick_ratio = max(upper_wick, lower_wick) / rng
    pin_like = wick_ratio >= 0.30
    # Marubozu / impulsion: corps > 60% du range OU corps très large par rapport à ATR proxy
    maru_like = (body / rng) >= 0.60
    return pin_like or maru_like

def close_inside_bb20(candle, lo20, up20) -> bool:
    return candle["close"] <= up20 and candle["close"] >= lo20

def touched_band(candle, lo, up, which="any", tol=0.0006) -> bool:
    """Contact sur borne haute/basse (ou both pour double)."""
    if which in ("any","lower"):
        if candle["low"] <= (lo * (1+tol)): return True
    if which in ("any","upper"):
        if candle["high"] >= (up * (1-tol)): return True
    return False

def bb20_outside_bb80(prev) -> bool:
    """Renvoie True si la bande blanche est franchement en dehors de la jaune (enfoncement)."""
    # blanche au-dessus de la jaune OU en dessous (écarts nets sur bornes)
    up_out = prev["bb20_up"] > prev["bb80_up"]
    lo_out = prev["bb20_lo"] < prev["bb80_lo"]
    return up_out or lo_out

def prolonged_double_exit(df, min_bars=4) -> bool:
    """
    Sortie prolongée: au moins `min_bars` barres consécutives EN DEHORS
    des 2 bandes (au-dessus des 2 ou au-dessous des 2).
    La bougie de réintégration (si elle clôture dedans) ne compte pas.
    """
    cnt = 0
    # on remonte depuis -2 (la dernière bougie est la bougie clôturée actuelle)
    i = -2
    last_side = None
    while abs(i) <= len(df):
        c = df.iloc[i]
        up_both = (c["high"] >= c["bb20_up"]) and (c["high"] >= c["bb80_up"])
        lo_both = (c["low"]  <= c["bb20_lo"]) and (c["low"]  <= c["bb80_lo"])
        if up_both:
            if last_side in (None,"up"): cnt += 1
            else: break
            last_side = "up"
        elif lo_both:
            if last_side in (None,"down"): cnt += 1
            else: break
            last_side = "down"
        else:
            # si on tombe dedans, on arrête: la réintégration n'est pas comptée
            break
        i -= 1
    return cnt >= min_bars

def detect_signal(df, state:dict, sym:str):
    """
    Détecte un signal à la dernière bougie CLÔTURÉE (df.iloc[-1]).
    Retourne None ou dict {side, regime, entry, sl, tp, rr, bullets}.
    Règles:
      1) Contact obligatoire (basse -> long ; haute -> short). Fenêtre 1-2 barres max.
      2) Réaction forte + clôture DANS/SUR la BB20.
      3) RR >= MIN_RR.
      4) Contre-tendance stricte: si BB20 est à l'extérieur de BB80 après enfoncement,
         la bougie de signal doit clôturer À L’INTÉRIEUR des 2 bandes (double réintégration).
      5) Sortie prolongée: si >=4 barres en dehors des 2 BB, ignorer le 1er signal suivants (cooldown).
    """
    if len(df) < 5: return None
    last = df.iloc[-1]     # bougie qui vient de CLÔTURER
    prev = df.iloc[-2]
    prev2= df.iloc[-3]

    # cooldown “sauter 1er trade” après sortie prolongée
    st = state.setdefault(sym, {})
    if st.get("cooldown", False):
        # on efface le flag et on ignore ce tour (= "sauter le 1er trade")
        st["cooldown"] = False
        return None
    if prolonged_double_exit(df, min_bars=4):
        st["cooldown"] = True
        return None

    bullets = []
    # fenêtre de contact: prev ou prev2
    long_contact  = touched_band(prev, prev["bb20_lo"], prev["bb20_up"], "lower") or \
                    touched_band(prev2, prev2["bb20_lo"], prev2["bb20_up"], "lower")
    short_contact = touched_band(prev, prev["bb20_lo"], prev["bb20_up"], "upper") or \
                    touched_band(prev2, prev2["bb20_lo"], prev2["bb20_up"], "upper")

    # réaction + close dans BB20
    react_ok = strong_reaction(last) and close_inside_bb20(last, last["bb20_lo"], last["bb20_up"])
    if not react_ok:
        return None

    # détection directionnelle
    side = None; regime = None
    above80 = last["close"] >= last["bb80_mid"]

    # tentative: on détermine si tendance (au-dessus/sous BB80_mid) ou contre-tendance
    if long_contact and above80:
        side, regime = "buy", "trend"
    elif short_contact and (not above80):
        side, regime = "sell", "trend"
    elif long_contact and (not above80):
        side, regime = "buy", "counter"
    elif short_contact and above80:
        side, regime = "sell", "counter"
    else:
        return None

    # contre-tendance stricte: si blanche “en dehors” de la jaune (enfoncement),
    # on exige une réintégration DANS LES DEUX bandes (bb20 ET bb80) sur la bougie de signal.
    if regime == "counter" and bb20_outside_bb80(prev):
        inside_both = (last["close"] <= min(last["bb20_up"], last["bb80_up"])) and \
                      (last["close"] >= max(last["bb20_lo"], last["bb80_lo"]))
        if not inside_both:
            return None

    # SL & TP théoriques
    entry = float(last["close"])
    atr = float(last["atr"])
    if side == "buy":
        sl = float(prev["low"]) - SL_ATR_CUSHION*atr
        tp = float(last["bb80_up"]) if regime=="trend" else float(last["bb20_up"])
        bullets.append("Contact bande basse BB20")
    else:
        sl = float(prev["high"]) + SL_ATR_CUSHION*atr
        tp = float(last["bb80_lo"]) if regime=="trend" else float(last["bb20_lo"])
        bullets.append("Contact bande haute BB20")

    bullets.append("Réaction (pattern fort) & close dans BB20")
    rr = abs((tp-entry)/(entry-sl)) if entry!=sl else 0.0
    if rr < MIN_RR:
        return None

    bullets.append(f"RR x{rr:.2f} (≥ {MIN_RR})")
    bullets.append("Tendance" if regime=="trend" else "Contre-tendance")

    return {"side":side,"regime":regime,"entry":entry,"sl":sl,"tp":tp,"rr":rr,"bullets":bullets}

# ========= ORDRES (papier) =========
def compute_qty(entry, sl, risk_amount):
    diff = abs(entry - sl)
    return risk_amount/diff if diff>0 else 0.0

# ========= COMMANDES TELEGRAM =========
_last_update_id = None
_bot_paused = False

def handle_commands(ex, universe, active_positions) -> None:
    """Lit les commandes Telegram et répond. Aucune purge automatique ici."""
    global _last_update_id, _bot_paused
    _last_update_id, msgs = tg_get_updates(_last_update_id)
    for m in msgs:
        text = (m["text"] or "").strip().lower()
        if not text: continue

        if text.startswith("/start"):
            mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
            tg_send_start_banner(mode, TF, int(RISK_PER_TRADE*100), MIN_RR)

        elif text.startswith("/mode"):
            tg_send(f"Mode actuel: *{'PAPER' if DRY_RUN else ('LIVE' if not BITGET_TESTNET else 'TESTNET')}*")

        elif text.startswith("/config"):
            tg_send(
                "*Config*\n"
                f"Mode: {'PAPER' if DRY_RUN else ('LIVE' if not BITGET_TESTNET else 'TESTNET')}\n"
                f"TF: {TF}\nTop: {UNIVERSE_SIZE} | Picks/h: {PICKS_PER_HOUR}\n"
                f"Risk: {int(RISK_PER_TRADE*100)}% | RR≥{MIN_RR}\nCSV: {TRADES_CSV}"
            )

        elif text.startswith("/stats"):
            tg_send("Stats: (journalisation locale CSV activée, résumé à développer si besoin)")

        elif text.startswith("/report"):
            tg_send("Rapport (compact) — le chat conserve *trades* et *signaux* du jour.")

        elif text.startswith("/orders"):
            if not active_positions:
                tg_send("Aucune position (papier).")
            else:
                lines=["*Positions papier*"]
                for s,p in active_positions.items():
                    lines.append(f"• {s} {p['side'].upper()} entry {p['entry']:.6f} RR x{p['rr']:.2f}")
                tg_send("\n".join(lines))

        elif text.startswith("/test"):
            tg_send("Test papier: (désactivé par défaut ici).")

        elif text.startswith("/closeallpaper"):
            n=len(active_positions); active_positions.clear()
            tg_send(f"🛑 Papier: {n} position(s) effacée(s).")

        elif text.startswith("/pause"):
            _bot_paused = True; tg_send("⏸️ Bot en pause.")

        elif text.startswith("/resume"):
            _bot_paused = False; tg_send("▶️ Bot relancé.")

        elif text.startswith("/logs"):
            tg_send("Logs: non exposés ici (hébergeur).")

        elif text.startswith("/ping"):
            tg_send("🏓 Ping ok.")

        elif text.startswith("/version"):
            tg_send("🤖 Darwin-Bitget v1.15")

        elif text.startswith("/restart"):
            tg_send("♻️ Redémarrage demandé… (à implémenter côté infra si souhaité)")

        elif text.startswith("/signals"):
            tg_send(signals_last_hour_text())

# ========= MAIN LOOP =========
def main():
    ex = create_exchange()
    universe = build_universe(ex)
    mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
    tg_send_start_banner(mode, TF, int(RISK_PER_TRADE*100), MIN_RR)

    # position papier (simple mémo — ici on n’exécute que des signaux/entrées)
    active_paper = {}

    state = {}                 # pour cooldown “sauter le 1er trade”
    last_closed_ts = None      # dernière bougie close traitée
    last_hour_processed = None # pour limiter à 1 vague de signaux par heure

    while True:
        try:
            handle_commands(ex, universe, active_paper)
            if _bot_paused:
                time.sleep(LOOP_DELAY); continue

            # On regarde l’heure actuelle — on n’émet les signaux qu’à la *clôture* et une fois par heure
            now = datetime.utcnow()

            # scan data (on a besoin du ts de la dernière bougie close pour caler l’horloge)
            # on prend un "leader" pour l’horloge (BTC)
            leader = universe[0] if universe else "BTC/USDT:USDT"
            try:
                df0 = fetch_ohlcv_df(ex, leader, TF, 100)
            except Exception:
                time.sleep(LOOP_DELAY); continue
            ts_close = df0.index[-1]

            # si pas de nouvelle clôture -> pas de signaux
            if last_closed_ts == ts_close:
                time.sleep(LOOP_DELAY); continue

            # nouvelle bougie H1 close => fenêtre de travail “une fois par heure”
            last_closed_ts = ts_close

            # 1) scanner l’univers et construire la liste des signaux candidats
            candidates = []
            for sym in universe:
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, 200)
                    sig = detect_signal(df, state, sym)
                    if not sig: 
                        continue
                    # calcul RR et ordre: on ne place *pas* l’ordre tout de suite
                    candidates.append((sym, sig))
                except Exception as e:
                    print("[SCAN]", sym, e)

            if not candidates:
                time.sleep(LOOP_DELAY); continue

            # 2) ne garder que les 4 meilleurs RR
            candidates.sort(key=lambda x: x[1]["rr"], reverse=True)
            picks = candidates[:PICKS_PER_HOUR]

            # 3) émission des *signaux* (pas d’entrée intrabarre – l’entrée se fera à l’ouverture suivante)
            for sym, sig in picks:
                # message carte
                mid = tg_send_signal_card(sym, sig["side"], sig["entry"], sig["sl"], sig["tp"], sig["rr"],
                                          sig["bullets"], sig["regime"], DRY_RUN)
                remember_signal_message(sym, sig["side"], sig["rr"], mid)

                # si tu veux *vraiment* entrer automatiquement à l’ouverture suivante,
                # tu peux mémoriser ici et exécuter sur la prochaine itération si ts change.
                # Pour rester 100% conforme: on n’entre pas ici, on ne fait que signaler.

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            tg_send("⛔ Arrêt manuel.")
            break
        except Exception as e:
            print("[LOOP ERR]", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
