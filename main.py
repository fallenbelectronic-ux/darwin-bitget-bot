import os, time, csv, math, requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands, AverageTrueRange
from dotenv import load_dotenv

from notifier import (
    tg_send, tg_get_updates, purge_chat, purge_all,
    send_document, remember_signal_message, signals_last_hour_text
)

load_dotenv()

# =============== ENV ==================
TG_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID", "")

BITGET_TESTNET  = os.getenv("BITGET_TESTNET","true").lower() in ("1","true","yes")
API_KEY         = os.getenv("BITGET_API_KEY","")
API_SECRET      = os.getenv("BITGET_API_SECRET","")
PASSPHRASE      = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF              = os.getenv("TIMEFRAME","1h")
RISK_PER_TRADE  = float(os.getenv("RISK_PER_TRADE","0.01"))
MIN_RR          = float(os.getenv("MIN_RR","3"))
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES","4"))
PICKS_PER_HOUR  = int(os.getenv("PICKS","4"))         # <= 4 signaux/heure
UNIVERSE_SIZE   = int(os.getenv("UNIVERSE_SIZE","30")) # Top 30
LOOP_DELAY      = int(os.getenv("LOOP_DELAY","5"))
TZ              = os.getenv("TIMEZONE","Europe/Lisbon")

# Paper / Live
DRY_RUN         = os.getenv("DRY_RUN","true").lower() in ("1","true","yes")

# Levier souhaité (cross 2x par défaut)
WANT_CROSS      = os.getenv("POSITION_MODE","cross")
MAX_LEVERAGE    = int(os.getenv("MAX_LEVERAGE","2"))

# CSV de journalisation (clôtures)
TRADES_CSV      = os.getenv("TRADES_CSV","/app/trades.csv")

# Indicateurs
ATR_WINDOW      = 14
SL_ATR_CUSHION  = 0.25

# Règles « réaction rapide » (tendance)
QUICK_BARS      = 3
QUICK_PROGRESS  = 0.30

# Fallback testnet
FALLBACK_TESTNET = [
    "BTC/USDT:USDT","ETH/USDT:USDT","XRP/USDT:USDT","LTC/USDT:USDT","BCH/USDT:USDT"
]

# =============== EXCHANGE =============
def create_exchange():
    ex = ccxt.bitget({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {"defaultType":"swap", "testnet":BITGET_TESTNET}
    })
    if BITGET_TESTNET:
        try: ex.set_sandbox_mode(True)
        except Exception: pass
    return ex

def try_set_leverage(ex, symbol, lev=2, mode="cross"):
    """
    Applique 2x Cross si possible – silencieux (pas de spam Telegram).
    """
    try:
        params = {}
        if mode.lower() == "cross":
            params["marginMode"] = "cross"
        else:
            params["marginMode"] = "isolated"
        ex.set_leverage(lev, symbol, params=params)
    except Exception:
        pass

# =============== DATA =================
def fetch_ohlcv_df(ex, symbol, timeframe, limit=300):
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    if not raw or len(raw) == 0:
        raise ValueError("No OHLCV")
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)

    bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb20_mid"] = bb20.bollinger_mavg()
    df["bb20_up"]  = bb20.bollinger_hband()
    df["bb20_lo"]  = bb20.bollinger_lband()

    # « BB Jaune » = BB(80,2) sur H1 (équivalent approx. H4)
    bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb80_mid"] = bb80.bollinger_mavg()
    df["bb80_up"]  = bb80.bollinger_hband()
    df["bb80_lo"]  = bb80.bollinger_lband()

    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()
    return df

def filter_working_symbols(ex, symbols, timeframe="1h"):
    ok=[]
    for s in symbols:
        try:
            ex.fetch_ohlcv(s, timeframe=timeframe, limit=2)
            ok.append(s)
        except Exception:
            pass
    return ok

def build_universe(ex):
    """
    Top 30 par volume 24h – swaps USDT linéaires – filtre testnet.
    """
    try:
        ex.load_markets()
        candidates=[]
        for m in ex.markets.values():
            if (m.get("symbol") and (m.get("type")=="swap" or m.get("swap"))
                and m.get("linear") and m.get("settle")=="USDT" and m.get("quote")=="USDT"):
                candidates.append(m["symbol"])
    except Exception:
        candidates=[]

    rows=[]
    try:
        tickers=ex.fetch_tickers(candidates if candidates else None)
        for s,t in tickers.items():
            if "/USDT" not in s and ":USDT" not in s: 
                continue
            vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try: vol=float(vol)
            except: vol=0.0
            rows.append((s, vol))
    except Exception:
        pass

    if rows:
        df=pd.DataFrame(rows,columns=["symbol","volume"]).sort_values("volume",ascending=False)
        uni=df.head(UNIVERSE_SIZE)["symbol"].tolist()
        if BITGET_TESTNET:
            uni=filter_working_symbols(ex,uni[:max(20,UNIVERSE_SIZE)],timeframe=TF) or FALLBACK_TESTNET
        return uni

    # Fallback testnet
    fb=filter_working_symbols(ex, FALLBACK_TESTNET, timeframe=TF)
    return fb or FALLBACK_TESTNET

# =============== RÈGLES DARWIN =================
def _contact_recent(df, which, lookback=2):
    """
    Contact/traversée avec la borne (bb20_up/lo ou bb80_up/lo) dans les 2 dernières bougies clôturées.
    """
    if len(df) < lookback+1: 
        return False
    sl = df.iloc[-(lookback+1):-1]  # exclude last (signal) bar
    if which == "bb20_lo":
        return any(sl["low"] <= sl["bb20_lo"])
    if which == "bb20_up":
        return any(sl["high"] >= sl["bb20_up"])
    if which == "bb80_lo":
        return any(sl["low"] <= sl["bb80_lo"])
    if which == "bb80_up":
        return any(sl["high"] >= sl["bb80_up"])
    return False

def _close_inside_bb20(candle):
    return (candle["close"] <= candle["bb20_up"]) and (candle["close"] >= candle["bb20_lo"])

def _reaction_pattern(prev, last):
    """
    Proxy des patterns (pinbar/méchage/marubozu/gap+impulsion).
    On exige une mèche significative ou un corps directionnel >= 30% ATR.
    """
    atr = float(last["atr"]) if not np.isnan(last["atr"]) else 0.0
    rng = float(last["high"]-last["low"])
    body= abs(float(last["close"]-last["open"]))
    if atr<=0: 
        return False
    # mèche notable : (rng-body) >= 0.3*atr
    if (rng - body) >= 0.3*atr:
        return True
    # corps impulsif >= 0.3*atr
    if body >= 0.3*atr:
        return True
    return False

def _prolonged_double_exit(df, min_bars=4):
    """
    Sortie prolongée (>=4 barres) des DEUX bandes.
    Si la bougie de réintégration clôture DANS BB20, elle NE compte pas pour la série.
    """
    if len(df) < min_bars+2:
        return False
    # on regarde en remontant tant que close hors des deux bandes
    cnt=0
    idx=-2
    while abs(idx) <= len(df):
        c=df.iloc[idx]
        inside_bb20 = _close_inside_bb20(c)
        up_both  = (c["high"]>=c["bb20_up"] and c["high"]>=c["bb80_up"])
        low_both = (c["low"] <=c["bb20_lo"] and c["low"] <=c["bb80_lo"])
        if inside_bb20:
            break
        if up_both or low_both:
            cnt+=1
            idx-=1
        else:
            break
    return cnt >= min_bars

def _need_double_reintegration(df, side):
    """
    Si la BB20 s'est maintenue à l'extérieur de la BB80 durant l'enfoncement,
    alors en contre-tendance on exige réintégration BB20 ET BB80.
    """
    if len(df)<5: return False
    prevs = df.iloc[-6:-1]
    if side=="buy":
        # bb20_lo bien sous bb80_lo (écart net) pendant la phase
        return any(prevs["bb20_lo"] < prevs["bb80_lo"] - 1e-12)
    else:
        return any(prevs["bb20_up"] > prevs["bb80_up"] + 1e-12)

def detect_signal(df, state, sym, rr_min=3.0):
    """
    Détection STRICTE à la clôture de la bougie :
    - 3 conditions obligatoires :
        1) contact/traversée récente (<=2 bougies) avec la borne pertinente
        2) réaction + clôture DANS/SUR la BB20
        3) RR >= 3
    - Tendance :
        • au-dessus de BB80 + contact/traversée BB20 basse + close dans/sur BB20 → LONG
        • en-dessous de BB80 + contact/traversée BB20 haute + close dans/sur BB20 → SHORT
    - Contre-tendance :
        • double extrême + réintégration (close dans/sur BB20) + contact dans les 2 bougies
        • si « enfoncement » avec BB20 dehors → réintégration des DEUX bandes obligatoire
    - Sortie prolongée double :
        • on ignore le premier trade validé après la série (cooldown 1 signal).
    """
    if len(df) < 100:  # besoin de profondeur pour BB80
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]

    # Cooldown après sortie prolongée double
    if _prolonged_double_exit(df, min_bars=4):
        st = state.setdefault(sym, {})
        st["cooldown"] = True
        return None

    if state.get(sym, {}).get("cooldown", False):
        # on « consomme » la pénalité et on ne prend pas CE premier signal
        state[sym]["cooldown"] = False
        return None

    above80 = last["close"] >= last["bb80_mid"]

    # Conditions communes : réaction + close dans/sur BB20
    if not _reaction_pattern(prev, last):
        return None
    if not _close_inside_bb20(last):
        return None

    # Détections
    side=None
    regime=None
    notes=[]

    # TENDANCE
    if above80 and _contact_recent(df,"bb20_lo",lookback=2):
        side, regime = "buy","trend"
        notes.append("Contact bande basse BB20")
    elif (not above80) and _contact_recent(df,"bb20_up",lookback=2):
        side, regime = "sell","trend"
        notes.append("Contact bande haute BB20")

    # CONTRE-TENDANCE
    if side is None:
        # double extrême = contact simultané BB20 et BB80 côté pertinent (dans les 2 dernières bougies)
        buy_ct  = _contact_recent(df,"bb20_lo",2) and _contact_recent(df,"bb80_lo",2)
        sell_ct = _contact_recent(df,"bb20_up",2) and _contact_recent(df,"bb80_up",2)
        if buy_ct:
            side, regime = "buy","counter"
            notes.append("Double extrême côté bas (BB20 & BB80)")
        elif sell_ct:
            side, regime = "sell","counter"
            notes.append("Double extrême côté haut (BB20 & BB80)")

        # si enfoncement « BB20 dehors », exiger réintégration des 2 bandes
        if side and _need_double_reintegration(df, side):
            # la bougie de signal doit être revenue dans les 2 enveloppes
            inside80 = (last["close"] <= last["bb80_up"] and last["close"] >= last["bb80_lo"])
            if not inside80:
                return None
            notes.append("Réintégration stricte BB20+BB80")

    if side is None:
        return None

    # SL/TP & RR
    entry = float(last["close"])
    atr   = float(last["atr"])
    if side=="buy":
        sl = min(float(prev["low"]), float(last["low"])) - SL_ATR_CUSHION*atr
        tp = float(last["bb80_up"]) if regime=="trend" else float(last["bb20_up"])
    else:
        sl = max(float(prev["high"]), float(last["high"])) + SL_ATR_CUSHION*atr
        tp = float(last["bb80_lo"]) if regime=="trend" else float(last["bb20_lo"])

    denom = abs(entry-sl)
    rr = abs((tp-entry)/denom) if denom>0 else 0.0
    if rr < rr_min:
        return None

    return {
        "side":side, "regime":regime,
        "entry":entry, "sl":sl, "tp":tp, "rr":rr,
        "notes":notes
    }

# =============== ORDRES (Paper ou Réel) =================
def ensure_trades_csv():
    if not os.path.exists(TRADES_CSV):
        with open(TRADES_CSV,"w",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow(
                ["ts","symbol","side","regime","entry","exit","pnl_pct","rr","result","mode"]
            )

def compute_qty(entry, sl, risk_amount):
    diff = abs(entry-sl)
    return risk_amount/diff if diff>0 else 0.0

# =======================
# TELEGRAM COMMANDS (robuste)
# =======================
_last_update_id = None  # offset pour getUpdates

def _cmd_text(msg: dict) -> str:
    return (msg.get("text") or "").strip()

def _is_from_allowed_chat(msg: dict) -> bool:
    # Si TELEGRAM_CHAT_ID est défini, on filtre strictement dessus
    try:
        if TG_CHAT_ID:
            return str(msg["chat"]["id"]) == str(TG_CHAT_ID)
        # Sinon on accepte tout (utile en test) :
        return True
    except Exception:
        return False

def poll_telegram_commands(ex, active_paper):
    """
    Appelée dans la boucle principale. Récupère les updates et traite les commandes.
    - offset géré proprement (None au premier appel)
    - ignore les updates d'autres chats si TG_CHAT_ID est défini
    """
    global _last_update_id
    try:
        # 1) Récupérer les updates
        offset = _last_update_id + 1 if _last_update_id is not None else None
        data = tg_get_updates(offset=offset, timeout=0)
        if not data or not data.get("ok"):
            return

        updates = data.get("result", [])
        if not updates:
            return

        for upd in updates:
            # Mémorise l’update_id (même si le message n’est pas pour nous)
            _last_update_id = upd.get("update_id", _last_update_id)

            msg = upd.get("message") or upd.get("edited_message")
            if not msg:
                continue
            if not _is_from_allowed_chat(msg):
                continue

            text = _cmd_text(msg).lower()

            # --- Routes de commandes ---
            if text.startswith("/start"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(f"🔔 Démarrage — *{mode}* • TF {TF} • Risk {int(RISK_PER_TRADE*100)}% • RR≥{MIN_RR}")

            elif text.startswith("/config"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(
                    "*Config*\n"
                    f"Mode: {mode}\n"
                    f"TF: {TF}\n"
                    f"Risk: {int(RISK_PER_TRADE*100)}% | RR≥{MIN_RR}\n"
                    f"Top: {UNIVERSE_SIZE} | Picks/h: {PICKS}\n"
                    f"Max trades: {MAX_OPEN_TRADES}\n"
                    f"CSV: {TRADES_CSV}"
                )

            elif text.startswith("/mode"):
                tg_send("Mode actuel : *PAPER*" if DRY_RUN else ("Mode actuel : *TESTNET*" if BITGET_TESTNET else "Mode actuel : *LIVE*"))

            elif text.startswith("/stats"):
                send_stats()

            elif text.startswith("/report"):
                daily_report()

            elif text.startswith("/exportcsv"):
                try:
                    if os.path.exists(TRADES_CSV):
                        requests.post(
                            f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument",
                            data={"chat_id": TG_CHAT_ID},
                            files={"document": open(TRADES_CSV, "rb")},
                            timeout=20,
                        )
                    else:
                        tg_send("Aucun fichier CSV trouvé.")
                except Exception as e:
                    tg_send(f"⚠️ export CSV: {e}")

            elif text.startswith("/orders"):
                if DRY_RUN:
                    if not active_paper:
                        tg_send("Aucune position (papier).")
                    else:
                        lines = ["*Positions (papier)*"]
                        from datetime import datetime
                        for sym, p in active_paper.items():
                            lines.append(
                                f"• {sym} {'LONG' if p['side']=='buy' else 'SHORT'} | entry {p['entry']:.4f} | SL {p['sl']:.4f} | TP {p['tp']:.4f} | RR x{p['rr']:.2f}"
                            )
                        tg_send("\n".join(lines))
                else:
                    try:
                        pos = ex.fetch_positions()
                        rows=[]
                        for p in pos:
                            sz = float(p.get("contracts") or 0)
                            if abs(sz)>0:
                                rows.append(f"• {p.get('symbol')} {p.get('side') or ('long' if sz>0 else 'short')} qty {abs(sz)}")
                        tg_send("*Positions réelles*\n" + ("\n".join(rows) if rows else "Aucune position."))
                    except Exception as e:
                        tg_send(f"⚠️ Impossible de lire les positions : {e}")

            elif text.startswith("/ping"):
                tg_send("📡 Ping ok.")

            elif text.startswith("/version"):
                tg_send("🤖 Darwin-Bitget v1.14")

            # /restart : à implémenter côté infra (Render API) si tu le souhaites
            elif text.startswith("/restart"):
                tg_send("♻️ Redémarrage demandé (non implémenté côté infra).")

            # NB: Les commandes de purge ont été volontairement désactivées. 
            # Si tu réactives une purge manuelle plus tard, remets le handler ici.

            # Exemples ‘papier’ optionnels :
            elif text.startswith("/test"):
                tg_send("🧪 Mode papier: commande /test non-implémentée dans cette build.")
            elif text.startswith("/closepaper"):
                tg_send("🧪 Mode papier: /closepaper non-implémentée dans cette build.")
            elif text.startswith("/closeallpaper"):
                tg_send("🧪 Mode papier: /closeallpaper non-implémentée dans cette build.")

            elif text.startswith("/pause"):
                tg_send("⏸️ Pause: non-implémenté (tu peux arrêter le service côté hébergeur).")
            elif text.startswith("/resume"):
                tg_send("▶️ Reprise: non-implémenté (le bot tourne déjà).")

            elif text.startswith("/logs"):
                tg_send("🧾 Derniers logs indisponibles dans cette build.")
            # ---------------------------

    except Exception as e:
        # On ne crashe pas le bot pour un souci Telegram
        print("[TELEGRAM] poll error:", e)

# =============== NOTIFICATIONS SIGNAL ===================
def notify_signal(symbol, sig):
    emoji = "📈" if sig["regime"]=="trend" else "🔄"
    side  = "LONG" if sig["side"]=="buy" else "SHORT"
    bullets = []
    bullets += sig.get("notes",[])
    bullets.append("Clôture dans/sur BB20")
    bullets.append(f"RR x{sig['rr']:.2f} (≥ {MIN_RR})")
    bullets.append("Tendance" if sig["regime"]=="trend" else "Contre-tendance")

    text = (
        f"📈 *Signal [PAPER]* `{symbol}` {side}\n"
        f"Entrée `{sig['entry']:.6f}` | SL `{sig['sl']:.6f}` | TP `{sig['tp']:.6f}`\n"
        + "\n".join([f"• {b}" for b in bullets])
    )
    msg_id = tg_send(text, remember_for_signals=True)
    # on mémorise pour /signals
    remember_signal_message(msg_id, symbol, sig)

# =============== MAIN LOOP ==============================
def main():
    ex = create_exchange()
    tg_send("▶️ Bot repris.")
    universe = build_universe(ex)

    # Optionnel : levier 2x cross silencieux
    for s in universe:
        try_set_leverage(ex, s, lev=MAX_LEVERAGE, mode=WANT_CROSS)

    last_bar_seen = {}
    state = {}  # cooldown etc.

    while True:
        try:
            poll_telegram_commands()
            if PAUSED:
                time.sleep(LOOP_DELAY)
                continue

            now = datetime.utcnow()
            # scanning uniquement à la clôture H1 : on compare la dernière barre close
            candidates = []

            for sym in universe:
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, limit=300)
                except Exception:
                    continue

                last_ts = df.index[-1]
                # si pas de nouvelle bougie, skip
                if last_bar_seen.get(sym) == last_ts:
                    continue

                # nouvelle clôture !
                last_bar_seen[sym] = last_ts

                sig = detect_signal(df, state, sym, rr_min=MIN_RR)
                if sig:
                    candidates.append((sym, sig))

            # Sélectionne les 4 meilleurs par RR
            if candidates:
                candidates.sort(key=lambda x: x[1]["rr"], reverse=True)
                for sym, sig in candidates[:PICKS_PER_HOUR]:
                    notify_signal(sym, sig)
                    # (Si tu veux exécuter des ordres, c’est ici, mais on reste « signaux ».)

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            tg_send("⛔ Arrêt manuel.")
            break
        except Exception as e:
            # évite spam – message unique raccourci
            try: tg_send(f"⚠️ Loop error: {e}")
            except Exception: pass
            time.sleep(5)

if __name__ == "__main__":
    main()
