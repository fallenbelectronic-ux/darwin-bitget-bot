# main.py
import os, time, csv, math, requests, traceback
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands, AverageTrueRange
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from notifier import tg_send, purge_chat, nightly_signals_purge  # <- requis

load_dotenv()

# =========================
# ENV / PARAMS
# =========================
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1","true","yes")
API_KEY          = os.getenv("BITGET_API_KEY")
API_SECRET       = os.getenv("BITGET_API_SECRET")
PASSPHRASE       = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF               = os.getenv("TIMEFRAME", "1h")
RISK_PER_TRADE   = float(os.getenv("RISK_PER_TRADE", "0.01"))
MIN_RR           = float(os.getenv("MIN_RR", "3"))
MAX_OPEN_TRADES  = int(os.getenv("MAX_OPEN_TRADES", "4"))
PICKS_PER_HOUR   = int(os.getenv("PICKS", "4"))           # <= 4
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE", "30"))  # top 30
LOOP_DELAY       = int(os.getenv("LOOP_DELAY", "5"))
TZ               = os.getenv("TIMEZONE", "Europe/Lisbon")
DRY_RUN          = os.getenv("DRY_RUN", "true").lower() in ("1","true","yes")

# MM / SL
ATR_WINDOW       = 14
SL_ATR_CUSHION   = 0.25

# Prolonged double exit
PROLONGED_MIN_BARS = 4   # ≥ 4 barres hors des 2 BB
# Si la réintégration se fait "dans" les BB, elle ne compte pas comme 4e barre

TRADES_CSV       = os.getenv("TRADES_CSV", "/app/trades.csv")

# =========================
# EXCHANGE
# =========================
def create_exchange():
    ex = ccxt.bitget({
        "apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}
    })
    if BITGET_TESTNET:
        try: ex.set_sandbox_mode(True)
        except Exception: pass
    return ex

def try_set_leverage(ex, symbol, leverage=2, mode="cross"):
    """
    Applique levier 2x cross si possible, sans spammer de logs.
    """
    try:
        params = {}
        if hasattr(ex, "private_mix_post_order_margin_mode"):
            # ccxt unifie: set_margin_mode / set_leverage
            try:
                ex.set_margin_mode(mode, symbol, params={})
            except Exception:
                pass
        try:
            ex.set_leverage(leverage, symbol, params={})
        except Exception:
            pass
    except Exception:
        pass

# =========================
# DATA / INDICATEURS
# =========================
def fetch_ohlcv_df(ex, symbol, timeframe, limit=300):
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    if not raw or len(raw) == 0:
        raise RuntimeError("empty ohlcv")

    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)

    # BB 20/2 (blanche)
    bb_fast = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb20_mid"] = bb_fast.bollinger_mavg()
    df["bb20_up"]  = bb_fast.bollinger_hband()
    df["bb20_lo"]  = bb_fast.bollinger_lband()

    # BB 80/2 (jaune)
    bb_slow = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb80_mid"] = bb_slow.bollinger_mavg()
    df["bb80_up"]  = bb_slow.bollinger_hband()
    df["bb80_lo"]  = bb_slow.bollinger_lband()

    # ATR
    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()

    return df

# =========================
# UNIVERS top 30 par volume
# =========================
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
    try:
        ex.load_markets()
        candidates = [m["symbol"] for m in ex.markets.values()
                      if (m.get("type")=="swap" or m.get("swap")) and m.get("linear")
                      and m.get("settle")=="USDT" and m.get("quote")=="USDT"]
    except Exception:
        candidates = []

    rows=[]
    try:
        tickers = ex.fetch_tickers(candidates if candidates else None)
        for s, t in tickers.items():
            if "/USDT" not in s and ":USDT" not in s: 
                continue
            vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try: vol=float(vol)
            except: vol=0.0
            rows.append((s, vol))
    except Exception:
        pass

    if rows:
        df = pd.DataFrame(rows, columns=["symbol","volume"]).sort_values("volume", ascending=False)
        uni = df.head(UNIVERSE_SIZE)["symbol"].tolist()
        if BITGET_TESTNET:
            uni = filter_working_symbols(ex, uni[:UNIVERSE_SIZE], timeframe=TF) or uni[:UNIVERSE_SIZE]
        return uni
    return filter_working_symbols(ex, candidates[:UNIVERSE_SIZE], timeframe=TF)

# =========================
# OUTILS DETECTION
# =========================
def _contact_last2(df, side, band="bb20"):
    """
    Contact/traversée sur la bougie précédente ou l'avant-dernière.
    band: 'bb20' ou 'bb80'
    side: 'buy' (contact basse) / 'sell' (contact haute)
    """
    if len(df) < 3: 
        return False
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]
    if band=="bb20":
        lo, up = "bb20_lo", "bb20_up"
    else:
        lo, up = "bb80_lo", "bb80_up"

    if side=="buy":
        touch1 = prev["low"]  <= prev[lo]
        touch2 = prev2["low"] <= prev2[lo]
    else:
        touch1 = prev["high"] >= prev[up]
        touch2 = prev2["high"] >= prev2[up]
    return bool(touch1 or touch2)

def _close_inside_bb20(candle):
    return (candle["close"] <= candle["bb20_up"]) and (candle["close"] >= candle["bb20_lo"])

def _reaction_pattern(prev, last, side):
    """
    Simplifiée: 'fort' si la bougie de signal est directionnelle (marubozu-ish / meche courte côté entrée)
    ou mèche de rejet sur la bande touchée. On reste volontairement strict.
    """
    body = abs(last["close"] - last["open"])
    range_ = last["high"] - last["low"]
    if range_ <= 0: 
        return False
    body_ratio = body / range_
    # seuils raisonnables
    if body_ratio >= 0.55:
        return True
    # mèche de rejet côté bande
    if side=="buy":
        # longue mèche basse sur prev ou last
        lower_wick = min(prev["open"], prev["close"]) - prev["low"]
        lower_wick2= min(last["open"], last["close"]) - last["low"]
        return (lower_wick/range_ >= 0.35) or (lower_wick2/(last["high"]-last["low"]+1e-12) >= 0.35)
    else:
        upper_wick = prev["high"] - max(prev["open"], prev["close"])
        upper_wick2= last["high"] - max(last["open"], last["close"])
        return (upper_wick/range_ >= 0.35) or (upper_wick2/(last["high"]-last["low"]+1e-12) >= 0.35)

def _bb20_outside_bb80(candle):
    """BB20 à l'extérieur de BB80 (fort enfoncement) : mid20 hors [lo80, up80]."""
    return (candle["bb20_mid"] > candle["bb80_up"]) or (candle["bb20_mid"] < candle["bb80_lo"])

def _prolonged_double_exit(df, min_bars=PROLONGED_MIN_BARS):
    """
    ≥ min_bars barres consécutives AVANT la bougie précédente avec high>bb20_up & >bb80_up (sell)
    ou low<bb20_lo & <bb80_lo (buy). Si la bougie de réintégration ferme "dans" les BB,
    elle ne compte pas comme 4e barre.
    Retourne 'up' / 'down' / None
    """
    if len(df) < (min_bars+2):
        return None
    # on regarde jusqu'à la bougie -2 (prev)
    count_up, count_down = 0, 0
    for i in range(-min_bars-2, -1):
        r = df.iloc[i]
        up_both = (r["high"] >= r["bb20_up"]) and (r["high"] >= r["bb80_up"])
        lo_both = (r["low"]  <= r["bb20_lo"]) and (r["low"]  <= r["bb80_lo"])
        if up_both:
            count_up += 1; count_down = 0
        elif lo_both:
            count_down += 1; count_up = 0
        else:
            count_up = 0; count_down = 0
    if count_up >= min_bars:
        return "up"
    if count_down >= min_bars:
        return "down"
    return None

# =========================
# DETECTION DARWIN
# =========================
def detect_signal(df, state, sym):
    """
    Retourne None ou dict {side, regime, entry, sl, tp, rr, notes, wait_bars}
    - entrée uniquement si la bougie de signal (last) **clôture dans/sur BB20**,
      sinon on autorise la **bougie suivante** comme 2e chance.
    - contact ≤ 2 bougies
    - contre-tendance: si BB20 en dehors BB80 => réintégration des 2 BB obligatoire
    - skip 1er trade après 'prolonged_double_exit'
    """
    if len(df) < 3: 
        return None

    last, prev, prev2 = df.iloc[-1], df.iloc[-2], df.iloc[-3]
    notes=[]
    above80 = last["close"] >= last["bb80_mid"]

    # réintégrations strictes (1 ou 2 barres) + close dans/sur BB20
    close_in_bb20 = _close_inside_bb20(last)

    # Contact conditions (≤ 2 barres)
    touch20_long  = _contact_last2(df, "buy",  "bb20")
    touch20_short = _contact_last2(df, "sell", "bb20")
    touch80_long  = _contact_last2(df, "buy",  "bb80")
    touch80_short = _contact_last2(df, "sell", "bb80")

    # Prolonged double exit handling
    long_short_prolonged = _prolonged_double_exit(df)
    st = state.setdefault(sym, {})
    if st.get("cooldown", False):
        # on vient de voir la première réintégration, on saute ce trade puis on lève le cooldown
        st["cooldown"] = False
        return None
    if long_short_prolonged in ("up","down"):
        st["cooldown"] = True
        return None

    # Trend (au-dessus/sous bb80_mid) + contact bb20 + close dans/sur bb20 + réaction
    regime = None; side=None
    if above80 and touch20_long and close_in_bb20 and _reaction_pattern(prev, last, "buy"):
        regime="trend"; side="buy"; notes+=["Contact bande basse BB20","Réaction (pattern fort) & close dans/sur BB20","Tendance"]
    elif (not above80) and touch20_short and close_in_bb20 and _reaction_pattern(prev, last, "sell"):
        regime="trend"; side="sell"; notes+=["Contact bande haute BB20","Réaction (pattern fort) & close dans/sur BB20","Tendance"]
    else:
        # Counter-trend : double extrême + réintégration
        # on exige contact BB20 + BB80 (selon le côté)
        # si BB20 en dehors BB80 => réintégration des 2 BB obligatoire (close last > loBB20 & > loBB80 pour long, etc.)
        outside = _bb20_outside_bb80(last) or _bb20_outside_bb80(prev)
        if close_in_bb20:
            if touch20_long and touch80_long and _reaction_pattern(prev, last, "buy"):
                if (not outside) or (outside and last["close"]>=last["bb80_lo"] and prev["low"]<=prev["bb80_lo"]):
                    regime="counter"; side="buy"; notes+=["Contact/traversée BB20 & BB80 bas","Réintégration & close dans/sur BB20","Contre-tendance"]
            elif touch20_short and touch80_short and _reaction_pattern(prev, last, "sell"):
                if (not outside) or (outside and last["close"]<=last["bb80_up"] and prev["high"]>=prev["bb80_up"]):
                    regime="counter"; side="sell"; notes+=["Contact/traversée BB20 & BB80 haut","Réintégration & close dans/sur BB20","Contre-tendance"]

    if not regime:
        return None

    entry=float(last["close"]); atr=float(last["atr"])
    tick = max(entry*0.0001, 0.01)

    if side=="buy":
        sl = min(float(prev["low"]), float(last["low"])) - SL_ATR_CUSHION*atr
        tp = float(last["bb80_up"]) - 2*tick if regime=="trend" else float(last["bb20_up"]) - 2*tick
    else:
        sl = max(float(prev["high"]), float(last["high"])) + SL_ATR_CUSHION*atr
        tp = float(last["bb80_lo"]) + 2*tick if regime=="trend" else float(last["bb20_lo"]) + 2*tick

    denom = abs(entry-sl)
    rr = abs((tp-entry)/(denom if denom>0 else 1e-12))

    if rr < MIN_RR:
        return None

    return {"side":side,"regime":regime,"entry":entry,"sl":sl,"tp":tp,"rr":rr,"notes":notes, "wait_bars":1}

# =========================
# ORDRES RÉELS / PAPER
# =========================
def has_open_position_real(ex, symbol):
    try:
        pos = ex.fetch_positions([symbol])
        for p in pos:
            if abs(float(p.get("contracts") or 0))>0: return True
        return False
    except: return False

def count_open_positions_real(ex):
    try:
        pos = ex.fetch_positions()
        return sum(1 for p in pos if abs(float(p.get("contracts") or 0))>0)
    except: return 0

def compute_qty(entry, sl, risk_amount):
    diff=abs(entry-sl)
    return risk_amount/diff if diff>0 else 0.0

# =========================
# HISTO / RAPPORTS (compacts)
# =========================
def ensure_trades_csv():
    if not os.path.exists(TRADES_CSV):
        with open(TRADES_CSV,"w",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow(["ts","symbol","side","regime","entry","exit","pnl_pct","rr","result","mode"])

def log_trade_close(symbol, side, regime, entry, exit_price, rr, result, mode):
    ensure_trades_csv()
    pnl_pct = (exit_price-entry)/entry*100.0 if side=="buy" else (entry-exit_price)/entry*100.0
    with open(TRADES_CSV,"a",newline="",encoding="utf-8") as f:
        csv.writer(f).writerow([datetime.utcnow().isoformat(), symbol, side, regime,
                                f"{entry:.8f}", f"{exit_price:.8f}", f"{pnl_pct:.4f}", f"{rr:.2f}", result, mode])
    return pnl_pct

def summarize(rows):
    n=len(rows)
    wins=sum(1 for x in rows if x["result"]=="win")
    losses=sum(1 for x in rows if x["result"]=="loss")
    bes=sum(1 for x in rows if x["result"]=="be")
    avg_rr = np.mean([float(x["rr"]) for x in rows]) if rows else 0.0
    total_pnl = np.sum([float(x["pnl_pct"]) for x in rows]) if rows else 0.0
    best = max(rows, key=lambda x: float(x["pnl_pct"])) if rows else None
    worst= min(rows, key=lambda x: float(x["pnl_pct"])) if rows else None
    winrate = 100*wins/max(1,wins+losses)
    return n,wins,losses,bes,avg_rr,total_pnl,best,worst,winrate

def send_stats():
    ensure_trades_csv()
    since = datetime.utcnow() - timedelta(days=1)
    rows_24=[]; rows_all=[]
    with open(TRADES_CSV,"r",encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            rows_all.append(row)
            try:
                if datetime.fromisoformat(row["ts"]) >= since:
                    rows_24.append(row)
            except: pass
    def block(title, rows):
        if not rows: return f"• {title}: aucun trade"
        n,w,l,be,avg,total,best,worst,wr = summarize(rows)
        lines=[f"• {title}: {n} clos | Winrate {wr:.1f}%", f"  P&L {total:+.2f}% | RR moy x{avg:.2f}"]
        if best:  lines.append(f"  Best {best['symbol']} {float(best['pnl_pct']):+.2f}%")
        if worst: lines.append(f"  Worst {worst['symbol']} {float(worst['pnl_pct']):+.2f}%")
        return "\n".join(lines)
    local_now = datetime.now(ZoneInfo(TZ)).strftime('%d %b %Y %H:%M')
    tg_send(f"📊 *Stats* — {local_now} ({TZ})\n" + block("24h", rows_24) + "\n" + block("Total", rows_all), kind="stat")

# =========================
# TELEGRAM COMMANDS
# =========================
_last_update_id = None
_paused = False
_version = os.getenv("APP_VERSION","v1")

def fmt_duration(sec):
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"

def poll_telegram_commands(ex, active_paper):
    global _last_update_id, _paused
    TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","")
    TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID","")
    if not TG_TOKEN or not TG_CHAT: 
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
        if _last_update_id is not None: url += f"?offset={_last_update_id+1}"
        data = requests.get(url, timeout=6).json()
        if not data.get("ok"): return
        for upd in data.get("result", []):
            _last_update_id = upd["update_id"]
            msg = upd.get("message") or upd.get("edited_message")
            if not msg: continue
            if str(msg["chat"]["id"]) != str(TG_CHAT): continue
            text = (msg.get("text") or "").strip().lower()

            if text.startswith("/start"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(f"🚀 Bot Bitget — Darwin-BB v{_version}\nMode *{mode}* • TF *{TF}* • Risk *{int(RISK_PER_TRADE*100)}%* • RR≥*{MIN_RR}*", kind="info")

            elif text.startswith("/config"):
                tg_send(f"*Config*\nMode: {'PAPER' if DRY_RUN else ('LIVE' if not BITGET_TESTNET else 'TESTNET')}\nTF: {TF}\nTop: {UNIVERSE_SIZE} | Picks/h: {PICKS_PER_HOUR}\nRisk: {int(RISK_PER_TRADE*100)}% | RR≥{MIN_RR}\nCSV: {TRADES_CSV}", kind="info")

            elif text.startswith("/stats"):
                send_stats()

            elif text.startswith("/mode"):
                tg_send(f"Mode actuel: {'PAPER' if DRY_RUN else ('LIVE' if not BITGET_TESTNET else 'TESTNET')}", kind="info")

            elif text.startswith("/report"):
                # compact
                tg_send("🧾 Rapport (compact) — le chat conserve *trades* et *signaux* du *jour*.", kind="info")

            elif text.startswith("/exportcsv"):
                # envoie juste un résumé (le vrai envoi de fichier demanderait sendDocument)
                tg_send(f"CSV: {TRADES_CSV}", kind="info")

            elif text.startswith("/orders"):
                if DRY_RUN:
                    if not active_paper:
                        tg_send("Aucune position (papier).", kind="info")
                    else:
                        lines=["*Positions (papier)*"]
                        now = datetime.utcnow()
                        for sym, p in active_paper.items():
                            dur = fmt_duration((now - p['ts']).total_seconds())
                            lines.append(f"• {sym} {p['side']} | entry {p['entry']:.6f} | SL {p['sl']:.6f} | TP {p['tp']:.6f} | RR x{p['rr']:.2f} | {dur}")
                        tg_send("\n".join(lines), kind="info")
                else:
                    try:
                        pos = ex.fetch_positions()
                        rows=[]
                        for p in pos:
                            size = float(p.get("contracts") or 0)
                            if abs(size)>0:
                                sym=p.get("symbol"); s = p.get("side") or ("long" if size>0 else "short")
                                rows.append(f"• {sym} {s} | qty {abs(size)}")
                        tg_send("*Positions réelles*\n" + ("\n".join(rows) if rows else "Aucune position."), kind="info")
                    except Exception as e:
                        tg_send(f"⚠️ Impossible de lire les positions : {e}", kind="info")

            elif text.startswith("/test"):
                tg_send("Mode PAPER: utilisez les signaux automatiques (entrée à l’ouverture H1).", kind="info")

            elif text.startswith("/closeallpaper"):
                n=len(active_paper); active_paper.clear()
                tg_send(f"🛑 {n} positions (papier) fermées.", kind="info")

            elif text.startswith("/pause"):
                _paused = True
                tg_send("⏸️ Bot en *pause* (scan interrompu).", kind="info")

            elif text.startswith("/resume"):
                _paused = False
                tg_send("▶️ Bot *repris*.", kind="info")

            elif text.startswith("/logs"):
                tg_send("Logs: (suppression des logs verbeux — seuls les *trades/signaux* sont notifiés).", kind="info")

            elif text.startswith("/ping"):
                tg_send("📡 Ping ok.", kind="info")

            elif text.startswith("/version"):
                tg_send(f"Version {_version}", kind="info")

            elif text.startswith("/restart"):
                tg_send("♻️ Redémarrage demandé…", kind="info")

            elif text.startswith("/purge"):
                purge_chat(keep_kinds=("signal","trade"))

    except Exception:
        # ne spam pas
        pass

# =========================
# NOTIFICATIONS
# =========================
def notify_signal(symbol, sig):
    side  = "LONG" if sig["side"]=="buy" else "SHORT"
    bullets = "\n".join([f"• {n}" for n in sig.get("notes",[])])
    tg_send(
        f"📈 *Signal {'[PAPER]' if DRY_RUN else ''}* [{symbol}] {side}\n"
        f"Entrée `{sig['entry']:.6f}` | SL `{sig['sl']:.6f}` | TP `{sig['tp']:.6f}`\n"
        f"RR x{sig['rr']:.2f}\n{bullets}",
        kind="signal"
    )

def notify_order_ok(symbol, side, qty, rr, regime):
    tg_send(f"🎯 {'PAPER ' if DRY_RUN else ''}{symbol} {side.upper()} qty *{qty:.6f}* (RR={rr:.2f}, {regime})", kind="trade")

def notify_close(symbol, pnl, rr):
    emo = "✅" if pnl>=0 else "❌"
    tg_send(f"{emo} *Trade clos {'[PAPER]' if DRY_RUN else ''}* {symbol}  P&L `{pnl:+.2f}%`  |  RR `x{rr:.2f}`", kind="trade")

def notify_error(context, err):
    tg_send(f"🧯 *Erreur* `{context}`\n{err}", kind="info")

# =========================
# MAIN
# =========================
def main():
    ex = create_exchange()
    mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
    tg_send(f"🔔 Démarrage — {mode} • TF {TF} • Risk {int(RISK_PER_TRADE*100)}% • RR≥{MIN_RR}", kind="info")

    universe = build_universe(ex)
    for s in universe:
        try_set_leverage(ex, s, leverage=2, mode="cross")

    state = {}
    active_paper = {}
    last_ts_seen = {}
    pending_entries = {}  # sym -> {"wait": n_bars, "ref_ts": ts, "sig": sig}

    while True:
        try:
            nightly_signals_purge()
            poll_telegram_commands(ex, active_paper)
            if _paused:
                time.sleep(LOOP_DELAY)
                continue

            # Ouvertures/fermetures PAPER intra-barre (BE/TP/SL) — si tu le souhaites, on peut réactiver
            # Ici, on se concentre sur les signaux/entrées uniquement à l'ouverture H1 -> donc rien intra-barre.

            # Scan
            # 1) Décrément des pending UNIQUEMENT quand une nouvelle bougie se ferme
            for sym in list(universe):
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, limit=300)
                except Exception:
                    continue

                last_ts = df.index[-1]

                # si nouvelle bougie close, décrémenter les pending
                p = pending_entries.get(sym)
                if p and p["ref_ts"] != last_ts:
                    p["wait"] -= 1
                    p["ref_ts"]  = last_ts

                # signal UNIQUEMENT à la CLÔTURE d’une nouvelle bougie
                if last_ts_seen.get(sym) == last_ts:
                    continue  # pas de nouveau close => pas de signal

                # nouvelle clôture détectée
                last_ts_seen[sym] = last_ts
                sig = detect_signal(df, state, sym)
                if sig:
                    # file d’attente : entrée à l’ouverture suivante
                    pending_entries[sym] = {"wait": sig["wait_bars"], "ref_ts": last_ts, "sig": sig}

            # 2) Sélectionner les meilleurs signaux à l’ouverture (wait==0), max PICKS_PER_HOUR
            candidates=[]
            for sym, p in pending_entries.items():
                if p["wait"] <= 0:
                    candidates.append((sym, p["sig"]))
            # trier par RR
            candidates.sort(key=lambda x: x[1]["rr"], reverse=True)
            # limiter aux 4 meilleurs
            candidates = candidates[:PICKS_PER_HOUR]

            # vérifier slots disponibles
            open_cnt = len(active_paper) if DRY_RUN else count_open_positions_real(ex)
            slots_avail = max(0, MAX_OPEN_TRADES - open_cnt)
            if slots_avail <= 0:
                # rien à faire, on purge ceux qui de toute façon étaient prêts
                for sym,_ in candidates: pending_entries.pop(sym, None)
            else:
                to_take = candidates[:slots_avail]
                for sym, sig in to_take:
                    notify_signal(sym, sig)

                    # risk sizing
                    try:
                        usdt = 1000.0 if DRY_RUN else float(ex.fetch_balance().get("USDT", {}).get("free", 0))
                    except Exception:
                        usdt = 1000.0 if DRY_RUN else 0.0
                    risk_amt = max(1.0, usdt*RISK_PER_TRADE)
                    qty = compute_qty(sig["entry"], sig["sl"], risk_amt)
                    if qty <= 0:
                        pending_entries.pop(sym, None)
                        continue

                    if DRY_RUN:
                        notify_order_ok(sym, sig["side"], qty, sig["rr"], sig["regime"])
                        active_paper[sym] = {
                            "entry":sig["entry"], "side":sig["side"], "regime":sig["regime"],
                            "sl":sig["sl"], "tp":sig["tp"], "rr":sig["rr"], "qty":qty,
                            "ts":datetime.utcnow()
                        }
                    else:
                        try:
                            ex.create_order(sym, "market", sig["side"], qty)
                            notify_order_ok(sym, sig["side"], qty, sig["rr"], sig["regime"])
                        except Exception as e:
                            notify_error("order", e)

                    pending_entries.pop(sym, None)

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            tg_send("⛔ Arrêt manuel.", kind="info")
            break
        except Exception as e:
            notify_error("loop", f"{e}\n{traceback.format_exc()}")
            time.sleep(5)

if __name__ == "__main__":
    main()
