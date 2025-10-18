# main.py
import os, time, csv, math, requests
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands, AverageTrueRange
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from notifier import tg_send

load_dotenv()

# =======================
# ENV & PARAMÈTRES
# =======================
BITGET_TESTNET     = os.getenv("BITGET_TESTNET", "true").lower() in ("1","true","yes")
API_KEY            = os.getenv("BITGET_API_KEY")
API_SECRET         = os.getenv("BITGET_API_SECRET")
PASSPHRASE         = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF                 = os.getenv("TIMEFRAME", "1h")          # H1 unique
RISK_PER_TRADE     = float(os.getenv("RISK_PER_TRADE", "0.01"))  # 1%
MIN_RR             = float(os.getenv("MIN_RR", "3"))       # RR mini 1:3
MAX_OPEN_TRADES    = int(os.getenv("MAX_OPEN_TRADES", "4"))
LOOP_DELAY         = int(os.getenv("LOOP_DELAY", "5"))
UNIVERSE_SIZE      = int(os.getenv("UNIVERSE_SIZE", "100"))
MIN_VOLUME_USDT    = float(os.getenv("MIN_VOLUME_USDT", "0"))

# Levier / mode de position
MAX_LEVERAGE       = int(os.getenv("MAX_LEVERAGE", "2"))
POSITION_MODE      = os.getenv("POSITION_MODE", "cross")  # cross | isolated

# SL & logique Darwin
ATR_WINDOW         = 14
SL_ATR_CUSHION     = 0.25     # 0.25 * ATR au-delà de la mèche

# “Réaction rapide” en tendance (alt 50% si trop lent)
QUICK_BARS         = 3        # doit avancer vite en <= 3 barres
QUICK_PROGRESS     = 0.30     # >= 30% du chemin vers TP

# Paper trading
DRY_RUN            = os.getenv("DRY_RUN", "false").lower() in ("1","true","yes")
TRADES_CSV         = os.getenv("TRADES_CSV", "/app/trades.csv")

# Rapports Telegram (heure locale TZ)
REPORT_HOUR        = int(os.getenv("REPORT_HOUR", "19"))           # 19h locales
REPORT_WEEKLY_HOUR = int(os.getenv("REPORT_WEEKLY_HOUR", "19"))    # 19h locales
REPORT_WEEKDAY     = int(os.getenv("REPORT_WEEKDAY", "6"))         # 0=lundi ... 6=dimanche
TZ                 = os.getenv("TIMEZONE", "Europe/Lisbon")

TG_TOKEN           = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID         = os.getenv("TELEGRAM_CHAT_ID", "")

# Fallback testnet
FALLBACK_TESTNET = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "LTC/USDT:USDT",
    "BCH/USDT:USDT", "XRP/USDT:USDT"
]

# Sortie prolongée : nb de bougies consécutives "dehors" (avant réintégration) pour activer le cooldown
PROLONGED_MIN_BARS = int(os.getenv("PROLONGED_MIN_BARS", "4"))

# =======================
# EXCHANGE
# =======================
def create_exchange():
    ex = ccxt.bitget({
        "apiKey": API_KEY, "secret": API_SECRET, "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}
    })
    if BITGET_TESTNET:
        try: ex.set_sandbox_mode(True); print("[INFO] Bitget sandbox mode ON (testnet)")
        except Exception as e: print("[WARN] set_sandbox_mode not available:", e)
    else:
        print("[INFO] Bitget LIVE mode")
    return ex

def try_set_leverage(ex, symbol, leverage=2, mode="cross"):
    """Essaie de configurer levier & mode pour le symbole (non bloquant si non supporté)."""
    try:
        # Certaines gateways CCXT exposent hedge/oneway. Ici on n'impose pas si non supporté.
        try:
            ex.set_position_mode(False)  # False => oneway ; True => hedge (selon l'exchange)
        except Exception:
            pass
        ex.set_leverage(leverage, symbol, params={"marginMode": mode.lower()})
        print(f"[LEVERAGE] {symbol}: {mode} x{leverage}")
    except Exception as e:
        print(f"[LEVERAGE] unable to set {symbol}: {e}")

# =======================
# DATA / INDICATEURS
# =======================
def fetch_ohlcv_df(ex, symbol, timeframe="1h", limit=500):
    """
    H1 unique :
      - BB blanche (rapide) : 20 / 2 sur H1
      - BB jaune  (lente)  : 80 / 2 sur H1 (proxy de BB(20/2) H4)
      - ATR(14)
    """
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms"); df.set_index("ts", inplace=True)

    # BB blanche 20/2
    bb_fast = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb_fast_mid"]   = bb_fast.bollinger_mavg()
    df["bb_fast_upper"] = bb_fast.bollinger_hband()
    df["bb_fast_lower"] = bb_fast.bollinger_lband()

    # BB jaune 80/2
    bb_slow = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb_slow_mid"]   = bb_slow.bollinger_mavg()
    df["bb_slow_upper"] = bb_slow.bollinger_hband()
    df["bb_slow_lower"] = bb_slow.bollinger_lband()

    # ATR
    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()
    return df

# =======================
# UNIVERS (Top 100) + Filtrage TESTNET
# =======================
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
    print("[UNIVERSE] building top by 24h volume...")
    try:
        ex.load_markets()
        candidates = [m["symbol"] for m in ex.markets.values()
                      if (m.get("type")=="swap" or m.get("swap")) and m.get("linear")
                      and m.get("settle")=="USDT" and m.get("quote")=="USDT"]
    except Exception as e:
        print("[UNIVERSE] load_markets failed:", e)
        candidates = []

    rows=[]
    try:
        tickers = ex.fetch_tickers(candidates if candidates else None)
        for s,t in tickers.items():
            if "/USDT" not in s and ":USDT" not in s: continue
            vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try: vol=float(vol or 0.0)
            except: vol=0.0
            if MIN_VOLUME_USDT<=0 or vol>=MIN_VOLUME_USDT:
                rows.append((s, vol))
    except Exception as e:
        print("[UNIVERSE] fetch_tickers failed:", e)

    if rows:
        df = pd.DataFrame(rows, columns=["symbol","volume"]).sort_values("volume", ascending=False)
        universe = df.head(UNIVERSE_SIZE)["symbol"].tolist()
        head10 = df.head(10).copy(); head10["volume"] = head10["volume"].round(0).astype(int)
        preview = ", ".join([f"{r.symbol}:{r.volume}" for r in head10.itertuples(index=False)])
        tg_send(f"📊 Univers LIVE top10: {preview}")
        if BITGET_TESTNET:
            u2 = filter_working_symbols(ex, universe[:20], timeframe=TF)
            if u2:
                tg_send(f"🧪 Testnet marchés OK: {', '.join(u2)}")
                return u2[:UNIVERSE_SIZE]
            else:
                print("[UNIVERSE] testnet: aucun symbole valide parmi le top -> fallback")
        return universe

    print("[UNIVERSE] empty after volume filter, using fallback list (testnet)")
    fb = [s for s in FALLBACK_TESTNET if s in candidates] or FALLBACK_TESTNET
    universe = filter_working_symbols(ex, fb, timeframe=TF)
    if not universe:
        probe = filter_working_symbols(ex, candidates[:30], timeframe=TF)
        universe = probe or fb
    universe = universe[:max(1, min(UNIVERSE_SIZE, len(universe)))]
    tg_send(f"🧪 Univers TESTNET: {', '.join(universe)}")
    return universe

# =======================
# RÈGLES DARWIN
# =======================
def prolonged_exit_count(df, side, look_from=-2):
    """
    Compte le nombre de bougies consécutives AVANT la bougie courante (réintégration potentielle),
    où le prix est en dehors des DEUX bandes du même côté.
    - On démarre à l'index look_from=-2, donc la bougie de réintégration (index -1) N'EST PAS comptée.
    """
    cnt = 0
    idx = look_from
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

def detect_prolonged_exit_event(df):
    """
    Détecte un *événement* de sortie prolongée qui vient de se clore :
    - au moins PROLONGED_MIN_BARS bougies consécutives AU-DEHORS des deux bandes (côté haut OU bas),
    - puis la dernière bougie (-1) revient *à l'intérieur* (réintégration dans le canal des bandes).
    Renvoie True/False (on ne décide pas du sens ici).
    """
    if len(df) < PROLONGED_MIN_BARS + 2:
        return False

    last = df.iloc[-1]
    # La réintégration = la dernière bougie n'est PAS en-dehors des deux bandes
    last_out_up   = (last["high"] >= max(last["bb_fast_upper"], last["bb_slow_upper"]))
    last_out_down = (last["low"]  <= min(last["bb_fast_lower"], last["bb_slow_lower"]))
    if last_out_up or last_out_down:
        return False  # encore dehors -> pas une réintégration

    # Vérifie une séquence prolongée côté haut OU bas juste avant
    up_cnt   = prolonged_exit_count(df, side="sell", look_from=-2)   # côté haut = signaux sell potentiels
    down_cnt = prolonged_exit_count(df, side="buy",  look_from=-2)   # côté bas  = signaux buy potentiels
    return (up_cnt >= PROLONGED_MIN_BARS) or (down_cnt >= PROLONGED_MIN_BARS)

def detect_signal(df):
    """
    Retourne None ou dict {side, regime, entry, stop, tp, rr, atr}
    - Réintégration possible sur 1–2 barres
    - Entrée à l’ouverture de la bougie suivante (gérée dans la loop)
    """
    if len(df) < 3: return None
    last  = df.iloc[-1]   # bougie qui vient de clore (réintégration éventuelle)
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]

    above_slow_mid = last["close"] >= last["bb_slow_mid"]

    # Réintégrations 1 ou 2 barres (sur BB blanche & jaune)
    reinteg_long_1  = (prev["low"]  <= min(prev["bb_fast_lower"], prev["bb_slow_lower"])) and (last["close"] > last["bb_fast_lower"])
    reinteg_short_1 = (prev["high"] >= max(prev["bb_fast_upper"], prev["bb_slow_upper"])) and (last["close"] < last["bb_fast_upper"])
    reinteg_long_2  = (prev2["low"]  <= min(prev2["bb_fast_lower"], prev2["bb_slow_lower"])) and (last["close"] > last["bb_fast_lower"])
    reinteg_short_2 = (prev2["high"] >= max(prev2["bb_fast_upper"], prev2["bb_slow_upper"])) and (last["close"] < last["bb_fast_upper"])

    # Tendance (au-dessus/de-sous la lente + contact/réintégration BB blanche)
    long_trend  = (above_slow_mid and (prev["low"]  <= prev["bb_fast_lower"] or prev2["low"]  <= prev2["bb_fast_lower"]) and (last["close"] > last["bb_fast_lower"]))
    short_trend = ((not above_slow_mid) and (prev["high"] >= prev["bb_fast_upper"] or prev2["high"] >= prev2["bb_fast_upper"]) and (last["close"] < last["bb_fast_upper"]))

    # Contre-tendance
    long_ct  = reinteg_long_1 or reinteg_long_2
    short_ct = reinteg_short_1 or reinteg_short_2

    side = regime = None
    if long_trend:   side, regime = "buy",  "trend"
    elif short_trend:side, regime = "sell", "trend"
    elif long_ct:    side, regime = "buy",  "counter"
    elif short_ct:   side, regime = "sell", "counter"
    else: return None

    entry = float(last["close"]); atr = float(last["atr"])
    tick  = max(entry * 0.0001, 0.01)

    # SL : mèche +/- cushion ATR
    if side=="buy":
        raw_sl = float(last["low"]) - 2*tick
        sl = min(raw_sl, float(prev["low"])) - SL_ATR_CUSHION*atr
    else:
        raw_sl = float(last["high"]) + 2*tick
        sl = max(raw_sl, float(prev["high"])) + SL_ATR_CUSHION*atr

    # TP : tendance -> BB80 opposée ; contre-tendance -> BB20 opposée
    if regime=="trend":
        tp = float(last["bb_slow_upper"] if side=="buy" else last["bb_slow_lower"])
    else:
        tp = float(last["bb_fast_upper"] if side=="buy" else last["bb_fast_lower"])

    denom = abs(entry - sl)
    rr = abs((tp - entry)/denom) if denom>0 else 0.0
    if rr < MIN_RR: return None

    return {"side":side,"regime":regime,"entry":entry,"stop":sl,"tp":tp,"rr":rr,"atr":atr}

# =======================
# OUTILS ORDRES / POSITIONS
# =======================
def has_open_position_real(ex, symbol):
    try:
        pos = ex.fetch_positions([symbol])
        for p in pos:
            if abs(float(p.get("contracts") or 0))>0: return True
        return False
    except Exception:
        return False

def count_open_positions_real(ex):
    try:
        pos = ex.fetch_positions()
        return sum(1 for p in pos if abs(float(p.get("contracts") or 0))>0)
    except Exception:
        return 0

def compute_qty(entry, sl, risk_amount):
    diff = abs(entry - sl)
    return risk_amount/diff if diff>0 else 0.0

def place_market(ex, symbol, side, qty):
    return ex.create_order(symbol, "market", side, qty)

# =======================
# JOURNAL & RAPPORTS
# =======================
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

def _summarize(rows):
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

def daily_report():
    ensure_trades_csv()
    since = datetime.utcnow() - timedelta(days=1)
    rows=[]
    with open(TRADES_CSV,"r",encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            try:
                if datetime.fromisoformat(row["ts"]) >= since:
                    rows.append(row)
            except: pass
    if not rows:
        tg_send("🧭 Rapport quotidien\nAucun trade clos sur 24h.")
        return
    n,w,l,be,avg_rr,total,best,worst,wr = _summarize(rows)
    msg = (
        f"🧭 *Rapport quotidien* — {datetime.now(ZoneInfo(TZ)).strftime('%d %b %Y %H:%M')} ({TZ})\n"
        f"• Trades clos : {n}\n"
        f"• Gagnants : {w} | Perdants : {l} | BE : {be}\n"
        f"• Winrate : {wr:.1f}%\n"
        f"• P&L total : {total:+.2f}%\n"
        f"• RR moyen : x{avg_rr:.2f}\n"
    )
    if best:  msg += f"• Meilleur : {best['symbol']} {float(best['pnl_pct']):+.2f}%\n"
    if worst: msg += f"• Pire    : {worst['symbol']} {float(worst['pnl_pct']):+.2f}%\n"
    tg_send(msg)

def weekly_report():
    ensure_trades_csv()
    since = datetime.utcnow() - timedelta(days=7)
    rows=[]
    with open(TRADES_CSV,"r",encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            try:
                if datetime.fromisoformat(row["ts"]) >= since:
                    rows.append(row)
            except: pass
    if not rows:
        tg_send("📒 Rapport hebdo\nAucun trade clos sur 7 jours.")
        return
    n,w,l,be,avg_rr,total,best,worst,wr = _summarize(rows)
    msg = (
        f"📒 *Rapport hebdomadaire* — semaine {datetime.now(ZoneInfo(TZ)).isocalendar().week}\n"
        f"• Trades clos : {n}\n"
        f"• Gagnants : {w} | Perdants : {l} | BE : {be}\n"
        f"• Winrate : {wr:.1f}%\n"
        f"• P&L total : {total:+.2f}%\n"
        f"• RR moyen : x{avg_rr:.2f}\n"
    )
    if best:  msg += f"• Meilleur : {best['symbol']} {float(best['pnl_pct']):+.2f}%\n"
    if worst: msg += f"• Pire    : {worst['symbol']} {float(worst['pnl_pct']):+.2f}%\n"
    tg_send(msg)

_last_report_day = None
_last_week_key   = None
def maybe_send_daily_report():
    global _last_report_day
    now = datetime.now(ZoneInfo(TZ))
    if now.hour==REPORT_HOUR and (_last_report_day!=now.date()):
        _last_report_day = now.date()
        try: daily_report()
        except Exception as e: tg_send(f"⚠️ Rapport quotidien échoué : {e}")

def maybe_send_weekly_report():
    global _last_week_key
    now = datetime.now(ZoneInfo(TZ))
    week_key = f"{now.isocalendar().year}-{now.isocalendar().week}"
    if now.weekday()==REPORT_WEEKDAY and now.hour==REPORT_WEEKLY_HOUR and _last_week_key!=week_key:
        _last_week_key = week_key
        try: weekly_report()
        except Exception as e: tg_send(f"⚠️ Rapport hebdo échoué : {e}")

# =======================
# TELEGRAM COMMANDS (light)
# =======================
_last_update_id = None
def fmt_duration(sec):
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"

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
        n,w,l,be,avg,total,best,worst,wr = _summarize(rows)
        lines=[f"• {title}: {n} clos | Winrate {wr:.1f}%", f"  P&L {total:+.2f}% | RR moy x{avg:.2f}"]
        if best:  lines.append(f"  Best {best['symbol']} {float(best['pnl_pct']):+.2f}%")
        if worst: lines.append(f"  Worst {worst['symbol']} {float(worst['pnl_pct']):+.2f}%")
        return "\n".join(lines)
    local_now = datetime.now(ZoneInfo(TZ)).strftime('%d %b %Y %H:%M')
    tg_send(f"📊 *Stats* — {local_now} ({TZ})\n" + block("24h", rows_24) + "\n" + block("Total", rows_all))

def poll_telegram_commands():
    global _last_update_id
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
        if _last_update_id is not None: url += f"?offset={_last_update_id+1}"
        data = requests.get(url, timeout=6).json()
        if not data.get("ok"): return
        for upd in data.get("result", []):
            _last_update_id = upd["update_id"]
            msg = upd.get("message") or upd.get("edited_message")
            if not msg: continue
            if str(msg["chat"]["id"]) != str(TG_CHAT_ID): continue
            text = (msg.get("text") or "").strip().lower()

            if text.startswith("/start"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(f"🔔 Bot démarré — {mode} — H1 — Risk {int(RISK_PER_TRADE*100)}% — RR≥{MIN_RR}")

            elif text.startswith("/config"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(
                    "*Config*\n"
                    f"Mode: {mode}\n"
                    f"TF: {TF}\n"
                    f"Risk: {int(RISK_PER_TRADE*100)}% | RR:min {MIN_RR}\n"
                    f"Max trades: {MAX_OPEN_TRADES}\n"
                    f"Rapports: {REPORT_HOUR}h / hebdo {REPORT_WEEKLY_HOUR}h dim — TZ {TZ}"
                )
            elif text.startswith("/stats"):
                send_stats()
            elif text.startswith("/report"):
                daily_report()
            elif text.startswith("/weekly"):
                weekly_report()
    except Exception:
        pass

# =======================
# NOTIFS
# =======================
def notify_signal(symbol, sig):
    emoji = "📈" if sig["regime"]=="trend" else "🔄"
    side  = "LONG" if sig["side"]=="buy" else "SHORT"
    paper = " [PAPER]" if DRY_RUN else ""
    tg_send(f"{emoji} *Signal{paper}* `{symbol}` {side}\nEntrée `{sig['entry']:.4f}` | SL `{sig['stop']:.4f}` | TP `{sig['tp']:.4f}`\nRR x{sig['rr']:.2f}")

def notify_close(symbol, pnl, rr):
    emo = "✅" if pnl>=0 else "❌"
    paper = " [PAPER]" if DRY_RUN else ""
    tg_send(f"{emo} *Trade clos{paper}* `{symbol}`  P&L `{pnl:+.2f}%`  |  RR `x{rr:.2f}`")

def notify_error(context, err):
    tg_send(f"⚠️ *Erreur* `{context}`\n{err}")

# =======================
# MAIN LOOP (entrées à l’ouverture)
# =======================
def main():
    ex = create_exchange()
    mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
    tg_send(f"🤖 Darwin H1 — BB20/2 & BB80/2 — Risk {int(RISK_PER_TRADE*100)}% — RR min {MIN_RR} — {mode}")

    if not API_KEY or not API_SECRET or not PASSPHRASE:
        tg_send("❌ BITGET_API_* manquantes"); return

    time.sleep(2)

    universe = build_universe(ex)
    # Essaye d’appliquer levier/mode par défaut (non bloquant)
    for sym in universe[:10]:
        try_set_leverage(ex, sym, leverage=MAX_LEVERAGE, mode=POSITION_MODE)

    last_bar_time = {}
    pending = {}           # {symbol: {"wait": 1, "ref_ts": last_ts, "sig": sig}}
    active_paper = {}      # positions simulées : sym -> dict
    cooldown = {}          # {symbol: True/False} — skip le 1er trade après un événement prolongé

    while True:
        try:
            maybe_send_daily_report()
            maybe_send_weekly_report()
            poll_telegram_commands()

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
                print("[WARN] fetch_balance:", e); usdt_free = 0.0

            open_cnt = (len(active_paper) if DRY_RUN else count_open_positions_real(ex))
            slots = max(0, MAX_OPEN_TRADES - open_cnt)

            if not universe:
                universe = build_universe(ex)

            # Détection à la clôture H1
            for sym in universe:
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, limit=300)
                    last_ts = df.index[-1]

                    # décrément pending quand une nouvelle bougie se clôt
                    if sym in pending:
                        p = pending[sym]
                        if p["ref_ts"] != last_ts:
                            p["wait"] -= 1
                            p["ref_ts"] = last_ts

                    # Traiter sur nouvelle clôture uniquement
                    if last_bar_time.get(sym) == last_ts:
                        continue
                    last_bar_time[sym] = last_ts

                    # 1) Détecter un *événement* de sortie prolongée -> activer cooldown (persistant)
                    if detect_prolonged_exit_event(df):
                        cooldown[sym] = True
                        tg_send(f"⏳ Cooldown activé `{sym}` après *sortie prolongée* (skip 1er trade futur).")
                        # on continue, mais on regarde aussi s'il y a un signal cette barre (qu'on ne prendra pas si cooldown actif)

                    # 2) Détecter signal
                    sig = detect_signal(df)
                    if not sig:
                        continue

                    # 3) Si cooldown actif => on SKIP ce 1er trade puis on désactive le cooldown
                    if cooldown.get(sym, False):
                        cooldown[sym] = False
                        tg_send(f"🙅‍♂️ Skip 1er trade `{sym}` (post prolongée). Cooldown terminé.")
                        continue

                    # Sinon, on programme l'entrée à l'ouverture suivante (wait=1)
                    pending[sym] = {"wait": 1, "ref_ts": last_ts, "sig": sig}
                    notify_signal(sym, sig)

                except Exception as e:
                    print(f"[ERROR] scan {sym}:", e)

            # Entrées à l’ouverture (wait == 0)
            to_delete=[]
            for sym, p in list(pending.items()):
                if p["wait"]>0: 
                    continue
                if slots<=0: 
                    continue
                if not DRY_RUN and has_open_position_real(ex, sym):
                    to_delete.append(sym); 
                    continue

                s = p["sig"]
                risk_amount = max(1.0, usdt_free * RISK_PER_TRADE)
                qty = round(compute_qty(s["entry"], s["stop"], risk_amount), 6)
                if qty<=0:
                    to_delete.append(sym); 
                    continue

                if DRY_RUN:
                    active_paper[sym] = {
                        "entry":s["entry"], "side":s["side"], "regime":s["regime"],
                        "sl":s["stop"], "tp":s["tp"], "rr":s["rr"], "qty":qty,
                        "ts":datetime.utcnow(), "be_applied":False, "partial_done":False
                    }
                    tg_send(f"🎯 [PAPER] {sym} {s['side'].upper()} @{s['entry']:.4f} RR={s['rr']:.2f} ({s['regime']})")
                    slots -= 1
                else:
                    try:
                        place_market(ex, sym, s["side"], qty)
                        tg_send(f"✅ {sym} {s['side'].upper()} @{s['entry']:.4f} RR={s['rr']:.2f} ({s['regime']})")
                        slots -= 1
                    except Exception as e:
                        print("[ERROR] market:", e)
                        tg_send(f"⚠️ Ordre market échoué {sym}: {e}")
                        to_delete.append(sym)
                        continue

                to_delete.append(sym)

            for sym in to_delete:
                pending.pop(sym, None)

            # Gestion de clôture papier simple (TP/SL/BE)
            if DRY_RUN and active_paper:
                for sym in list(active_paper.keys()):
                    try:
                        df = fetch_ohlcv_df(ex, sym, TF, limit=5)
                        last = df.iloc[-1]
                        pos = active_paper[sym]
                        price = float(last["close"])

                        # Contre-tendance : BE à la MM blanche
                        if pos["regime"]=="counter" and not pos["be_applied"]:
                            if (pos["side"]=="buy" and price>=float(last["bb_fast_mid"])) or \
                               (pos["side"]=="sell" and price<=float(last["bb_fast_mid"])):
                                pos["be_applied"]=True
                                tg_send(f"🛡️ BE (papier) sur `{sym}` à `{pos['entry']:.4f}` (contre-tendance)")

                        # Alt 50% si lente en tendance
                        if pos["regime"]=="trend" and not pos["partial_done"]:
                            dist_full = abs(pos["tp"]-pos["entry"])
                            dist_now  = abs(price-pos["entry"])
                            if dist_full>0 and (dist_now/dist_full)<QUICK_PROGRESS:
                                pos["partial_done"]=True
                                tg_send(f"✂️ Alt 50% (papier) `{sym}` sur MM(BB20)")

                        hit_tp = (price>=pos["tp"] if pos["side"]=="buy" else price<=pos["tp"])
                        hit_sl = (price<=pos["sl"] if pos["side"]=="buy" else price>=pos["sl"])
                        hit_be = pos["be_applied"] and ((price<=pos["entry"] and pos["side"]=="buy") or (price>=pos["entry"] and pos["side"]=="sell"))
                        if hit_tp or hit_sl or hit_be:
                            exit_price = pos["tp"] if hit_tp else (pos["entry"] if hit_be else pos["sl"])
                            result = "be" if hit_be else ("win" if hit_tp else "loss")
                            pnl = log_trade_close(sym, pos["side"], pos["regime"], pos["entry"], exit_price, pos["rr"], result, "paper")
                            notify_close(sym, pnl, pos["rr"])
                            del active_paper[sym]
                    except Exception as e:
                        print("[PAPER CLOSE] error", e)

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            tg_send("⛔ Arrêt manuel du bot."); break
        except Exception as e:
            notify_error("loop", e); time.sleep(5)

if __name__ == "__main__":
    main()
