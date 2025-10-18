# -*- coding: utf-8 -*-
import os, time, csv, math, requests, traceback
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands, AverageTrueRange
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from notifier import tg_send, tg_send_document, tg_send_codeblock

load_dotenv()
VERSION = "Darwin-Bitget v1.14"

# =======================
# ENV
# =======================
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1","true","yes")
DRY_RUN          = os.getenv("DRY_RUN", "true").lower() in ("1","true","yes")
API_KEY          = os.getenv("BITGET_API_KEY", "")
API_SECRET       = os.getenv("BITGET_API_SECRET", "")
PASSPHRASE       = os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASE", "")

TF               = os.getenv("TIMEFRAME", "1h")
RISK_PER_TRADE   = float(os.getenv("RISK_PER_TRADE", "0.01"))
MIN_RR           = float(os.getenv("MIN_RR", "3"))
MAX_OPEN_TRADES  = int(os.getenv("MAX_OPEN_TRADES", "4"))
LOOP_DELAY       = int(os.getenv("LOOP_DELAY", "5"))
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE", "100"))
MIN_VOLUME_USDT  = float(os.getenv("MIN_VOLUME_USDT", "0"))
MAX_LEVERAGE     = int(os.getenv("MAX_LEVERAGE", "2"))
POSITION_MODE    = os.getenv("POSITION_MODE", "cross").lower()

ATR_WINDOW       = 14
SL_ATR_CUSHION   = 0.25
BAND_TOL         = 0.0006
TP_TICKS         = 2

REPORT_HOUR        = int(os.getenv("REPORT_HOUR", "19"))
REPORT_WEEKLY_HOUR = int(os.getenv("REPORT_WEEKLY_HOUR", "19"))
REPORT_WEEKDAY     = int(os.getenv("REPORT_WEEKDAY", "6"))
TRADES_CSV         = os.getenv("TRADES_CSV", "/app/trades.csv")
TZ                 = os.getenv("TIMEZONE", "Europe/Lisbon")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

FALLBACK_TESTNET = ["BTC/USDT:USDT","ETH/USDT:USDT","XRP/USDT:USDT","LTC/USDT:USDT","BCH/USDT:USDT"]

# =======================
# Logs circulaires (/logs)
# =======================
LOG_BUFFER = []
def log(*args):
    line = " ".join(str(a) for a in args)
    print(line, flush=True)
    LOG_BUFFER.append(f"{datetime.utcnow().isoformat()} {line}")
    if len(LOG_BUFFER) > 300:
        del LOG_BUFFER[:len(LOG_BUFFER)-300]

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
            log("[INFO] Bitget sandbox mode ON (testnet)")
        except Exception as e:
            log("[WARN] set_sandbox_mode not available:", e)
    else:
        log("[INFO] Bitget LIVE mode")
    return ex

def try_set_leverage(ex, symbol, lev=2, margin_mode="cross"):
    """Règle le levier sans spammer les logs (log seulement en échec)."""
    try:
        params = {}
        if margin_mode in ("cross","isolated"):
            params["marginMode"] = margin_mode
        ex.set_leverage(lev, symbol, params)
        return True
    except Exception as e:
        log(f"[LEV WARN] {symbol}: levier non appliqué ->", e)
        return False

# =======================
# DATA / INDICATEURS (H1)
# =======================
def fetch_ohlcv_df(ex, symbol, timeframe="1h", limit=300):
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    if len(df) < 50:
        raise ValueError(f"[DATA] {symbol}: OHLCV insuffisant ({len(df)} barres)")
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)

    # BB20/2 (blanche)
    bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb20_mid"] = bb20.bollinger_mavg()
    df["bb20_up"]  = bb20.bollinger_hband()
    df["bb20_lo"]  = bb20.bollinger_lband()

    # BB80/2 (jaune)
    bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb80_mid"] = bb80.bollinger_mavg()
    df["bb80_up"]  = bb80.bollinger_hband()
    df["bb80_lo"]  = bb80.bollinger_lband()

    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()
    return df

# =======================
# UNIVERS (Top volume)
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
    log("[UNIVERSE] building top by 24h volume...")
    try:
        ex.load_markets()
        candidates = [m["symbol"] for m in ex.markets.values()
                      if (m.get("type")=="swap" or m.get("swap")) and m.get("linear")
                      and m.get("settle")=="USDT" and m.get("quote")=="USDT"]
    except Exception as e:
        log("[UNIVERSE] load_markets failed:", e)
        candidates=[]

    rows=[]
    try:
        tickers = ex.fetch_tickers(candidates if candidates else None)
        for s,t in tickers.items():
            if "/USDT" not in s and ":USDT" not in s: 
                continue
            vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try: vol=float(vol or 0.0)
            except: vol=0.0
            rows.append((s, vol))
    except Exception as e:
        log("[UNIVERSE] fetch_tickers failed:", e)

    if rows:
        df = pd.DataFrame(rows, columns=["symbol","volume"]).sort_values("volume", ascending=False)
        uni = df.head(UNIVERSE_SIZE)["symbol"].tolist()
        if BITGET_TESTNET:
            uni = filter_working_symbols(ex, uni[:20], timeframe=TF) or FALLBACK_TESTNET
            tg_send("🧪 *Testnet actifs* : " + ", ".join(uni))
        else:
            preview = ", ".join([f"{r.symbol}:{int(r.volume)}" for r in df.head(10).itertuples(index=False)])
            tg_send(f"📊 *Univers LIVE (Top10)*\n{preview}")
        return uni

    uni = filter_working_symbols(ex, FALLBACK_TESTNET, timeframe=TF)
    tg_send("🧪 *Univers TESTNET* : " + ", ".join(uni))
    return uni

# =======================
# PATTERNS (Réaction du prix)
# =======================
def is_pinbar_bull(c):
    rng = c["high"] - c["low"]
    if rng <= 0: return False
    lower = min(c["open"], c["close"]) - c["low"]
    return (lower / rng) >= 0.30

def is_pinbar_bear(c):
    rng = c["high"] - c["low"]
    if rng <= 0: return False
    upper = c["high"] - max(c["open"], c["close"])
    return (upper / rng) >= 0.30

def is_double_marubozu(prev, last):
    rb = abs(prev["close"]-prev["open"])
    rl = prev["high"]-prev["low"] + 1e-12
    sb = abs(last["close"]-last["open"])
    sl = last["high"]-last["low"] + 1e-12
    return (rb/rl >= 0.7) and (sb/sl >= 0.7)

def has_impulsion_gap(prev, last, side):
    # Heuristique simple
    if side=="buy":
        return (last["open"] >= prev["close"]) and (last["close"] > last["open"])
    else:
        return (last["open"] <= prev["close"]) and (last["close"] < last["open"])

def pattern_ok(prev, last, side):
    if side=="buy":
        return is_pinbar_bull(last) or is_double_marubozu(prev,last) or has_impulsion_gap(prev,last,"buy")
    else:
        return is_pinbar_bear(last) or is_double_marubozu(prev,last) or has_impulsion_gap(prev,last,"sell")

# =======================
# CONDITIONS (les 3 obligatoires)
# =======================
def candle_inside_bb20(c):
    return (c["close"] <= c["bb20_up"]) and (c["close"] >= c["bb20_lo"])

def touched_lower(c):
    tol20 = c["bb20_lo"]*BAND_TOL
    tol80 = c["bb80_lo"]*BAND_TOL
    return (c["low"] <= c["bb20_lo"]+tol20) or (c["low"] <= c["bb80_lo"]+tol80)

def touched_upper(c):
    tol20 = c["bb20_up"]*BAND_TOL
    tol80 = c["bb80_up"]*BAND_TOL
    return (c["high"] >= c["bb20_up"]-tol20) or (c["high"] >= c["bb80_up"]-tol80)

def prolonged_double_exit(df, min_bars=4):
    """>=4 barres consécutives totalement hors des 2 bandes, avant la bougie de signal."""
    cnt=0
    for i in range(-6,-1):  # regarde un peu en arrière
        r = df.iloc[i]
        outside_both = ((r["high"]>=r["bb20_up"] and r["high"]>=r["bb80_up"]) or
                        (r["low"] <=r["bb20_lo"] and r["low"] <=r["bb80_lo"]))
        inside20 = candle_inside_bb20(r)
        if outside_both and not inside20:
            cnt += 1
        else:
            cnt = 0
    return cnt >= min_bars

def detect_signal(df, state, sym):
    """
    RIGIDE : on ne valide un signal QUE si les 3 conditions sont vraies :
    (1) Contact (prev ou prev2) • (2) Réaction + clôture last DANS BB20 • (3) RR ≥ 3.
    Détection UNIQUEMENT à la clôture H1 (appelée par la boucle après nouvelle bougie).
    """
    if len(df) < 5: 
        return None

    last  = df.iloc[-1]   # bougie qui vient de clôturer (bougie de signal)
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]

    # 0) Bougie de signal doit clôturer DANS BB20
    if not candle_inside_bb20(last):
        return None

    # 00) Sortie prolongée -> ignorer le PREMIER signal qui suit (cooldown)
    st = state.setdefault(sym, {"cooldown": False})
    if st.get("cooldown", False):
        st["cooldown"] = False
        return None
    if prolonged_double_exit(df):
        st["cooldown"] = True
        return None

    # 1) CONTACT sur prev OU prev2
    contact_low  = touched_lower(prev)  or touched_lower(prev2)
    contact_high = touched_upper(prev)  or touched_upper(prev2)

    # 2) RÉACTION (pattern fort) sur la bougie de signal (last)
    long_react  = pattern_ok(prev, last, "buy")
    short_react = pattern_ok(prev, last, "sell")

    # Direction & régime (BB80_mid)
    above80 = last["close"] >= last["bb80_mid"]
    side=None; regime=None
    if contact_low and long_react:
        side = "buy";  regime = "trend" if above80 else "counter"
    elif contact_high and short_react:
        side = "sell"; regime = "trend" if not above80 else "counter"
    else:
        return None

    # 3) RR >= 3 (SL = mèche + 0.25 ATR ; TP = borne opposée -/+ offset)
    entry=float(last["close"]); atr=float(last["atr"])
    tick = max(entry*0.0001, 0.01)*TP_TICKS
    if side=="buy":
        sl = min(float(prev["low"]), float(prev2["low"])) - SL_ATR_CUSHION*atr
        tp = (last["bb80_up"] if regime=="trend" else last["bb20_up"]) - tick
    else:
        sl = max(float(prev["high"]), float(prev2["high"])) + SL_ATR_CUSHION*atr
        tp = (last["bb80_lo"] if regime=="trend" else last["bb20_lo"]) + tick

    rr = abs((tp-entry)/(entry-sl)) if entry!=sl else 0
    if rr < MIN_RR:
        return None

    notes = [
        "Contact bande" + (" basse" if side=="buy" else " haute"),
        "Réaction (pattern fort) & close dans BB20",
        f"RR x{rr:.2f} (≥ {MIN_RR})",
        "Tendance" if regime=="trend" else "Contre-tendance"
    ]
    return {"side":side,"regime":regime,"entry":entry,"sl":sl,"tp":tp,"rr":rr,"notes":notes}

# =======================
# POSITIONS / ORDRES
# =======================
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
    diff = abs(entry-sl)
    return risk_amount/diff if diff>0 else 0.0

# =======================
# CSV / RAPPORTS / STATS
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

def daily_report():
    ensure_trades_csv()
    since = datetime.utcnow()-timedelta(days=1)
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
    n,w,l,be,avg,total,best,worst,wr=summarize(rows)
    msg = (
        f"🧭 *Rapport quotidien* — {datetime.now(ZoneInfo(TZ)).strftime('%d %b %Y %H:%M')} ({TZ})\n"
        f"• Trades clos : {n}\n"
        f"• Gagnants : {w} | Perdants : {l} | BE : {be}\n"
        f"• Winrate : {wr:.1f}%\n"
        f"• P&L total : {total:+.2f}%\n"
        f"• RR moyen : x{avg:.2f}\n"
    )
    if best:  msg += f"• Meilleur : {best['symbol']} {float(best['pnl_pct']):+.2f}%\n"
    if worst: msg += f"• Pire    : {worst['symbol']} {float(worst['pnl_pct']):+.2f}%\n"
    tg_send(msg)

def weekly_report():
    ensure_trades_csv()
    since = datetime.utcnow()-timedelta(days=7)
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
    n,w,l,be,avg,total,best,worst,wr=summarize(rows)
    msg = (
        f"📒 *Rapport hebdomadaire* — semaine {datetime.now(ZoneInfo(TZ)).isocalendar().week}\n"
        f"• Trades clos : {n}\n"
        f"• Gagnants : {w} | Perdants : {l} | BE : {be}\n"
        f"• Winrate : {wr:.1f}%\n"
        f"• P&L total : {total:+.2f}%\n"
        f"• RR moyen : x{avg:.2f}\n"
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
    key = f"{now.isocalendar().year}-{now.isocalendar().week}"
    if now.weekday()==REPORT_WEEKDAY and now.hour==REPORT_WEEKLY_HOUR and key!=_last_week_key:
        _last_week_key = key
        try: weekly_report()
        except Exception as e: tg_send(f"⚠️ Rapport hebdo échoué : {e}")

# =======================
# Telegram commands
# =======================
_last_update_id = None
PAUSED = False
RESTART_REQUESTED = False

def fmt_duration(sec):
    m,s = divmod(int(sec),60); h,m = divmod(m,60)
    return f"{h}h{m:02d}m"

def send_stats():
    ensure_trades_csv()
    since = datetime.utcnow()-timedelta(days=1)
    rows24=[]; rows=[]
    with open(TRADES_CSV,"r",encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            rows.append(row)
            try:
                if datetime.fromisoformat(row["ts"])>=since:
                    rows24.append(row)
            except: pass
    def block(title, rows):
        if not rows: return f"• {title}: aucun trade"
        n,w,l,be,avg,total,best,worst,wr=summarize(rows)
        out=[f"• {title}: {n} clos | Winrate {wr:.1f}%", f"  P&L {total:+.2f}% | RR moy x{avg:.2f}"]
        if best:  out.append(f"  Best {best['symbol']} {float(best['pnl_pct']):+.2f}%")
        if worst: out.append(f"  Worst {worst['symbol']} {float(worst['pnl_pct']):+.2f}%")
        return "\n".join(out)
    local_now = datetime.now(ZoneInfo(TZ)).strftime('%d %b %Y %H:%M')
    tg_send(f"📊 *Stats* — {local_now} ({TZ})\n{block('24h',rows24)}\n{block('Total',rows)}")

def poll_telegram_commands(ex):
    global _last_update_id, PAUSED, RESTART_REQUESTED
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
        if _last_update_id is not None:
            url += f"?offset={_last_update_id+1}"
        data = requests.get(url, timeout=10).json()
        if not data.get("ok"):
            return
        for upd in data.get("result", []):
            _last_update_id = upd["update_id"]
            msg = upd.get("message") or upd.get("edited_message")
            if not msg: continue
            if str(msg["chat"]["id"]) != str(TG_CHAT_ID): 
                continue
            text = (msg.get("text") or "").strip()
            low  = text.lower()

            if low.startswith("/start"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(f"🤖 {VERSION}\nMode: *{mode}* • TF {TF} • Risk {int(RISK_PER_TRADE*100)}% • RR≥{MIN_RR}\nTape /help pour la liste complète.")

            elif low.startswith("/help"):
                tg_send(
                    "*Commandes*\n"
                    "/start — lance le bot\n"
                    "/config — affiche la config\n"
                    "/stats — perfs 24h/total\n"
                    "/mode — LIVE/TESTNET/PAPER\n"
                    "/report — rapport quotidien\n"
                    "/exportcsv — journal CSV\n"
                    "/orders — positions ouvertes\n"
                    "/pause — pause\n"
                    "/resume — reprise\n"
                    "/logs — 30 logs\n"
                    "/ping — test connexion (silence si OK)\n"
                    "/version — version\n"
                    "/restart — redémarre"
                )

            elif low.startswith("/config"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(
                    "*Config*\n"
                    f"Mode: {mode}\n"
                    f"TF: {TF} | Risk: {int(RISK_PER_TRADE*100)}% | RR≥{MIN_RR}\n"
                    f"Max trades: {MAX_OPEN_TRADES}\n"
                    f"Tz: {TZ} | Daily: {REPORT_HOUR}h | Weekly: dim {REPORT_WEEKLY_HOUR}h"
                )

            elif low.startswith("/mode"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(f"🧭 Mode actuel: *{mode}*")

            elif low.startswith("/stats"):
                send_stats()

            elif low.startswith("/report"):
                daily_report()

            elif low.startswith("/exportcsv"):
                ensure_trades_csv()
                tg_send_document(TRADES_CSV, "Journal des trades")

            elif low.startswith("/orders"):
                try:
                    pos = ex.fetch_positions()
                    rows=[]
                    for p in pos:
                        size=float(p.get("contracts") or 0)
                        if abs(size)>0:
                            rows.append(f"• {p.get('symbol')} {p.get('side')} qty {abs(size)}")
                    tg_send("*Positions*\n" + ("\n".join(rows) if rows else "Aucune position."))
                except Exception as e:
                    tg_send(f"⚠️ Lecture positions impossible: {e}")

            elif low.startswith("/pause"):
                PAUSED = True
                tg_send("⏸️ Pause activée. (/resume pour relancer)")

            elif low.startswith("/resume"):
                PAUSED = False
                tg_send("▶️ Reprise.")

            elif low.startswith("/logs"):
                tg_send_codeblock(LOG_BUFFER[-30:] or ["(vide)"])

            elif low.startswith("/ping"):
                try:
                    ex.load_markets()
                    # Silence si OK
                except Exception as e:
                    tg_send(f"🏓 Ping KO: {e}")

            elif low.startswith("/version"):
                tg_send(f"ℹ️ {VERSION}")

            elif low.startswith("/restart"):
                RESTART_REQUESTED = True
                tg_send("♻️ Redémarrage demandé…")

    except Exception as e:
        log("[TG POLL ERR]", e)

# =======================
# NOTIFS
# =======================
def notify_signal(symbol, sig):
    emoji = "📈" if sig["regime"]=="trend" else "🔄"
    side  = "LONG" if sig["side"]=="buy" else "SHORT"
    paper = " [PAPER]" if DRY_RUN else ""
    bullets = "\n".join([f"• {n}" for n in sig.get("notes",[])])
    tg_send(f"{emoji} *Signal{paper}* `{symbol}` {side}\nEntrée `{sig['entry']:.4f}` | SL `{sig['sl']:.4f}` | TP `{sig['tp']:.4f}`\nRR x{sig['rr']:.2f}\n{bullets}")

# =======================
# MAIN LOOP (1x/heure à la clôture)
# =======================
def main():
    ex = create_exchange()
    mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
    tg_send(f"🚀 Bot Bitget — {VERSION}\nMode *{mode}* • TF {TF} • Risk {int(RISK_PER_TRADE*100)}% • RR≥{MIN_RR}")

    if not API_KEY or not API_SECRET or not PASSPHRASE:
        log("[FATAL] Missing API keys")
        tg_send("❌ Clés API Bitget manquantes.")
        return

    universe = build_universe(ex)
    state = {}                 # cooldown par symbole
    last_bar_seen = {}         # dernière clôture H1 vue par symbole

    # Levier en avance (pas de spam logs)
    for s in universe:
        try_set_leverage(ex, s, MAX_LEVERAGE, POSITION_MODE)

    while True:
        try:
            if RESTART_REQUESTED:
                log("[RESTART] demandé — exit")
                time.sleep(1)
                os._exit(0)

            poll_telegram_commands(ex)
            if PAUSED:
                time.sleep(1); continue

            maybe_send_daily_report()
            maybe_send_weekly_report()

            # solde USDT
            try:
                bal = ex.fetch_balance()
                usdt = 0.0
                if isinstance(bal, dict):
                    if "USDT" in bal:
                        usdt = float(bal["USDT"].get("free", 0) or bal["USDT"].get("available", 0))
                    elif "free" in bal and isinstance(bal["free"], dict):
                        usdt = float(bal["free"].get("USDT", 0))
            except Exception as e:
                log("[WARN] fetch_balance:", e)
                usdt = 0.0

            open_cnt = count_open_positions_real(ex) if not DRY_RUN else 0

            # --- SCAN UNIQUEMENT À LA CLOTURE H1 ---
            for sym in list(universe):
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, 300)
                except Exception as e:
                    log("[WARN] fetch_ohlcv", sym, e)
                    continue

                last_ts = df.index[-1]
                if last_bar_seen.get(sym) == last_ts:
                    continue  # empêche tout double signal — 1 fois par heure
                last_bar_seen[sym] = last_ts

                sig = detect_signal(df, state=state, sym=sym)
                if not sig:
                    continue

                if open_cnt >= MAX_OPEN_TRADES:
                    continue
                if not DRY_RUN and has_open_position_real(ex, sym):
                    continue

                notify_signal(sym, sig)

                # Taille & levier
                risk_amt = max(1.0, usdt*RISK_PER_TRADE) if not DRY_RUN else 1000.0*RISK_PER_TRADE
                qty = compute_qty(sig["entry"], sig["sl"], risk_amt)
                if qty<=0:
                    continue
                try_set_leverage(ex, sym, MAX_LEVERAGE, POSITION_MODE)

                if DRY_RUN:
                    tg_send(f"🎯 *PAPER* {sym} {sig['side'].upper()} @`{sig['entry']:.4f}` RR={sig['rr']:.2f}")
                    open_cnt += 1
                else:
                    try:
                        ex.create_order(sym, "market", sig["side"], qty)
                        # placer TP/SL (reduceOnly)
                        opp = "sell" if sig["side"]=="buy" else "buy"
                        ex.create_order(sym, "limit", opp, qty, sig["tp"], {"reduceOnly": True})
                        ex.create_order(sym, "stop",  opp, qty, params={"stopPrice": sig["sl"], "reduceOnly": True})
                        tg_send(f"✅ Ordre réel placé {sym} qty `{qty:.6f}`")
                        open_cnt += 1
                    except Exception as e:
                        tg_send(f"⚠️ Ordre market échoué {sym}: {e}")

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            tg_send("⛔ Arrêt manuel.")
            break
        except Exception as e:
            log("[FATAL LOOP]", e, traceback.format_exc())
            tg_send(f"🚨 Fatal: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
