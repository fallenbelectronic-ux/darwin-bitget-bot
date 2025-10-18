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

VERSION = "Darwin-Bitget v1.12"

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
POSITION_MODE    = os.getenv("POSITION_MODE", "cross").lower()  # cross/isolated

# SL “pro”
ATR_WINDOW       = 14
SL_ATR_CUSHION   = 0.25       # 0.25*ATR au-delà des mèches

# Réaction rapide tendance
QUICK_BARS       = 3          # <= 3 barres
QUICK_PROGRESS   = 0.30       # >= 30% vers TP80 sinon prise 50% sur MM(BB20)

# Rapports programmés
REPORT_HOUR        = int(os.getenv("REPORT_HOUR", "19"))
REPORT_WEEKLY_HOUR = int(os.getenv("REPORT_WEEKLY_HOUR", "19"))
REPORT_WEEKDAY     = int(os.getenv("REPORT_WEEKDAY", "6"))  # dim
TRADES_CSV         = os.getenv("TRADES_CSV", "/app/trades.csv")
TZ                 = os.getenv("TIMEZONE", "Europe/Lisbon")

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

FALLBACK_TESTNET = [
    "BTC/USDT:USDT","ETH/USDT:USDT","LTC/USDT:USDT","BCH/USDT:USDT","XRP/USDT:USDT"
]

# =======================
# Logs circulaires pour /logs
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
    """Essaye de positionner levier et mode de marge (cross/isolated)."""
    try:
        # ccxt bitget: set_leverage(symbol, leverage, params)
        params = {}
        if margin_mode in ("cross","isolated"):
            params["marginMode"] = margin_mode
        ex.set_leverage(lev, symbol, params)
        log(f"[LEV OK] {symbol} levier {lev}x {margin_mode}")
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
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)

    # BB blanche = 20/2
    bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb20_mid"] = bb20.bollinger_mavg()
    df["bb20_up"]  = bb20.bollinger_hband()
    df["bb20_lo"]  = bb20.bollinger_lband()

    # BB jaune = 80/2 (superposée à H1)
    bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb80_mid"] = bb80.bollinger_mavg()
    df["bb80_up"]  = bb80.bollinger_hband()
    df["bb80_lo"]  = bb80.bollinger_lband()

    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()

    return df

# =======================
# UNIVERS (Top N)
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
        candidates = []
        for m in ex.markets.values():
            if (
                (m.get("type")=="swap" or m.get("swap")) and
                m.get("linear") and
                m.get("settle")=="USDT" and m.get("quote")=="USDT" and
                m.get("symbol")
            ):
                candidates.append(m["symbol"])
    except Exception as e:
        log("[UNIVERSE] load_markets failed:", e)
        candidates = []

    rows=[]
    try:
        tickers = ex.fetch_tickers(candidates if candidates else None)
        for s,t in tickers.items():
            if "/USDT" not in s and ":USDT" not in s: 
                continue
            vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try: vol=float(vol or 0.0)
            except: vol=0.0
            if MIN_VOLUME_USDT<=0 or vol>=MIN_VOLUME_USDT:
                rows.append((s, vol))
    except Exception as e:
        log("[UNIVERSE] fetch_tickers failed:", e)

    if rows:
        df = pd.DataFrame(rows, columns=["symbol","volume"]).sort_values("volume", ascending=False)
        universe = df.head(UNIVERSE_SIZE)["symbol"].tolist()
        head10 = df.head(10).copy()
        head10["volume"]=head10["volume"].round(0).astype(int)
        preview = ", ".join([f"{r.symbol}:{r.volume}" for r in head10.itertuples(index=False)])
        log(f"[UNIVERSE] size={len(universe)}")
        tg_send(f"📊 Univers LIVE top10: {preview}")
        if BITGET_TESTNET:
            u2 = filter_working_symbols(ex, universe[:20], timeframe=TF)
            if u2:
                tg_send(f"🧪 Testnet marchés OK: {', '.join(u2)}")
                return u2[:UNIVERSE_SIZE]
            log("[UNIVERSE] testnet: aucun symbole valide -> fallback")
        return universe

    # fallback testnet
    if candidates:
        fb = [s for s in FALLBACK_TESTNET if s in candidates] or FALLBACK_TESTNET
    else:
        fb = FALLBACK_TESTNET
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
def prolonged_double_exit(df, lookback=6):
    """
    True si *avant* la bougie précédente on a eu >=3 bougies consécutives
    dont les extrêmes étaient au-delà des 2 bandes (BB20 & BB80).
    Si la bougie d'« intégration » est déjà *à l’intérieur* de BB20, elle ne compte pas.
    """
    cnt = 0
    side = None
    # on ne regarde que les barres *avant* prev
    for i in range(-lookback-2, -2):
        r = df.iloc[i]
        up_both = (r["high"]>=r["bb20_up"]) and (r["high"]>=r["bb80_up"])
        lo_both = (r["low"] <=r["bb20_lo"]) and (r["low"] <=r["bb80_lo"])
        if up_both:
            cnt = cnt+1 if side in (None,"up") else 1; side="up"
        elif lo_both:
            cnt = cnt+1 if side in (None,"down") else 1; side="down"
        else:
            cnt=0; side=None
    return cnt>=3

def candle_inside_bb20(c):
    return (c["close"] <= c["bb20_up"]) and (c["close"] >= c["bb20_lo"])

def strong_reaction(prev, last, side):
    """
    Réaction du prix (schéma : contact/traversée -> réaction 1–2 bougies).
    On encode simplement : pinbar/méchage significatif ou grande
    bougie impulsive dans le sens.
    """
    body_prev = abs(prev["close"]-prev["open"])
    range_prev= prev["high"]-prev["low"] + 1e-12
    wick_ratio = 1.0 - (body_prev/range_prev)  # proche de 1 = beaucoup de mèche

    impulsive = abs(last["close"]-last["open"]) > 0.6*(last["high"]-last["low"]+1e-12)
    wick_ok = wick_ratio>0.4

    if side=="buy":
        # réintégration depuis le bas -> last >= mid20 et/ou mèche basse marquée avant
        return (last["close"]>=prev["bb20_mid"]) or wick_ok or impulsive
    else:
        return (last["close"]<=prev["bb20_mid"]) or wick_ok or impulsive

def detect_signal(df, state=None, sym=None):
    if len(df)<4: 
        return None

    last  = df.iloc[-1]    # bougie qui vient de CLORE
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]

    # Condition obligatoire : la bougie de signal DOIT clôturer *à l’intérieur* de la BB20
    if not candle_inside_bb20(last):
        return None

    above80 = last["close"] >= last["bb80_mid"]

    # Réintégrations (fenêtre 1–2 bougies) + réaction
    reinteg_long  = ((prev["low"]  <= min(prev["bb20_lo"], prev["bb80_lo"])) and strong_reaction(prev,last,"buy")) \
                    or ((prev2["low"] <= min(prev2["bb20_lo"], prev2["bb80_lo"])) and strong_reaction(prev2,last,"buy"))

    reinteg_short = ((prev["high"] >= max(prev["bb20_up"], prev["bb80_up"])) and strong_reaction(prev,last,"sell")) \
                    or ((prev2["high"]>= max(prev2["bb20_up"], prev2["bb80_up"])) and strong_reaction(prev2,last,"sell"))

    long_trend  =  above80 and reinteg_long
    short_trend = (not above80) and reinteg_short

    # Règle : premier trade après une sortie prolongée -> SKIP
    if state is not None and sym is not None:
        st = state.setdefault(sym, {"cooldown":False})
        if st.get("cooldown", False):
            st["cooldown"] = False
            return None
        if prolonged_double_exit(df):
            st["cooldown"] = True
            return None

    if long_trend:
        side, regime = "buy", "trend"
        notes = ["Tendance: au-dessus *BB80* + réintégration *BB20 bas* + réaction"]
    elif short_trend:
        side, regime = "sell","trend"
        notes = ["Tendance: sous *BB80* + réintégration *BB20 haut* + réaction"]
    elif reinteg_long:
        side, regime = "buy","counter"
        notes = ["Contre-tendance: réintégration *BB20 bas* + réaction"]
    elif reinteg_short:
        side, regime = "sell","counter"
        notes = ["Contre-tendance: réintégration *BB20 haut* + réaction"]
    else:
        return None

    entry=float(last["close"])
    atr=float(last["atr"])

    if side=="buy":
        sl = float(prev["low"]) - SL_ATR_CUSHION*atr
        # TP dyn : sur BB opposée avec léger offset avant la borne
        tp = float(last["bb80_up"] if regime=="trend" else last["bb20_up"]) - max(entry*0.0001,0.01)*2
    else:
        sl = float(prev["high"]) + SL_ATR_CUSHION*atr
        tp = float(last["bb80_lo"] if regime=="trend" else last["bb20_lo"]) + max(entry*0.0001,0.01)*2

    rr = abs((tp-entry)/(entry-sl)) if entry!=sl else 0
    if rr < MIN_RR:
        return None

    return {"side":side,"regime":regime,"entry":entry,"sl":sl,"tp":tp,"rr":rr,"notes":notes}

# =======================
# POSITIONS réelles
# =======================
def has_open_position_real(ex, symbol):
    try:
        pos = ex.fetch_positions([symbol])
        for p in pos:
            if abs(float(p.get("contracts") or 0))>0:
                return True
        return False
    except: 
        return False

def count_open_positions_real(ex):
    try:
        pos = ex.fetch_positions()
        return sum(1 for p in pos if abs(float(p.get("contracts") or 0))>0)
    except:
        return 0

def compute_qty(entry, sl, risk_amount):
    diff=abs(entry-sl)
    return risk_amount/diff if diff>0 else 0.0

# =======================
# Historique / CSV / Stats
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

# Paper book (avec IDs)
PAPER_ID_SEQ = 1
paper_by_id = {}        # id -> dict
paper_by_symbol = {}    # sym -> id

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
    global _last_update_id, PAUSED, RESTART_REQUESTED, PAPER_ID_SEQ
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

            # --- Commands ---
            if low.startswith("/start"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(f"🤖 {VERSION}\nMode: *{mode}* • TF {TF} • Risk {int(RISK_PER_TRADE*100)}% • RR≥{MIN_RR}\nTape /help pour la liste complète.")

            elif low.startswith("/help"):
                tg_send(
                    "*Commandes*\n"
                    "/start — lance le bot\n"
                    "/config — affiche la config\n"
                    "/stats — perfs 24h/total\n"
                    "/mode — montre le mode (LIVE/TESTNET/PAPER)\n"
                    "/report — rapport quotidien immédiat\n"
                    "/exportcsv — envoie le journal CSV\n"
                    "/orders — positions ouvertes (papier/réel)\n"
                    "/test — ouvre un trade *papier*\n"
                    "/closepaper <id> — ferme un trade papier\n"
                    "/closeallpaper — ferme tout le papier\n"
                    "/pause — met en pause l’exécution\n"
                    "/resume — relance\n"
                    "/logs — 30 lignes de logs\n"
                    "/ping — test connexion\n"
                    "/version — version du bot\n"
                    "/restart — redémarre le worker"
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
                if DRY_RUN:
                    if not paper_by_id:
                        tg_send("Aucune position *papier*.")
                    else:
                        lines=["*Positions papier*"]
                        now = datetime.utcnow()
                        for pid, p in paper_by_id.items():
                            dur = fmt_duration((now-p["ts"]).total_seconds())
                            side = "LONG" if p["side"]=="buy" else "SHORT"
                            lines.append(
                                f"#{pid} {p['symbol']} {side} | entry {p['entry']:.4f} | SL {p['sl']:.4f} | TP {p['tp']:.4f} | RR x{p['rr']:.2f} | {dur}"
                            )
                        tg_send("\n".join(lines))
                else:
                    try:
                        pos = ex.fetch_positions()
                        rows=[]
                        for p in pos:
                            size=float(p.get("contracts") or 0)
                            if abs(size)>0:
                                rows.append(f"• {p.get('symbol')} {p.get('side')} qty {abs(size)}")
                        tg_send("*Positions réelles*\n" + ("\n".join(rows) if rows else "Aucune position."))
                    except Exception as e:
                        tg_send(f"⚠️ Lecture positions impossible: {e}")

            elif low.startswith("/test"):
                if not DRY_RUN:
                    tg_send("ℹ️ Le mode /test nécessite DRY_RUN=true.")
                    continue
                parts = text.split()
                # /test [SYMBOL] [buy/sell] [qty]
                sym = parts[1] if len(parts)>1 else "BTC/USDT:USDT"
                side= parts[2].lower() if len(parts)>2 else "buy"
                qty = float(parts[3]) if len(parts)>3 else 1.0
                # prix actuel
                try:
                    px = float(ex.fetch_ticker(sym)["last"])
                except Exception:
                    tg_send(f"⚠️ Symbol invalide pour /test: {sym}")
                    continue
                tick = max(px*0.0001,0.01)
                sl = px - 10*tick if side=="buy" else px + 10*tick
                tp = px + 30*tick if side=="buy" else px - 30*tick
                rr = abs((tp-px)/(px-sl))
                # enregistre papier
                pid = PAPER_ID_SEQ; PAPER_ID_SEQ += 1
                paper_by_id[pid] = {
                    "id":pid, "symbol":sym, "side":side, "regime":"manual",
                    "entry":px, "sl":sl, "tp":tp, "rr":rr, "qty":qty, "ts":datetime.utcnow(),
                    "be_applied":False, "partial_done":False
                }
                paper_by_symbol[sym] = pid
                tg_send(f"🧪 Test papier ouvert #{pid} {sym} {side.upper()} @ {px:.4f} RR~x{rr:.2f}")

            elif low.startswith("/closepaper"):
                if not DRY_RUN:
                    tg_send("ℹ️ /closepaper n’est dispo qu’en DRY_RUN.")
                    continue
                parts = text.split()
                if len(parts)<2:
                    tg_send("Usage: /closepaper <id> [prix]")
                    continue
                try:
                    pid = int(parts[1])
                except:
                    tg_send("Id invalide.")
                    continue
                pos = paper_by_id.get(pid)
                if not pos:
                    tg_send("Introuvable.")
                    continue
                price = None
                if len(parts)>=3:
                    try: price=float(parts[2])
                    except: price=None
                if price is None:
                    try: price=float(ex.fetch_ticker(pos["symbol"])["last"])
                    except: price=pos["entry"]
                pnl = log_trade_close(pos["symbol"], pos["side"], pos["regime"], pos["entry"], price, pos["rr"], "manual", "paper")
                paper_by_symbol.pop(pos["symbol"], None)
                paper_by_id.pop(pid, None)
                tg_send(f"✅ Papier #{pid} clos {pos['symbol']} P&L {pnl:+.2f}%")

            elif low.startswith("/closeallpaper"):
                if not DRY_RUN:
                    tg_send("ℹ️ /closeallpaper n’est dispo qu’en DRY_RUN.")
                    continue
                n=len(paper_by_id)
                paper_by_id.clear(); paper_by_symbol.clear()
                tg_send(f"🧹 {n} positions papier fermées.")

            elif low.startswith("/pause"):
                PAUSED = True
                tg_send("⏸️ Bot en pause. (/resume pour relancer)")

            elif low.startswith("/resume"):
                PAUSED = False
                tg_send("▶️ Reprise de l’exécution.")

            elif low.startswith("/logs"):
                tg_send_codeblock(LOG_BUFFER[-30:] or ["(vide)"])

            elif low.startswith("/ping"):
                try:
                    ex.load_markets()
                    tg_send("🏓 Ping OK.")
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
# Notifications
# =======================
def notify_signal(symbol, sig):
    emoji = "📈" if sig["regime"]=="trend" else "🔄"
    side  = "LONG" if sig["side"]=="buy" else "SHORT"
    paper = " [PAPER]" if DRY_RUN else ""
    bullets = "\n".join([f"• {n}" for n in sig.get("notes",[])])
    tg_send(f"{emoji} *Signal{paper}* `{symbol}` {side}\nEntrée `{sig['entry']:.4f}` | SL `{sig['sl']:.4f}` | TP `{sig['tp']:.4f}`\nRR x{sig['rr']:.2f}\n{bullets}")

def notify_close(symbol, pnl, rr):
    emo = "✅" if pnl>=0 else "❌"
    paper = " [PAPER]" if DRY_RUN else ""
    tg_send(f"{emo} *Trade clos{paper}* `{symbol}`  P&L `{pnl:+.2f}%`  |  RR `x{rr:.2f}`")

# =======================
# MAIN
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
    state = {}                 # pour cooldown "premier trade après sortie prolongée"
    last_bar_seen = {}         # dernier timestamp H1 vu
    active_paper = {}          # (déprécié au profit de paper_by_id/paper_by_symbol) — conservé pour compat
    # Set levier en avance (meilleure chance que ça passe)
    for s in universe:
        try_set_leverage(ex, s, MAX_LEVERAGE, POSITION_MODE)

    while True:
        try:
            if RESTART_REQUESTED:
                log("[RESTART] demandé par Telegram — exit(0)")
                time.sleep(1)
                os._exit(0)

            poll_telegram_commands(ex)
            if PAUSED:
                time.sleep(1)
                continue

            maybe_send_daily_report()
            maybe_send_weekly_report()

            # solde USDT (si dispo)
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

            open_cnt = len(paper_by_id) if DRY_RUN else count_open_positions_real(ex)

            for sym in list(universe):
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, 300)
                except Exception as e:
                    log("[WARN] fetch_ohlcv", sym, e)
                    continue

                last_ts = df.index[-1]
                # n’AGIT QU’A LA *CLOTURE* d’une nouvelle H1
                if last_bar_seen.get(sym) == last_ts:
                    continue

                last_bar_seen[sym] = last_ts

                sig = detect_signal(df, state=state, sym=sym)
                if not sig: 
                    continue

                # Slots disponibles
                if open_cnt >= MAX_OPEN_TRADES:
                    continue
                if not DRY_RUN and has_open_position_real(ex, sym):
                    continue

                # Notifier le signal
                notify_signal(sym, sig)

                # Taille
                risk_amt = max(1.0, usdt*RISK_PER_TRADE) if not DRY_RUN else 1000.0*RISK_PER_TRADE
                qty = compute_qty(sig["entry"], sig["sl"], risk_amt)
                if qty<=0:
                    continue

                # Levier
                try_set_leverage(ex, sym, MAX_LEVERAGE, POSITION_MODE)

                # Entrée: PAPER vs REAL
                if DRY_RUN:
                    pid = globals().get("PAPER_ID_SEQ", 1)
                    globals()["PAPER_ID_SEQ"] = pid+1
                    paper_by_id[pid] = {
                        "id":pid, "symbol":sym, "side":sig["side"], "regime":sig["regime"],
                        "entry":sig["entry"], "sl":sig["sl"], "tp":sig["tp"], "rr":sig["rr"], "qty":qty,
                        "ts":datetime.utcnow(), "be_applied":False, "partial_done":False
                    }
                    paper_by_symbol[sym] = pid
                    tg_send(f"🎯 *PAPER* {sym} {sig['side'].upper()} @`{sig['entry']:.4f}` RR={sig['rr']:.2f}")
                    open_cnt += 1
                else:
                    try:
                        ex.create_order(sym, "market", sig["side"], qty)
                        tg_send(f"🎯 {sym} {sig['side'].upper()} envoyé (réel) qty `{qty:.6f}`")
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
