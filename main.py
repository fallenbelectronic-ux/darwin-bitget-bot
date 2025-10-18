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
# ENV
# =======================
BITGET_TESTNET     = os.getenv("BITGET_TESTNET", "true").lower() in ("1","true","yes")
API_KEY            = os.getenv("BITGET_API_KEY")
API_SECRET         = os.getenv("BITGET_API_SECRET")
PASSPHRASE         = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF                 = os.getenv("TIMEFRAME", "1h")
RISK_PER_TRADE     = float(os.getenv("RISK_PER_TRADE", "0.01"))
MIN_RR             = float(os.getenv("MIN_RR", "3"))
MAX_OPEN_TRADES    = int(os.getenv("MAX_OPEN_TRADES", "4"))
LOOP_DELAY         = int(os.getenv("LOOP_DELAY", "5"))
UNIVERSE_SIZE      = int(os.getenv("UNIVERSE_SIZE", "100"))

ATR_WINDOW         = 14
SL_ATR_CUSHION     = 0.25

QUICK_BARS         = 3
QUICK_PROGRESS     = 0.30

# Rapports (heure locale gérée par TZ)
REPORT_HOUR        = int(os.getenv("REPORT_HOUR", "19"))           # 19h locales
REPORT_WEEKLY_HOUR = int(os.getenv("REPORT_WEEKLY_HOUR", "19"))    # 19h locales
REPORT_WEEKDAY     = int(os.getenv("REPORT_WEEKDAY", "6"))         # 0=lundi ... 6=dimanche
TRADES_CSV         = os.getenv("TRADES_CSV", "/app/trades.csv")
TZ                 = os.getenv("TIMEZONE", "Europe/Lisbon")

FALLBACK_TESTNET   = ["BTC/USDT:USDT","ETH/USDT:USDT","XRP/USDT:USDT"]
DRY_RUN            = os.getenv("DRY_RUN", "false").lower() in ("1","true","yes")

TG_TOKEN           = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID         = os.getenv("TELEGRAM_CHAT_ID", "")

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
        try: ex.set_sandbox_mode(True)
        except Exception: pass
    return ex

# =======================
# DATA
# =======================
def fetch_ohlcv_df(ex, symbol, timeframe, limit=300):
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms"); df.set_index("ts", inplace=True)

    bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb20_mid"], df["bb20_up"], df["bb20_lo"] = bb20.bollinger_mavg(), bb20.bollinger_hband(), bb20.bollinger_lband()

    bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb80_mid"], df["bb80_up"], df["bb80_lo"] = bb80.bollinger_mavg(), bb80.bollinger_hband(), bb80.bollinger_lband()

    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()
    return df

def filter_working_symbols(ex, symbols, timeframe="1h"):
    ok=[]
    for s in symbols:
        try: ex.fetch_ohlcv(s, timeframe=timeframe, limit=2); ok.append(s)
        except Exception: pass
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
        for s,t in tickers.items():
            if "/USDT" not in s and ":USDT" not in s: continue
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
# RÈGLES DARWIN
# =======================
def prolonged_double_exit(df, lookback=6):
    side=None; count=0
    for i in range(-lookback-3, -1):
        r = df.iloc[i]
        up_both = (r["high"]>=r["bb20_up"]) and (r["high"]>=r["bb80_up"])
        lo_both = (r["low"] <=r["bb20_lo"]) and (r["low"] <=r["bb80_lo"])
        if up_both:
            count = count+1 if side in (None,"up") else 1; side="up"
        elif lo_both:
            count = count+1 if side in (None,"down") else 1; side="down"
        else:
            count=0; side=None
    return count>=3

def detect_signal(df, skip_first_after_prolonged=True, state=None, sym=None):
    if len(df)<3: return None
    last, prev = df.iloc[-1], df.iloc[-2]
    notes=[]
    above80 = last["close"]>=last["bb80_mid"]

    reinteg_long  = (prev["low"]  <= min(prev["bb20_lo"], prev["bb80_lo"])) and (last["close"] > last["bb20_lo"])
    reinteg_short = (prev["high"] >= max(prev["bb20_up"], prev["bb80_up"])) and (last["close"] < last["bb20_up"])

    long_trend  =  above80 and reinteg_long
    short_trend = (not above80) and reinteg_short

    if skip_first_after_prolonged and state is not None and sym is not None:
        if state.get(sym,{}).get("cooldown", False):
            state[sym]["cooldown"]=False
            return None
        if prolonged_double_exit(df):
            st=state.setdefault(sym,{}); st["cooldown"]=True
            return None

    if long_trend:
        side, regime = "buy","trend";   notes.append("Au-dessus *BB80* + réintégration *BB20 basse*")
    elif short_trend:
        side, regime = "sell","trend";  notes.append("Sous *BB80* + réintégration *BB20 haute*")
    elif reinteg_long:
        side, regime = "buy","counter"; notes.append("Contre-tendance : réintégration *BB20 basse*")
    elif reinteg_short:
        side, regime = "sell","counter";notes.append("Contre-tendance : réintégration *BB20 haute*")
    else:
        return None

    entry=float(last["close"]); atr=float(last["atr"])
    if side=="buy":
        sl=float(prev["low"])  - SL_ATR_CUSHION*atr
        tp=float(last["bb80_up"]) if regime=="trend" else float(last["bb20_up"])
    else:
        sl=float(prev["high"]) + SL_ATR_CUSHION*atr
        tp=float(last["bb80_lo"]) if regime=="trend" else float(last["bb20_lo"])

    rr = abs((tp-entry)/(entry-sl)) if entry!=sl else 0
    if rr < MIN_RR: return None
    return {"side":side,"regime":regime,"entry":entry,"sl":sl,"tp":tp,"rr":rr,"notes":notes}

# =======================
# ORDRES RÉELS
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
    diff=abs(entry-sl)
    return risk_amount/diff if diff>0 else 0

def place_bracket_orders(ex, symbol, side, qty, sl, tp):
    opp = "sell" if side=="buy" else "buy"
    try:
        ex.create_order(symbol, "market", side, qty, params={
            "stopLossPrice": sl, "takeProfitPrice": tp, "reduceOnly": False, "triggerType":"mark_price",
        })
        return "bracket"
    except Exception as e:
        print("[ORDERS] bracket failed:", e)
    ok=False
    try:
        ex.create_order(symbol, "stop", opp, qty, params={
            "stopPrice": sl, "triggerType":"mark_price", "reduceOnly": True
        }); ok=True
    except Exception as e:
        print("[ORDERS] stop failed:", e)
    try:
        ex.create_order(symbol, "limit", opp, qty, price=tp, params={"reduceOnly": True}); ok=True
    except Exception as e:
        print("[ORDERS] tp failed:", e)
    return "separate" if ok else "basic"

def modify_stop_to_be(ex, symbol, side, qty, be_price):
    opp = "sell" if side=="buy" else "buy"
    try:
        ex.create_order(symbol, "stop", opp, qty, params={
            "stopPrice": be_price, "triggerType":"mark_price", "reduceOnly": True
        }); return True
    except Exception as e:
        print("[ORDERS] BE stop failed:", e); return False

# =======================
# HISTORIQUE & RAPPORTS
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
    n,w,l,be,avg_rr,total,best,worst,wr = summarize(rows)
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
    n,w,l,be,avg_rr,total,best,worst,wr = summarize(rows)
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

# — déclencheurs à l’heure locale Lisbonne —
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
# TELEGRAM COMMANDS
# =======================
_last_update_id = None

def fmt_duration(sec):
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"

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

def poll_telegram_commands(ex, active_paper):
    """ /help /stats /open /config /panic /report /weekly """
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

            if text.startswith("/help"):
                tg_send(
                    "*Commandes*\n"
                    "/stats  — stats 24h & total\n"
                    "/open   — positions en cours\n"
                    "/config — paramètres clés\n"
                    "/panic  — fermeture d'urgence (papier/réel)\n"
                    "/report — envoyer le rapport quotidien maintenant\n"
                    "/weekly — envoyer le rapport hebdo maintenant"
                )

            elif text.startswith("/config"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(
                    "*Config*\n"
                    f"Mode: {mode}\n"
                    f"TF: {TF}\n"
                    f"Risk: {int(RISK_PER_TRADE*100)}% | RR:min {MIN_RR}\n"
                    f"Max trades: {MAX_OPEN_TRADES}\n"
                    f"Rapports: {REPORT_HOUR}h (quotidien), {REPORT_WEEKLY_HOUR}h dim (hebdo) — TZ: {TZ}"
                )

            elif text.startswith("/open"):
                if DRY_RUN:
                    if not active_paper:
                        tg_send("Aucune position (papier).")
                    else:
                        lines=["*Positions (papier)*"]
                        now = datetime.utcnow()
                        for sym, p in active_paper.items():
                            dur = fmt_duration((now - p["ts"]).total_seconds())
                            side = "LONG" if p["side"]=="buy" else "SHORT"
                            lines.append(
                                f"• {sym} {side} | entry {p['entry']:.4f} | SL {p['sl']:.4f} | TP {p['tp']:.4f} | RR x{p['rr']:.2f} | {dur}"
                            )
                        tg_send("\n".join(lines))
                else:
                    try:
                        pos = ex.fetch_positions()
                        rows=[]
                        for p in pos:
                            size = float(p.get("contracts") or 0)
                            if abs(size)>0:
                                sym=p.get("symbol"); s = p.get("side") or ("long" if size>0 else "short")
                                rows.append(f"• {sym} {s} | qty {abs(size)}")
                        tg_send("*Positions réelles*\n" + ("\n".join(rows) if rows else "Aucune position."))
                    except Exception as e:
                        tg_send(f"⚠️ Impossible de lire les positions : {e}")

            elif text.startswith("/panic"):
                if DRY_RUN:
                    n=len(active_paper); active_paper.clear()
                    tg_send(f"🛑 PANIC (papier) — {n} positions simulées effacées.")
                else:
                    try:
                        closed=0
                        pos = ex.fetch_positions()
                        for p in pos:
                            size = float(p.get("contracts") or 0)
                            if abs(size)>0:
                                sym = p.get("symbol")
                                side = "sell" if (p.get("side") in ("long","buy") or size>0) else "buy"
                                ex.create_order(sym, "market", side, abs(size), params={"reduceOnly": True})
                                closed+=1
                        tg_send(f"🛑 PANIC — ordres de clôture envoyés pour {closed} positions.")
                    except Exception as e:
                        tg_send(f"⚠️ PANIC échec : {e}")

            elif text.startswith("/stats"):
                send_stats()

            elif text.startswith("/report"):
                daily_report()

            elif text.startswith("/weekly"):
                weekly_report()

    except Exception:
        pass

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
    tg_send(f"📊 *Stats* — {local_now} ({TZ})\n" + block("24h", rows_24) + "\n" + block("Total", rows_all))

# =======================
# NOTIFS
# =======================
def notify_signal(symbol, sig):
    emoji = "📈" if sig["regime"]=="trend" else "🔄"
    side  = "LONG" if sig["side"]=="buy" else "SHORT"
    paper = " [PAPER]" if DRY_RUN else ""
    bullets = "\n".join([f"• {n}" for n in sig.get("notes",[])])
    tg_send(f"{emoji} *Signal{paper}* `{symbol}` {side}\nEntrée `{sig['entry']:.4f}` | SL `{sig['sl']:.4f}` | TP `{sig['tp']:.4f}`\nRR x{sig['rr']:.2f}\n{bullets}")

def notify_order_ok(symbol, side, qty, be_rule=None, tp_rule=None):
    side_txt = "LONG" if side=="buy" else "SHORT"
    paper = " [PAPER]" if DRY_RUN else ""
    out = [f"🎯 *Trade exécuté{paper}* `{symbol}` {side_txt}\nTaille `{qty:.6f}`"]
    if be_rule: out.append(f"• BE : {be_rule}")
    if tp_rule: out.append(f"• TP : {tp_rule}")
    tg_send("\n".join(out))

def notify_close(symbol, pnl, rr):
    emo = "✅" if pnl>=0 else "❌"
    paper = " [PAPER]" if DRY_RUN else ""
    tg_send(f"{emo} *Trade clos{paper}* `{symbol}`  P&L `{pnl:+.2f}%`  |  RR `x{rr:.2f}`")

def notify_error(context, err):
    tg_send(f"⚠️ *Erreur* `{context}`\n{err}")

# =======================
# MAIN
# =======================
def main():
    ex = create_exchange()
    mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
    tg_send(f"🔔 Bot démarré — {mode} — H1 — Risk {int(RISK_PER_TRADE*100)}% — RR≥{MIN_RR} — Rapports 19:00 {TZ}")

    universe = build_universe(ex)
    valid = set(universe)

    state = {}
    active_paper = {}  # sym -> {...}

    last_ts_seen = {}
    while True:
        try:
            maybe_send_daily_report()
            maybe_send_weekly_report()
            poll_telegram_commands(ex, active_paper)

            for sym in list(universe):
                if sym not in valid: continue

                # Données marché
                try: df = fetch_ohlcv_df(ex, sym, TF, 300)
                except Exception as e: print("[WARN] fetch_ohlcv:", e); continue

                last_ts = df.index[-1]
                same_bar = (last_ts_seen.get(sym) == last_ts)

                # --- Gestion positions PAPIER (surveillance intra-barre) ---
                if DRY_RUN and sym in active_paper:
                    pos = active_paper[sym]
                    last = df.iloc[-1]
                    price = float(last["close"])
                    # BE en contre-tendance
                    if pos["regime"]=="counter" and not pos["be_applied"]:
                        if (pos["side"]=="buy"  and price>=float(last["bb20_mid"])) or \
                           (pos["side"]=="sell" and price<=float(last["bb20_mid"])):
                            pos["be_applied"]=True
                            tg_send(f"🛡️ BE (papier) sur `{sym}` à `{pos['entry']:.4f}` (contre-tendance)")
                    # ALT 50% si tendance lente après 3H
                    if pos["regime"]=="trend" and not pos["partial_done"]:
                        elapsed_h = (datetime.utcnow() - pos["ts"]).total_seconds()/3600.0
                        dist_full = abs(pos["tp"]-pos["entry"]); dist_now=abs(price-pos["entry"])
                        if elapsed_h >= QUICK_BARS and dist_full>0 and (dist_now/dist_full)<QUICK_PROGRESS:
                            pos["partial_done"]=True
                            tg_send(f"✂️ Alt 50% (papier) `{sym}` à MM BB20 `{float(last['bb20_mid']):.4f}`")
                    # Clôtures papier
                    hit_tp = (price>=pos["tp"] if pos["side"]=="buy" else price<=pos["tp"])
                    hit_sl = (price<=pos["sl"] if pos["side"]=="buy" else price>=pos["sl"])
                    hit_be = pos["be_applied"] and ((price<=pos["entry"] and pos["side"]=="buy") or (price>=pos["entry"] and pos["side"]=="sell"))
                    if hit_tp or hit_sl or hit_be:
                        exit_price = pos["tp"] if hit_tp else (pos["entry"] if hit_be else pos["sl"])
                        result = "be" if hit_be else ("win" if hit_tp else "loss")
                        pnl = log_trade_close(sym, pos["side"], pos["regime"], pos["entry"], exit_price, pos["rr"], result, "paper")
                        notify_close(sym, pnl, pos["rr"])
                        del active_paper[sym]

                if same_bar:
                    continue  # on n'entre qu'à la clôture H1

                last_ts_seen[sym] = last_ts
                sig = detect_signal(df, skip_first_after_prolonged=True, state=state, sym=sym)
                if not sig: continue

                open_cnt = len(active_paper) if DRY_RUN else count_open_positions_real(ex)
                if open_cnt >= MAX_OPEN_TRADES: continue
                if not DRY_RUN and has_open_position_real(ex, sym): continue

                notify_signal(sym, sig)

                try:
                    usdt = 1000.0 if DRY_RUN else float(ex.fetch_balance().get("USDT", {}).get("free", 0))
                except Exception:
                    usdt = 0.0
                risk_amt = max(1.0, usdt*RISK_PER_TRADE)
                qty = compute_qty(sig["entry"], sig["sl"], risk_amt)
                if qty<=0: continue

                be_rule = "Pas de BE si réaction rapide ; sinon ALT 50% sur MM(BB20)." if sig["regime"]=="trend" else "BE à la MM(BB20)."
                tp_rule = "TP dynamique BB80 opposée." if sig["regime"]=="trend" else "TP sur borne BB20 opposée."

                if DRY_RUN:
                    notify_order_ok(sym, sig["side"], qty, be_rule=be_rule, tp_rule=tp_rule)
                    active_paper[sym] = {
                        "entry":sig["entry"], "side":sig["side"], "regime":sig["regime"],
                        "sl":sig["sl"], "tp":sig["tp"], "rr":sig["rr"], "qty":qty,
                        "ts":datetime.utcnow(), "be_applied":False, "partial_done":False
                    }
                else:
                    try:
                        ex.create_order(sym, "market", sig["side"], qty)
                        mode = place_bracket_orders(ex, sym, sig["side"], qty, sig["sl"], sig["tp"])
                        notify_order_ok(sym, sig["side"], qty, be_rule=be_rule, tp_rule=f"{tp_rule} (*{mode}*)")
                    except Exception as e:
                        notify_error("order", e)

            time.sleep(LOOP_DELAY)

        except KeyboardInterrupt:
            tg_send("⛔ Arrêt manuel du bot."); break
        except Exception as e:
            notify_error("loop", e); time.sleep(5)

if __name__ == "__main__":
    main()
