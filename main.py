import os, time, csv, math, requests
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands, AverageTrueRange
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from notifier import tg_send

load_dotenv()

# =======================
# ENV / PARAMÈTRES
# =======================
BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY       = os.getenv("BITGET_API_KEY")
API_SECRET    = os.getenv("BITGET_API_SECRET")
PASSPHRASE    = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF                 = os.getenv("TIMEFRAME", "1h")          # H1 unique
RISK_PER_TRADE     = float(os.getenv("RISK_PER_TRADE", "0.01"))  # 1%
MIN_RR             = float(os.getenv("MIN_RR", "3"))       # RR mini 1:3
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

# “Réaction rapide” en tendance (papier auto)
QUICK_BARS         = 3        # doit avancer vite en <= 3 barres
QUICK_PROGRESS     = 0.30     # >= 30% du chemin vers TP

# Pyramide (réservé)
PYRAMID_MAX        = 1

# Fallback testnet : marchés USDT-perp disponibles le plus souvent
FALLBACK_TESTNET = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "LTC/USDT:USDT",
    "BCH/USDT:USDT", "XRP/USDT:USDT"
]

# Mode papier & rapports
DRY_RUN            = os.getenv("DRY_RUN", "false").lower() in ("1","true","yes")
TRADES_CSV         = os.getenv("TRADES_CSV", "/app/trades.csv")          # journal pour rapports
PAPER_CSV          = os.getenv("PAPER_CSV", "/app/paper_trades.csv")     # journal des /test & /order (papier)
TZ                 = os.getenv("TIMEZONE", "Europe/Lisbon")
REPORT_HOUR        = int(os.getenv("REPORT_HOUR", "19"))
REPORT_WEEKLY_HOUR = int(os.getenv("REPORT_WEEKLY_HOUR", "19"))
REPORT_WEEKDAY     = int(os.getenv("REPORT_WEEKDAY", "6"))  # 0=lundi ... 6=dimanche

# Telegram
TG_TOKEN           = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID         = os.getenv("TELEGRAM_CHAT_ID", "")

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
        candidates = []
        for m in ex.markets.values():
            if (
                (m.get("type") == "swap" or m.get("swap") is True) and
                (m.get("linear") is True) and
                (m.get("settle") == "USDT") and
                (m.get("quote") == "USDT") and
                (m.get("symbol") is not None)
            ):
                candidates.append(m["symbol"])
    except Exception as e:
        print("[UNIVERSE] load_markets failed:", e)
        candidates = []

    rows = []
    try:
        tickers = ex.fetch_tickers(candidates if candidates else None)
        for s, t in tickers.items():
            if "/USDT" not in s and ":USDT" not in s:
                continue
            vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try:
                vol = float(vol or 0.0)
            except Exception:
                vol = 0.0
            if MIN_VOLUME_USDT <= 0 or vol >= MIN_VOLUME_USDT:
                rows.append((s, vol))
    except Exception as e:
        print("[UNIVERSE] fetch_tickers failed:", e)

    if rows:
        df = pd.DataFrame(rows, columns=["symbol", "volume"]).sort_values("volume", ascending=False)
        universe = df.head(UNIVERSE_SIZE)["symbol"].tolist()
        head10 = df.head(10).copy()
        head10["volume"] = head10["volume"].round(0).astype(int)
        preview = ", ".join([f"{r.symbol}:{r.volume}" for r in head10.itertuples(index=False)])
        print(f"[UNIVERSE] size={len(universe)} (ranked by 24h volume)")
        print(f"[UNIVERSE] top10: {preview}")
        tg_send(f"📊 Univers LIVE top10: {preview}")

        if BITGET_TESTNET:
            u2 = filter_working_symbols(ex, universe[:20], timeframe=TF)
            if u2:
                print(f"[UNIVERSE] testnet filtered working={len(u2)}: {', '.join(u2)}")
                tg_send(f"🧪 Testnet marchés OK: {', '.join(u2)}")
                return u2[:UNIVERSE_SIZE]
            else:
                print("[UNIVERSE] testnet: aucun symbole valide parmi le top -> fallback")
        return universe

    print("[UNIVERSE] empty after volume filter, using fallback list (testnet)")
    if candidates:
        fb = [s for s in FALLBACK_TESTNET if s in candidates] or FALLBACK_TESTNET
    else:
        fb = FALLBACK_TESTNET
    universe = filter_working_symbols(ex, fb, timeframe=TF)
    if not universe:
        probe = filter_working_symbols(ex, candidates[:30], timeframe=TF)
        universe = probe or fb
    universe = universe[:max(1, min(UNIVERSE_SIZE, len(universe)))]
    print(f"[UNIVERSE] size={len(universe)} (fallback)")
    print(f"[UNIVERSE] list: {', '.join(universe)}")
    tg_send(f"🧪 Univers TESTNET: {', '.join(universe)}")
    return universe

# =======================
# DÉTECTION (fenêtre 1–2 barres) + traversée prolongée
# =======================
def count_prolonged_extreme(df, side):
    cnt = 0
    idx = -2
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
    - entrée à l’ouverture de la bougie suivante
    - si traversée prolongée (>=2 barres) -> entry_delay_bars=1 (on saute la 1ʳᵉ opportunité)
    """
    if len(df) < 3:
        return None
    last  = df.iloc[-1]   # bougie qui vient de clore
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]

    above_slow_mid = last["close"] >= last["bb_slow_mid"]

    # Réintégrations (1 ou 2 bougies)
    reinteg_long_1  = (prev["low"]  <= min(prev["bb_fast_lower"], prev["bb_slow_lower"])) and (last["close"] > last["bb_fast_lower"])
    reinteg_short_1 = (prev["high"] >= max(prev["bb_fast_upper"], prev["bb_slow_upper"])) and (last["close"] < last["bb_fast_upper"])
    reinteg_long_2  = (prev2["low"]  <= min(prev2["bb_fast_lower"], prev2["bb_slow_lower"])) and (last["close"] > last["bb_fast_lower"])
    reinteg_short_2 = (prev2["high"] >= max(prev2["bb_fast_upper"], prev2["bb_slow_upper"])) and (last["close"] < last["bb_fast_upper"])

    # Tendance (1–2 barres)
    long_trend  = (above_slow_mid and ((prev["low"] <= prev["bb_fast_lower"]) or (prev2["low"] <= prev2["bb_fast_lower"])) and (last["close"] > last["bb_fast_lower"]))
    short_trend = ((not above_slow_mid) and ((prev["high"] >= prev["bb_fast_upper"]) or (prev2["high"] >= prev2["bb_fast_upper"])) and (last["close"] < last["bb_fast_upper"]))

    # Contre-tendance = double extrême + réintégration
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

    entry  = float(last["close"])  # référence; l'entrée réelle = open de la suivante
    atr    = float(last["atr"])
    tick   = max(entry * 0.0001, 0.01)

    # SL mèche +/- cushion ATR
    if side == "buy":
        raw_sl = float(last["low"]) - 2 * tick
        sl = min(raw_sl, float(prev["low"])) - SL_ATR_CUSHION * atr
    else:
        raw_sl = float(last["high"]) + 2 * tick
        sl = max(raw_sl, float(prev["high"])) + SL_ATR_CUSHION * atr

    # TP théorique
    if regime == "trend":
        tp = float(last["bb_slow_upper"] if side == "buy" else last["bb_slow_lower"])
    else:
        tp = float(last["bb_fast_upper"] if side == "buy" else last["bb_fast_lower"])

    denom = abs(entry - sl)
    rr = abs((tp - entry) / denom) if denom > 0 else 0.0
    if rr < MIN_RR:
        return None

    # traversée prolongée -> retarder l’entrée d’1 barre
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
# RAPPORTS & STATS (trades.csv)
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

def summarize_rows(rows):
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
    n,w,l,be,avg_rr,total,best,worst,wr = summarize_rows(rows)
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
    n,w,l,be,avg_rr,total,best,worst,wr = summarize_rows(rows)
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
# PAPER TRADES (journal / commandes)
# =======================
def _paper_ensure_header():
    if not os.path.exists(PAPER_CSV):
        with open(PAPER_CSV,"w",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow(
                ["ts","id","symbol","side","entry","sl","tp","risk_pct","leverage","qty","status"]
            )

def _paper_read():
    _paper_ensure_header()
    with open(PAPER_CSV,"r",newline="",encoding="utf-8") as f:
        return list(csv.DictReader(f))

def _paper_write(rows):
    if not rows: _paper_ensure_header(); return
    with open(PAPER_CSV,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)

def _paper_append(row):
    rows = _paper_read()
    for k in ["ts","id","symbol","side","entry","sl","tp","risk_pct","leverage","qty","status"]:
        row.setdefault(k,"")
    rows.append(row); _paper_write(rows)

def _paper_find_open(tid):
    rows = _paper_read()
    for i,r in enumerate(rows):
        if r["id"]==tid and r["status"]=="OPEN":
            return i,r,rows
    return None,None,rows

# =======================
# NOTIFS
# =======================
def notify_signal(symbol, sig):
    emoji = "📈" if sig["regime"]=="trend" else "🔄"
    side  = "LONG" if sig["side"]=="buy" else "SHORT"
    paper = " [PAPER]" if DRY_RUN else ""
    tg_send(f"{emoji} *Signal{paper}* `{symbol}` {side}\n"
            f"Entrée `{sig['entry']:.4f}` | SL `{sig['stop']:.4f}` | TP `{sig['tp']:.4f}`\nRR x{sig['rr']:.2f}")

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
# GESTION POST-ENTRÉE (LIVE)
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
    try:
        px = ex.fetch_ticker(symbol)["last"]
    except Exception:
        px = entry
    tick = get_tick(px)

    b = latest_bands(ex, symbol)
    tp80 = b["slow_up"] - 2 * tick if side == "buy" else b["slow_lo"] + 2 * tick

    try:
        ex.create_order(symbol, "limit", "sell" if side == "buy" else "buy", qty, tp80, {"reduceOnly": True})
    except Exception as e:
        print("[WARN] place TP80:", e)

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

        prog = abs(px - entry)
        if dist_total > 0:
            progressed = max(progressed, prog / dist_total)

        bars += 1

        if (not took_half) and (bars > QUICK_BARS) and (progressed < QUICK_PROGRESS):
            b = latest_bands(ex, symbol)
            tp_mid = (b["fast_mid"] - 1 * tick) if side == "buy" else (b["fast_mid"] + 1 * tick)
            try:
                ex.create_order(symbol, "limit", "sell" if side == "buy" else "buy", max(qty * 0.5, 0.000001),
                                tp_mid, {"reduceOnly": True})
                tg_send(f"🟨 {symbol} tendance: 50% pris sur MM(BB20), pas de BE")
                took_half = True
            except Exception as e:
                print("[WARN] place 50% mid:", e)

        if bars % 6 == 0:
            b = latest_bands(ex, symbol)
            tp80 = (b["slow_up"] - 2 * tick) if side == "buy" else (b["slow_lo"] + 2 * tick)

        if not has_open_position(ex, symbol):
            tg_send(f"✅ Position clôturée {symbol} (tendance)")
            break

def manage_counter(ex, symbol, side, entry, sl, qty):
    tick = get_tick(entry)
    try:
        ex.create_order(symbol, "stop_market", "sell" if side == "buy" else "buy", qty, None,
                        {"stopPrice": sl, "reduceOnly": True})
    except Exception as e:
        print("[WARN] place SL:", e)

    while True:
        time.sleep(LOOP_DELAY)
        b = latest_bands(ex, symbol)
        be_stop = b["fast_mid"]
        try:
            ex.create_order(symbol, "stop_market", "sell" if side == "buy" else "buy", qty, None,
                            {"stopPrice": be_stop, "reduceOnly": True})
        except Exception as e:
            print("[WARN] move BE to fast_mid:", e)

        tp_fast = b["fast_up"] - 2 * tick if side == "buy" else b["fast_lo"] + 2 * tick
        try:
            ex.create_order(symbol, "limit", "sell" if side == "buy" else "buy", qty, tp_fast, {"reduceOnly": True})
        except Exception as e:
            print("[WARN] place TP fast:", e)

        if not has_open_position(ex, symbol):
            tg_send(f"✅ Position clôturée {symbol} (contre-tendance)")
            break

# =======================
# TELEGRAM COMMANDS (+ /order)
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
        n,w,l,be,avg,total,best,worst,wr = summarize_rows(rows)
        lines=[f"• {title}: {n} clos | Winrate {wr:.1f}%", f"  P&L {total:+.2f}% | RR moy x{avg:.2f}"]
        if best:  lines.append(f"  Best {best['symbol']} {float(best['pnl_pct']):+.2f}%")
        if worst: lines.append(f"  Worst {worst['symbol']} {float(worst['pnl_pct']):+.2f}%")
        return "\n".join(lines)
    local_now = datetime.now(ZoneInfo(TZ)).strftime('%d %b %Y %H:%M')
    tg_send(f"📊 *Stats* — {local_now} ({TZ})\n" + block("24h", rows_24) + "\n" + block("Total", rows_all))

def cmd_orders(active_paper=None):
    lines=[]
    if active_paper:
        lines.append("*Positions (papier: stratégie auto)*")
        for sym,p in active_paper.items():
            lines.append(f"• {sym} {'LONG' if p['side']=='buy' else 'SHORT'} | entry {p['entry']:.4f} SL {p['stop']:.4f} TP {p['tp']:.4f} | RR x{p['rr']:.2f}")
    rows = _paper_read()
    opens = [r for r in rows if r["status"]=="OPEN"]
    if opens:
        lines.append("*Trades papier manuels (ouverts)*")
        for r in opens:
            lines.append(f"• `{r['id']}` {r['symbol']} {r['side']} | entry {r['entry']}")
    if lines: tg_send("\n".join(lines))
    else: tg_send("ℹ️ Aucun trade papier ouvert.")

def cmd_exportcsv():
    try:
        with open(TRADES_CSV, "rb") as f:
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument",
                data={"chat_id": TG_CHAT_ID},
                files={"document": ("trades.csv", f, "text/csv")}
            )
        tg_send("📦 `trades.csv` envoyé.")
    except Exception as e:
        tg_send(f"❌ Export CSV échoué : {e}")

def cmd_test_open(exchange, text):
    if not DRY_RUN:
        tg_send("⚠️ /test disponible uniquement en mode PAPER (DRY_RUN=true)."); return
    parts = text.split()
    symbol = parts[1] if len(parts)>1 else "BTC/USDT:USDT"
    side   = parts[2].lower() if len(parts)>2 else "long"
    risk   = float(parts[3]) if len(parts)>3 else 0.01
    rr     = float(parts[4]) if len(parts)>4 else 3.0
    sltk   = float(parts[5]) if len(parts)>5 else 0.004
    lev    = int(parts[6]) if len(parts)>6 else 2

    ticker = exchange.fetch_ticker(symbol)
    entry = float(ticker.get("last") or ticker.get("close") or 0)
    if entry <= 0: tg_send("❌ Prix invalide."); return
    sl_dist = entry*sltk
    if side=="long": sl=entry-sl_dist; tp=entry+rr*sl_dist
    else: sl=entry+sl_dist; tp=entry-rr*sl_dist
    virtual_equity = 10_000.0
    qty = (virtual_equity*risk)/abs(entry-sl)
    tid = f"paper-{int(datetime.now().timestamp())}"

    _paper_append({
        "ts":datetime.utcnow().isoformat(),"id":tid,"symbol":symbol,"side":side.upper(),
        "entry":f"{entry:.6f}","sl":f"{sl:.6f}","tp":f"{tp:.6f}",
        "risk_pct":f"{risk:.6f}","leverage":str(lev),"qty":f"{qty:.6f}","status":"OPEN"
    })
    tg_send(f"🧪 TEST PAPER OPEN\n`{tid}` {symbol} {side.upper()} entry {entry:.2f} SL {sl:.2f} TP {tp:.2f}")

def _rr_done(side, entry, sl, exit_price):
    entry=float(entry); sl=float(sl); exit=float(exit_price)
    risk=abs(entry-sl) or 1e-9
    return (exit-entry)/risk if side.upper()=="LONG" else (entry-exit)/risk

def cmd_closepaper(exchange, text):
    if not DRY_RUN:
        tg_send("⚠️ /closepaper uniquement en DRY_RUN."); return
    parts=text.split()
    if len(parts)<2: tg_send("Usage: /closepaper <id> [prix]"); return
    tid=parts[1]
    px = float(parts[2]) if len(parts)>=3 else None
    i,row,rows = _paper_find_open(tid)
    if not row: tg_send("❌ Trade papier non trouvé."); return
    symbol=row["symbol"]
    if px is None:
        tk = exchange.fetch_ticker(symbol)
        px = float(tk.get("last") or tk.get("close") or 0)
        if px<=0: tg_send("❌ Prix invalide."); return
    rr = _rr_done(row["side"], row["entry"], row["sl"], px)
    side = "buy" if row["side"].upper()=="LONG" else "sell"
    pnl = log_trade_close(symbol, side, "manual", float(row["entry"]), float(px), rr, "win" if rr>0 else "loss", "paper-manual")
    rows[i]["status"]="CLOSED"; _paper_write(rows)
    tg_send(f"🏁 PAPER CLOSE `{tid}` {symbol} RR {rr:.2f} | P&L {pnl:+.2f}%")

def cmd_closeallpaper(exchange):
    if not DRY_RUN:
        tg_send("⚠️ /closeallpaper uniquement en DRY_RUN."); return
    rows = _paper_read()
    open_ids = [r["id"] for r in rows if r["status"]=="OPEN"]
    for tid in open_ids:
        try: cmd_closepaper(exchange, f"/closepaper {tid}")
        except Exception as e: tg_send(f"⚠️ {tid}: {e}")
    tg_send(f"🏁 Fermeture {len(open_ids)} trades papier.")

def cmd_order(exchange, text):
    """
    /order <symbol> <long|short> [risk=0.01] [rr=3] [use_rr=1]
      - DRY_RUN=true => ouvre un papier (et journalise)
      - DRY_RUN=false => envoie un ordre réel + bracket SL/TP
    SL = mèche +/- 2*ticks +/- cushion*ATR
    TP = RR*risque OU BB80 opposée si use_rr=0
    """
    parts = text.split()
    if len(parts) < 3:
        tg_send("Usage: `/order <symbol> <long|short> [risk] [rr] [use_rr(0/1)]`")
        return
    symbol = parts[1]
    side_h = parts[2].lower()
    if side_h not in ("long","short"):
        tg_send("⚠️ Side doit être `long` ou `short`."); return
    risk   = float(parts[3]) if len(parts)>3 else 0.01
    rr_in  = float(parts[4]) if len(parts)>4 else 3.0
    use_rr = int(parts[5]) if len(parts)>5 else 1

    df = fetch_ohlcv_df(exchange, symbol, TF, limit=120)
    last = df.iloc[-1]
    entry = float(last["close"])
    tick  = max(entry*0.0001, 0.01)
    atr   = float(last["atr"])
    if side_h=="long":
        sl = min(float(last["low"]) - 2*tick, float(df.iloc[-2]["low"])) - SL_ATR_CUSHION*atr
        if use_rr:
            tp = entry + rr_in*abs(entry-sl)
        else:
            tp = float(last["bb_slow_upper"]) - 2*tick
        api_side = "buy"
    else:
        sl = max(float(last["high"]) + 2*tick, float(df.iloc[-2]["high"])) + SL_ATR_CUSHION*atr
        if use_rr:
            tp = entry - rr_in*abs(entry-sl)
        else:
            tp = float(last["bb_slow_lower"]) + 2*tick
        api_side = "sell"

    # taille
    try:
        usdt = 1000.0 if DRY_RUN else float(exchange.fetch_balance().get("USDT", {}).get("free", 0))
    except Exception:
        usdt = 0.0
    risk_amt = max(1.0, usdt * risk)
    qty = round(compute_qty(entry, sl, risk_amt), 6)
    if qty <= 0:
        tg_send("❌ Taille nulle."); return

    if DRY_RUN:
        tid = f"paper-{int(datetime.now().timestamp())}"
        _paper_append({
            "ts":datetime.utcnow().isoformat(),"id":tid,"symbol":symbol,"side":side_h.upper(),
            "entry":f"{entry:.6f}","sl":f"{sl:.6f}","tp":f"{tp:.6f}",
            "risk_pct":f"{risk:.6f}","leverage":str(MAX_LEVERAGE),"qty":f"{qty:.6f}","status":"OPEN"
        })
        tg_send(f"🧪 PAPER ORDER {symbol} {side_h.upper()} entry {entry:.2f} SL {sl:.2f} TP {tp:.2f} qty {qty}")
    else:
        try:
            place_market(exchange, symbol, api_side, qty)
            mode = place_bracket_orders(exchange, symbol, api_side, qty, sl, tp)
            tg_send(f"✅ ORDER {symbol} {api_side.upper()} qty {qty} | SL {sl:.2f} TP {tp:.2f} ({mode})")
        except Exception as e:
            tg_send(f"❌ ORDER échoué: {e}")

def poll_telegram_commands(ex, active_paper):
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
            text = (msg.get("text") or "").strip()
            text_l = text.lower()

            if text_l.startswith("/help"):
                tg_send(
                    "*Commandes*\n"
                    "/config, /stats, /open, /orders, /panic\n"
                    "/report, /weekly, /exportcsv\n"
                    "/test, /closepaper <id> [prix], /closeallpaper\n"
                    "/order <sym> <long|short> [risk] [rr] [use_rr]"
                )

            elif text_l.startswith("/config"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(
                    "*Config*\n"
                    f"Mode: {mode}\n"
                    f"TF: {TF}\n"
                    f"Risk: {int(RISK_PER_TRADE*100)}% | RR:min {MIN_RR}\n"
                    f"Max trades: {MAX_OPEN_TRADES}\n"
                    f"Rapports: {REPORT_HOUR}h (quotidien), {REPORT_WEEKLY_HOUR}h dim (hebdo) — TZ: {TZ}"
                )

            elif text_l.startswith("/open"):
                if DRY_RUN:
                    if not active_paper:
                        tg_send("Aucune position (papier) suivie en mémoire.")
                    else:
                        lines=["*Positions (papier: stratégie auto)*"]
                        now = datetime.utcnow()
                        for sym, p in active_paper.items():
                            dur = fmt_duration((now - p["ts"]).total_seconds())
                            side = "LONG" if p["side"]=="buy" else "SHORT"
                            lines.append(
                                f"• {sym} {side} | entry {p['entry']:.4f} | SL {p['stop']:.4f} | TP {p['tp']:.4f} | RR x{p['rr']:.2f} | {dur}"
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

            elif text_l.startswith("/orders"):
                cmd_orders(active_paper=active_paper)

            elif text_l.startswith("/exportcsv"):
                cmd_exportcsv()

            elif text_l.startswith("/panic"):
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

            elif text_l.startswith("/stats"):
                send_stats()

            elif text_l.startswith("/report"):
                daily_report()

            elif text_l.startswith("/weekly"):
                weekly_report()

            elif text_l.startswith("/test"):
                cmd_test_open(ex, text)

            elif text_l.startswith("/closepaper"):
                cmd_closepaper(ex, text)

            elif text_l.startswith("/closeallpaper"):
                cmd_closeallpaper(ex)

            elif text_l.startswith("/order"):
                cmd_order(ex, text)

    except Exception:
        pass

# =======================
# MAIN LOOP (entrées à l’ouverture)
# =======================
def main():
    ex = create_exchange()
    mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
    tg_send(f"🤖 Darwin H1 — BB20/2 & BB80/2 — Risk {int(RISK_PER_TRADE*100)}% — RR min {MIN_RR} — {mode}")

    if not API_KEY or not API_SECRET or not PASSPHRASE:
        print("[FATAL] Missing API keys")
        tg_send("❌ BITGET_API_* manquantes")
        return

    time.sleep(3)
    universe = build_universe(ex)
    last_bar_time = {}
    pending = {}            # {symbol: {"wait": n_bars, "ref_ts": last_ts, "sig": sig}}

    # Suivi papier (auto)
    active_paper = {}  # sym -> {...}

    while True:
        try:
            maybe_send_daily_report()
            maybe_send_weekly_report()
            poll_telegram_commands(ex, active_paper)

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

            open_cnt = len(active_paper) if DRY_RUN else count_open_positions(ex)
            slots = max(0, MAX_OPEN_TRADES - open_cnt)

            if not universe:
                universe = build_universe(ex)

            for sym in universe:
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, limit=300)
                    last_ts = df.index[-1]

                    # update pending countdown à chaque nouvelle bougie
                    if sym in pending:
                        p = pending[sym]
                        if p["ref_ts"] != last_ts:
                            p["wait"] -= 1
                            p["ref_ts"] = last_ts

                    # nouveau signal uniquement si nouvelle clôture
                    if last_bar_time.get(sym) != last_ts:
                        last_bar_time[sym] = last_ts
                        sig = detect_signal(df)
                        if sig:
                            enter_after = 1 + sig["entry_delay_bars"]
                            pending[sym] = {"wait": enter_after, "ref_ts": last_ts, "sig": sig}
                            print(f"[SIGNAL] {sym} {sig['side']} ({sig['regime']}) RR={sig['rr']:.2f} wait={enter_after}")
                            notify_signal(sym, {"side":sig["side"],"regime":sig["regime"],"entry":sig["entry"],"stop":sig["stop"],"tp":sig["tp"],"rr":sig["rr"]})
                except Exception as e:
                    print(f"[ERROR] scan {sym}:", e)

            # Entrées à l’OUVERTURE (quand wait == 0)
            to_delete = []
            for sym, p in pending.items():
                if p["wait"] > 0: 
                    continue
                if slots <= 0: 
                    continue
                if (not DRY_RUN) and has_open_position(ex, sym):
                    to_delete.append(sym); 
                    continue

                s = p["sig"]
                risk_amount = max(1.0, (1000.0 if DRY_RUN else usdt_free) * RISK_PER_TRADE)
                qty = round(compute_qty(s["entry"], s["stop"], risk_amount), 6)
                if qty <= 0:
                    to_delete.append(sym); 
                    continue

                be_rule = "Pas de BE si réaction rapide ; sinon ALT 50% sur MM(BB20)." if s["regime"]=="trend" else "BE à la MM(BB20)."
                tp_rule = "TP dynamique BB80 opposée." if s["regime"]=="trend" else "TP sur borne BB20 opposée."

                if DRY_RUN:
                    notify_order_ok(sym, s["side"], qty, be_rule=be_rule, tp_rule=tp_rule)
                    active_paper[sym] = {
                        "entry":s["entry"], "side":s["side"], "regime":s["regime"],
                        "stop":s["stop"], "tp":s["tp"], "rr":s["rr"], "qty":qty,
                        "ts":datetime.utcnow(), "be_applied":False, "partial_done":False
                    }
                    slots -= 1
                else:
                    try:
                        place_market(ex, sym, s["side"], qty)
                        mode = place_bracket_orders(ex, sym, s["side"], qty, s["stop"], s["tp"])
                        notify_order_ok(sym, s["side"], qty, be_rule=be_rule, tp_rule=f"{tp_rule} (*{mode}*)")
                        # gestion post-entrée (bloquante par design d’origine)
                        if s["regime"] == "trend":
                            manage_trend(ex, sym, s["side"], s["entry"], s["stop"], s["tp"], qty)
                        else:
                            manage_counter(ex, sym, s["side"], s["entry"], s["stop"], qty)
                        slots -= 1
                    except Exception as e:
                        print("[ERROR] market:", e)
                        tg_send(f"⚠️ Ordre market échoué {sym}: {e}")
                to_delete.append(sym)

            for sym in to_delete:
                pending.pop(sym, None)

            # Gestion des sorties (papier auto)
            if DRY_RUN and active_paper:
                for sym, pos in list(active_paper.items()):
                    try:
                        df = fetch_ohlcv_df(ex, sym, TF, limit=50)
                        last = df.iloc[-1]
                        price = float(last["close"])
                        # BE contre-tendance sur MM(BB20)
                        if pos["regime"]=="counter" and not pos["be_applied"]:
                            if (pos["side"]=="buy"  and price>=float(last["bb_fast_mid"])) or \
                               (pos["side"]=="sell" and price<=float(last["bb_fast_mid"])):
                                pos["be_applied"]=True
                                tg_send(f"🛡️ BE (papier) sur `{sym}` à `{pos['entry']:.4f}` (contre-tendance)")
                        # ALT 50% tendance lente
                        if pos["regime"]=="trend" and not pos["partial_done"]:
                            elapsed_h = (datetime.utcnow() - pos["ts"]).total_seconds()/3600.0
                            dist_full = abs(pos["tp"]-pos["entry"]); dist_now=abs(price-pos["entry"])
                            if elapsed_h >= QUICK_BARS and dist_full>0 and (dist_now/dist_full)<QUICK_PROGRESS:
                                pos["partial_done"]=True
                                tg_send(f"✂️ Alt 50% (papier) `{sym}` à MM(BB20)")

                        hit_tp = (price>=pos["tp"] if pos["side"]=="buy" else price<=pos["tp"])
                        hit_sl = (price<=pos["stop"] if pos["side"]=="buy" else price>=pos["stop"])
                        hit_be = pos["be_applied"] and ((price<=pos["entry"] and pos["side"]=="buy") or (price>=pos["entry"] and pos["side"]=="sell"))
                        if hit_tp or hit_sl or hit_be:
                            exit_price = pos["tp"] if hit_tp else (pos["entry"] if hit_be else pos["stop"])
                            result = "be" if hit_be else ("win" if hit_tp else "loss")
                            pnl = log_trade_close(sym, pos["side"], pos["regime"], pos["entry"], exit_price, pos["rr"], result, "paper")
                            notify_close(sym, pnl, pos["rr"])
                            del active_paper[sym]
                    except Exception as e:
                        print("[WARN] paper manage:", e)

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
