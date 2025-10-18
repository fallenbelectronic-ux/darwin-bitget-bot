import os, time, csv, math, io
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands, AverageTrueRange
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from notifier import tg_send

load_dotenv()

# =======================
# ENV
# =======================
BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1","true","yes")
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

# gestion "réaction rapide"
QUICK_BARS         = 3
QUICK_PROGRESS     = 0.30  # 30% vers le TP en <= QUICK_BARS sinon ALT 50% (tendance)

# rapport quotidien
REPORT_HOUR        = int(os.getenv("REPORT_HOUR", "0"))  # minuit par défaut
TRADES_CSV         = os.getenv("TRADES_CSV", "/app/trades.csv")

FALLBACK_TESTNET   = ["BTC/USDT:USDT", "ETH/USDT:USDT", "XRP/USDT:USDT"]

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
# OHLCV + INDICATEURS
# =======================
def fetch_ohlcv_df(ex, symbol, timeframe, limit=300):
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)

    bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb20_mid"] = bb20.bollinger_mavg()
    df["bb20_up"]  = bb20.bollinger_hband()
    df["bb20_lo"]  = bb20.bollinger_lband()

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
            try: vol = float(vol)
            except: vol = 0.0
            rows.append((s, vol))
    except Exception:
        pass

    if rows:
        df = pd.DataFrame(rows, columns=["symbol","volume"]).sort_values("volume", ascending=False)
        universe = df.head(UNIVERSE_SIZE)["symbol"].tolist()
        top10 = df.head(10)
        preview = ", ".join([f"{r.symbol}:{int(r.volume)}" for r in top10.itertuples(index=False)])
        tg_send(f"📊 *Univers LIVE (Top10)*\n{preview}")
        if BITGET_TESTNET:
            universe = filter_working_symbols(ex, universe[:20], timeframe=TF) or FALLBACK_TESTNET
            tg_send("🧪 *Testnet actifs* : " + ", ".join(universe))
        return universe

    # fallback testnet
    universe = filter_working_symbols(ex, FALLBACK_TESTNET, timeframe=TF)
    tg_send("🧪 *Univers TESTNET* : " + ", ".join(universe))
    return universe

# =======================
# LOGIQUE DARWIN
# =======================
def prolonged_double_exit(df, lookback=6):
    streak = 0
    side = None
    for i in range(-lookback-3, -1):
        r = df.iloc[i]
        up_both = (r["high"] >= r["bb20_up"]) and (r["high"] >= r["bb80_up"])
        lo_both = (r["low"]  <= r["bb20_lo"]) and (r["low"]  <= r["bb80_lo"])
        if up_both:
            if side in (None, "up"): streak += 1; side = "up"
            else: streak = 1; side = "up"
        elif lo_both:
            if side in (None, "down"): streak += 1; side="down"
            else: streak = 1; side="down"
        else:
            streak = 0; side = None
    return streak >= 3

def detect_signal(df, skip_first_after_prolonged=True, state=None, sym=None):
    if len(df) < 3: return None
    last, prev = df.iloc[-1], df.iloc[-2]
    notes=[]
    above80 = last["close"] >= last["bb80_mid"]

    reinteg_long  = (prev["low"]  <= min(prev["bb20_lo"], prev["bb80_lo"])) and (last["close"] > last["bb20_lo"])
    reinteg_short = (prev["high"] >= max(prev["bb20_up"], prev["bb80_up"])) and (last["close"] < last["bb20_up"])

    long_trend  =  above80 and reinteg_long
    short_trend = (not above80) and reinteg_short

    if skip_first_after_prolonged and state is not None and sym is not None:
        if state.get(sym, {}).get("cooldown", False):
            state[sym]["cooldown"] = False
            notes.append("⏳ 1er signal après *sortie prolongée* — ignoré")
            return None
        if prolonged_double_exit(df):
            st = state.setdefault(sym, {})
            st["cooldown"] = True
            notes.append("⚠️ *Sortie prolongée* détectée — prochain signal sera ignoré")
            return None

    if long_trend:
        side, regime = "buy","trend"; notes.append("Au-dessus *BB80* + réintégration *BB20 basse*")
    elif short_trend:
        side, regime = "sell","trend"; notes.append("Sous *BB80* + réintégration *BB20 haute*")
    elif reinteg_long:
        side, regime = "buy","counter"; notes.append("Contre-tendance : réintégration *BB20 basse*")
    elif reinteg_short:
        side, regime = "sell","counter"; notes.append("Contre-tendance : réintégration *BB20 haute*")
    else:
        return None

    entry = float(last["close"])
    atr   = float(last["atr"])
    if side=="buy":
        sl = float(prev["low"])  - SL_ATR_CUSHION*atr
        tp = float(last["bb80_up"]) if regime=="trend" else float(last["bb20_up"])
    else:
        sl = float(prev["high"]) + SL_ATR_CUSHION*atr
        tp = float(last["bb80_lo"]) if regime=="trend" else float(last["bb20_lo"])

    rr = abs((tp-entry)/(entry-sl)) if entry!=sl else 0
    if rr < MIN_RR: return None

    return {"side":side, "regime":regime, "entry":entry, "sl":sl, "tp":tp, "rr":rr, "notes":notes}

# =======================
# POSITIONS & ORDRES CONDITIONNELS
# =======================
def has_open_position(ex, symbol):
    try:
        pos = ex.fetch_positions([symbol])
        for p in pos:
            if abs(float(p.get("contracts") or 0))>0:
                return True
        return False
    except:
        return False

def count_open_positions(ex):
    try:
        pos = ex.fetch_positions()
        return sum(1 for p in pos if abs(float(p.get("contracts") or 0))>0)
    except:
        return 0

def compute_qty(entry, sl, risk_amount):
    diff = abs(entry-sl)
    return risk_amount/diff if diff>0 else 0

def place_bracket_orders(ex, symbol, side, qty, sl, tp):
    """
    Essaie 3 façons :
    1) Bracket (takeProfit/stopLoss)
    2) Stop conditionnel + TP reduceOnly
    3) TP reduceOnly seul (fallback)
    """
    opposite = "sell" if side=="buy" else "buy"

    # 1) Bracket (si support par ccxt/bitget)
    try:
        ex.create_order(symbol, "market", side, qty, params={
            "stopLossPrice": sl,
            "takeProfitPrice": tp,
            "reduceOnly": False,
            "triggerType": "mark_price",
        })
        return "bracket"
    except Exception as e:
        print("[ORDERS] bracket failed:", e)

    # 2) Stop conditionnel + TP reduceOnly
    ok_any = False
    try:
        # stop (reduceOnly=True est parfois refusé côté SL)
        ex.create_order(symbol, "stop", opposite, qty, params={
            "stopPrice": sl,                 # alias
            "stopLossPrice": sl,            # alias
            "triggerType": "mark_price",
            "reduceOnly": True
        })
        ok_any = True
    except Exception as e:
        print("[ORDERS] stop failed:", e)

    try:
        ex.create_order(symbol, "limit", opposite, qty, price=tp, params={
            "reduceOnly": True
        })
        ok_any = True
    except Exception as e:
        print("[ORDERS] tp failed:", e)

    return "separate" if ok_any else "basic"

def modify_stop_to_be(ex, symbol, side, qty, be_price):
    """
    Pose un stop reduceOnly au niveau du BE (entrée).
    (On ne dépend pas d'une modification d'ordre existant, on crée un nouveau stop.)
    """
    opposite = "sell" if side=="buy" else "buy"
    try:
        ex.create_order(symbol, "stop", opposite, qty, params={
            "stopPrice": be_price,
            "triggerType": "mark_price",
            "reduceOnly": True
        })
        return True
    except Exception as e:
        print("[ORDERS] BE stop failed:", e)
        return False

# =======================
# TRACKING & RAPPORT
# =======================
def ensure_trades_csv():
    if not os.path.exists(TRADES_CSV):
        with open(TRADES_CSV, "w", newline="", encoding="utf-8") as f:
            w=csv.writer(f)
            w.writerow(["ts","symbol","side","regime","entry","exit","pnl_pct","rr","result"])

def log_trade_close(symbol, side, regime, entry, exit_price, rr, result):
    ensure_trades_csv()
    pnl_pct = (exit_price-entry)/entry*100.0 if side=="buy" else (entry-exit_price)/entry*100.0
    with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
        w=csv.writer(f)
        w.writerow([datetime.utcnow().isoformat(), symbol, side, regime,
                    f"{entry:.8f}", f"{exit_price:.8f}", f"{pnl_pct:.4f}", f"{rr:.2f}", result])
    return pnl_pct

def daily_report():
    ensure_trades_csv()
    since = datetime.utcnow() - timedelta(days=1)
    rows=[]
    with open(TRADES_CSV, "r", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            try:
                ts = datetime.fromisoformat(row["ts"])
                if ts >= since:
                    rows.append(row)
            except:
                pass
    if not rows:
        tg_send("🧭 Rapport quotidien\nAucun trade clos sur les dernières 24h.")
        return

    n=len(rows)
    wins=sum(1 for x in rows if x["result"]=="win")
    losses=sum(1 for x in rows if x["result"]=="loss")
    bes=sum(1 for x in rows if x["result"]=="be")
    avg_rr = np.mean([float(x["rr"]) for x in rows]) if rows else 0.0
    total_pnl = np.sum([float(x["pnl_pct"]) for x in rows]) if rows else 0.0
    best = max(rows, key=lambda x: float(x["pnl_pct"]))
    worst= min(rows, key=lambda x: float(x["pnl_pct"]))
    winrate = 100*wins/max(1,wins+losses)

    msg = (
        f"🧭 *Rapport quotidien* — {datetime.utcnow().strftime('%d %b %Y')}\n\n"
        f"• Trades clos : {n}\n"
        f"• Gagnants : {wins} | Perdants : {losses} | BE : {bes}\n"
        f"• Winrate : {winrate:.1f} %\n"
        f"• P&L total : {total_pnl:+.2f} %\n"
        f"• Meilleur : {best['symbol']} {float(best['pnl_pct']):+.2f}%\n"
        f"• Pire    : {worst['symbol']} {float(worst['pnl_pct']):+.2f}%\n"
        f"• RR moyen : x{avg_rr:.2f}\n"
        f"----------------------------------------\n"
        f"⚙️ Bot actif — H1 — Risk {int(RISK_PER_TRADE*100)}%\n"
    )
    tg_send(msg)

_last_report_day = None
def maybe_send_daily_report():
    global _last_report_day
    now = datetime.utcnow()
    if now.hour == REPORT_HOUR and (not _last_report_day or _last_report_day != now.date()):
        _last_report_day = now.date()
        try:
            daily_report()
        except Exception as e:
            tg_send(f"⚠️ Rapport quotidien échoué : {e}")

# =======================
# NOTIFS
# =======================
def notify_signal(symbol, sig):
    regime_emoji = "📈" if sig["regime"]=="trend" else "🔄"
    side_txt = "LONG" if sig["side"]=="buy" else "SHORT"
    bullet = "\n".join([f"• {n}" for n in sig.get("notes",[])])
    msg = (
        f"{regime_emoji} *Signal* `{symbol}` {side_txt}\n"
        f"Entrée ~ `{sig['entry']:.4f}`  |  SL ~ `{sig['sl']:.4f}`  |  TP ~ `{sig['tp']:.4f}`\n"
        f"RR `x{sig['rr']:.2f}`\n{bullet}"
    )
    tg_send(msg)

def notify_order_ok(symbol, side, qty, be_rule=None, tp_rule=None):
    side_txt = "LONG" if side=="buy" else "SHORT"
    lines=[f"🎯 *Trade exécuté* `{symbol}` {side_txt}\nTaille : `{qty:.6f}`"]
    if be_rule: lines.append(f"• BE : {be_rule}")
    if tp_rule: lines.append(f"• TP : {tp_rule}")
    tg_send("\n".join(lines))

def notify_error(context, err):
    tg_send(f"⚠️ *Erreur* `{context}`\n{err}")

# =======================
# MAIN LOOP (avec BE + ALT50 + rapport)
# =======================
def main():
    ex = create_exchange()
    tg_send(f"🔔 Bot démarré — H1 — Risk {int(RISK_PER_TRADE*100)}% — RR≥{MIN_RR}")
    universe = build_universe(ex)
    valid = set(universe)

    # Etat pour : cooldown, positions suivies
    state = {}
    # positions actives trackées par le bot
    # sym -> {entry, side, regime, sl, tp, rr, ts, qty, be_applied, partial_done, bar_entered}
    active = {}

    last_ts_seen = {}
    while True:
        try:
            maybe_send_daily_report()

            for sym in list(universe):
                if sym not in valid:
                    continue

                # OHLCV
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, 300)
                except Exception as e:
                    print("[WARN] fetch_ohlcv:", e)
                    continue

                last_ts = df.index[-1]
                if last_ts_seen.get(sym) == last_ts:
                    # ——— Surveillance des positions déjà ouvertes (BE / ALT / clôture) ———
                    if sym in active:
                        sig = active[sym]
                        last = df.iloc[-1]
                        # BE en CONTRE-TENDANCE : à la MM blanche
                        if sig["regime"]=="counter" and not sig["be_applied"]:
                            if sig["side"]=="buy" and last["close"]>=last["bb20_mid"]:
                                if modify_stop_to_be(ex, sym, sig["side"], sig["qty"], sig["entry"]):
                                    sig["be_applied"]=True
                                    tg_send(f"🛡️ BE posé sur `{sym}` à `{sig['entry']:.4f}` (contre-tendance)")
                            if sig["side"]=="sell" and last["close"]<=last["bb20_mid"]:
                                if modify_stop_to_be(ex, sym, sig["side"], sig["qty"], sig["entry"]):
                                    sig["be_applied"]=True
                                    tg_send(f"🛡️ BE posé sur `{sym}` à `{sig['entry']:.4f}` (contre-tendance)")

                        # ALT 50% en TENDANCE si progression lente
                        if sig["regime"]=="trend" and not sig["partial_done"]:
                            bars = (datetime.utcnow() - sig["ts"]).total_seconds() / 3600.0  # approx H1
                            if bars >= QUICK_BARS - 1e-9:
                                # distance vers TP et progression actuelle
                                dist_full = abs(sig["tp"]-sig["entry"])
                                dist_now  = abs(last["close"]-sig["entry"])
                                if dist_full>0 and (dist_now/dist_full) < QUICK_PROGRESS:
                                    # place un limit reduceOnly 50% à bb20_mid
                                    mid = float(last["bb20_mid"])
                                    half = sig["qty"]/2.0
                                    opp = "sell" if sig["side"]=="buy" else "buy"
                                    try:
                                        ex.create_order(sym, "limit", opp, half, price=mid, params={"reduceOnly": True})
                                        sig["partial_done"]=True
                                        tg_send(f"✂️ Alt 50% posé sur `{sym}` à la MM BB20 `{mid:.4f}` (tendance, progression lente)")
                                    except Exception as e:
                                        print("[ALT50] failed:", e)

                        # Détection de clôture → log + notif
                        still = has_open_position(ex, sym)
                        if not still:
                            # tente de déduire le prix de sortie depuis les ordres fermés récents
                            exit_price = float(df.iloc[-1]["close"])
                            try:
                                # récupère derniers trades pour approx
                                mytrades = ex.fetch_my_trades(sym, since=int(sig["ts"].timestamp()*1000))
                                if mytrades:
                                    # dernier fill côté reduceOnly
                                    exfills = [t for t in mytrades if t.get("side") in ("buy","sell")]
                                    if exfills:
                                        exit_price = float(exfills[-1]["price"])
                            except Exception:
                                pass

                            pnl = log_trade_close(sym, sig["side"], sig["regime"], sig["entry"], exit_price, sig["rr"],
                                                  "win" if ((sig["side"]=="buy" and exit_price>sig["entry"]) or
                                                            (sig["side"]=="sell" and exit_price<sig["entry"])) else "loss")
                            emo = "✅" if pnl>=0 else "❌"
                            tg_send(f"{emo} *Trade clos* `{sym}`  P&L `{pnl:+.2f}%`  |  RR `x{sig['rr']:.2f}`")
                            del active[sym]

                    continue  # rien de nouveau niveau signaux

                last_ts_seen[sym] = last_ts

                # nouveau signal (bougie close)
                sig = detect_signal(df, skip_first_after_prolonged=True, state=state, sym=sym)
                if not sig: 
                    continue

                if count_open_positions(ex) >= MAX_OPEN_TRADES: 
                    continue
                if has_open_position(ex, sym):
                    continue

                notify_signal(sym, sig)

                # taille
                try:
                    bal = ex.fetch_balance()
                    usdt = float(bal.get("USDT", {}).get("free", 0))
                except Exception:
                    usdt = 0.0
                risk_amt = max(1.0, usdt*RISK_PER_TRADE)
                qty = compute_qty(sig["entry"], sig["sl"], risk_amt)
                if qty<=0:
                    continue

                # règles pour notif
                be_rule=None; tp_rule=None
                if sig["regime"]=="trend":
                    be_rule="Pas de BE si réaction rapide ; sinon *50%* à *MM BB20* (ALT)."
                    tp_rule="TP dynamique sur *BB80 opposée*."
                else:
                    be_rule="BE à la *MM BB20*."
                    tp_rule="TP sur *borne BB20 opposée*."

                # Exécution + conditionnels
                try:
                    ex.create_order(sym, "market", sig["side"], qty)
                    mode = place_bracket_orders(ex, sym, sig["side"], qty, sig["sl"], sig["tp"])
                    notify_order_ok(sym, sig["side"], qty, be_rule=be_rule, tp_rule=f"{tp_rule}  (*{mode}*)")
                except Exception as e:
                    notify_error("order", e)
                    continue

                # Track la position pour BE/ALT50/fermeture
                active[sym] = {
                    "entry": sig["entry"], "side": sig["side"], "regime": sig["regime"],
                    "sl": sig["sl"], "tp": sig["tp"], "rr": sig["rr"], "qty": qty,
                    "ts": datetime.utcnow(), "be_applied": False, "partial_done": False
                }

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
