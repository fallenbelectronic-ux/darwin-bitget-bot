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

# ============================
# ENVIRONNEMENT / CONFIG
# ============================
BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1","true","yes")
API_KEY        = os.getenv("BITGET_API_KEY")
API_SECRET     = os.getenv("BITGET_API_SECRET")
PASSPHRASE     = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF             = os.getenv("TIMEFRAME", "1h")
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.01"))
MIN_RR         = float(os.getenv("MIN_RR", "3"))
MAX_OPEN_TRADES= int(os.getenv("MAX_OPEN_TRADES", "4"))
LOOP_DELAY     = int(os.getenv("LOOP_DELAY", "5"))
UNIVERSE_SIZE  = int(os.getenv("UNIVERSE_SIZE", "100"))

ATR_WINDOW     = 14
SL_ATR_CUSHION = 0.25

REPORT_HOUR        = int(os.getenv("REPORT_HOUR", "19"))
REPORT_WEEKLY_HOUR = int(os.getenv("REPORT_WEEKLY_HOUR", "19"))
REPORT_WEEKDAY     = int(os.getenv("REPORT_WEEKDAY", "6"))
TRADES_CSV         = os.getenv("TRADES_CSV", "./trades.csv")
TZ                 = os.getenv("TIMEZONE", "Europe/Lisbon")

DRY_RUN            = os.getenv("DRY_RUN", "false").lower() in ("1","true","yes")

TG_TOKEN           = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID         = os.getenv("TELEGRAM_CHAT_ID", "")

MAX_LEVERAGE       = int(os.getenv("MAX_LEVERAGE", "2"))

FALLBACK_TESTNET = ["BTC/USDT:USDT", "ETH/USDT:USDT", "XRP/USDT:USDT", "LTC/USDT:USDT"]

# ============================
# EXCHANGE
# ============================
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

def try_set_leverage(ex, symbol, lev=MAX_LEVERAGE):
    """Tente de configurer le levier sur le marché donné."""
    try:
        ex.set_leverage(lev, symbol)
        print(f"[LEV] {symbol}: levier {lev}x appliqué")
    except Exception as e:
        print(f"[LEV WARN] {symbol}: levier non appliqué -> {e}")

# ============================
# INDICATEURS / DATA
# ============================
def fetch_ohlcv_df(ex, symbol, timeframe="1h", limit=300):
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)

    # BB20/2
    bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb20_mid"] = bb20.bollinger_mavg()
    df["bb20_up"]  = bb20.bollinger_hband()
    df["bb20_lo"]  = bb20.bollinger_lband()

    # BB80/2 (équivalent à 4h)
    bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
    df["bb80_mid"] = bb80.bollinger_mavg()
    df["bb80_up"]  = bb80.bollinger_hband()
    df["bb80_lo"]  = bb80.bollinger_lband()

    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=ATR_WINDOW)
    df["atr"] = atr.average_true_range()
    return df

# ============================
# DÉTECTION SIGNALS
# ============================
def prolonged_double_exit(df, lookback=6):
    """Retourne True si le prix est resté 4+ bougies au-delà des 2 bandes sans réintégrer."""
    side=None; count=0
    for i in range(-lookback-3, -1):
        r = df.iloc[i]
        up_both = (r["high"]>=r["bb20_up"]) and (r["high"]>=r["bb80_up"])
        lo_both = (r["low"] <=r["bb20_lo"]) and (r["low"] <=r["bb80_lo"])
        inside_now = (r["close"]<=r["bb20_up"]) and (r["close"]>=r["bb20_lo"])
        if up_both and not inside_now:
            count += 1; side="up"
        elif lo_both and not inside_now:
            count += 1; side="down"
        else:
            count=0
    return count>=4

def candle_pattern_ok(c):
    """Validation du pattern : Pinbar / marubozu / mèche significative."""
    body = abs(c["close"] - c["open"])
    full = c["high"] - c["low"]
    upper = c["high"] - max(c["close"], c["open"])
    lower = min(c["close"], c["open"]) - c["low"]
    if full == 0: return False
    up_ratio = upper / full
    low_ratio = lower / full
    if (low_ratio>0.6 and c["close"]>c["open"]) or (up_ratio>0.6 and c["close"]<c["open"]):
        return True
    if body/full > 0.7:  # Marubozu
        return True
    return False

def detect_signal(df, state, sym):
    """Retourne un signal valide si toutes les conditions sont remplies."""
    if len(df)<5: return None
    last, prev = df.iloc[-1], df.iloc[-2]
    above80 = last["close"] >= last["bb80_mid"]

    # Sortie prolongée -> ignorer premier signal
    if state.get(sym, {}).get("cooldown", False):
        state[sym]["cooldown"] = False
        return None
    if prolonged_double_exit(df):
        state.setdefault(sym, {})["cooldown"] = True
        return None

    # Conditions principales
    reinteg_long  = (prev["low"]  <= min(prev["bb20_lo"], prev["bb80_lo"])) and (last["close"] > last["bb20_lo"])
    reinteg_short = (prev["high"] >= max(prev["bb20_up"], prev["bb80_up"])) and (last["close"] < last["bb20_up"])

    # Réaction du prix (pattern + clôture dans BB20)
    in_bb20 = (last["close"]<=last["bb20_up"]) and (last["close"]>=last["bb20_lo"])
    valid_pattern = candle_pattern_ok(last)
    if not valid_pattern or not in_bb20:
        return None

    if above80 and reinteg_long:
        side, regime = "buy","trend"
    elif (not above80) and reinteg_short:
        side, regime = "sell","trend"
    elif reinteg_long:
        side, regime = "buy","counter"
    elif reinteg_short:
        side, regime = "sell","counter"
    else:
        return None

    entry=float(last["close"])
    atr=float(last["atr"])
    if side=="buy":
        sl=float(prev["low"]) - SL_ATR_CUSHION*atr
        tp=float(last["bb80_up"]) - atr*0.2
    else:
        sl=float(prev["high"]) + SL_ATR_CUSHION*atr
        tp=float(last["bb80_lo"]) + atr*0.2

    rr = abs((tp-entry)/(entry-sl)) if entry!=sl else 0
    if rr < MIN_RR: return None
    return {"side":side,"regime":regime,"entry":entry,"sl":sl,"tp":tp,"rr":rr}

# ============================
# UTILITAIRES ORDRES / PAPER
# ============================
def compute_qty(entry, sl, risk_amount):
    diff = abs(entry - sl)
    return risk_amount / diff if diff>0 else 0

def has_open_position(ex, sym):
    try:
        pos = ex.fetch_positions([sym])
        for p in pos:
            if abs(float(p.get("contracts") or 0))>0:
                return True
    except: pass
    return False

def count_open_positions(ex):
    try:
        pos = ex.fetch_positions()
        return sum(1 for p in pos if abs(float(p.get("contracts") or 0))>0)
    except: return 0

# ============================
# RAPPORTS
# ============================
def ensure_trades_csv():
    if not os.path.exists(TRADES_CSV):
        with open(TRADES_CSV,"w",newline="",encoding="utf-8") as f:
            csv.writer(f).writerow(["ts","symbol","side","entry","exit","pnl_pct","rr","mode"])

def log_trade_close(symbol, side, entry, exit_price, rr, mode):
    ensure_trades_csv()
    pnl_pct = (exit_price-entry)/entry*100.0 if side=="buy" else (entry-exit_price)/entry*100.0
    with open(TRADES_CSV,"a",newline="",encoding="utf-8") as f:
        csv.writer(f).writerow([datetime.utcnow().isoformat(),symbol,side,entry,exit_price,pnl_pct,rr,mode])
    return pnl_pct

# ============================
# MAIN LOOP
# ============================
def main():
    ex = create_exchange()
    tg_send(f"🚀 Bot Bitget {'TESTNET' if BITGET_TESTNET else 'LIVE'} — Mode {'PAPER' if DRY_RUN else 'REAL'} — H1 — Levier {MAX_LEVERAGE}x")

    # Univers
    try:
        ex.load_markets()
        symbols = [s for s in ex.symbols if ":USDT" in s and "SWAP" in s.upper()]
        universe = symbols[:UNIVERSE_SIZE] or FALLBACK_TESTNET
    except:
        universe = FALLBACK_TESTNET
    for s in universe:
        try_set_leverage(ex, s, MAX_LEVERAGE)

    state = {}
    active_paper = {}
    last_ts_seen = {}

    while True:
        try:
            for sym in universe:
                df = fetch_ohlcv_df(ex, sym, TF)
                last_ts = df.index[-1]
                if last_ts_seen.get(sym)==last_ts:
                    continue
                last_ts_seen[sym]=last_ts
                sig = detect_signal(df, state, sym)
                if not sig: continue

                open_cnt = len(active_paper) if DRY_RUN else count_open_positions(ex)
                if open_cnt >= MAX_OPEN_TRADES: continue
                if not DRY_RUN and has_open_position(ex, sym): continue

                tg_send(f"📈 Signal {sym} {sig['side']} RR={sig['rr']:.2f} — entrée à {sig['entry']:.4f}")

                try:
                    usdt = 1000.0 if DRY_RUN else float(ex.fetch_balance().get("USDT", {}).get("free", 0))
                except:
                    usdt=1000.0
                qty = compute_qty(sig["entry"], sig["sl"], usdt*RISK_PER_TRADE)
                if qty<=0: continue

                if DRY_RUN:
                    active_paper[sym] = {"side":sig["side"],"entry":sig["entry"],"sl":sig["sl"],"tp":sig["tp"],"rr":sig["rr"],"ts":datetime.utcnow()}
                    tg_send(f"🧪 PAPER {sym} {sig['side']} RR={sig['rr']:.2f}")
                else:
                    ex.create_order(sym,"market",sig["side"],qty)
                    ex.create_order(sym,"limit","sell" if sig["side"]=="buy" else "buy",qty,sig["tp"],{"reduceOnly":True})
                    ex.create_order(sym,"stop","sell" if sig["side"]=="buy" else "buy",qty,params={"stopPrice":sig["sl"],"reduceOnly":True})
                    tg_send(f"✅ Ordre réel placé {sym}")

            time.sleep(LOOP_DELAY)
        except KeyboardInterrupt:
            tg_send("🛑 Arrêt manuel"); break
        except Exception as e:
            tg_send(f"⚠️ Erreur boucle : {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
