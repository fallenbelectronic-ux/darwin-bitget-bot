# main.py
import os, time, math, csv, io, requests, traceback
import ccxt
import pandas as pd
import numpy as np
from collections import deque
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from ta.volatility import BollingerBands, AverageTrueRange

load_dotenv()

# ========================
# ENV / PARAMS
# ========================
BITGET_TESTNET   = os.getenv("BITGET_TESTNET","true").lower() in ("1","true","yes")
API_KEY          = os.getenv("BITGET_API_KEY")
API_SECRET       = os.getenv("BITGET_API_SECRET")
PASSPHRASE       = os.getenv("BITGET_API_PASSWORD") or os.getenv("BITGET_PASSPHRASE")

TF               = os.getenv("TIMEFRAME", "1h")
DRY_RUN          = os.getenv("DRY_RUN","true").lower() in ("1","true","yes")
UNIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE","30"))   # Top-30
MAX_PER_HOUR     = int(os.getenv("PICKS","4"))            # 4 meilleurs / heure
MAX_OPEN_TRADES  = int(os.getenv("MAX_OPEN_TRADES","4"))
RISK_PER_TRADE   = float(os.getenv("RISK_PER_TRADE","0.01"))
MIN_RR           = float(os.getenv("MIN_RR","3.0"))
ATR_WINDOW       = 14
SL_ATR_CUSHION   = 0.25
TP_TICKS         = 2          # TP un peu avant la bande
CONTACT_TOL      = 0.0008     # tolérance contact (~0.08%)

TRADES_CSV       = os.getenv("TRADES_CSV", "./trades.csv")
TZNAME           = os.getenv("TIMEZONE", "Europe/Lisbon")

# Telegram
TG_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN","")
TG_CHATID = os.getenv("TELEGRAM_CHAT_ID","")

# ========================
# TELEGRAM HELPERS
# ========================
SENT_MSGS = deque(maxlen=6000)   # [{id, ts(UTC), kind}] kind in {'trade','signal','info','error','keep'}
LAST_UPDATE_ID = None
RUNNING = True
LAST_PURGE_HOUR = None
LAST_SIGNAL_PURGE_DATE = None  # 'YYYY-MM-DD' (TZ local)

def tg_api(method, payload=None, files=None, timeout=10):
    if not TG_TOKEN: return None
    url = f"https://api.telegram.org/bot{TG_TOKEN}/{method}"
    try:
        if files:
            r = requests.post(url, data=payload or {}, files=files, timeout=timeout)
        else:
            r = requests.post(url, json=payload or {}, timeout=timeout)
        if not r.ok: return None
        data = r.json()
        return data if data.get("ok") else None
    except Exception:
        return None

def tg_send(text:str, kind:str="info", keep=False):
    """kind: 'trade'|'signal'|'info'|'error'|'keep' ; keep=True empêche les purges."""
    if not TG_TOKEN or not TG_CHATID: return None
    data = tg_api("sendMessage", {"chat_id": TG_CHATID, "text": text, "parse_mode":"Markdown"})
    if data and data.get("result"):
        mid = data["result"]["message_id"]
        SENT_MSGS.append({"id": mid, "ts": datetime.now(timezone.utc), "kind": ("trade" if kind=="trade" else ("keep" if keep else kind))})
        return mid
    return None

def tg_delete(mid:int):
    tg_api("deleteMessage", {"chat_id": TG_CHATID, "message_id": mid})

def tg_send_document(filename:str, bytes_data:bytes, caption:str=""):
    files = {'document': (filename, bytes_data)}
    payload = {"chat_id": TG_CHATID, "caption": caption}
    tg_api("sendDocument", payload, files=files)

def utc_to_local(utc_dt: datetime) -> datetime:
    # simple tz conversion using fixed offset via time.tzname is unreliable; use offset from env if needed.
    # Here we use Python's zoneinfo if available (Py>=3.9). If not, we approximate with system localtime.
    try:
        from zoneinfo import ZoneInfo
        return utc_dt.astimezone(ZoneInfo(TZNAME))
    except Exception:
        return utc_dt.astimezone()

def purge_chat(keep_kinds=("trade","signal","keep"), only_older_than_utc: datetime|None=None, remove_signals=False):
    """
    Supprime les messages envoyés par le bot.
    - keep_kinds: types conservés
    - only_older_than_utc: si fourni, on ne supprime que les messages plus anciens que cette date UTC
    - remove_signals: si True, on supprime aussi les 'signal' (même s'ils sont dans keep_kinds)
    """
    survivors = deque(maxlen=SENT_MSGS.maxlen)
    for m in list(SENT_MSGS):
        k = m["kind"]
        if remove_signals and k == "signal":
            tg_delete(m["id"])
            continue
        if k in keep_kinds:
            if only_older_than_utc and m["ts"] > only_older_than_utc:
                survivors.append(m); continue
            if only_older_than_utc is None:
                survivors.append(m); continue
        # supprime si non-conforme
        if only_older_than_utc and m["ts"] > only_older_than_utc:
            survivors.append(m); continue
        tg_delete(m["id"])
    SENT_MSGS.clear()
    for x in survivors: SENT_MSGS.append(x)

def hourly_auto_purge():
    """Chaque heure: on supprime info/erreur et on garde *trades* + *signaux du jour*."""
    global LAST_PURGE_HOUR
    now_utc = datetime.now(timezone.utc)
    key = now_utc.strftime("%Y-%m-%d %H")
    if LAST_PURGE_HOUR == key:
        return
    if now_utc.minute == 0 and now_utc.second <= 10:
        # garde trades et signaux du *jour courant* (TZ)
        today_local = utc_to_local(now_utc).date()
        survivors = deque(maxlen=SENT_MSGS.maxlen)
        for m in list(SENT_MSGS):
            if m["kind"] in ("trade","keep"):
                survivors.append(m); continue
            if m["kind"] == "signal":
                if utc_to_local(m["ts"]).date() == today_local:
                    survivors.append(m)
                else:
                    tg_delete(m["id"])
                continue
            # info / error -> delete
            tg_delete(m["id"])
        SENT_MSGS.clear()
        for x in survivors: SENT_MSGS.append(x)
        LAST_PURGE_HOUR = key

def nightly_signals_purge():
    """Chaque nuit (minuit local): supprimer *tous* les signaux de la veille et antérieurs, garder trades & keep uniquement."""
    global LAST_SIGNAL_PURGE_DATE
    now_local = utc_to_local(datetime.now(timezone.utc))
    today_key = now_local.strftime("%Y-%m-%d")
    if LAST_SIGNAL_PURGE_DATE == today_key:
        return
    if now_local.hour == 0 and now_local.minute <= 2:
        # supprimer tout 'signal'
        purge_chat(keep_kinds=("trade","keep"), remove_signals=True)
        LAST_SIGNAL_PURGE_DATE = today_key

# ========================
# EXCHANGE
# ========================
def create_exchange():
    ex = ccxt.bitget({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "password": PASSPHRASE,
        "enableRateLimit": True,
        "options": {"defaultType": "swap", "testnet": BITGET_TESTNET}
    })
    if BITGET_TESTNET:
        try: ex.set_sandbox_mode(True)
        except Exception: pass
    return ex

# ========================
# DATA / INDICATEURS
# ========================
def fetch_ohlcv_df(ex, symbol, timeframe, limit=300):
    raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts","open","high","low","close","vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
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
    return df.dropna()

def get_tick(p):                 
    return max(p*0.0001, 0.0000001)

def within(x, target, tol):
    return abs(x-target) <= tol

def had_contact_last_or_prev(df, side:str) -> bool:
    """Contact BB20 sur *dernière* ou *précédente* bougie (pas plus loin)."""
    if len(df) < 2: return False
    last, prev = df.iloc[-1], df.iloc[-2]
    tol_last = max(last["close"]*CONTACT_TOL, get_tick(last["close"]))
    tol_prev = max(prev["close"]*CONTACT_TOL, get_tick(prev["close"]))

    if side == "buy":
        c1 = (last["low"] <= last["bb20_lo"] + tol_last) or within(last["low"], last["bb20_lo"], tol_last)
        c2 = (prev["low"] <= last["bb20_lo"] + tol_prev) or within(prev["low"], last["bb20_lo"], tol_prev)
        return c1 or c2
    else:
        c1 = (last["high"] >= last["bb20_up"] - tol_last) or within(last["high"], last["bb20_up"], tol_last)
        c2 = (prev["high"] >= last["bb20_up"] - tol_prev) or within(prev["high"], last["bb20_up"], tol_prev)
        return c1 or c2

def strong_reaction(candle: pd.Series) -> bool:
    o,h,l,c = float(candle["open"]),float(candle["high"]),float(candle["low"]),float(candle["close"])
    rng  = max(h-l, 1e-12); body = abs(c-o)
    up_wick   = h-max(c,o); down_wick = min(c,o)-l
    return (body/rng >= 0.30) or (up_wick/rng >= 0.30) or (down_wick/rng >= 0.30)

def bb20_outside_bb80(c: pd.Series) -> bool:
    return (c["bb20_lo"] < c["bb80_lo"]) or (c["bb20_up"] > c["bb80_up"])

def prolonged_outside_count(df) -> int:
    """Enfoncement 'deux bandes' avant la bougie de signal.
       La 4ᵉ barre n’est pas comptée si la dernière clôture est déjà dans BB20.
    """
    if len(df) < 5: return 0
    last = df.iloc[-1]
    end_idx = -2 if (last["close"]<=last["bb20_up"] and last["close"]>=last["bb20_lo"]) else -1
    cnt = 0; i = end_idx
    while abs(i) <= len(df):
        r = df.iloc[i]
        out_up   = (r["high"] >= r["bb20_up"]) and (r["high"] >= r["bb80_up"])
        out_down = (r["low"]  <= r["bb20_lo"]) and (r["low"]  <= r["bb80_lo"])
        if out_up or out_down: cnt += 1; i -= 1
        else: break
    return cnt

# ========================
# UNIVERS (Top-30 volume)
# ========================
def build_universe(ex):
    try:
        ex.load_markets()
        cands = []
        for m in ex.markets.values():
            if (m.get("type")=="swap" or m.get("swap")) and m.get("linear") \
               and m.get("quote")=="USDT" and m.get("settle")=="USDT":
                cands.append(m["symbol"])
    except Exception:
        cands = []

    rows=[]
    try:
        tks = ex.fetch_tickers(cands if cands else None)
        for s,t in tks.items():
            if "/USDT" not in s and ":USDT" not in s: 
                continue
            vol = t.get("quoteVolume") or t.get("baseVolume") or 0
            try: vol=float(vol)
            except: vol=0.0
            rows.append((s,vol))
    except Exception:
        pass

    if rows:
        df = pd.DataFrame(rows, columns=["symbol","vol"]).sort_values("vol", ascending=False)
        return df.head(UNIVERSE_SIZE)["symbol"].tolist()

    return ["BTC/USDT:USDT","ETH/USDT:USDT","XRP/USDT:USDT"]

# ========================
# SIGNALS (règles TWIN)
# ========================
def detect_signal(df, state, sym):
    if len(df) < 3: 
        return None
    last, prev = df.iloc[-1], df.iloc[-2]
    atr = float(last["atr"])
    entry = float(last["close"])
    tick  = get_tick(entry)

    # 3 conditions : contact (last/prev), réaction forte, close dans/sur BB20
    in_bb20 = (last["close"]<=last["bb20_up"]) and (last["close"]>=last["bb20_lo"])
    if not in_bb20: return None
    if not strong_reaction(last): return None

    above80 = last["close"] >= last["bb80_mid"]
    ext_cnt = prolonged_outside_count(df)
    need_double_reint = bb20_outside_bb80(last) and (ext_cnt >= 4)

    # ---- Contre-tendance (double extrême) ----
    if above80:
        side_ct = "sell"
        has_contact = had_contact_last_or_prev(df, "sell")
        double_ok = (last["close"]<=last["bb80_up"] and last["close"]>=last["bb80_lo"])
        if has_contact and ( (not need_double_reint) or double_ok ):
            st = state.setdefault(sym, {"cooldown_done":False})
            if need_double_reint and not st.get("cooldown_done", False):
                st["cooldown_done"]=True
                return None
            sl = max(prev["high"], last["high"]) + SL_ATR_CUSHION*atr
            tp = float(last["bb20_lo"] - TP_TICKS*tick)
            rr = abs((tp-entry) / (abs(entry-sl) or 1e-12))
            if rr >= MIN_RR: 
                return {"side":side_ct,"regime":"counter","entry":entry,"sl":float(sl),"tp":tp,"rr":float(rr)}
    else:
        side_ct = "buy"
        has_contact = had_contact_last_or_prev(df, "buy")
        double_ok = (last["close"]<=last["bb80_up"] and last["close"]>=last["bb80_lo"])
        if has_contact and ( (not need_double_reint) or double_ok ):
            st = state.setdefault(sym, {"cooldown_done":False})
            if need_double_reint and not st.get("cooldown_done", False):
                st["cooldown_done"]=True
                return None
            sl = min(prev["low"], last["low"]) - SL_ATR_CUSHION*atr
            tp = float(last["bb20_up"] + TP_TICKS*tick)
            rr = abs((tp-entry) / (abs(entry-sl) or 1e-12))
            if rr >= MIN_RR: 
                return {"side":side_ct,"regime":"counter","entry":entry,"sl":float(sl),"tp":tp,"rr":float(rr)}

    # ---- Tendance ----
    if above80:
        side="buy"
        if not had_contact_last_or_prev(df,"buy"): return None
        sl = min(prev["low"], last["low"]) - SL_ATR_CUSHION*atr
        tp = float(last["bb80_up"] - TP_TICKS*tick)
    else:
        side="sell"
        if not had_contact_last_or_prev(df,"sell"): return None
        sl = max(prev["high"], last["high"]) + SL_ATR_CUSHION*atr
        tp = float(last["bb80_lo"] + TP_TICKS*tick)

    rr = abs((tp-entry) / (abs(entry-sl) or 1e-12))
    if rr < MIN_RR: return None
    return {"side":side,"regime":"trend","entry":entry,"sl":float(sl),"tp":tp,"rr":float(rr)}

# ========================
# ORDRES & RISK
# ========================
def acct_usdt_free(ex) -> float:
    try:
        b = ex.fetch_balance()
        if isinstance(b, dict) and "USDT" in b:
            return float(b["USDT"].get("free",0) or b["USDT"].get("available",0) or 0)
        if "free" in b and isinstance(b["free"], dict):
            return float(b["free"].get("USDT",0))
    except Exception:
        pass
    return 0.0

def qty_from_risk(entry, sl, risk_usdt):
    diff = abs(entry-sl)
    return max(risk_usdt / diff, 0.0) if diff>0 else 0.0

# ========================
# CSV JOURNAL
# ========================
def ensure_trades_csv():
    if not os.path.exists(TRADES_CSV):
        with open(TRADES_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["ts","symbol","side","regime","entry","sl","tp","rr","mode"])

def log_open_trade(symbol, side, regime, entry, sl, tp, rr, mode):
    ensure_trades_csv()
    with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([datetime.utcnow().isoformat(), symbol, side, regime, f"{entry:.8f}", f"{sl:.8f}", f"{tp:.8f}", f"{rr:.2f}", mode])

# ========================
# COMMANDES TELEGRAM
# ========================
ACTIVE_PAPER = {}  # id -> dict
PAPER_SEQ = 0

def poll_commands(ex):
    global LAST_UPDATE_ID, RUNNING
    if not TG_TOKEN or not TG_CHATID: return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    if LAST_UPDATE_ID is not None: url += f"?offset={LAST_UPDATE_ID+1}"
    try:
        res = requests.get(url, timeout=8).json()
        if not res.get("ok"): return
        for upd in res.get("result", []):
            LAST_UPDATE_ID = upd["update_id"]
            msg = upd.get("message") or upd.get("edited_message")
            if not msg: continue
            if str(msg["chat"]["id"]) != str(TG_CHATID): continue
            text = (msg.get("text") or "").strip()
            low = text.lower()

            if low.startswith("/start"):
                tg_send("🤖 Bot lancé. Je conserve les *trades* et les *signaux du jour* (purge auto toutes les heures pour le reste). Purge des *signaux* chaque nuit.", keep=True)

            elif low.startswith("/config"):
                mode = "PAPER" if DRY_RUN else ("LIVE" if not BITGET_TESTNET else "TESTNET")
                tg_send(f"*Config*\nMode: {mode}\nTF: {TF}\nTop: {UNIVERSE_SIZE} | Picks/h: {MAX_PER_HOUR}\nRisk: {int(RISK_PER_TRADE*100)}% | RR≥{MIN_RR}\nCSV: {TRADES_CSV}")

            elif low.startswith("/stats"):
                ensure_trades_csv()
                try:
                    df = pd.read_csv(TRADES_CSV)
                    n = len(df)
                    rr = df["rr"].astype(float).mean() if n else 0.0
                    tg_send(f"*Stats*\nTrades loggés: {n}\nRR moyen: x{rr:.2f}")
                except Exception:
                    tg_send("*Stats*\nAucune donnée.")

            elif low.startswith("/mode"):
                tg_send(f"*Mode actuel*: {'PAPER' if DRY_RUN else ('LIVE' if not BITGET_TESTNET else 'TESTNET')}")

            elif low.startswith("/exportcsv"):
                try:
                    with open(TRADES_CSV, "rb") as f:
                        tg_send_document("trades.csv", f.read(), caption="Journal des trades")
                except Exception:
                    tg_send("⚠️ Pas de journal disponible.")

            elif low.startswith("/orders"):
                if DRY_RUN:
                    if not ACTIVE_PAPER:
                        tg_send("Aucune position (papier)."); continue
                    lines=["*Positions (papier)*"]
                    for pid, p in ACTIVE_PAPER.items():
                        lines.append(f"• #{pid} {p['symbol']} {p['side']} entry {p['entry']:.6f}")
                    tg_send("\n".join(lines))
                else:
                    try:
                        pos = ex.fetch_positions()
                        rows=[]
                        for p in pos:
                            size = float(p.get("contracts") or 0)
                            if abs(size)>0:
                                rows.append(f"• {p.get('symbol')} qty {abs(size)}")
                        tg_send("*Positions*\n"+("\n".join(rows) if rows else "aucune"))
                    except Exception as e:
                        tg_send(f"⚠️ orders: {e}")

            elif low.startswith("/test"):
                if not DRY_RUN:
                    tg_send("⚠️ /test uniquement en PAPER.")
                    continue
                global PAPER_SEQ
                PAPER_SEQ += 1
                pid = PAPER_SEQ
                ACTIVE_PAPER[pid] = {"symbol":"BTC/USDT:USDT","side":"buy","entry":100.0}
                tg_send(f"✅ Test: ouverture papier #{pid} BTC/USDT:USDT", kind="trade")

            elif low.startswith("/closepaper"):
                if not DRY_RUN:
                    tg_send("⚠️ /closepaper uniquement en PAPER."); continue
                parts = low.split()
                if len(parts)>=2:
                    try:
                        pid = int(parts[1])
                        if ACTIVE_PAPER.pop(pid, None):
                            tg_send(f"✅ Papier fermé #{pid}")
                        else:
                            tg_send("⚠️ ID introuvable.")
                    except:
                        tg_send("Usage: /closepaper <id>")
                else:
                    tg_send("Usage: /closepaper <id>")

            elif low.startswith("/closeallpaper"):
                if not DRY_RUN:
                    tg_send("⚠️ /closeallpaper uniquement en PAPER."); continue
                n=len(ACTIVE_PAPER); ACTIVE_PAPER.clear()
                tg_send(f"✅ Fermeture papier : {n} positions")

            elif low.startswith("/pause"):
                RUNNING = False
                tg_send("⏸️ Pause : scan suspendu (les commandes restent actives).")

            elif low.startswith("/resume"):
                RUNNING = True
                tg_send("▶️ Reprise : scan relancé.")

            elif low.startswith("/logs"):
                tg_send("🗒️ Logs : silencieux (anti-spam).")

            elif low.startswith("/ping"):
                tg_send("🏓 Ping ok.")

            elif low.startswith("/report"):
                tg_send("🧭 Rapport (compact) — le chat conserve *trades* et *signaux du jour*.")

            elif low.startswith("/restart"):
                tg_send("♻️ Redémarrage demandé (placeholder).")

            elif low.startswith("/purge"):
                # Nettoyage « bruit » : garde trades + signaux du *jour*
                hourly_auto_purge()
                tg_send("🧹 Purge: seuls *trades* et *signaux du jour* sont conservés.", keep=True)

            elif low.startswith("/purgesignals"):
                # Efface *tous* les signaux
                purge_chat(keep_kinds=("trade","keep"), remove_signals=True)
                tg_send("🧹 Purge *signaux* effectuée (trades conservés).", keep=True)

    except Exception:
        pass

# ========================
# BOUCLE PRINCIPALE
# ========================
def main():
    ex = create_exchange()
    universe = build_universe(ex)

    last_bar = {}       # symbole -> timestamp dernière bougie traitée
    state = {}          # cooldown / état étendu

    tg_send(f"🤖 *Bot prêt* — Mode {'PAPER' if DRY_RUN else ('LIVE' if not BITGET_TESTNET else 'TESTNET')} — TF {TF} — Top{len(universe)} — Picks/h {MAX_PER_HOUR} — RR≥{MIN_RR}", keep=True)

    while True:
        try:
            # commandes & purges
            poll_commands(ex)
            hourly_auto_purge()
            nightly_signals_purge()

            if not RUNNING:
                time.sleep(3)
                continue

            picked = []
            for sym in universe:
                try:
                    df = fetch_ohlcv_df(ex, sym, TF, 300)
                except Exception:
                    continue
                last_ts = df.index[-1]
                # agir uniquement à l'ouverture d'une *nouvelle* bougie H1
                if last_bar.get(sym) == last_ts:
                    continue
                last_bar[sym] = last_ts

                sig = detect_signal(df, state, sym)
                if sig:
                    picked.append((sym, sig))

            # 4 meilleurs RR
            if picked:
                picked.sort(key=lambda x: x[1]["rr"], reverse=True)
                picked = picked[:MAX_PER_HOUR]

                # slots réels (si non PAPER)
                open_slots = MAX_OPEN_TRADES
                if not DRY_RUN:
                    try:
                        pos = ex.fetch_positions()
                        open_now = sum(1 for p in pos if abs(float(p.get("contracts") or 0))>0)
                        open_slots = max(0, MAX_OPEN_TRADES - open_now)
                    except Exception:
                        pass
                if not DRY_RUN and open_slots<=0:
                    picked=[]

                usdt = 1000.0 if DRY_RUN else acct_usdt_free(ex)
                risk_usdt = max(1.0, usdt*RISK_PER_TRADE)

                for sym, s in picked:
                    qty = qty_from_risk(s["entry"], s["sl"], risk_usdt)
                    if qty <= 0: 
                        continue

                    side_txt = "LONG" if s["side"]=="buy" else "SHORT"
                    bullets = [
                        "Contact bande " + ("basse BB20" if s["side"]=="buy" else "haute BB20"),
                        "Clôture dans/sur BB20",
                        f"RR x{s['rr']:.2f} (≥ {MIN_RR})",
                        "Tendance" if s["regime"]=="trend" else "Contre-tendance"
                    ]
                    tg_send(
                        f"📈 *Signal {'[PAPER]' if DRY_RUN else ''}* `{sym}` {side_txt}\n"
                        f"Entrée `{s['entry']:.6f}` | SL `{s['sl']:.6f}` | TP `{s['tp']:.6f}`\n" +
                        "\n".join([f"• {b}" for b in bullets]),
                        kind="signal"
                    )

                    if DRY_RUN:
                        tg_send(f"🎯 *PAPER* {sym} {'BUY' if s['side']=='buy' else 'SELL'} @{s['entry']:.6f}  RR={s['rr']:.2f}", kind="trade")
                        log_open_trade(sym, s["side"], s["regime"], s["entry"], s["sl"], s["tp"], s["rr"], "paper")
                    else:
                        try:
                            ex.create_order(sym, "market", s["side"], qty)
                            opp = "sell" if s["side"]=="buy" else "buy"
                            ex.create_order(sym, "stop", opp, qty, params={"stopPrice": s["sl"], "reduceOnly": True})
                            ex.create_order(sym, "limit", opp, qty, price=s["tp"], params={"reduceOnly": True})
                            tg_send(f"✅ {sym} {side_txt} @{s['entry']:.6f}  RR≈{s['rr']:.2f}", kind="trade")
                            log_open_trade(sym, s["side"], s["regime"], s["entry"], s["sl"], s["tp"], s["rr"], "real")
                        except Exception as e:
                            tg_send(f"⚠️ {sym} ordre échoué : {e}", kind="error")

            time.sleep(4)

        except KeyboardInterrupt:
            tg_send("⛔ Arrêt manuel.", keep=True); break
        except Exception as e:
            tg_send(f"🚨 Loop error: {e}", kind="error")
            time.sleep(5)

if __name__ == "__main__":
    main()
