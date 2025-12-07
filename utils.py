# Fichier: utils.py
import ccxt
import pandas as pd
import numpy as np
import time
import random
from typing import Optional
from ta.volatility import BollingerBands, AverageTrueRange

_MIN_ROWS = 100          # pour BB80 + ATR confortablement
_EPS = 1e-9              # tolérance numérique

def get_universe_by_market_cap(ex, universe_size):
    """
    Construit un univers de paires USDT-perp triées par volume/turnover 24h (avec fallbacks).

    MISE À JOUR :
    - Priorise une liste fixe de bases (BTC, EGLD, KITE, etc.) fournie par l'utilisateur.
    - Les paires correspondant à ces bases sont ajoutées en tête de l'univers.
    - Ensuite, on complète avec les autres perps classés par volume.
    - universe_size reste le NOMBRE MAXIMUM TOTAL de paires retournées, mais on garantit
      que toutes les paires prioritaires présentes sur l'exchange sont incluses, quitte
      à dépasser la valeur si universe_size < nb_prioritaires.

    Ne renvoie jamais une liste vide : plusieurs niveaux de repli, puis un 'core set' si nécessaire.

    Args:
        ex: instance ccxt (Bybit/Bitget) déjà configurée (rate limit, defaultType=swap si possible)
        universe_size: nombre maximum de paires à renvoyer (prioritaires + auto)

    Returns:
        list[str]: symboles CCXT (priorité perp 'BASE/USDT:USDT', fallback spot 'BASE/USDT')
    """
    # ---------- Bases prioritaires (liste de l'utilisateur) ----------
    preferred_bases = [
        "BTC", "EGLD", "KITE", "ALGO", "HBAR", "ETH", "AVAX", "BEAM", "CAKE", "RSR",
        "LIGHT", "KAS", "SUI", "XLM", "ZEC", "XRP", "SOL", "TAO", "BNB", "DOT", "NEAR",
        "TURBO", "LQTY", "GALA", "EIGEN", "CFX", "APT", "LTC", "ETC", "ADA", "ALICE",
        "LINK", "TON", "IMX", "ZK", "IOTA", "STX", "OP", "MANA", "VET", "ZIL", "DOGE",
        "SHIB", "RUNE", "1INCH", "CHZ", "RVN", "DYDX", "JASMY", "FET", "MINA", "DODO",
        "ARB", "FLUX", "QTUM", "BCH",
    ]
    preferred_bases = [b.upper() for b in preferred_bases]

    # ---------- Helpers locaux ----------
    def _is_usdt_perp(mkt):
        try:
            if not mkt or not mkt.get("active", True):
                return False
            q = (mkt.get("quote") or "").upper()
            is_swap = bool(mkt.get("swap")) or (str(mkt.get("type", "")).lower() == "swap")
            return is_swap and q == "USDT"
        except Exception:
            return False

    def _sym_candidates(sym):
        # Normalise: propose perp puis spot
        if not sym:
            return []
        base = sym.split("/")[0] if "/" in sym else sym.replace("USDT", "")
        out = []
        for cand in (f"{base}/USDT:USDT", f"{base}/USDT"):
            if cand not in out:
                out.append(cand)
        return out

    def _pick_existing_symbol(markets, sym):
        for cand in _sym_candidates(sym):
            if cand in markets:
                return cand
        return None

    def _ticker_last_price(t):
        # Essaie last/close/info.lastPrice/markPrice
        if not isinstance(t, dict):
            return None
        for k in ("last", "close"):
            v = t.get(k)
            try:
                return float(v)
            except Exception:
                pass
        info = t.get("info") if isinstance(t.get("info"), dict) else {}
        for k in ("lastPrice", "markPrice", "close", "last"):
            try:
                v = info.get(k)
                if v is not None:
                    return float(v)
            except Exception:
                pass
        return None

    def _ticker_quote_volume(sym, t):
        # 1) quoteVolume direct
        try:
            qv = t.get("quoteVolume")
            if qv is not None:
                return float(qv)
        except Exception:
            pass
        # 2) turnover/volume USD-like dans info
        info = t.get("info") if isinstance(t, dict) else {}
        for k in ("turnover24h", "volumeUsd24h", "usdVolume", "quoteVol", "volUsd24h", "volCcy24h"):
            try:
                v = info.get(k)
                if v is not None:
                    return float(v)
            except Exception:
                pass
        # 3) baseVolume * last
        last = _ticker_last_price(t) or 0.0
        try:
            bv = t.get("baseVolume")
            if bv is not None and last > 0:
                return float(bv) * float(last)
        except Exception:
            pass
        return 0.0

    # ---------- 1) Charger les marchés ----------
    try:
        ex.load_markets()
    except Exception as e:
        print(f"[utils.get_universe_by_market_cap] load_markets() a échoué: {e}")

    markets = getattr(ex, "markets", {}) or {}
    if not markets:
        print("⚠️ [utils.get_universe_by_market_cap] markets vide après load_markets().")

    # ---------- 2) Filtrer USDT perp ----------
    perp_symbols = []
    for sym, mkt in markets.items():
        try:
            if _is_usdt_perp(mkt):
                perp_symbols.append(sym)
        except Exception:
            continue

    # ---------- 3) Récup tickers (tri par volume) ----------
    tickers = {}
    try:
        tickers = ex.fetch_tickers(perp_symbols) if perp_symbols else {}
    except Exception as e:
        print(f"[utils.get_universe_by_market_cap] fetch_tickers() a échoué: {e}")
        tickers = {}

    scored = []
    if perp_symbols:
        for sym in perp_symbols:
            t = tickers.get(sym) or {}
            vol = _ticker_quote_volume(sym, t)
            scored.append((sym, vol))

    # ---------- 4) Fallback: essayer variantes symboles si volume nul ----------
    if not scored or all(v <= 0 for _, v in scored):
        alt_scored = []
        for sym in perp_symbols:
            if sym in tickers:
                continue
            alt = _pick_existing_symbol(markets, sym)
            if alt and alt != sym:
                try:
                    t = ex.fetch_ticker(alt)
                except Exception:
                    t = {}
                vol = _ticker_quote_volume(alt, t)
                alt_scored.append((alt, vol))
        if alt_scored:
            scored = alt_scored

    # ---------- 5) Fallback: heuristique à partir de markets.info ----------
    if not scored or all(v <= 0 for _, v in scored):
        for sym, mkt in markets.items():
            if not _is_usdt_perp(mkt):
                continue
            info = mkt.get("info") or {}
            vol = 0.0
            for k in ("turnover24h", "volumeUsd24h", "openInterestUsd", "openInterestValue"):
                try:
                    v = info.get(k)
                    if v is not None:
                        vol = max(vol, float(v))
                except Exception:
                    pass
            scored.append((sym, vol))

    # ---------- 6) Dernier filet: core set si toujours vide ----------
    if not scored or all(v <= 0 for _, v in scored):
        core = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT"]
        picked = []
        for base in core:
            sym = _pick_existing_symbol(markets, base)
            if sym:
                picked.append((sym, 1.0))  # volume fictif pour ranking
        if picked:
            scored = picked

    # ---------- 7) Tri, priorisation, dédoublonnage, découpe ----------
    if not scored:
        print("⚠️ utils.get_universe_by_market_cap: aucun marché scoré après tous les fallbacks.")
        return []

    scored_sorted = sorted(scored, key=lambda x: (x[1], x[0]), reverse=True)

    # 7.a) Sélection des symboles prioritaires (bases dans preferred_bases)
    preferred_symbols = []
    seen_pref = set()
    for sym, _vol in scored_sorted:
        mkt = markets.get(sym, {})
        base = (mkt.get("base") or "").upper()
        if base in preferred_bases and sym not in seen_pref:
            preferred_symbols.append(sym)
            seen_pref.add(sym)

    # 7.b) Reste des symboles auto (non prioritaires)
    auto_symbols = []
    for sym, _vol in scored_sorted:
        if sym in seen_pref:
            continue
        auto_symbols.append(sym)

    # 7.c) Taille max de l'univers (on garantit au moins toutes les priorités)
    try:
        max_n_raw = int(universe_size or 30)
    except Exception:
        max_n_raw = 30
    max_n = max(1, max(len(preferred_symbols), max_n_raw))

    final_symbols = []
    seen_all = set()

    # D'abord les priorités
    for sym in preferred_symbols:
        if sym in seen_all:
            continue
        seen_all.add(sym)
        final_symbols.append(sym)

    # Puis le reste jusqu'à max_n
    for sym in auto_symbols:
        if len(final_symbols) >= max_n:
            break
        if sym in seen_all:
            continue
        seen_all.add(sym)
        final_symbols.append(sym)

    if not final_symbols:
        print("⚠️ utils.get_universe_by_market_cap: univers vide après tous les fallbacks.")
        minimal = []
        for cand in ("BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT"):
            if cand in markets:
                minimal.append(cand)
        if not minimal:
            for cand in ("BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"):
                if cand in markets:
                    minimal.append(cand)
        return minimal[:max_n]

    return final_symbols



def fetch_and_prepare_df(ex: ccxt.Exchange, symbol: str, timeframe: str, limit: int = 200) -> Optional[pd.DataFrame]:
    """
    Récupère l'OHLCV et calcule:
      - MM(80):   mm80 (moyenne mobile simple 80 périodes)
      - BB(20,2): bb20_up, bb20_mid, bb20_lo
      - BB(80,2): bb80_up, bb80_mid, bb80_lo
      - ATR(14):  atr
    Retourne None si données insuffisantes.
    """
    try:
        if not getattr(ex, "markets", None):
            ex.load_markets()

        # Récupération OHLCV avec retries robustes (5xx/timeouts)
        ohlcv = _safe_fetch_ohlcv_with_retries(ex, symbol, timeframe, limit=limit, params={})
        if not ohlcv or len(ohlcv) < _MIN_ROWS:
            return None

        df = pd.DataFrame(
            ohlcv,
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

        # Index temporel propre (UTC, trié)
        ts = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df.drop(columns=["timestamp"])
        df.index = ts
        df.index.name = "timestamp"
        df.sort_index(inplace=True)

        # Cast robustes
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # ========================================================================
        # CALCUL MOYENNE MOBILE 80 (mm80) - AJOUTÉ
        # ========================================================================
        df["mm80"] = df["close"].rolling(window=80).mean()

        # ========================================================================
        # CALCUL BOLLINGER BANDS 20
        # ========================================================================
        bb20 = BollingerBands(close=df["close"], window=20, window_dev=2)
        df["bb20_up"]  = bb20.bollinger_hband()
        df["bb20_mid"] = bb20.bollinger_mavg()
        df["bb20_lo"]  = bb20.bollinger_lband()

        # ========================================================================
        # CALCUL BOLLINGER BANDS 80
        # ========================================================================
        bb80 = BollingerBands(close=df["close"], window=80, window_dev=2)
        df["bb80_up"]  = bb80.bollinger_hband()
        df["bb80_mid"] = bb80.bollinger_mavg()
        df["bb80_lo"]  = bb80.bollinger_lband()

        # ========================================================================
        # CALCUL ATR 14
        # ========================================================================
        atr = AverageTrueRange(
            high=df["high"], low=df["low"], close=df["close"], window=14
        ).average_true_range()
        df["atr"] = atr

        # Nettoyage final
        df = df.dropna().copy()
        if len(df) < _MIN_ROWS:
            return None

        return df

    except Exception as e:
        print(f"fetch_and_prepare_df error on {symbol} {timeframe}: {e}")
        return None
        
def _safe_fetch_ohlcv_with_retries(ex, symbol: str, timeframe: str, limit: int = 200, params: Optional[dict] = None):
    """
    Wrapper robuste autour ex.fetch_ohlcv avec retries exponentiels + jitter.
    Retourne [] en cas d'échec final (le caller gère ensuite len/None).
    """
    if params is None:
        params = {}

    max_retries = 4
    backoffs = [0.5, 1.0, 2.0, 4.0]  # secondes

    for attempt in range(max_retries):
        try:
            return ex.fetch_ohlcv(symbol, timeframe, limit=limit, params=params)
        except Exception as e:
            msg = str(e)
            retriable = any(substr in msg for substr in (
                "502", "504", "429", "timeout", "timed out",
                "Service Unavailable", "Bad Gateway", "Temporary", "Connection", "Network"
            ))
            if attempt < max_retries - 1 and retriable:
                sleep_s = backoffs[min(attempt, len(backoffs)-1)] + random.random() * 0.3
                time.sleep(sleep_s)
                continue
            print(f"_safe_fetch_ohlcv_with_retries final error on {symbol} {timeframe}: {e}")
            return []

def close_inside_bb20(close_price: float, bb_lo: float, bb_up: float) -> bool:
    """Vrai si la clôture est à l'intérieur (ou sur) la BB20. Garde-fous NaN."""
    try:
        c = float(close_price); lo = float(bb_lo); up = float(bb_up)
    except (TypeError, ValueError):
        return False
    if np.isnan(c) or np.isnan(lo) or np.isnan(up):
        return False
    return (lo - _EPS) <= c <= (up + _EPS)

def touched_or_crossed(low: float, high: float, level: float, side: str) -> bool:
    """
    Vrai si la bougie a touché/croisé un niveau.
    - side='buy'  : on attend un contact vers le bas (support), donc low <= level <= high OU low <= level.
    - side='sell' : contact vers le haut (résistance), donc low <= level <= high OU high >= level.
    Petite tolérance numérique (_EPS).
    """
    try:
        lo = float(low); hi = float(high); lvl = float(level)
    except (TypeError, ValueError):
        return False
    if any(np.isnan(x) for x in (lo, hi, lvl)):
        return False

    # contact strict “dans la bougie”
    in_range = (lo - _EPS) <= lvl <= (hi + _EPS)

    if side == "buy":
        return in_range or (lo - _EPS) <= lvl
    if side == "sell":
        return in_range or (hi + _EPS) >= lvl
    return False
