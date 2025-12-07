# Fichier: trader.py
import os
import time
import ccxt
import pandas as pd
from typing import Dict, Any, Optional, Tuple, List
import database
import notifier
import charting
import utils

# --- Paramètres de Trading ---
RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_PER_TRADE_PERCENT", "1.0"))
LEVERAGE = int(os.getenv("LEVERAGE", "2"))
TIMEFRAME = os.getenv("TIMEFRAME", "1h")
MIN_RR = float(os.getenv("MIN_RR", "3.0"))
MM_DEAD_ZONE_PERCENT = float(os.getenv("MM_DEAD_ZONE_PERCENT", "0.1"))
MIN_NOTIONAL_VALUE = float(os.getenv("MIN_NOTIONAL_VALUE", "5"))

# --- Filtres de réaction (paternes) ---
REACTION_WINDOW_BARS = 3
PINBAR_MAX_BODY = 0.30      # ≤ 30% du range
IMPULSE_MIN_BODY = 0.35     # ≥ 35% du range
SIMPLE_WICK_MIN = 0.35      # ≥ 35% du range

# Confirmation obligatoire après une pinbar-contact
PINBAR_CONFIRM_MAX_BARS = 2     # nb de bougies max pour voir la réaction
PINBAR_CONFIRM_MIN_BODY = 0.20  # corps min de la bougie de réaction

# --- Frais & BE ---
FEE_ENTRY_PCT   = float(os.getenv("FEE_ENTRY_PCT", "0.0010"))  # 0.1% typique taker
FEE_EXIT_PCT    = float(os.getenv("FEE_EXIT_PCT",  "0.0010"))  # 0.1% typique taker
BE_BUFFER_PCT   = float(os.getenv("BE_BUFFER_PCT", "0.0020"))  # +0.2% au-dessus du VRAI BE
BE_BUFFER_USDT  = float(os.getenv("BE_BUFFER_USDT","0.0"))     # buffer absolu optionnel (USDT). Laisse 0 si tu n’en veux pas.

# ==============================================================================
# ANALYSE DE LA BOUGIE (Nouvelle Section)
# ==============================================================================
def get_universe_size() -> int:
    """
    Lit UNIVERSE_SIZE depuis la base (fallback sur l'env, défaut 500).
    À appeler à chaque itération de scan pour prise en compte immédiate.
    """
    try:
        val = database.get_setting('UNIVERSE_SIZE', os.getenv("UNIVERSE_SIZE", "500"))
        return max(1, int(val))
    except Exception:
        try:
            return max(1, int(os.getenv("UNIVERSE_SIZE", "500")))
        except Exception:
            return 500

def get_universe_by_market_cap(ex: ccxt.Exchange, size: int) -> List[str]:
    """
    Retourne la liste des paires futures USDT Bitget (format CCXT) triées par market cap (CoinGecko),
    sans limite artificielle à 100. Supporte jusqu'à 500 via pagination (250/par page).
    On retourne strictement les 'size' premières paires disponibles sur Bitget.

    Cache léger en RAM pour la journée courante + taille demandée.
    """
    import time
    import requests

    # --- cache process-local (clé = (jour_utc, size)) ---
    now_day = time.gmtime().tm_yday
    key = (now_day, int(size))
    if not hasattr(get_universe_by_market_cap, "_cache"):
        get_universe_by_market_cap._cache = {}
    cache = get_universe_by_market_cap._cache
    if key in cache:
        return cache[key][:size]

    # Charger les marchés Bitget une fois
    try:
        ex.load_markets()
    except Exception:
        pass
    markets = getattr(ex, "markets", {}) or {}
    symbols_set = set(markets.keys()) if isinstance(markets, dict) else set()

    def _to_ccxt_candidates(base: str) -> List[str]:
        base = (base or "").upper().replace(" ", "").replace("-", "")
        # variantes les plus fréquentes côté Bitget futures USDT
        return [f"{base}/USDT:USDT", f"{base}/USDT"]

    # --- Pagination CoinGecko: 250 par page, autant de pages que nécessaire ---
    per_page = 250
    pages = (int(size) + per_page - 1) // per_page
    picked: List[str] = []

    for page in range(1, pages + 1):
        try:
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": per_page,
                "page": page,
                "price_change_percentage": "24h",
                "sparkline": "false",
            }
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            items = r.json() or []
        except Exception:
            break  # en cas d'erreur réseau, on sort proprement (build_universe gère le fallback)

        for it in items:
            # On prend le 'symbol' CoinGecko (ex: "btc", "eth", "sol")
            base = str(it.get("symbol", "")).upper()
            if not base:
                continue
            for cand in _to_ccxt_candidates(base):
                if cand in symbols_set:
                    picked.append(cand)
                    break  # on a mappé cette base => passe à la suivante
            if len(picked) >= size:
                break
        if len(picked) >= size:
            break

    # Mémorise dans le cache du jour (même si incomplet, on laisse le fallback du caller gérer)
    cache[key] = picked[:]
    return picked[:size]



def _coingecko_coin_list_cached() -> list:
    """
    Retourne la liste CoinGecko (id, symbol, name) avec cache 1×/jour
    dans settings.COINGECKO_COIN_LIST_JSON et settings.COINGECKO_COIN_LIST_TS.
    """
    import time, json, requests
    try:
        ts = float(database.get_setting('COINGECKO_COIN_LIST_TS', '0') or '0')
    except Exception:
        ts = 0.0
    now = time.time()
    if now - ts < 23 * 3600:
        try:
            raw = database.get_setting('COINGECKO_COIN_LIST_JSON', '[]') or '[]'
            data = json.loads(raw)
            if isinstance(data, list) and data:
                return data
        except Exception:
            pass

    url = "https://api.coingecko.com/api/v3/coins/list"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json() if r.content else []
        if isinstance(data, list):
            database.set_setting('COINGECKO_COIN_LIST_JSON', json.dumps(data))
            database.set_setting('COINGECKO_COIN_LIST_TS', str(now))
            return data
    except Exception:
        pass
    return []


def _coingecko_market_caps_for_symbols(bases: list, sym_to_ids: dict) -> dict:
    """
    Pour chaque base (ex: 'BTC','ETH'), interroge CoinGecko /coins/markets
    sur tous les ids possibles du symbole et retient la market cap max.
    Retourne { 'BTC': mcap_usd, ... }.
    """
    import math, json, requests
    result = {}
    if not bases:
        return result

    # Construire la liste des ids à interroger à partir des symboles
    ids = []
    for base in bases:
        ids.extend(sym_to_ids.get(base.upper(), []))
    # Dédupliquer
    ids = list(dict.fromkeys([i for i in ids if i]))

    if not ids:
        return {b: 0.0 for b in bases}

    # CoinGecko limite per_page à 250
    per_page = 200
    pages = int(math.ceil(len(ids) / per_page))

    id_to_mcap = {}
    for p in range(pages):
        chunk = ids[p * per_page:(p + 1) * per_page]
        if not chunk:
            continue
        try:
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                "vs_currency": "usd",
                "ids": ",".join(chunk),
                "order": "market_cap_desc",
                "per_page": len(chunk),
                "page": 1,
                "sparkline": "false",
                "price_change_percentage": "24h"
            }
            r = requests.get(url, params=params, timeout=25)
            r.raise_for_status()
            data = r.json() if r.content else []
            for item in data or []:
                cid = str(item.get('id') or '')
                mcap = float(item.get('market_cap') or 0.0)
                if cid and mcap > 0:
                    id_to_mcap[cid] = max(id_to_mcap.get(cid, 0.0), mcap)
        except Exception:
            continue

    # Pour chaque base, prendre la meilleure mcap parmi ses ids
    for base in bases:
        best = 0.0
        for cid in sym_to_ids.get(base.upper(), []):
            mc = id_to_mcap.get(cid, 0.0)
            if mc > best:
                best = mc
        result[base] = best

    return result


def _inside(val: float, lo: float, up: float) -> bool:
    return float(lo) <= float(val) <= float(up)
    
def _near_bb80_with_tolerance(extreme: float, bb80_band: float, side: str, tol_pct: float, atr: float, atr_k: float) -> bool:
    """
    Tolérance SEULEMENT pour la BOUJIE DE CONTACT (pas SL/TP):
      - BUY  : low <= bb80_lo  OU low <= bb80_lo*(1+tol_pct)  OU low <= bb80_lo + ATR*atr_k
      - SELL : high >= bb80_up OU high >= bb80_up*(1-tol_pct) OU high >= bb80_up - ATR*atr_k
    """
    extreme = float(extreme); bb80_band = float(bb80_band)
    tol_pct = float(tol_pct); atr = float(atr); atr_k = float(atr_k)
    if side == 'buy':
        return (extreme <= bb80_band) or (extreme <= bb80_band * (1.0 + tol_pct)) or (extreme <= bb80_band + atr * atr_k)
    else:
        return (extreme >= bb80_band) or (extreme >= bb80_band * (1.0 - tol_pct)) or (extreme >= bb80_band - atr * atr_k)

def _touched_with_tol(price: float, band: float, side: str, tol_pct: float) -> bool:
    """
    Tolérance de contact sur la BB80 (jaune).
    - Long : on accepte si low <= bb80_lo * (1 + tol_pct)
    - Short: on accepte si high >= bb80_up * (1 - tol_pct)
    """
    band = float(band); price = float(price); tol = float(tol_pct)
    if side == 'buy':
        return price <= band * (1.0 + tol)
    else:
        return price >= band * (1.0 - tol)

def _maybe_improve_rr_with_cut_wick(prev: pd.Series, entry: float, sl: float, tp: float, side: str) -> Tuple[float, float]:
    """
    Si CUT_WICK_FOR_RR est ON (DB) et si RR initial < MIN_RR mais >= 2.8,
    recalcule un RR en 'coupant la mèche' (SL basé sur le corps de la bougie de déclenchement).
    Retourne (rr_alternatif, sl_original_ignoré). Le SL réel n’est pas modifié ici.
    """
    enabled = str(database.get_setting('CUT_WICK_FOR_RR', 'false')).lower() == 'true'
    if not enabled:
        if side == 'buy':
            return ((tp - entry) / (entry - sl), sl)
        else:
            return ((entry - tp) / (sl - entry), sl)

    open_, close_ = float(prev['open']), float(prev['close'])
    body_high, body_low = (max(open_, close_), min(open_, close_))
    if side == 'buy':
        sl_body = body_low  # coupe la mèche basse
        rr_alt = (tp - entry) / (entry - sl_body) if (entry - sl_body) > 0 else 0.0
        return rr_alt, sl
    else:
        sl_body = body_high # coupe la mèche haute
        rr_alt = (entry - tp) / (sl_body - entry) if (sl_body - entry) > 0 else 0.0
        return rr_alt, sl

def is_valid_reaction_candle(candle: pd.Series, side: str, prev: Optional[pd.Series] = None) -> bool:
    """Validation stricte de la bougie de réaction.
    Règles DB:
      - DOJI_BODY_MAX (def 0.10)
      - PINBAR_MAX_BODY (def 0.30)
      - SIMPLE_WICK_MIN (def 0.30)   # >= 30% du range pour la grande mèche
      - PINBAR_OPP_WICK_MAX (def 0.20)
      - MARUBOZU_MIN_BODY (def 0.30)
      - WICK_HUGE_MAX (def 0.70)
    Spécifique:
      - PINBAR: couleur indifférente ; on exige seulement la bonne mèche (opposée au trade) et les ratios ci-dessus.
      - Autres patterns (impulsion/marubozu/gap/double): direction dans le sens du trade.
    """
    try:
        doji_max            = float(database.get_setting('DOJI_BODY_MAX', 0.10))
        pinbar_max_body     = float(database.get_setting('PINBAR_MAX_BODY', 0.30))
        simple_wick_min     = float(database.get_setting('SIMPLE_WICK_MIN', 0.30))
        pinbar_opp_wick_max = float(database.get_setting('PINBAR_OPP_WICK_MAX', 0.20))
        marubozu_min_body   = float(database.get_setting('MARUBOZU_MIN_BODY', 0.30))
        wick_huge_max       = float(database.get_setting('WICK_HUGE_MAX', 0.70))
    except Exception:
        doji_max, pinbar_max_body, simple_wick_min = 0.10, 0.30, 0.30
        pinbar_opp_wick_max, marubozu_min_body, wick_huge_max = 0.20, 0.30, 0.70

    o, c = float(candle['open']), float(candle['close'])
    h, l = float(candle['high']), float(candle['low'])
    rng  = max(1e-12, h - l)
    body = abs(c - o)
    w_up = max(0.0, h - max(o, c))
    w_dn = max(0.0, min(o, c) - l)

    body_r = body / rng
    w_up_r = w_up / rng
    w_dn_r = w_dn / rng

    # (A) Rejet sécurité : mèche énorme du côté trade
    if side == 'buy' and w_dn_r > wick_huge_max:
        return False
    if side == 'sell' and w_up_r > wick_huge_max:
        return False

    # (B) PINBAR (couleur indifférente) — uniquement des conditions de mèches/corps
    if body_r <= pinbar_max_body:
        if side == 'buy':
            # Long ⇒ grande mèche basse (>= 30%), petite mèche haute (<= opp max)
            if w_dn_r >= simple_wick_min and w_up_r <= pinbar_opp_wick_max:
                return True
        else:
            # Short ⇒ grande mèche haute, petite mèche basse
            if w_up_r >= simple_wick_min and w_dn_r <= pinbar_opp_wick_max:
                return True
        # si pinbar non valide, on continue aux autres patterns

    # (C) Éliminer les dojis trop faibles pour le reste
    if body_r < doji_max:
        return False

    # (D) Pour les autres patterns, on exige la direction dans le sens du trade
    is_bull = c > o
    if (side == 'buy' and not is_bull) or (side == 'sell' and is_bull):
        return False

    # (E) Impulsion / marubozu directionnels
    if body_r >= marubozu_min_body:
        return True

    # (F) Motifs inter-bougies (avec direction du cur déjà validée ci-dessus)
    if prev is not None:
        if _is_gap_impulse(prev, candle, side):
            return True
        if _is_double_marubozu(prev, candle, side):
            return True

    return False



def _compute_body_wicks(candle: pd.Series) -> Tuple[float, float, float, float]:
    """
    Retourne (body_ratio, wick_up_ratio, wick_down_ratio, range) par rapport au range de la bougie.
    """
    o, c = float(candle['open']), float(candle['close'])
    h, l = float(candle['high']), float(candle['low'])
    rng = max(1e-12, h - l)
    body = abs(c - o)
    w_up = max(0.0, h - max(o, c))
    w_dn = max(0.0, min(o, c) - l)
    return body / rng, w_up / rng, w_dn / rng, rng

def _body_ratio(candle: pd.Series) -> float:
    o, c = float(candle['open']), float(candle['close'])
    h, l = float(candle['high']), float(candle['low'])
    rng = max(1e-12, h - l)
    return abs(c - o) / rng


def _is_gap_impulse(prev: pd.Series, cur: pd.Series, side: str) -> bool:
    """
    Détection 'Gap + Impulsion' entre prev et cur (agnostique à la couleur).
      - GAP_MIN_PCT (def 0.001 = 0.1%)
      - IMPULSE_MIN_BODY (def 0.35 = 35% du range)
    """
    if prev is None or cur is None:
        return False
    try:
        gap_min_pct      = float(database.get_setting('GAP_MIN_PCT', 0.001))
        impulse_min_body = float(database.get_setting('IMPULSE_MIN_BODY', 0.35))
    except Exception:
        gap_min_pct, impulse_min_body = 0.001, 0.35

    prev_close = float(prev['close'])
    cur_open   = float(cur['open'])
    if prev_close <= 0:
        return False

    gap_ratio = abs(cur_open - prev_close) / prev_close
    if gap_ratio < gap_min_pct:
        return False

    body_r, _, _, _ = _compute_body_wicks(cur)
    if body_r < impulse_min_body:
        return False

    return True

def _is_double_marubozu(prev: pd.Series, cur: pd.Series, side: str) -> bool:
    """
    Détection 'Double marubozu 30%' (agnostique à la couleur) :
      - body_ratio(prev) ≥ DOUBLE_MARUBOZU_MIN (def 0.30)
      - body_ratio(cur)  ≥ DOUBLE_MARUBOZU_MIN (def 0.30)
      - mèches de chaque bougie ≤ DOUBLE_MARUBOZU_WICK_MAX (def 0.10)
    """
    if prev is None or cur is None:
        return False
    try:
        min_body_ratio = float(database.get_setting('DOUBLE_MARUBOZU_MIN', 0.30))
        max_wick_ratio = float(database.get_setting('DOUBLE_MARUBOZU_WICK_MAX', 0.10))
    except Exception:
        min_body_ratio, max_wick_ratio = 0.30, 0.10

    b1, u1, d1, _ = _compute_body_wicks(prev)
    b2, u2, d2, _ = _compute_body_wicks(cur)

    if not (b1 >= min_body_ratio and b2 >= min_body_ratio):
        return False
    if any(x > max_wick_ratio for x in (u1, d1, u2, d2)):
        return False

    return True

def _anchor_sl_from_extreme(df: pd.DataFrame, side: str) -> float:
    """
    (MISE À JOUR) SL depuis l’ancre avec offset hybride = max(% , ATR*k).
      - Short : SL = HIGH_ancre * (1 + eff_pct)
      - Long  : SL = LOW_ancre  * (1 - eff_pct)
    """
    if df is None or len(df) < 3:
        return 0.0

    # % de base (historique)
    try:
        pct = float(database.get_setting('SL_OFFSET_PCT', 0.006))
    except Exception:
        pct = 0.006
    # ATR*k pour SL
    try:
        atr_k = float(database.get_setting('SL_ATR_K', 1.00))
    except Exception:
        atr_k = 1.00

    # fenêtre d’ancre
    try:
        window = int(database.get_setting('ANCHOR_WINDOW', 3))
    except Exception:
        window = 3
    window = max(1, min(window, len(df) - 1))
    search = df.iloc[-(window+1):-1].copy()
    if len(search) == 0:
        search = df.iloc[:-1]
        if len(search) == 0:
            return 0.0

    # ATR courant pour convertir en %
    try:
        last_atr = float(df.iloc[-1].get('atr', 0.0))
    except Exception:
        last_atr = 0.0

    # Helpers mèche
    def wick_high(row):
        o, c, h = float(row['open']), float(row['close']), float(row['high'])
        body_top = max(o, c)
        return max(0.0, h - body_top)

    def wick_low(row):
        o, c, l = float(row['open']), float(row['close']), float(row['low'])
        body_bot = min(o, c)
        return max(0.0, body_bot - l)

    if side == 'sell':
        idx_max_high = search['high'].astype(float).idxmax()
        candidate = search.loc[idx_max_high]
        same_high = search[search['high'].astype(float) == float(candidate['high'])]
        if len(same_high) > 1:
            idx = same_high.apply(wick_high, axis=1).astype(float).idxmax()
            anchor = same_high.loc[idx]
        else:
            anchor = candidate

        high_anchor = float(anchor['high'])
        # eff_pct hybride
        eff_pct = max(pct, (atr_k * last_atr) / high_anchor if high_anchor > 0 else pct)
        return high_anchor * (1.0 + eff_pct)

    else:  # side == 'buy'
        idx_min_low = search['low'].astype(float).idxmin()
        candidate = search.loc[idx_min_low]
        same_low = search[search['low'].astype(float) == float(candidate['low'])]
        if len(same_low) > 1:
            idx = same_low.apply(wick_low, axis=1).astype(float).idxmax()
            anchor = same_low.loc[idx]
        else:
            anchor = candidate

        low_anchor = float(anchor['low'])
        eff_pct = max(pct, (atr_k * last_atr) / low_anchor if low_anchor > 0 else pct)
        return low_anchor * (1.0 - eff_pct)

def _is_valid_reaction(df: pd.DataFrame, i: int, direction: str) -> bool:
    """
    Valide la 'bougie de réaction' selon le preset Balanced, pour LONG et SHORT.
    Exigences :
      1) Close À L’INTÉRIEUR de la BB20 (entre bb20_low et bb20_high).
      2) Structure directionnelle OBLIGATOIRE :
         - impulse (corps >= IMPULSE_MIN_BODY) dans le sens du trade
           OU
         - pin-bar (mèche dominante >= SIMPLE_WICK_MIN, corps <= PINBAR_MAX_BODY)
           avec la mèche opposée <= PINBAR_OPP_WICK_MAX dans le sens du trade.
    Args:
        df: DataFrame avec colonnes ['open','high','low','close','bb20_low','bb20_high'].
        i: index (int) de la bougie candidate.
        direction: 'long' ou 'short'.
    Returns:
        bool
    """
    row = df.iloc[i]
    o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
    bb_low = float(row["bb20_low"])
    bb_high = float(row["bb20_high"])

    # 1) Réintégration BB20 obligatoire
    if not (bb_low <= c <= bb_high):
        return False

    # Mesures de bougie
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    body_pct = body / rng
    up_wick = h - max(o, c)
    dn_wick = min(o, c) - l
    up_wick_pct = up_wick / rng
    dn_wick_pct = dn_wick / rng

    # Tolérances (fallback si non définies plus haut dans le fichier)
    pinbar_opp_max = globals().get("PINBAR_OPP_WICK_MAX", 0.24)

    # 2) Structure directionnelle
    if direction == "long":
        impulse_ok = (c > o) and (body_pct >= IMPULSE_MIN_BODY)
        pinbar_ok = (dn_wick_pct >= SIMPLE_WICK_MIN) and (body_pct <= PINBAR_MAX_BODY) and (up_wick_pct <= pinbar_opp_max)
        return bool(impulse_ok or pinbar_ok)

    if direction == "short":
        impulse_ok = (c < o) and (body_pct >= IMPULSE_MIN_BODY)
        pinbar_ok = (up_wick_pct >= SIMPLE_WICK_MIN) and (body_pct <= PINBAR_MAX_BODY) and (dn_wick_pct <= pinbar_opp_max)
        return bool(impulse_ok or pinbar_ok)

    return False

def _reaction_trigger_levels(df: pd.DataFrame, i: int, direction: str) -> Optional[Dict[str, float]]:
    """
    Donne les niveaux d'EXÉCUTION issus de la bougie de réaction validée :
      - entrée uniquement sur CASSURE du high (long) / low (short) de la réaction
      - SL de référence = low (long) / high (short) de la réaction (les coussins %/ATR sont ajoutés ailleurs)
    Args:
        df: DataFrame OHLC avec index i existant.
        i: index (int) de la bougie de réaction (déjà validée par _is_valid_reaction).
        direction: 'long' ou 'short'.
    Returns:
        dict avec {'entry_trigger','sl_ref'} ou None si direction invalide.
    """
    row = df.iloc[i]
    h, l = float(row["high"]), float(row["low"])

    if direction == "long":
        return {"entry_trigger": h, "sl_ref": l}
    if direction == "short":
        return {"entry_trigger": l, "sl_ref": h}
    return None


def _sl_from_contact_candle(contact: pd.Series, side: str, atr_contact: Optional[float] = None) -> float:
    """
    Calcule le SL à partir de la bougie de CONTACT (bougie 1) avec offset hybride.
      - Long  (buy)  : ancre = low(contact)  → SL = low * (1 - max(pct, ATR*k/low))
      - Short (sell) : ancre = high(contact) → SL = high * (1 + max(pct, ATR*k/high))

    Args:
        contact: pd.Series contenant au minimum open/close/high/low (et idéalement 'atr').
        side: 'buy' | 'sell'
        atr_contact: ATR de la bougie contact si déjà calculé (optionnel). Sinon on tente contact['atr'].

    Returns:
        float: prix de stop-loss calculé (>= 0). En cas de données manquantes, renvoie l’ancre brute.
    """
    try:
        s = str(side or "").strip().lower()
        if s not in ("buy", "sell"):
            return 0.0

        # Récupération robuste des champs
        h = float(contact.get("high")) if "high" in contact else float(contact["high"])
        l = float(contact.get("low"))  if "low"  in contact else float(contact["low"])
        if not (h > 0 and l > 0):
            # Données insuffisantes : on renvoie un fallback neutre
            return max(0.0, l if s == "buy" else h)

        # ATR de référence (si fourni, sinon depuis la bougie)
        atr_val = None
        if atr_contact is not None:
            try:
                atr_val = float(atr_contact)
            except Exception:
                atr_val = None
        if atr_val is None:
            try:
                atr_val = float(contact.get("atr", 0.0))
            except Exception:
                atr_val = 0.0

        # Ancre = extrême de la bougie de contact dans le sens opposé à l'entrée
        anchor = l if s == "buy" else h
        if anchor <= 0:
            return max(0.0, anchor)

        # Offset hybride (pourcentage vs ATR*k converti en %) appliqué sur l’ancre
        sl = adjust_sl_for_offset(
            raw_sl=float(anchor),
            side=s,
            atr=float(atr_val or 0.0),
            ref_price=float(anchor)  # conversion ATR→% par rapport à l’ancre
        )

        # Garde-fous numériques
        if not (sl > 0):
            return max(0.0, anchor)
        return float(sl)

    except Exception:
        # En cas d’imprévu, on renvoie l’ancre (comportement conservateur)
        try:
            if str(side).lower() == "buy":
                return float(contact.get("low", 0.0))
            else:
                return float(contact.get("high", 0.0))
        except Exception:
            return 0.0

def _is_first_after_prolonged_bb80_exit(
    df: pd.DataFrame,
    is_long: bool,
    min_streak: int = 5,
    lookback: int = 50
) -> bool:
    """
    Détecte si la DERNIÈRE bougie est la PREMIÈRE clôture revenue à l'intérieur du canal BB80
    après une séquence de `min_streak` clôtures consécutives en dehors.
    
    - is_long = True  : on regarde les excès SOUS bb80_lo (sortie basse prolongée) – protège les longs.
    - is_long = False : on regarde les excès AU-DESSUS de bb80_up – protège les shorts.
    
    Utilisé comme gate pour ignorer le premier trade après excès prolongé (tendance et CT).
    """
    if df is None or len(df) < min_streak + 1:
        return False

    try:
        tail = df.iloc[-min(len(df), lookback):].copy()
    except Exception:
        return False

    if len(tail) < min_streak + 1:
        return False

    outside_streak = 0
    first_inside_after_prolonged_idx = None

    for idx, row in enumerate(tail.itertuples()):
        try:
            close = float(row.close)
            bb80_lo = float(row.bb80_lo)
            bb80_up = float(row.bb80_up)
        except Exception:
            outside = False
            inside = False
        else:
            if is_long:
                # Excès prolongé SOUS la BB80 basse pour les futurs longs
                outside = (close <= bb80_lo)
            else:
                # Excès prolongé AU-DESSUS de la BB80 haute pour les futurs shorts
                outside = (close >= bb80_up)
            inside = (bb80_lo <= close <= bb80_up)

        if outside:
            outside_streak += 1
        else:
            if outside_streak >= min_streak and first_inside_after_prolonged_idx is None and inside:
                # Première bougie qui réintègre le couloir BB80 après un excès prolongé
                first_inside_after_prolonged_idx = idx
            outside_streak = 0

    if first_inside_after_prolonged_idx is None:
        return False

    # On veut que la DERNIÈRE bougie de la fenêtre soit justement cette première réintégration
    return first_inside_after_prolonged_idx == (len(tail) - 1)


def _find_contact_index(df: pd.DataFrame, base_exclude_last: bool = True, max_lookback: int = 5) -> Optional[int]:
    """
    Retourne l'index de la dernière bougie de CONTACT (avant la réaction si base_exclude_last=True),
    c.-à-d. une bougie qui touche la BB20_lo (pour un long) ou la BB20_up (pour un short).
    On ne déduit PAS le sens ici : on cherche juste la bougie la plus récente qui touche au moins une borne BB20.
    """
    if df is None or len(df) < 3:
        return None

    start = -2 if base_exclude_last else -1
    base_idx = len(df) + start
    lb = max(1, int(max_lookback))

    for back in range(1, lb + 1):
        idx = base_idx - back
        if idx < 0:
            break
        row = df.iloc[idx]
        # "touch" générique : low <= bb20_lo OU high >= bb20_up
        try:
            touched_lo = float(row['low'])  <= float(row['bb20_lo'])
            touched_up = float(row['high']) >= float(row['bb20_up'])
        except Exception:
            continue
        if touched_lo or touched_up:
            return idx
    return None

def _previous_wave_by_bb80(df: pd.DataFrame, idx: int, dead_zone_pct: float) -> Optional[str]:
    """
    Détermine le sens de la VAGUE PRÉCÉDENTE uniquement via CLOSE vs bb80_mid,
    en sortant de la dead-zone autour de bb80_mid.
    Retourne 'up' | 'down' | None.
    """
    if df is None or len(df) == 0:
        return None
    i = max(0, min(idx, len(df) - 1))
    for k in range(i - 1, -1, -1):
        try:
            close_k = float(df.iloc[k]['close'])
            mid_k   = float(df.iloc[k]['bb80_mid'])
            dz_k    = mid_k * (dead_zone_pct / 100.0)
        except Exception:
            continue
        if abs(close_k - mid_k) >= dz_k:
            return 'up' if close_k > mid_k else 'down'
    return None

def _is_inside_both_bb(row, is_long: bool) -> bool:
    """
    Retourne True si la clôture est à l'intérieur des DEUX bandes :
      - BB20 (bb20_lo / bb20_up)
      - BB80 (bb80_lo / bb80_up)
    C'est notre définition de "réintégration claire" pour la CT.
    """
    try:
        c = float(row['close'])
        bb20_up = float(row['bb20_up'])
        bb20_lo = float(row['bb20_lo'])
        bb80_up = float(row['bb80_up'])
        bb80_lo = float(row['bb80_lo'])
    except Exception:
        return False

    return (bb20_lo <= c <= bb20_up) and (bb80_lo <= c <= bb80_up)


def _check_ct_reintegration_window(df: pd.DataFrame, is_long: bool, max_window: int = 3) -> bool:
    """
    Gate spécifique CONTRE-TENDANCE.

    Vérifie, dans les (max_window+1) dernières bougies clôturées avant l'entrée :
      1) présence d'un CONTACT BB80 (haut pour un short, bas pour un long),
      2) une RÉINTÉGRATION en clôture à l'intérieur des DEUX BB (20 et 80),
      3) aucune "ressortie" en clôture des BB20/BB80 entre cette réintégration
         et la bougie de réaction.

    Si la séquence n'est pas respectée → False (on rejette le signal CT).
    """
    try:
        if df is None:
            return False
        # Besoin d'au moins (max_window + 2) bougies :
        #  - 1 bougie courante (open d'entrée)
        #  - 1 bougie de réaction
        #  - max_window bougies de contexte.
        if len(df) < max_window + 2:
            return False

        # On travaille sur les bougies clôturées : on exclut la dernière,
        # qui sert d'open d'entrée (CT_ENTRY_ON_NEXT_BAR).
        # Exemple max_window=3 → on regarde les 4 dernières bougies clôturées
        # (dont la bougie de réaction).
        window = df.iloc[-(max_window + 1):-1]
    except Exception:
        return False

    if window is None or len(window) == 0:
        return False

    # 1) Recherche du contact BB80 dans la fenêtre (on prend le DERNIER contact trouvé).
    contact_idx = None
    for idx, row in window.iterrows():
        try:
            if is_long:
                touched = float(row['low']) <= float(row['bb80_lo'])
            else:
                touched = float(row['high']) >= float(row['bb80_up'])
        except Exception:
            touched = False

        if touched:
            contact_idx = idx

    if contact_idx is None:
        # Pas de contact BB80 → séquence CT invalide
        return False

    # 2) Recherche de la première bougie de réintégration (clôture dans BB20 ET BB80)
    reintegration_idx = None
    idxs = list(window.index)
    start_found = False
    for idx in idxs:
        if idx == contact_idx:
            start_found = True
        if not start_found:
            continue

        row = window.loc[idx]
        if _is_inside_both_bb(row, is_long):
            reintegration_idx = idx
            break

    if reintegration_idx is None:
        # Pas de réintégration claire après le contact
        return False

    # 3) Vérifier qu'entre la réintégration et la bougie de réaction
    #    on ne ressort pas des BB20/BB80 en clôture.
    start_pos = idxs.index(reintegration_idx)
    for idx in idxs[start_pos:]:
        row = window.loc[idx]
        if not _is_inside_both_bb(row, is_long):
            # Ressortie → séquence CT invalide
            return False

    return True


def detect_signal(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """(MIS À JOUR) SL sur bougie 1 (contact) + offset hybride ; TP avant borne
    (BB80 en tendance / BB20 UP/LO en contre-tendance) avec offset hybride ;
    RR recalculé et rejet si RR < MIN_RR (cut-wick optionnel)."""
    if df is None or len(df) < 81:
        return None

    # --- Paramètres ---
    try:
        reaction_max_bars = int(database.get_setting('REACTION_MAX_BARS', 3))
    except Exception:
        reaction_max_bars = 3
    try:
        tol_yellow = float(database.get_setting('YELLOW_BB_CONTACT_TOL_PCT', 0.001))
    except Exception:
        tol_yellow = 0.001
    try:
        enforce_next_bar = str(database.get_setting('CT_ENTRY_ON_NEXT_BAR', 'true')).lower() == 'true'
    except Exception:
        enforce_next_bar = True
    try:
        atr_tol_k = float(database.get_setting('BB80_ATR_TOL_K', 0.125))
    except Exception:
        atr_tol_k = 0.125
    try:
        ct_reintegrate_both = str(database.get_setting('CT_REINTEGRATE_BOTH_BB', 'true')).lower() == 'true'
    except Exception:
        ct_reintegrate_both = True
    try:
        cut_wick_enabled = str(database.get_setting('CUT_WICK_FOR_RR', 'false')).lower() == 'true'
    except Exception:
        cut_wick_enabled = False

    tp_pct = get_tp_offset_pct()
    try:
        tp_atr_k = float(database.get_setting('TP_ATR_K', 0.50))
    except Exception:
        tp_atr_k = 0.50

    # --- Anti-excès BB80 ---
    try:
        skip_threshold = int(database.get_setting('SKIP_AFTER_BB80_STREAK', 5))
    except Exception:
        skip_threshold = 5
    lookback = max(skip_threshold, 8)
    allow_countertrend = True
    if len(df) >= lookback:
        recent = df.iloc[-lookback:]
        streak_up = 0
        for i in range(len(recent)):
            row = recent.iloc[-1 - i]
            if float(row['close']) >= float(row['bb80_up']):
                streak_up += 1
            else:
                break
        streak_down = 0
        for i in range(len(recent)):
            row = recent.iloc[-1 - i]
            if float(row['close']) <= float(row['bb80_lo']):
                streak_down += 1
            else:
                break
        if streak_up >= skip_threshold or streak_down >= skip_threshold:
            allow_countertrend = False

    # --- Bougie de référence pour l’Option E ---
    last_idx = -2 if (enforce_next_bar and len(df) >= 2) else -1
    last = df.iloc[last_idx]
    base_idx = len(df) + last_idx

    # Prix d'entrée utilisé pour le sizing/RR
    if enforce_next_bar and len(df) >= 1:
        entry_px_for_rr = float(df.iloc[-1]['open'])
    else:
        entry_px_for_rr = float(last['close'])

    # --- Cherche la bougie CONTACT ---
    contact, contact_idx = None, None
    for back in range(1, reaction_max_bars + 1):
        idx = base_idx - back
        if idx < 0:
            break
        cand = df.iloc[idx]
        touched_lo = utils.touched_or_crossed(cand['low'], cand['high'], cand['bb20_lo'], "buy")
        touched_up = utils.touched_or_crossed(cand['low'], cand['high'], cand['bb20_up'], "sell")
        if touched_lo or touched_up:
            contact, contact_idx = cand, idx
            break
    if contact is None:
        return None

    # --- Sens via bougie de réaction ---
    side_guess = 'buy' if float(last['close']) > float(last['open']) else 'sell'

    # --- Neutralité MM80 via vague précédente (BB80 mid) ---
    bb80_mid_last = float(last['bb80_mid'])
    dead_zone = bb80_mid_last * (MM_DEAD_ZONE_PERCENT / 100.0)
    in_dead_zone = abs(float(last['close']) - bb80_mid_last) < dead_zone

    prev_wave = None
    if in_dead_zone:
        prev_wave = _previous_wave_by_bb80(df, base_idx, MM_DEAD_ZONE_PERCENT)

    # Bougies fenêtre (c1 = contact)
    c1 = contact
    c2 = df.iloc[contact_idx + 1] if (contact_idx + 1) < len(df) else None
    c3 = df.iloc[contact_idx + 2] if (contact_idx + 2) < len(df) else None

    # Détermination finale du sens MM80 (vague précédente si dead-zone)
    if in_dead_zone and prev_wave in ('up', 'down'):
        is_above_mm80 = (prev_wave == 'up')
    else:
        is_above_mm80 = float(last['close']) > float(last['bb80_mid'])

    def _inside_both(candle):
        return _inside(float(candle['close']), float(candle['bb80_lo']), float(candle['bb80_up'])) and \
               _inside(float(candle['close']), float(candle['bb20_lo']), float(candle['bb20_up']))

    def _inside_bb20_only(candle):
        return _inside(float(candle['close']), float(candle['bb20_lo']), float(candle['bb20_up']))

    # (1) PINBAR contact + confirmations
    pinbar_confirmed = False
    try:
        pinbar_opp_wick_max = float(database.get_setting('PINBAR_OPP_WICK_MAX', 0.25))
    except Exception:
        pinbar_opp_wick_max = 0.25
    try:
        pinbar_confirm_max = int(database.get_setting('PINBAR_CONFIRM_MAX_BARS', PINBAR_CONFIRM_MAX_BARS))
    except Exception:
        pinbar_confirm_max = PINBAR_CONFIRM_MAX_BARS
    try:
        pinbar_confirm_min_body = float(database.get_setting('PINBAR_CONFIRM_MIN_BODY', PINBAR_CONFIRM_MIN_BODY))
    except Exception:
        pinbar_confirm_min_body = PINBAR_CONFIRM_MIN_BODY
    try:
        simple_wick_min_dyn = float(database.get_setting('SIMPLE_WICK_MIN', SIMPLE_WICK_MIN))
    except Exception:
        simple_wick_min_dyn = SIMPLE_WICK_MIN

    is_pinbar_contact = False
    if _body_ratio(c1) <= PINBAR_MAX_BODY and _inside_both(c1):
        o1, c1v = float(c1['open']), float(c1['close'])
        h1, l1 = float(c1['high']), float(c1['low'])
        rng1 = max(1e-12, h1 - l1)
        w_up1 = max(0.0, h1 - max(o1, c1v)) / rng1
        w_dn1 = max(0.0, min(o1, c1v) - l1) / rng1
        if side_guess == 'buy':
            is_pinbar_contact = (w_dn1 >= simple_wick_min_dyn and w_up1 <= pinbar_opp_wick_max)
        else:
            is_pinbar_contact = (w_up1 >= simple_wick_min_dyn and w_dn1 <= pinbar_opp_wick_max)

    if is_pinbar_contact:
        pb_high, pb_low = float(c1['high']), float(c1['low'])
        for j in range(1, pinbar_confirm_max + 1):
            k = contact_idx + j
            if k >= len(df):
                break
            rc = df.iloc[k]
            o, c = float(rc['open']), float(rc['close'])
            h, l = float(rc['high']), float(rc['low'])
            rng = max(1e-12, h - l)
            body_r = abs(c - o) / rng
            if side_guess == 'buy':
                if (c >= o) and (body_r >= pinbar_confirm_min_body) and (h >= pb_high):
                    pinbar_confirmed = True
                    break
            else:
                if (c <= o) and (body_r >= pinbar_confirm_min_body) and (l <= pb_low):
                    pinbar_confirmed = True
                    break

    # (2) Réintégration fenêtre (barres 2–3)
    inside_both_seen = False
    inside_bb20_seen = False
    for cx in (c2, c3):
        if cx is None:
            continue
        if _inside_both(cx):
            inside_both_seen = True
            inside_bb20_seen = True
        elif _inside_bb20_only(cx):
            inside_bb20_seen = True

    trend_reintegration_ok = inside_bb20_seen
    ct_reintegration_ok = inside_both_seen if ct_reintegrate_both else inside_bb20_seen

    # (3) Pattern valide (1→4) dans la fenêtre
    pat_ok = False
    for cx, pv in ((c2, c1), (c3, c2)):
        if cx is None:
            continue
        if is_valid_reaction_candle(cx, side_guess, prev=pv):
            pat_ok = True
            break
    if pinbar_confirmed:
        pat_ok = True
    if not pat_ok:
        return None

    # ATR
    last_atr = float(last.get('atr', c1.get('atr', 0.0)))
    atr_contact = float(c1.get('atr', last_atr))

    signal = None
    touched_bb20_low  = utils.touched_or_crossed(c1['low'],  c1['high'], c1['bb20_lo'], "buy")
    touched_bb20_high = utils.touched_or_crossed(c1['low'],  c1['high'], c1['bb20_up'], "sell")

    # --- TENDANCE (corrigé : utilise is_above_mm80 / vague précédente) ---
    if is_above_mm80 and touched_bb20_low and trend_reintegration_ok:
        regime = "Tendance"
        entry = entry_px_for_rr
        # SL depuis bougie 1 (contact) + offset hybride
        sl = float(_sl_from_contact_candle(c1, 'buy', atr_contact))

        # TP avant BB80_up (offset hybride)
        target_band = float(last['bb80_up'])
        eff_pct = max(tp_pct, (tp_atr_k * last_atr) / target_band if target_band > 0 else tp_pct)
        tp = target_band * (1.0 - eff_pct)
        if tp <= entry:
            return None

        if (entry - sl) > 0:
            rr = (tp - entry) / (entry - sl)
            rr_final = rr
            if rr < MIN_RR and rr >= 2.8 and cut_wick_enabled:
                rr_alt, _ = _maybe_improve_rr_with_cut_wick(c1, entry, sl, tp, 'buy')
                rr_final = max(rr, rr_alt)
            if rr_final >= MIN_RR:
                signal = {"side": "buy", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr_final}

    elif (not is_above_mm80) and touched_bb20_high and trend_reintegration_ok:
        regime = "Tendance"
        entry = entry_px_for_rr
        sl = float(_sl_from_contact_candle(c1, 'sell', atr_contact))

        target_band = float(last['bb80_lo'])
        eff_pct = max(tp_pct, (tp_atr_k * last_atr) / target_band if target_band > 0 else tp_pct)
        tp = target_band * (1.0 + eff_pct)
        if tp >= entry:
            return None

        if (sl - entry) > 0:
            rr = (entry - tp) / (sl - entry)
            rr_final = rr
            if rr < MIN_RR and rr >= 2.8 and cut_wick_enabled:
                rr_alt, _ = _maybe_improve_rr_with_cut_wick(c1, entry, sl, tp, 'sell')
                rr_final = max(rr, rr_alt)
            if rr_final >= MIN_RR:
                signal = {"side": "sell", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr_final}

    # --- Garde-fou CT après excès ---
    if not allow_countertrend:
        if signal:
            signal['bb20_mid'] = last['bb20_mid']
            signal['entry_atr'] = c1.get('atr', 0.0)
            signal['entry_rsi'] = 0.0
            return signal
        return None

    # --- CONTRE-TENDANCE (TP = BB20 UP/LO, pas MID) ---
    if not signal:
        prev = c1

        touched_ct_low  = (float(prev['low'])  <= float(prev['bb20_lo'])) and (
            float(prev['low'])  <= float(prev['bb80_lo']) or _near_bb80_with_tolerance(float(prev['low']),  float(prev['bb80_lo']), 'buy',  tol_yellow, atr_contact, atr_tol_k)
        )
        touched_ct_high = (float(prev['high']) >= float(prev['bb20_up'])) and (
            float(prev['high']) >= float(prev['bb80_up']) or _near_bb80_with_tolerance(float(prev['high']), float(prev['bb80_up']), 'sell', tol_yellow, atr_contact, atr_tol_k)
        )

        if touched_ct_low and ct_reintegration_ok:
            regime = "Contre-tendance"
            entry = entry_px_for_rr
            sl = float(_sl_from_contact_candle(c1, 'buy', atr_contact))

            # ❗ TP dynamique avant BB20_UP (pas MID) pour un long CT
            target_band = float(last['bb20_up'])
            eff_pct = max(tp_pct, (tp_atr_k * last_atr) / target_band if target_band > 0 else tp_pct)
            tp = target_band * (1.0 - eff_pct)
            if tp <= entry:
                return None

            if (entry - sl) > 0:
                rr = (tp - entry) / (entry - sl)
                rr_final = rr
                if rr < MIN_RR and rr >= 2.8 and cut_wick_enabled:
                    rr_alt, _ = _maybe_improve_rr_with_cut_wick(prev, entry, sl, tp, 'buy')
                    rr_final = max(rr, rr_alt)
                if rr_final >= MIN_RR:
                    signal = {"side": "buy", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr_final}

        elif touched_ct_high and ct_reintegration_ok:
            regime = "Contre-tendance"
            entry = entry_px_for_rr
            sl = float(_sl_from_contact_candle(c1, 'sell', atr_contact))

            # ❗ TP dynamique avant BB20_LO (pas MID) pour un short CT
            target_band = float(last['bb20_lo'])
            eff_pct = max(tp_pct, (tp_atr_k * last_atr) / target_band if target_band > 0 else tp_pct)
            tp = target_band * (1.0 + eff_pct)
            if tp >= entry:
                return None

            if (sl - entry) > 0:
                rr = (entry - tp) / (sl - entry)
                rr_final = rr
                if rr < MIN_RR and rr >= 2.8 and cut_wick_enabled:
                    rr_alt, _ = _maybe_improve_rr_with_cut_wick(prev, entry, sl, tp, 'sell')
                    rr_final = max(rr, rr_alt)
                if rr_final >= MIN_RR:
                    signal = {"side": "sell", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr_final}

    if signal:
        signal['bb20_mid'] = last['bb20_mid']
        signal['entry_atr'] = c1.get('atr', 0.0)
        signal['entry_rsi'] = 0.0
        return signal

    return None


def scan_symbol_for_signals(ex: ccxt.Exchange, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
    """
    Charge le DF préparé, appelle detect_signal(), enregistre le signal et notifie.
    Différencie désormais clairement :
      - PENDING : dès qu’un signal est détecté
      - VALID / SKIPPED : mis à jour plus tard par execute_trade() selon issue d’exécution
    Retourne le signal si valide, sinon None.
    """
    try:
        df = utils.fetch_and_prepare_df(ex, symbol, timeframe)
        if df is None or len(df) < 81:
            return None

        sig = detect_signal(symbol, df)
        ts = int(time.time() * 1000)

        if not sig:
            # Traçage léger optionnel (SKIPPED) — tu peux commenter si tu ne veux pas de logs
            try:
                record_signal_from_trader(
                    symbol=symbol, side="-", timeframe=timeframe, ts=ts,
                    price=float(df.iloc[-1]["close"]), rr=0.0, regime="-",
                    pattern="NONE", status="SKIPPED", meta={"reason": "no_pattern"}
                )
            except Exception:
                pass
            return None

        # ✅ marquer immédiatement le signal comme PENDING (en attente d’exécution)
        sig["ts"] = ts
        try:
            record_signal_from_trader(
                symbol=symbol,
                side=sig.get("side", "-"),
                timeframe=timeframe,
                ts=ts,
                price=float(sig.get("entry") or df.iloc[-1]["close"]),
                rr=float(sig.get("rr", 0.0)),
                regime=str(sig.get("regime", "-")),
                pattern="AUTO",
                status="PENDING",
                meta={"entry": sig.get("entry"), "tp": sig.get("tp"), "sl": sig.get("sl")}
            )
        except Exception:
            pass

        try:
            notifier.send_signal_notification(symbol, timeframe, sig)
        except Exception:
            pass

        return sig
    except Exception:
        return None


def record_signal_from_trader(
    symbol: str,
    side: str,
    timeframe: str,
    ts: int,
    price: float,
    rr: float,
    regime: str,
    pattern: str,
    status: str = "PENDING",
    meta: Optional[Dict[str, Any]] = None
) -> None:
    """
    Enregistre/Met à jour un signal détecté par le trader.
    - Clé logique: (symbol, timeframe, ts) ⇒ permet de passer PENDING → VALID/SKIPPED sans doublon.
    - Utilise database.upsert_signal(sig, state) si dispo, sinon fallback tolérant.
    - ⚠️ Alimente désormais 'entry'/'sl'/'tp' dans la table signals (au lieu de seulement 'price' dans meta).
    """
    meta = dict(meta or {})
    entry = float(meta.get("entry", price if price is not None else 0.0))
    sl    = float(meta.get("sl",    0.0))
    tp    = float(meta.get("tp",    0.0))

    sig_payload = {
        "symbol": str(symbol),
        "side": str(side).lower(),
        "timeframe": str(timeframe),
        "ts": int(ts),
        "regime": str(regime) if regime is not None else "",
        "entry": float(entry),
        "sl": float(sl),
        "tp": float(tp),
        "rr": float(rr) if rr is not None else 0.0,
    }
    state = str(status or "PENDING")

    try:
        if hasattr(database, "upsert_signal"):
            database.upsert_signal(sig_payload, state=state)
        elif hasattr(database, "insert_signal"):
            try:
                if hasattr(database, "update_signal_state"):
                    database.update_signal_state(symbol, timeframe, ts, state, meta)
                else:
                    database.insert_signal(**sig_payload, state=state)  # type: ignore[arg-type]
            except Exception:
                database.insert_signal(**sig_payload, state=state)      # type: ignore[arg-type]
        elif hasattr(database, "save_signal"):
            database.save_signal(**sig_payload, state=state)            # type: ignore[arg-type]
        else:
            try:
                notifier.tg_send(
                    f"ℹ️ Signal non persisté (API DB manquante): "
                    f"{symbol} {side} {timeframe} @ {entry} [{state}]"
                )
            except Exception:
                pass
    except Exception as e:
        try:
            notifier.tg_send(
                f"⚠️ Échec enregistrement signal: {symbol} {side} {timeframe} @ {entry} — {e}"
            )
        except Exception:
            pass


# ==============================================================================
# LOGIQUE D'EXÉCUTION (Améliorée)
# ==============================================================================
def get_account_balance_usdt(ex=None) -> Optional[float]:
    """
    Retourne le solde total en USDT (et le met en cache dans settings.CURRENT_BALANCE_USDT).
    Supporte Bybit/Bitget via ccxt.fetchBalance().
    """
    try:
        if ex is None and hasattr(globals(), "create_exchange"):
            ex = create_exchange()
        if ex is None:
            return None

        bal = ex.fetch_balance()  # ccxt unified
        total = None

        # Essais robustes
        for key in ("USDT", "usdT", "usdt"):
            try:
                wallet = bal.get(key) or {}
                total = wallet.get("total") or wallet.get("free") or wallet.get("used")
                if total is not None:
                    total = float(total)
                    break
            except Exception:
                continue

        # Bitget Bybit dérivés: parfois balance['info'] contient la valeur
        if total is None:
            info = bal.get("info") or {}
            # Bybit v5: list
            try:
                if isinstance(info, dict) and "result" in info:
                    result = info["result"]
                    if isinstance(result, dict) and "list" in result:
                        for acc in result["list"]:
                            if str(acc.get("coin", "")).upper() == "USDT":
                                total = float(acc.get("walletBalance"))
                                break
            except Exception:
                pass
            # Bitget: data list
            if total is None:
                try:
                    data = info.get("data") if isinstance(info, dict) else None
                    if isinstance(data, list):
                        for acc in data:
                            if str(acc.get("marginCoin", "")).upper() == "USDT":
                                total = float(acc.get("available")) + float(acc.get("frozen", 0))
                                break
                except Exception:
                    pass

        if total is None:
            return None

        try:
            database.set_setting('CURRENT_BALANCE_USDT', f"{total:.6f}")
        except Exception:
            pass
        return float(total)
    except Exception:
        return None

def _import_exchange_position_to_db(ex: ccxt.Exchange, symbol: str, side: str, quantity: float, entry_px: float) -> None:
    """
    Crée/insère en DB une position détectée sur l'exchange mais absente de la DB.
    Regime = 'Importé', TP/SL init = entry_px (seront gérés ensuite par manage_open_positions).
    """
    try:
        management_strategy = str(database.get_setting('STRATEGY_MODE', 'NORMAL')).upper()
        entry_atr = 0.0
        try:
            df_tmp = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
            if df_tmp is not None and len(df_tmp) > 0:
                entry_atr = float(df_tmp.iloc[-1].get('atr', 0.0))
        except Exception:
            pass

        database.create_trade(
            symbol=symbol,
            side=side,
            regime="Importé",
            entry_price=float(entry_px),
            sl_price=float(entry_px),
            tp_price=float(entry_px),
            quantity=float(quantity),
            risk_percent=RISK_PER_TRADE_PERCENT,
            management_strategy=("SPLIT" if management_strategy == "SPLIT" else "NORMAL"),
            entry_atr=entry_atr,
            entry_rsi=0.0,
        )
        try:
            notifier.tg_send(f"♻️ Import DB: {symbol} {side} qty≈{quantity}, entry≈{entry_px}")
        except Exception:
            pass
    except Exception as e:
        notifier.tg_send_error(f"Import position {symbol} -> DB", e)

def _estimate_pnl_for_closed_trade(ex, row: Dict[str, Any]) -> float:
    """
    Estime le PnL d'un trade fermé côté exchange alors que la DB pense encore qu'il est ouvert.
    Utilisé uniquement dans sync_positions_with_exchange lorsque l'exchange est FLAT pour un symbole.

    Approche:
      - side: 'buy'/'sell'
      - entry: row['entry_price'] (ou 'entry')
      - qty:   row['quantity'] (ou 'qty')
      - exit:  row['exit_price']/'close_price' si présent, sinon dernier prix du ticker.

    Si une info clé manque → retourne 0.0 (comportement précédent).
    """
    try:
        symbol = str(row.get("symbol") or "")
        if not symbol:
            return 0.0

        side_raw = str(row.get("side") or "").lower()
        side = "buy" if side_raw in ("buy", "long") else "sell" if side_raw in ("sell", "short") else ""
        if not side:
            return 0.0

        entry = row.get("entry_price", row.get("entry"))
        qty = row.get("quantity", row.get("qty"))

        entry = float(entry or 0.0)
        qty = float(qty or 0.0)
        if entry <= 0.0 or qty <= 0.0:
            return 0.0

        # Prix de sortie: on privilégie ce qui est déjà stocké en DB
        exit_price = row.get("exit_price") or row.get("close_price") or row.get("avg_exit_price")
        if exit_price is None and ex is not None:
            try:
                t = ex.fetch_ticker(symbol) or {}
                exit_price = t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0
            except Exception:
                exit_price = 0.0
        exit_price = float(exit_price or 0.0)
        if exit_price <= 0.0:
            return 0.0

        if side == "buy":
            pnl = (exit_price - entry) * qty
        else:
            pnl = (entry - exit_price) * qty
        return float(pnl)
    except Exception:
        return 0.0

def sync_positions_with_exchange(ex) -> None:
    """
    Synchronise la table trades avec L’EXCHANGE COMME SOURCE DE VÉRITÉ (agrégation par symbole).
    - 1 seul trade OPEN par symbole côté DB (on agrège et on ferme les doublons).
    - Si exchange est flat pour un symbole ⇒ on ferme en DB (CLOSED_BY_EXCHANGE) en estimant le PnL.
    - Si exchange a une position et DB n’en a pas ⇒ on crée (regime='Importé').
    - On met à jour side/quantity/entry_price pour refléter l’exchange.
    - On recopie TP/SL depuis les ordres ouverts exchange si disponibles (sans créer/modifier les ordres ici).
    """
    try:
        if ex is None and hasattr(globals(), "create_exchange"):
            ex = create_exchange()
        if ex is None:
            return

        # --- Positions réelles exchange (nettes) ---
        ex_positions = _fetch_positions_safe(ex, None) or []
        # Normalise par symbole → une seule entrée par symbole avec side/qty/entry
        ex_map: Dict[str, Dict[str, Any]] = {}
        for p in ex_positions:
            sym = p.get("symbol")
            if not sym:
                continue
            raw_size = float(p.get("size") or p.get("contracts") or p.get("positionAmt") or 0.0)
            if raw_size == 0:
                continue
            side = p.get("side") or ("long" if raw_size > 0 else "short")
            qty = abs(raw_size)
            entry = float(p.get("entryPrice") or 0.0)
            # Canonise side pour notre DB: 'buy'/'sell'
            side_db = "buy" if str(side).lower() in ("long", "buy") else "sell"
            ex_map[sym] = {"symbol": sym, "side": side_db, "qty": qty, "entry": entry}

        # --- DB: liste des OPEN ---
        db_open = database.get_open_positions() or []
        # Grouper par symbole
        db_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
        for r in db_open:
            db_by_symbol.setdefault(r.get("symbol", ""), []).append(r)

        # Ensemble des symboles impliqués
        symbols_all = set(db_by_symbol.keys()) | set(ex_map.keys())

        for sym in symbols_all:
            ex_info = ex_map.get(sym)            # None si flat côté exchange
            db_list = db_by_symbol.get(sym, [])  # [] si pas de trade DB

            # --- Cas A: exchange FLAT, DB a des OPEN → fermer tous en DB (avec PnL estimé)
            if ex_info is None and db_list:
                for row in db_list:
                    try:
                        estimated_pnl = _estimate_pnl_for_closed_trade(ex, row)
                    except Exception:
                        estimated_pnl = 0.0
                    try:
                        database.close_trade(
                            int(row["id"]),
                            status="CLOSED_BY_EXCHANGE",
                            pnl=float(estimated_pnl),
                        )
                    except Exception:
                        # fallback: ancien comportement (pnl=0)
                        try:
                            database.close_trade(
                                int(row["id"]),
                                status="CLOSED_BY_EXCHANGE",
                                pnl=0.0,
                            )
                        except Exception:
                            pass
                continue

            # --- Cas B: exchange a une position, DB n’a rien → créer + recopie TP/SL si trouvés
            if ex_info is not None and not db_list:
                try:
                    # Crée un trade importé
                    database.create_trade(
                        symbol=sym,
                        side=ex_info["side"],
                        regime="Importé",
                        entry_price=float(ex_info["entry"] or 0.0),
                        sl_price=float(ex_info["entry"] or 0.0),
                        tp_price=float(ex_info["entry"] or 0.0),
                        quantity=float(ex_info["qty"] or 0.0),
                        risk_percent=RISK_PER_TRADE_PERCENT,
                        management_strategy=str(database.get_setting('STRATEGY_MODE', 'NORMAL') or 'NORMAL'),
                        entry_atr=0.0,
                        entry_rsi=0.0,
                    )
                except Exception:
                    pass
                # Recopie TP/SL éventuels depuis les ordres
                try:
                    tp_ex, sl_ex = _fetch_existing_tp_sl(ex, sym)
                    if tp_ex or sl_ex:
                        # retrouver le trade nouvellement créé (le plus récent pour ce symbole)
                        fresh = [t for t in database.get_open_positions() if t.get("symbol") == sym]
                        if fresh:
                            keep = max(fresh, key=lambda x: int(x.get("open_timestamp") or 0))
                            if tp_ex:
                                database.update_trade_tp(int(keep["id"]), float(tp_ex))
                            if sl_ex:
                                try:
                                    database.update_trade_sl(int(keep["id"]), float(sl_ex))
                                except AttributeError:
                                    database.update_trade_to_breakeven(
                                        int(keep["id"]),
                                        float(keep.get("quantity") or 0.0),
                                        float(sl_ex),
                                    )
                except Exception:
                    pass
                continue

            # --- Cas C: exchange a une position, DB a ≥1 OPEN → agrège: on garde 1, on ferme les autres
            if ex_info is not None and db_list:
                # Sélectionne le "keeper": le plus récent (open_timestamp) puis id
                try:
                    keeper = max(db_list, key=lambda x: (int(x.get("open_timestamp") or 0), int(x.get("id") or 0)))
                except Exception:
                    keeper = db_list[0]
                keep_id = int(keeper["id"])

                # Ferme les doublons
                for row in db_list:
                    rid = int(row["id"])
                    if rid == keep_id:
                        continue
                    try:
                        database.close_trade(rid, status='MERGED_BY_SYNC', pnl=0.0)
                    except Exception:
                        pass

                # Met à jour le trade conservé pour refléter l’exchange (side/qty/entry)
                try:
                    database.update_trade_core(
                        trade_id=keep_id,
                        side=str(ex_info["side"]),
                        entry_price=float(ex_info["entry"] or 0.0),
                        quantity=float(ex_info["qty"] or 0.0),
                        regime=keeper.get("regime") or "Importé"
                    )
                except Exception:
                    pass

                # Recopie TP/SL si présents sur l’exchange
                try:
                    tp_ex, sl_ex = _fetch_existing_tp_sl(ex, sym)
                    if tp_ex:
                        database.update_trade_tp(keep_id, float(tp_ex))
                    if sl_ex:
                        try:
                            database.update_trade_sl(keep_id, float(sl_ex))
                        except AttributeError:
                            database.update_trade_to_breakeven(
                                keep_id,
                                float(ex_info["qty"] or 0.0),
                                float(sl_ex),
                            )
                except Exception:
                    pass

        # Optionnel: cas exotiques déjà couverts par la clé exacte 'symbol'

    except Exception as e:
        print(f"[sync_positions_with_exchange] error: {e}")
            

def _validate_tp_for_side(side: str, tp_price: float, current_price: float, tick_size: float) -> float:
    """
    Garantit la règle Bitget :
      - short  (sell) : TP < current_price
      - long   (buy)  : TP > current_price
    Corrige automatiquement de 1 tick si la condition est violée.
    """
    if tick_size <= 0:
        return tp_price

    def _round_to_tick(px: float) -> float:
        # Arrondi au tick vers la grille la plus proche
        ticks = round(px / tick_size)
        return float(ticks) * float(tick_size)

    if str(side).lower() in ("sell", "short"):
        # si TP >= prix courant, pousse-le sous le marché d'un tick
        if tp_price >= current_price:
            tp_price = _round_to_tick(current_price - tick_size)
    else:
        # buy/long : si TP <= prix courant, pousse-le au-dessus d'un tick
        if tp_price <= current_price:
            tp_price = _round_to_tick(current_price + tick_size)

    return tp_price

def _bitget_tick_size(market: dict) -> float:
    """
    Retourne le tick_size Bitget à partir du market ccxt.
    Essaie d'abord market['limits']['price']['min'] si présent, sinon precision->price.
    """
    try:
        lim = market.get("limits", {}).get("price", {})
        if isinstance(lim.get("min"), (int, float)) and lim["min"] > 0:
            return float(lim["min"])
    except Exception:
        pass
    # fallback via precision décimale
    prec = None
    try:
        prec = market.get("precision", {}).get("price", None)
    except Exception:
        prec = None
    if isinstance(prec, int) and prec >= 0:
        return 10 ** (-prec) if prec > 0 else 1.0
    # dernier recours : 1e-4
    return 0.0001

def _prepare_validated_tp(exchange, symbol: str, side: str, raw_tp: float) -> float:
    ticker = exchange.fetch_ticker(symbol) or {}
    current_price = float(
        ticker.get("last") or
        ticker.get("close") or
        (ticker.get("info") or {}).get("last", 0) or
        0
    )
    market = exchange.market(symbol) or {}
    tick_size = _bitget_tick_size(market)
    return _validate_tp_for_side(side, float(raw_tp), current_price, tick_size)
    
# --- à placer près de _prepare_validated_tp / _bitget_tick_size ---

def _current_mark_price(exchange, symbol: str) -> float:
    """Renvoie un proxy du 'current price' pertinent pour les triggers mark."""
    t = exchange.fetch_ticker(symbol) or {}
    info = t.get("info") or {}
    # Plusieurs clés possibles selon ccxt/route
    for k in ("markPrice", "mark", "indexPrice", "last", "close", "bid", "ask"):
        v = info.get(k) if k in info else t.get(k)
        if v:
            try: return float(v)
            except Exception: pass
    return float(t.get("last") or t.get("close") or 0.0)

def _validate_sl_for_side(side: str, sl_price: float, current_mark: float, tick_size: float) -> float:
    """
    Bitget: 
      - short  : SL > current_mark
      - long   : SL < current_mark
    On pousse d'au moins 2 ticks pour être safe vs micro-écarts.
    """
    if tick_size <= 0: tick_size = 0.0001
    if str(side).lower() in ("sell", "short"):
        if sl_price <= current_mark:
            sl_price = current_mark + 2.0 * tick_size
    else:
        if sl_price >= current_mark:
            sl_price = current_mark - 2.0 * tick_size
    return sl_price

def _extract_tp_sl_from_orders(orders: list) -> Tuple[Optional[float], Optional[float]]:
    """Retourne (tp_price, sl_price) détectés dans les ordres ouverts."""
    tp, sl = None, None
    for o in (orders or []):
        info = o.get('info', {}) or {}
        # Essais multi-champs (Bitget/ccxt)
        for k in ('takeProfitPrice', 'tpTriggerPx', 'tpTriggerPrice', 'tpPrice', 'takeProfit'):
            v = info.get(k) or o.get(k)
            if v and tp is None:
                try: tp = float(v)
                except Exception: pass
        for k in ('stopLossPrice', 'slTriggerPx', 'slTriggerPrice', 'slPrice', 'stopLoss', 'stopPrice'):
            v = info.get(k) or o.get(k)
            if v and sl is None:
                try: sl = float(v)
                except Exception: pass
    return tp, sl


def _fetch_existing_tp_sl(exchange, symbol: str) -> Tuple[Optional[float], Optional[float]]:
    """Lit les ordres ouverts et tente d’en extraire TP/SL courants."""
    try:
        orders = exchange.fetch_open_orders(symbol)
    except Exception:
        orders = []
    return _extract_tp_sl_from_orders(orders)


def _apply_manual_override_if_needed(ex, pos: Dict[str, Any], tick_size: float) -> Dict[str, bool]:
    """
    Si l’utilisateur a déplacé TP/SL à la main, aligne la DB sur l’exchange
    et renvoie des flags pour la gestion.
    """
    symbol = pos['symbol']
    is_long = (pos['side'] == 'buy')
    tp_ex, sl_ex = _fetch_existing_tp_sl(ex, symbol)

    changed_tp = False
    changed_sl = False
    eps = max(float(tick_size), 1e-12)

    # TP
    if tp_ex:
        try:
            tp_db = float(pos.get('tp_price') or 0.0)
            if abs(float(tp_ex) - tp_db) > eps / 2:
                database.update_trade_tp(pos['id'], float(tp_ex))
                pos['tp_price'] = float(tp_ex)
                changed_tp = True
        except Exception:
            pass

    # SL
    if sl_ex:
        try:
            sl_db = float(pos.get('sl_price') or pos['entry_price'])
            if abs(float(sl_ex) - sl_db) > eps / 2:
                try:
                    database.update_trade_sl(pos['id'], float(sl_ex))
                except AttributeError:
                    database.update_trade_to_breakeven(pos['id'], float(pos['quantity']), float(sl_ex))
                pos['sl_price'] = float(sl_ex)
                changed_sl = True
        except Exception:
            pass

    # Si le SL manuel est déjà >= BE (long) ou <= BE (short), marque BE actif
    try:
        be_side = 'long' if is_long else 'short'
        be_price = compute_fee_safe_be_price(
            entry=float(pos['entry_price']),
            side=be_side,
            qty=float(pos['quantity']),
            fee_in_pct=FEE_ENTRY_PCT,
            fee_out_pct=FEE_EXIT_PCT,
            buffer_pct=BE_BUFFER_PCT,
            buffer_usdt=BE_BUFFER_USDT
        )
        sl_cur = float(pos.get('sl_price') or pos['entry_price'])
        if (is_long and sl_cur >= be_price - eps) or ((not is_long) and sl_cur <= be_price + eps):
            # Active le statut BE en DB pour éviter que le bloc BE “re-calcule”
            try:
                database.update_trade_to_breakeven(pos['id'], float(pos['quantity']), float(sl_cur))
                pos['breakeven_status'] = 'ACTIVE'
            except Exception:
                pass
    except Exception:
        pass

    return {"tp_changed": changed_tp, "sl_changed": changed_sl}

def _bitget_positions_params() -> Dict[str, str]:
    return {"productType": "USDT-FUTURES", "marginCoin": "USDT"}


def _ensure_bitget_mix_options(ex: ccxt.Exchange) -> None:
    """
    Idempotent: prépare l'instance ccxt pour Bitget “mix” USDT linéaire.
    - defaultType='swap' (perp)
    - defaultSubType='linear' (USDT)
    - initialise ex.params avec des valeurs sûres (subType/productType)
    - désactive l'obligation 'price' pour les BUY market (amount ≡ cost)
    Ne lève jamais d’exception.
    """
    try:
        if not hasattr(ex, "options") or ex.options is None:
            ex.options = {}

        # Perp par défaut
        if ex.options.get("defaultType") != "swap":
            ex.options["defaultType"] = "swap"

        if getattr(ex, "id", "") == "bitget":
            # Linear (USDT) par défaut
            if ex.options.get("defaultSubType") not in ("linear", "inverse"):
                ex.options["defaultSubType"] = "linear"
            ex.options.setdefault("defaultSettle", "USDT")
            # ❗ clé pour éviter l'erreur ccxt sur les market BUY
            ex.options["createMarketBuyOrderRequiresPrice"] = False

        # Paramètres génériques pour les requêtes
        if not hasattr(ex, "params") or ex.params is None:
            ex.params = {}
        if getattr(ex, "id", "") == "bitget":
            ex.params.setdefault("subType", ex.options.get("defaultSubType", "linear"))
            ex.params.setdefault("productType", "USDT-UMCBL")
    except Exception:
        pass

def create_market_order_smart(ex: ccxt.Exchange, symbol: str, side: str, amount: float,
                              ref_price: Optional[float] = None,
                              params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Envoi MARKET robuste.
    - Bitget BUY: `amount` est interprété comme COÛT (USDT). On convertit qty→cost via ref_price|ticker.
    - Autres cas: inchangé (amount = quantité).
    """
    _ensure_bitget_mix_options(ex)
    params = params or {}

    s = (side or "").lower()
    exid = getattr(ex, "id", "")

    if exid == "bitget" and s == "buy":
        px = None
        if ref_price is not None:
            try:
                px = float(ref_price)
            except Exception:
                px = None
        if px is None or px <= 0:
            try:
                t = ex.fetch_ticker(symbol) or {}
                px = float(t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0)
            except Exception:
                px = 0.0
        cost = float(amount) * float(px) if px and px > 0 else float(amount)
        return ex.create_order(symbol, "market", "buy", float(cost), None, params)

    return ex.create_order(symbol, "market", side, float(amount), None, params)


def _resolve_bitget_route(exchange):
    try:
        if not hasattr(exchange, "options") or not isinstance(exchange.options, dict):
            exchange.options = {}
        exchange.options["defaultType"] = "swap"
        exchange.options["defaultSettle"] = "USDT"
        exchange.options["productType"] = "USDT-FUTURES"
    except Exception:
        pass

    return {
        "marginCoin": "USDT",
        "primary": "USDT-FUTURES",
    }

def _fetch_positions_safe(ex: ccxt.Exchange, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Récupère les positions ouvertes de façon robuste.
    - Supporte Bitget (swap USDT) et fallback silencieux si non supporté.
    - Ne lève pas d’exception : retourne [] en cas d’échec.
    """
    try:
        # Certaines implémentations exigent les marchés chargés
        try:
            if not getattr(ex, "markets", None):
                ex.load_markets()
        except Exception:
            pass

        # Si la bourse ne supporte pas fetchPositions, on sort proprement
        if not getattr(ex, "has", {}).get("fetchPositions", False):
            return []

        # Appel principal (symbols peut être None)
        positions = ex.fetch_positions(symbols=symbols) if symbols is not None else ex.fetch_positions()

        # Normalisation légère / garde-fous
        out: List[Dict[str, Any]] = []
        for p in positions or []:
            try:
                sym   = p.get("symbol") or p.get("info", {}).get("symbol")
                size  = float(p.get("contracts") or p.get("contractsSize") or p.get("positionAmt") or 0.0)
                side  = p.get("side") or ("long" if size > 0 else "short" if size < 0 else None)
                entry = float(p.get("entryPrice") or p.get("averagePrice") or 0.0)
                lev   = float(p.get("leverage") or 0.0)
                upnl  = float(p.get("unrealizedPnl") or p.get("unrealizedProfit") or 0.0)
                if sym:
                    out.append({
                        "symbol": sym,
                        "side": side,
                        "size": size,
                        "entryPrice": entry,
                        "leverage": lev,
                        "unrealizedPnl": upnl,
                        "raw": p
                    })
            except Exception:
                # On skippe les lignes corrompues sans casser le flux
                continue
        return out
    except Exception:
        return []
            
def _fetch_balance_safe(exchange):
    try:
        exchange.load_markets()
    except Exception:
        pass

    try:
        if getattr(exchange, "id", "") == "bitget":
            from os import getenv
            if not hasattr(exchange, "options") or not isinstance(exchange.options, dict):
                exchange.options = {}
            exchange.options["defaultType"] = "swap"

            margin_coin = (getenv("MARGIN_COIN", "USDT") or "USDT").strip().upper()
            try:
                db_margin = str(database.get_setting("MARGIN_COIN", "")).strip().upper()
                if db_margin:
                    margin_coin = db_margin
            except Exception:
                pass
            exchange.options["defaultSettle"] = margin_coin
            exchange.options["productType"] = "USDT-FUTURES" if margin_coin in ("USDT", "USDC") else "COIN-FUTURES"

            pt_candidates = ["USDT-FUTURES"] if margin_coin in ("USDT", "USDC") else ["COIN-FUTURES"]

            last_err = None
            for pt in pt_candidates:
                try:
                    bal = exchange.fetch_balance({"type": "swap", "productType": pt, "marginCoin": margin_coin})
                    if bal:
                        return bal
                except Exception as e:
                    last_err = e
                    continue

            try:
                notifier.tg_send(f"❌ Erreur: Récupération du solde\nbitget {str(last_err)}")
            except Exception:
                pass
            return {}

        bal = exchange.fetch_balance()
        return bal if bal else {}
    except Exception as e:
        try:
            notifier.tg_send(f"❌ Erreur: Récupération du solde\n{getattr(exchange,'id','')} {str(e)}")
        except Exception:
            pass
        return {}




def get_portfolio_equity_usdt(exchange) -> float:
    """
    Renvoie l'équity totale convertie en USDT pour l'affichage/statistiques.
    - Bitget: privilégie les champs usdtEquity / totalEquity / equity dans info.data.
    - Fallback: utilise les champs normalisés CCXT (USDT total/free, etc.).
    Met systématiquement à jour settings.CURRENT_BALANCE_USDT si une valeur cohérente est trouvée.
    """
    try:
        bal = _fetch_balance_safe(exchange)
    except Exception:
        return 0.0

    if not bal:
        return 0.0

    from typing import List
    candidates: List[float] = []

    # --- Cas spécifique Bitget : on privilégie l'équity portefeuille ---
    try:
        if getattr(exchange, "id", "") == "bitget":
            info = bal.get("info") or {}
            data = info.get("data") or info.get("result") or {}

            # data dict: certains endpoints renvoient un seul bloc
            if isinstance(data, dict):
                for key in ("usdtEquity", "totalEquity", "equity"):
                    v = data.get(key)
                    if v is not None:
                        try:
                            candidates.append(float(v))
                        except Exception:
                            pass

            # data list: plusieurs comptes / coins
            elif isinstance(data, list):
                total_usdt_equity = 0.0
                for acc in data:
                    try:
                        mc = str(acc.get("marginCoin") or acc.get("marginCoinName") or "").upper()
                    except Exception:
                        mc = ""
                    # si marginCoin est renseigné, on garde uniquement les comptes USDT
                    if mc and "USDT" not in mc:
                        continue
                    v = acc.get("usdtEquity") or acc.get("totalEquity") or acc.get("equity")
                    if v is None:
                        continue
                    try:
                        total_usdt_equity += float(v)
                    except Exception:
                        continue
                if total_usdt_equity > 0:
                    candidates.append(total_usdt_equity)
    except Exception:
        # on ne casse jamais la fonction sur une bizarrerie de payload
        pass

    # --- Fallback générique CCXT (USDT total/free/used) ---
    try:
        usdt = bal.get("USDT") or bal.get("USDT:USDT")
        if isinstance(usdt, dict):
            for key in ("total", "free", "used", "availableBalance", "available"):
                v = usdt.get(key)
                if v is not None:
                    try:
                        candidates.append(float(v))
                    except Exception:
                        pass
    except Exception:
        pass

    # total['USDT'] / total['USDT:USDT']
    try:
        total = bal.get("total") or {}
        if isinstance(total, dict):
            for k in ("USDT", "USDT:USDT"):
                v = total.get(k)
                if v is not None:
                    try:
                        candidates.append(float(v))
                    except Exception:
                        pass
    except Exception:
        pass

    if not candidates:
        return 0.0

    equity = float(max(candidates))

    # Mémorisation dans settings pour réutilisation (reporting, dashboard…)
    try:
        database.set_setting("CURRENT_BALANCE_USDT", f"{equity:.6f}")
    except Exception:
        pass

    return equity

def _cap_qty_for_margin_and_filters(exchange, symbol: str, side: str, qty: float, price: float) -> Tuple[float, Dict[str, Any]]:
    """
    (MIS À JOUR) Borne la quantité par la marge disponible et respecte les filtres du marché.
    - Utilise _fetch_balance_safe() (évite l’erreur Bitget 'productType cannot be empty').
    - Retourne (qty_cappée, meta).
    """
    meta = {
        "reason": None, "available_margin": None, "leverage": None,
        "max_notional": None, "max_qty_by_margin": None,
        "min_qty": None, "qty_step": None, "min_notional": None
    }
    try:
        # 1) Marché & limites
        exchange.load_markets()
        m = exchange.market(symbol)
        limits = m.get("limits", {}) if m else {}
        amt_limits = limits.get("amount", {}) if limits else {}
        not_limits = limits.get("cost", {}) if limits else {}

        min_qty      = float(amt_limits.get("min") or 0.0)
        qty_precision = float((m.get("precision") or {}).get("amount") or 0.0)  # décimales, pas toujours un step réel
        qty_step     = 0.0  # inconnu dans ccxt pour certains marchés → on ne force pas
        min_notional = float(not_limits.get("min") or 0.0)

        meta.update({"min_qty": min_qty, "qty_step": qty_step, "min_notional": min_notional})

        # helper: floor to step (tolère step==0)
        def _floor_to_step(v: float, step: float) -> float:
            if step and step > 0:
                return (int(v / step)) * step
            return v

        # 2) Marge disponible (USDT futures) — version robuste
        bal = _fetch_balance_safe(exchange) or {}
        available = 0.0
        try:
            u = bal.get("USDT") or bal.get("USDC") or {}
            # préférer 'free' si présent, sinon 'availableBalance' / 'available'
            available = float(u.get("free") or u.get("availableBalance") or u.get("available") or 0.0)
        except Exception:
            available = 0.0
        meta["available_margin"] = available

        # 3) Levier (issu de la config globale existante)
        try:
            lev = int(LEVERAGE)
        except Exception:
            lev = 1
        meta["leverage"] = lev

        # 4) Cap par la marge (2% buffer)
        max_notional = available * lev * 0.98
        meta["max_notional"] = max_notional
        max_qty_by_margin = max_notional / float(price) if price else 0.0
        max_qty_by_margin = _floor_to_step(max_qty_by_margin, qty_step)
        meta["max_qty_by_margin"] = max_qty_by_margin

        capped_qty = min(float(qty), max_qty_by_margin) if max_qty_by_margin > 0 else 0.0

        # 5) Respect min_qty / min_notional (on ne force pas vers le haut)
        if capped_qty < min_qty or (price and capped_qty * float(price) < min_notional):
            meta["reason"] = "INSUFFICIENT_AFTER_CAP"
            return 0.0, meta

        # 6) Si on a réduit la taille
        if capped_qty < float(qty):
            meta["reason"] = "CAPPED_BY_MARGIN"

        # 7) Arrondi doux via amount_to_precision (si dispo)
        try:
            capped_qty = float(exchange.amount_to_precision(symbol, capped_qty))
        except Exception:
            pass

        return capped_qty, meta

    except Exception as e:
        # En cas d'imprévu, on ne casse jamais l'exécution: on renvoie la qty initiale telle quelle.
        meta["reason"] = f"GUARD_ERROR:{e}"
        return float(qty), meta


def place_order(exchange, symbol: str, side: str, order_type: str, qty: float,
                price: Optional[float] = None, params: Optional[Dict[str, Any]] = None):
    """
    (MODIFIÉ) Envoi d'ordre avec garde-fous:
      - Cap par marge + respect min_qty/step/min_notional
      - Annulation propre + notif TG si solde insuffisant
      - Bitget BUY market: conversion qty→cost via create_market_order_smart()
    """
    try:
        q = abs(float(qty))
        p = float(price) if price is not None else None

        ref_price = p
        if ref_price is None:
            try:
                t = exchange.fetch_ticker(symbol)
                ref_price = float(t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0)
            except Exception:
                ref_price = 0.0

        capped_qty, meta = _cap_qty_for_margin_and_filters(exchange, symbol, side, q, ref_price or 0.0)

        if capped_qty <= 0.0 and meta.get("reason") == "INSUFFICIENT_AFTER_CAP":
            try:
                need_notional = (q * (ref_price or 0.0))
                max_notional = meta.get("max_notional")
                txt = (
                    f"❌ <b>Ordre annulé</b> (solde insuffisant)\n"
                    f"• {symbol} {side.upper()} {order_type.upper()}\n"
                    f"• Notional requise: <code>{need_notional:.2f} USDT</code>\n"
                    f"• Max possible (marge): <code>{(max_notional or 0.0):.2f} USDT</code>\n"
                    f"• Levier: <code>{meta.get('leverage')}</code>\n"
                    f"• Marge dispo: <code>{(meta.get('available_margin') or 0.0):.2f} USDT</code>\n"
                    f"• Filtres marché: min_qty=<code>{meta.get('min_qty')}</code>, "
                    f"min_notional=<code>{meta.get('min_notional')}</code>\n"
                )
                notifier.tg_send(txt)
            except Exception:
                pass
            return None

        if meta.get("reason") == "CAPPED_BY_MARGIN":
            try:
                reduced_pct = (1.0 - (capped_qty / q)) * 100.0 if q > 0 else 0.0
                notifier.tg_send(
                    f"⚠️ Taille réduite par marge\n"
                    f"• {symbol} {side.upper()} {order_type.upper()}\n"
                    f"• Demandée: <code>{q}</code> → Envoyée: <code>{capped_qty}</code> "
                    f"(-{reduced_pct:.2f}%)\n"
                    f"• Levier: <code>{meta.get('leverage')}</code> | Marge dispo: <code>{(meta.get('available_margin') or 0.0):.2f} USDT</code>\n"
                )
            except Exception:
                pass

        params = params or {}
        if order_type.lower() == "market":
            # Bitget BUY ⇒ amount=cost
            return create_market_order_smart(exchange, symbol, side, capped_qty, ref_price=ref_price, params=params)
        else:
            return exchange.create_order(symbol, order_type, side, capped_qty, p, params)

    except Exception as e:
        try:
            notifier.tg_send(f"❌ Erreur: Envoi d'ordre {symbol} {side.upper()} {order_type.upper()} — {e}")
        except Exception:
            pass
        raise


def adjust_tp_for_bb_offset(raw_tp: float, side: str, atr: float = 0.0, ref_price: Optional[float] = None) -> float:
    """
    (MISE À JOUR) Offset hybride pour le TP : max(pourcentage, ATR*k).
    - ref_price: la borne visée (BB80_up/lo ou BB20_mid) pour convertir ATR en %.
    - Sans ref_price, on retombe sur l'ancien comportement (pourcentage seul).
    """
    try:
        pct = float(database.get_setting('TP_BB_OFFSET_PCT', '0.003'))  # 0.30%
    except Exception:
        pct = 0.003
    try:
        atr_k = float(database.get_setting('TP_ATR_K', '0.50'))
    except Exception:
        atr_k = 0.50

    eff_pct = pct
    if ref_price and ref_price > 0 and atr > 0:
        eff_pct = max(pct, (atr_k * float(atr)) / float(ref_price))

    s = (side or "").lower()
    if s in ("buy", "long"):
        return float(raw_tp) * (1.0 - eff_pct)
    if s in ("sell", "short"):
        return float(raw_tp) * (1.0 + eff_pct)
    return float(raw_tp)


def adjust_sl_for_offset(raw_sl: float, side: str, atr: float = 0.0, ref_price: Optional[float] = None) -> float:
    """
    (MISE À JOUR) Offset hybride pour le SL : min(pourcentage, ATR*k) pour éviter un SL trop éloigné.
    - ref_price: l’ancre (high/low de la bougie d’ancrage) pour convertir ATR en %.
    - Sans ref_price, on retombe sur l'ancien comportement (pourcentage seul).
    """
    try:
        pct = float(database.get_setting('SL_OFFSET_PCT', '0.003'))  # 0.30%
    except Exception:
        pct = 0.003
    try:
        atr_k = float(database.get_setting('SL_ATR_K', '0.50'))
    except Exception:
        atr_k = 0.50

    eff_pct = pct
    if ref_price and ref_price > 0 and atr > 0:
        # Avant : eff_pct = max(pct, (atr_k * atr) / ref_price) -> pouvait envoyer le SL très loin.
        # Maintenant : on CAPE l'effet ATR pour garder un SL plus proche (≤ pct).
        atr_pct = (atr_k * float(atr)) / float(ref_price)
        eff_pct = min(pct, atr_pct)

    s = (side or "").lower()
    if s in ("buy", "long"):
        return float(raw_sl) * (1.0 - eff_pct)
    if s in ("sell", "short"):
        return float(raw_sl) * (1.0 + eff_pct)
    return float(raw_sl)



def _update_signal_state(
    symbol: str,
    timeframe: str,
    signal: Dict[str, Any],
    entry_price: float,
    state: str,
    reason: Optional[str] = None,
    tp: Optional[float] = None,
    sl: Optional[float] = None,
) -> None:
    """Met à jour l’état d’un signal déjà persisté via record_signal_from_trader()."""
    try:
        ts_sig = int(signal.get("ts", 0) or 0)
        if ts_sig <= 0:
            return
        meta: Dict[str, Any] = {}
        if tp is not None:
            meta["tp"] = float(tp)
        if sl is not None:
            meta["sl"] = float(sl)
        if reason:
            meta["reason"] = str(reason)

        record_signal_from_trader(
            symbol=symbol,
            side=(signal.get("side") or "-"),
            timeframe=timeframe,
            ts=ts_sig,
            price=float(entry_price),
            rr=float(signal.get("rr", 0.0)),
            regime=str(signal.get("regime", "-")),
            pattern="AUTO",
            status=state,
            meta=meta,
        )
    except Exception:
        pass
        
def _is_reaction_candle(row: pd.Series, is_long: bool) -> bool:
    """
    Détection d'une bougie de réaction (Tendance & Contre-Tendance).

    Utilise les ratios déjà définis dans la stratégie :
    - Pinbar (grosse mèche opposée, petit corps)
    - Wick simple (mèche significative côté réaction)
    - Impulsion directionnelle (marubozu / gros corps dans le sens du trade)

    Si des tags de pattern existent déjà dans la ligne (ex: 'pattern'),
    ils sont utilisés en priorité, sinon on retombe sur l'analyse OHLC.
    """
    # 1) Si la ligne porte déjà un tag de pattern exploitable
    try:
        pattern = str(row.get("pattern", "")).lower()
        if is_long and pattern in ("pinbar_long", "wick_long", "impulse_long", "reaction_long"):
            return True
        if (not is_long) and pattern in ("pinbar_short", "wick_short", "impulse_short", "reaction_short"):
            return True
    except Exception:
        pass

    # 2) Analyse directe OHLC
    try:
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
    except Exception:
        return False

    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l

    body_pct = body / rng
    upper_pct = upper / rng
    lower_pct = lower / rng

    # Seuils proches de ceux de la strat (pinbar / wick / marubozu)
    # PINBAR_MAX_BODY ≈ 0.35
    # SIMPLE_WICK_MIN ≈ 0.27
    # PINBAR_OPP_WICK_MAX ≈ 0.24
    # MARUBOZU_MIN_BODY ≈ 0.28

    if is_long:
        bullish = c >= o
        pinbar = (body_pct <= 0.35 and lower_pct >= 0.45 and upper_pct <= 0.24)
        simple_wick = (lower_pct >= 0.27 and body_pct <= 0.60)
        impulsion = bullish and body_pct >= 0.28 and upper_pct <= 0.25
        return bullish and (pinbar or simple_wick or impulsion)
    else:
        bearish = c <= o
        pinbar = (body_pct <= 0.35 and upper_pct >= 0.45 and lower_pct <= 0.24)
        simple_wick = (upper_pct >= 0.27 and body_pct <= 0.60)
        impulsion = bearish and body_pct >= 0.28 and lower_pct <= 0.25
        return bearish and (pinbar or simple_wick or impulsion)


def _check_reaction_before_entry(df: pd.DataFrame, signal: Dict[str, Any], is_long: bool) -> bool:
    """
    Vérifie la présence d'une bougie de réaction OBLIGATOIRE avant l'entrée.
    S'applique en Tendance ET en Contre-Tendance.

    Logique :
      1) Si le signal indique déjà explicitement une réaction (has_reaction / idx_reaction) → OK.
      2) Sinon, on cherche une bougie de réaction dans les 1 à 3 bougies AVANT la bougie d'entrée.
         - entry_index dans le signal si présent
         - sinon, dernière bougie clôturée (len(df) - 2)

    En cas de doute ou d'erreur → on considère qu'il n'y a PAS de réaction (fail-safe).
    """
    # 1) Signal déjà taggé comme "avec réaction"
    try:
        if bool(signal.get("has_reaction")):
            return True
    except Exception:
        pass

    # 2) Index de bougie de réaction explicite
    try:
        idx_react = signal.get("idx_reaction")
        if idx_react is not None:
            idx_react = int(idx_react)
            if 0 <= idx_react < len(df):
                return True
    except Exception:
        pass

    # 3) Recherche locale autour de l'entrée
    if df is None or len(df) < 3:
        return False

    try:
        entry_idx = signal.get("entry_index")
        if entry_idx is None:
            # on prend la dernière bougie clôturée comme référence
            entry_idx = len(df) - 2
        entry_idx = int(entry_idx)
    except Exception:
        entry_idx = len(df) - 2

    # fenêtre: 1 à 3 bougies avant l'entrée
    start = max(0, entry_idx - 3)
    end = max(0, entry_idx - 1)

    if end < start:
        return False

    window = df.iloc[start : end + 1]

    for _, row in window.iterrows():
        if _is_reaction_candle(row, is_long):
            return True

    return False

def execute_signal_with_gates(
    ex: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    signal: Dict[str, Any],
    entry_price: float,
) -> Tuple[bool, str]:
    """Encapsule le recalcul live SL/TP + gate RR + exécution robuste."""
    side = (signal.get('side') or '').lower()
    regime = str(signal.get('regime', 'Tendance'))
    is_long = (side == 'buy')
    entry_px = float(entry_price)

    if df is None or len(df) < 3:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="df_short_for_entry_gate")
        return False, "Rejeté: données insuffisantes pour valider l’entrée."

    # --- GATE RÉACTION OBLIGATOIRE (TENDANCE + CT) ---
    # Sans bougie de réaction valide avant l'entrée, on NE prend PAS le trade.
    try:
        if not _check_reaction_before_entry(df, signal, is_long):
            _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="no_reaction_candle")
            return False, "Rejeté: aucune bougie de réaction valide avant l’entrée."
    except Exception:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="reaction_check_error")
        return False, "Rejeté: erreur lors du contrôle de la bougie de réaction."

    last = df.iloc[-1]
    last_atr = float(last.get('atr', 0.0))
    try:
        tp_pct = get_tp_offset_pct()
    except Exception:
        tp_pct = 0.003
    try:
        tp_atr_k = float(database.get_setting('TP_ATR_K', 0.50))
    except Exception:
        tp_atr_k = 0.50
    try:
        cut_wick_enabled = str(database.get_setting('CUT_WICK_FOR_RR', 'false')).lower() == 'true'
    except Exception:
        cut_wick_enabled = False

    contact_idx = _find_contact_index(df, base_exclude_last=True, max_lookback=5)
    contact = (df.iloc[contact_idx] if contact_idx is not None else None)

    if contact is not None:
        sl_live = float(_sl_from_contact_candle(contact, ('buy' if is_long else 'sell'), float(contact.get('atr', 0.0))))
    else:
        sl_live = float(signal.get('sl', 0.0))

    if regime == "Tendance":
        target_band = float(last['bb80_up'] if is_long else last['bb80_lo'])
        eff_pct = max(tp_pct, (tp_atr_k * last_atr) / target_band if target_band > 0 else tp_pct)
        tp_live = target_band * (1.0 - eff_pct) if is_long else target_band * (1.0 + eff_pct)
    else:
        target_band = float(last['bb20_up'] if is_long else last['bb20_lo'])
        eff_pct = max(tp_pct, (tp_atr_k * last_atr) / target_band if target_band > 0 else tp_pct)
        tp_live = target_band * (1.0 - eff_pct) if is_long else target_band * (1.0 + eff_pct)

    # ---- Gate spécifique CONTRE-TENDANCE : séquence BB80 + réintégration BB20/BB80 sans ressortie ----
    if regime != "Tendance":
        ok_ct_bb = _check_ct_reintegration_window(df, is_long, max_window=REACTION_WINDOW_BARS)
        if not ok_ct_bb:
            signal['entry'] = entry_px
            signal['sl'] = float(sl_live)
            signal['tp'] = float(tp_live)
            try:
                current_rr = float(signal.get('rr', 0.0) or 0.0)
            except Exception:
                current_rr = 0.0
            signal['rr'] = current_rr
            _update_signal_state(
                symbol,
                timeframe,
                signal,
                entry_px,
                "SKIPPED",
                reason="ct_bb_reintegration_failed",
                tp=float(tp_live),
                sl=float(sl_live),
            )
            return False, "Rejeté: séquence contre-tendance BB20+BB80 non conforme."

    # ---- Gate sortie prolongée BB80 (tendance & contre-tendance) : éviter le 1er trade après excès prolongé ----
    try:
        streak_thr = int(database.get_setting('SKIP_AFTER_BB80_STREAK', 5))
    except Exception:
        streak_thr = 5
    if streak_thr > 0:
        try:
            if _is_first_after_prolonged_bb80_exit(df, is_long, streak_thr):
                signal['entry'] = entry_px
                signal['sl'] = float(sl_live)
                signal['tp'] = float(tp_live)
                # on laisse signal['rr'] tel qu'il vient de detect_signal (ou 0.0 si absent)
                _update_signal_state(
                    symbol,
                    timeframe,
                    signal,
                    entry_px,
                    "SKIPPED",
                    reason="skip_first_after_prolonged_bb80",
                    tp=float(tp_live),
                    sl=float(sl_live),
                )
                return False, "Rejeté: premier trade après sortie prolongée des BB80."
        except Exception:
            # On ne casse jamais l'exécution sur ce filtre : si le check échoue, on continue normalement.
            pass

    # ---- RR et éventuel cut-wick ----
    rr = 0.0
    if is_long:
        if (entry_px > sl_live) and (tp_live > entry_px):
            rr = (tp_live - entry_px) / (entry_px - sl_live)
    else:
        if (sl_live > entry_px) and (tp_live < entry_px):
            rr = (entry_px - tp_live) / (sl_live - entry_px)

    rr_final = rr
    if rr_final < MIN_RR and rr_final >= 2.8 and cut_wick_enabled and contact is not None:
        rr_alt, _ = _maybe_improve_rr_with_cut_wick(contact, entry_px, sl_live, tp_live, ('buy' if is_long else 'sell'))
        rr_final = max(rr_final, rr_alt)

    if rr_final < MIN_RR or rr_final <= 0.0:
        signal['entry'] = entry_px
        signal['sl'] = float(sl_live)
        signal['tp'] = float(tp_live)
        signal['rr'] = float(rr_final)
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED",
                             reason=f"rr_entry_gate({rr_final:.2f})", tp=float(tp_live), sl=float(sl_live))
        return False, f"Rejeté: RR entrée {rr_final:.2f} < MIN_RR ({MIN_RR})."

    signal['entry'] = entry_px
    signal['sl'] = float(sl_live)
    signal['tp'] = float(tp_live)
    signal['rr'] = float(rr_final)

    is_paper_mode = str(database.get_setting('PAPER_TRADING_MODE', 'true')).lower() == 'true'
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv('MAX_OPEN_POSITIONS', 3)))

    # Sync eventuelle avant exécution
    try:
        if str(database.get_setting('SYNC_BEFORE_EXECUTE', 'true')).lower() == 'true':
            sync_positions_with_exchange(ex)
    except Exception:
        pass

    # ---- Fermeture obligatoire sur signal inverse ----
    try:
        open_positions = database.get_open_positions()
        for pos_open in open_positions:
            try:
                if pos_open.get('symbol') != symbol:
                    continue
                pos_side = str(pos_open.get('side', '')).lower()
                if not pos_side or pos_side == side:
                    # même sens ou inconnu → on ne ferme pas ici
                    continue
                # sens inverse sur le même symbole → on ferme avant d'envisager le nouveau trade
                close_position_manually(ex, int(pos_open['id']))
            except Exception:
                continue
    except Exception:
        pass

    # Slots & position déjà ouverte (même sens)
    if len(database.get_open_positions()) >= max_pos:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason=f"max_positions({max_pos})")
        return False, f"Rejeté: Max positions ({max_pos}) atteint."
    if database.is_position_open(symbol):
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="already_open_in_db")
        return False, "Rejeté: Position déjà ouverte (DB)."

    balance = get_usdt_balance(ex)
    if balance is None or balance <= 10:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason=f"low_balance({balance or 0:.2f} USDT)")
        return False, f"Rejeté: Solde insuffisant ({balance or 0:.2f} USDT) ou erreur API."

    price_ref = entry_px
    sl = float(signal['sl'])
    tp = float(signal['tp'])

    try:
        gap_pct = float(database.get_setting('SL_MIN_GAP_PCT', 0.0003))
    except Exception:
        gap_pct = 0.0003
    if is_long:
        if sl >= price_ref:
            sl = price_ref * (1.0 - gap_pct)
        if tp <= price_ref:
            tp = price_ref * (1.0 + gap_pct)
    else:
        if sl <= price_ref:
            sl = price_ref * (1.0 + gap_pct)
        if tp >= price_ref:
            tp = price_ref * (1.0 - gap_pct)

    market = ex.market(symbol) or {}
    tick_size = _bitget_tick_size(market)

    mark_now = _current_mark_price(ex, symbol)
    sl = _validate_sl_for_side(side, float(sl), mark_now, tick_size)

    try:
        side_for_tp = ('buy' if is_long else 'sell')
        tp = _prepare_validated_tp(ex, symbol, side_for_tp, float(tp))
    except Exception:
        t = ex.fetch_ticker(symbol) or {}
        last_px = float(t.get("last") or t.get("close") or price_ref)
        if is_long and tp <= last_px:
            tp = last_px * (1.0 + gap_pct)
        if (not is_long) and tp >= last_px:
            tp = last_px * (1.0 - gap_pct)

    quantity = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, price_ref, sl)
    if quantity <= 0:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="qty_zero_after_sizing")
        return False, f"Rejeté: Quantité calculée nulle ({quantity})."

    ref_price = price_ref
    if not ref_price:
        try:
            t = ex.fetch_ticker(symbol) or {}
            ref_price = float(t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0)
        except Exception:
            ref_price = price_ref

    capped_qty, meta = _cap_qty_for_margin_and_filters(ex, symbol, side, abs(float(quantity)), ref_price or price_ref)
    if capped_qty <= 0.0 and meta.get("reason") == "INSUFFICIENT_AFTER_CAP":
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="insufficient_after_cap")
        try:
            need_notional = (abs(float(quantity)) * (ref_price or price_ref or 0.0))
            max_notional = meta.get("max_notional")
            txt = (
                f"❌ <b>Ordre annulé</b> (solde insuffisant)\n"
                f"• {symbol} {side.upper()} MARKET\n"
                f"• Notional requise: <code>{need_notional:.2f} USDT</code>\n"
                f"• Max possible (marge): <code>{(max_notional or 0.0):.2f} USDT</code>\n"
                f"• Levier: <code>{meta.get('leverage')}</code>\n"
                f"• Marge dispo: <code>{(meta.get('available_margin') or 0.0):.2f} USDT</code>\n"
                f"• Filtres marché: min_qty=<code>{meta.get('min_qty')}</code>, "
                f"min_notional=<code>{meta.get('min_notional')}</code>\n"
            )
            notifier.tg_send(txt)
        except Exception:
            pass
        return False, "Ordre annulé: solde insuffisant."

    notional_value = capped_qty * price_ref
    if notional_value < MIN_NOTIONAL_VALUE:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED",
                             reason=f"below_min_notional({notional_value:.2f}<{MIN_NOTIONAL_VALUE})")
        return False, f"Rejeté: Valeur du trade ({notional_value:.2f} USDT) < min requis ({MIN_NOTIONAL_VALUE} USDT)."

    try:
        sl = float(ex.price_to_precision(symbol, sl))
        tp = float(ex.price_to_precision(symbol, tp))
        quantity = float(ex.amount_to_precision(symbol, capped_qty))
    except Exception:
        quantity = float(capped_qty)

    if quantity <= 0:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="qty_zero_after_precision")
        return False, "Rejeté: quantité finale arrondie à 0."

    final_entry_price = price_ref
    management_strategy = "NORMAL"
    if str(database.get_setting('STRATEGY_MODE', 'NORMAL')).upper() == 'SPLIT':
        management_strategy = "SPLIT"

    common_params = {'tdMode': 'cross', 'posMode': 'oneway'}

    if not is_paper_mode:
        try:
            try:
                ex.set_leverage(LEVERAGE, symbol)
                try:
                    ex.set_margin_mode('cross', symbol)
                except Exception:
                    pass
                try:
                    ex.set_position_mode(False, symbol)
                except Exception:
                    pass
            except Exception:
                pass

            order = create_market_order_smart(ex, symbol, side, quantity, ref_price=final_entry_price, params=common_params)
            if order and order.get('price'):
                final_entry_price = float(order['price'])

            close_side = 'sell' if is_long else 'buy'
            mark_now = _current_mark_price(ex, symbol)
            sl = _validate_sl_for_side(side, float(sl), mark_now, tick_size)

            ex.create_order(
                symbol, 'market', close_side, quantity, price=None,
                params={**common_params, 'stopLossPrice': float(sl), 'reduceOnly': True, 'triggerType': 'mark'}
            )
            ex.create_order(
                symbol, 'market', close_side, quantity, price=None,
                params={**common_params, 'takeProfitPrice': float(tp), 'reduceOnly': True, 'triggerType': 'mark'}
            )

        except Exception as e:
            try:
                close_side = 'sell' if is_long else 'buy'
                create_market_order_smart(
                    ex, symbol, close_side, quantity, ref_price=final_entry_price,
                    params={'reduceOnly': True, 'tdMode': 'cross', 'posMode': 'oneway'}
                )
            except Exception:
                pass
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason=f"execution_error:{e}")
            return False, f"Erreur d'exécution: {e}"

    signal['entry'] = final_entry_price
    signal['sl'] = float(sl)
    signal['tp'] = float(tp)

    database.create_trade(
        symbol=symbol,
        side=side,
        regime=regime,
        entry_price=final_entry_price,
        sl_price=float(sl),
        tp_price=float(tp),
        quantity=float(quantity),
        risk_percent=RISK_PER_TRADE_PERCENT,
        management_strategy=management_strategy,
        entry_atr=float(signal.get('entry_atr', 0.0) or 0.0),
        entry_rsi=float(signal.get('entry_rsi', 0.0) or 0.0),
    )

    _update_signal_state(symbol, timeframe, signal, final_entry_price, "VALID", tp=float(tp), sl=float(sl))

    try:
        chart_image = charting.generate_trade_chart(symbol, df, signal)
    except Exception:
        chart_image = None

    mode_text = "PAPIER" if is_paper_mode else "RÉEL"
    trade_message = notifier.format_trade_message(symbol, signal, quantity, mode_text, RISK_PER_TRADE_PERCENT)

    try:
        if chart_image is not None:
            notifier.tg_send_with_photo(photo_buffer=chart_image, caption=trade_message)
        else:
            notifier.tg_send(trade_message)
    except Exception:
        pass

    return True, "Position ouverte avec succès."



def get_tp_offset_pct() -> float:
    """Retourne le pourcentage d'offset (ex: 0.003 = 0.3%) pour TP/SL depuis la DB,
    clampé pour garantir que le TP se place AVANT la borne (jamais 0)."""
    try:
        v = float(database.get_setting('TP_BB_OFFSET_PCT', 0.003))
    except Exception:
        v = 0.003
    # Clamp : min 0.05% ; max 10%
    if v < 0.0005: v = 0.0005
    if v > 0.1:     v = 0.1
    return v

def compute_fee_safe_be_price(
    entry: float,
    side: str,                 # 'long' | 'short'
    qty: float,                # taille position (en "coin" pour linéaires USDT)
    fee_in_pct: float,
    fee_out_pct: float,
    buffer_pct: float = 0.0,   # petit surplus pour finir > 0
    buffer_usdt: float = 0.0   # OU buffer absolu sur la position
) -> float:
    """
    Retourne le prix de stop 'break-even' qui couvre:
      - PnL +/- (exit - entry) * qty
      - frais d'entrée: fee_in_pct * entry * qty
      - frais de sortie: fee_out_pct * exit  * qty
      - buffer: soit % de notional d'entrée (buffer_pct * entry * qty), soit absolu USDT (buffer_usdt)
    Formules (linéaire USDT):
      Long  : exit >= (E*(1+fin) + b_per_qty)/(1 - fout)
      Short : exit <= (E*(1-fin) - b_per_qty)/(1 + fout)
    """
    side = (side or "").lower()
    E = float(entry)
    Q = max(0.0, float(qty))
    fin = max(0.0, float(fee_in_pct))
    fout = max(0.0, float(fee_out_pct))

    # buffer exprimé par "unité de qty"
    b_per_qty = float(buffer_pct) * E
    if buffer_usdt and Q > 0:
        b_per_qty += float(buffer_usdt) / Q

    if side == 'long':
        # exit >= (E*(1+fin) + b_per_qty) / (1 - fout)
        denom = (1.0 - fout)
        if denom <= 0:
            # sécurité extrême (frais erronés) : fallback sans fout
            return E * (1.0 + fin) + b_per_qty
        return (E * (1.0 + fin) + b_per_qty) / denom

    elif side == 'short':
        # exit <= (E*(1-fin) - b_per_qty) / (1 + fout)
        denom = (1.0 + fout)
        if denom <= 0:
            # sécurité extrême : fallback sans fout
            return E * (1.0 - fin) - b_per_qty
        return (E * (1.0 - fin) - b_per_qty) / denom

    else:
        return E  # si side inconnu, ne bouge pas

def _compute_trailing_sl(mark_price: float, side: str, atr: float) -> float:
    """
    Trailing pro (mix) : distance = max(d%, k*ATR).
      - d%   = TRAIL_PCT (def 0.0035 = 0.35%)
      - kATR = TRAIL_ATR_K (def 1.0)
    Retourne le prix de SL cible par rapport au prix 'mark'.
    """
    try:
        d_pct = float(database.get_setting('TRAIL_PCT', 0.0035))     # 0.35%
    except Exception:
        d_pct = 0.0035
    try:
        k_atr = float(database.get_setting('TRAIL_ATR_K', 1.0))      # 1×ATR
    except Exception:
        k_atr = 1.0

    mark = float(mark_price)
    atr  = max(0.0, float(atr))

    # distance en prix
    dist_pct = abs(d_pct) * mark
    dist_atr = abs(k_atr) * atr
    dist = max(dist_pct, dist_atr)

    if (side or "").lower() in ("buy", "long"):
        return max(0.0, mark - dist)
    else:
        return max(0.0, mark + dist)

def execute_trade(ex: ccxt.Exchange, symbol: str, timeframe: str, signal: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Wrapper d’exécution attendu par le reste du projet.
    - (Option) Sync positions avant exécution si SYNC_BEFORE_EXECUTE = true
    - Charge le DF préparé
    - Détermine un prix d’entrée « live » robuste (respecte CT_ENTRY_ON_NEXT_BAR si activé)
    - Délègue à execute_signal_with_gates (recalc SL/TP/RR + envoi ordres + persistance)
    """
    try:
        # 1) Sync optionnelle avant exécution
        try:
            if str(database.get_setting('SYNC_BEFORE_EXECUTE', 'true')).lower() == 'true':
                sync_positions_with_exchange(ex)
        except Exception:
            pass

        # 2) Données marché
        df = utils.fetch_and_prepare_df(ex, symbol, timeframe)
        if df is None or len(df) < 3:
            return False, "DF indisponible ou trop court pour exécuter."

        # 3) Politique d’entrée (next bar vs live ticker) + indices pour le chart
        try:
            enforce_next_bar = str(database.get_setting('CT_ENTRY_ON_NEXT_BAR', 'true')).lower() == 'true'
        except Exception:
            enforce_next_bar = True

        n = len(df)
        entry_index: Optional[int] = None
        reaction_index: Optional[int] = None

        if n >= 1:
            if enforce_next_bar and n >= 2:
                # Bougie d’entrée = dernière, bougie de réaction = précédante
                entry_index = n - 1
                reaction_index = n - 2
            else:
                # Entrée sur la bougie courante (pas de next-bar strict)
                entry_index = n - 1
                reaction_index = n - 1

        entry_price: float = 0.0
        if enforce_next_bar and entry_index is not None:
            # Entrée stricte: OPEN de la bougie d’entrée (proxy du “next bar open” en live)
            try:
                entry_price = float(df.iloc[entry_index].get('open', 0.0))
            except Exception:
                entry_price = 0.0
        else:
            # Entrée sur la clôture de la bougie d’entrée
            if entry_index is not None:
                try:
                    entry_price = float(df.iloc[entry_index].get('close', 0.0))
                except Exception:
                    entry_price = 0.0

        # Fallback sur ticker si nécessaire
        if not entry_price or entry_price <= 0.0:
            try:
                t = ex.fetch_ticker(symbol) or {}
                entry_price = float(t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0)
            except Exception:
                entry_price = 0.0

        # Dernier fallback: close/open du DF
        if not entry_price or entry_price <= 0.0:
            try:
                last_row = df.iloc[-1]
                entry_price = float(last_row.get('close') or last_row.get('open') or 0.0)
            except Exception:
                entry_price = 0.0

        if entry_price <= 0.0:
            return False, "Impossible d’estimer un prix d’entrée."

        # 3.bis) Indices pour le graphique : contact / réaction / entrée
        try:
            contact_idx = _find_contact_index(df, base_exclude_last=True, max_lookback=5)
        except Exception:
            contact_idx = None

        try:
            if contact_idx is not None:
                signal["contact_index"] = int(contact_idx)
        except Exception:
            pass

        try:
            if reaction_index is not None:
                signal["reaction_index"] = int(reaction_index)
        except Exception:
            pass

        try:
            if entry_index is not None:
                signal["entry_index"] = int(entry_index)
        except Exception:
            pass

        # 4) Normalisations minimales du signal
        if not signal.get("ts"):
            signal["ts"] = int(time.time() * 1000)
        if not signal.get("side"):
            last = df.iloc[-1]
            signal["side"] = "buy" if float(last.get("close", 0.0)) >= float(last.get("open", 0.0)) else "sell"
        if not signal.get("regime"):
            signal["regime"] = "Tendance"

        # 5) Délégation à l’exécuteur avec garde-fous RR/SL/TP
        return execute_signal_with_gates(
            ex=ex,
            symbol=symbol,
            timeframe=timeframe,
            df=df,
            signal=signal,
            entry_price=float(entry_price),
        )

    except Exception as e:
        try:
            notifier.tg_send(f"❌ execute_trade({symbol}) a échoué: {e}")
        except Exception:
            pass
        return False, f"Erreur interne execute_trade: {e}"


def _progress_to_tp(entry: float, tp: float, mark: float, is_long: bool) -> float:
    """
    Retourne la progression normalisée du prix entre l'entrée et le TP:
      - 0.0  : encore loin de l'objectif
      - 1.0+ : TP atteint ou dépassé
    Utilisé pour activer / resserrer le trailing uniquement quand on 'accroche' le TP.
    """
    try:
        entry = float(entry)
        tp = float(tp)
        mark = float(mark)
    except Exception:
        return 0.0

    if entry <= 0 or tp <= 0:
        return 0.0

    if is_long:
        # Long : progression (entry → tp)
        if tp <= entry or mark <= entry:
            return 0.0
        if mark >= tp:
            return 1.0
        return max(0.0, min(1.0, (mark - entry) / (tp - entry)))
    else:
        # Short : progression (entry → tp) mais dans l'autre sens
        if tp >= entry or mark >= entry:
            return 0.0
        if mark <= tp:
            return 1.0
        return max(0.0, min(1.0, (entry - mark) / (entry - tp)))


def _find_last_swing_anchor(df: pd.DataFrame, is_long: bool, max_lookback: int = 15) -> Optional[float]:
    """
    Cherche le DERNIER swing confirmé dans les dernières bougies :
      - SHORT : swing high (top local)
      - LONG  : swing low  (bottom local)
    Utilisé comme ancre de SL pour le passage à BE (avec offset hybride).
    """
    try:
        if df is None:
            return None
        n = len(df)
        if n < 3:
            return None

        lb = max(3, int(max_lookback))
        start = max(1, n - lb - 1)
        end = n - 2  # on évite la dernière bougie (encore en formation)

        if end <= 0 or start > end:
            return None

        if is_long:
            # Swing LOW (bottom) pour les longs
            for i in range(end, start - 1, -1):
                cur = df.iloc[i]
                prev = df.iloc[i - 1]
                nxt = df.iloc[i + 1]
                l = float(cur["low"])
                l_prev = float(prev["low"])
                l_next = float(nxt["low"])
                if l < l_prev and l < l_next:
                    return l
        else:
            # Swing HIGH (top) pour les shorts
            for i in range(end, start - 1, -1):
                cur = df.iloc[i]
                prev = df.iloc[i - 1]
                nxt = df.iloc[i + 1]
                h = float(cur["high"])
                h_prev = float(prev["high"])
                h_next = float(nxt["high"])
                if h > h_prev and h > h_next:
                    return h
    except Exception:
        return None

    return None


def manage_open_positions(ex: ccxt.Exchange):
    """TP dynamique (tendance→BB80 opposée ; CT→BB20 up/lo) + Break-Even + Trailing live après BE.
    Règles BE:
      • Tendance: BE autorisé UNIQUEMENT si stratégie SPLIT.
      • CT: BE autorisé même sans SPLIT.
      • Déclencheur BE: franchissement OU contact de la BB20_mid.
      • Prix BE: entry ajusté frais/buffer via compute_fee_safe_be_price().
    Trailing (MIS À JOUR):
      • Actif uniquement APRÈS BE + lorsque le prix a déjà bien progressé vers le TP.
      • Proximité TP mesurée par _progress_to_tp(entry, tp, mark, side).
      • Deux zones:
          - zone 'near TP' : trailing plus serré.
          - zone 'ultra near TP' : trailing encore plus serré.
      • Paramètres configurables via la DB:
          - TRAIL_PROGRESS_ACTIVATION (def 0.60)
          - TRAIL_PROGRESS_TIGHT      (def 0.85)
          - TRAIL_NEAR_TP_PCT         (def 0.0020)
          - TRAIL_NEAR_TP_ATR_K       (def 0.7)
          - TRAIL_ULTRA_NEAR_TP_PCT   (def 0.0010)
          - TRAIL_ULTRA_NEAR_TP_ATR_K (def 0.5)
      • Ne recule jamais le SL ; mise à jour live à chaque boucle.
      • Conserve le TP (les deux coexistent).
      • Si override manuel et FOLLOW_MANUAL_SL_WITH_TRAILING=true, on continue à suivre.
    """
    _ensure_bitget_mix_options(ex)

    # Sync (optionnel) avant gestion
    try:
        need_sync = str(database.get_setting('SYNC_BEFORE_MANAGE', 'true')).lower() == 'true'
    except Exception:
        need_sync = True
    if need_sync:
        sync_positions_with_exchange(ex)

    open_positions = database.get_open_positions()
    if not open_positions:
        return

    # --- Sélection d'un trade ACTIF par symbole (le plus récent) ---
    latest_by_symbol: Dict[str, Dict[str, Any]] = {}
    for p in open_positions:
        sym = p.get('symbol')
        if not sym:
            continue
        try:
            cur_ts = int(p.get('open_timestamp') or 0)
        except Exception:
            cur_ts = 0
        try:
            cur_id = int(p.get('id') or 0)
        except Exception:
            cur_id = 0
        ref = latest_by_symbol.get(sym)
        if ref is None:
            latest_by_symbol[sym] = p
        else:
            try:
                ref_ts = int(ref.get('open_timestamp') or 0)
            except Exception:
                ref_ts = 0
            try:
                ref_id = int(ref.get('id') or 0)
            except Exception:
                ref_id = 0
            if (cur_ts, cur_id) > (ref_ts, ref_id):
                latest_by_symbol[sym] = p

    latest_id_by_symbol: Dict[str, int] = {}
    for sym, row in latest_by_symbol.items():
        try:
            latest_id_by_symbol[sym] = int(row.get('id') or 0)
        except Exception:
            continue

    filtered_positions: List[Dict[str, Any]] = []
    for p in open_positions:
        sym = p.get('symbol')
        if not sym:
            continue
        try:
            pid = int(p.get('id') or 0)
        except Exception:
            pid = 0
        active_id = latest_id_by_symbol.get(sym)
        if active_id is not None and pid != active_id:
            try:
                database.close_trade(pid, status='STALE_DUPLICATE', pnl=0.0)
            except Exception:
                pass
            continue
        filtered_positions.append(p)

    open_positions = filtered_positions
    if not open_positions:
        return

    # Carte des positions réelles (pour fermer en DB si plus ouvertes)
    try:
        symbols = list({p['symbol'] for p in open_positions})
        ex_pos_list = _fetch_positions_safe(ex, symbols)
        live_map = {}
        for p in (ex_pos_list or []):
            try:
                sym = p.get('symbol') or (p.get('raw', {}) or {}).get('symbol')
                size_val = float(p.get('size') or p.get('contracts') or p.get('positionAmt') or 0.0)
                if sym:
                    live_map[sym] = live_map.get(sym, 0.0) + max(0.0, size_val)
            except Exception:
                continue
    except Exception:
        live_map = {}

    for pos in open_positions:
        symbol = pos['symbol']
        try:
            real_qty = float(live_map.get(symbol, 0.0))
            if real_qty <= 0:
                try:
                    _cancel_all_orders_safe(ex, symbol)
                except Exception:
                    pass
                try:
                    database.close_trade(pos['id'], status='CLOSED_BY_EXCHANGE', pnl=0.0)
                    notifier.tg_send(
                        f"✅ Fermeture auto (exchange) détectée sur {symbol} — "
                        f"Trade #{pos['id']} clôturé en DB et ordres annulés."
                    )
                except Exception:
                    pass
                continue
        except Exception:
            pass

        is_long = (pos['side'] == 'buy')
        close_side = 'sell' if is_long else 'buy'

        regime_raw = pos.get('regime', 'Tendance')
        if isinstance(regime_raw, str) and regime_raw.lower().startswith('import'):
            continue

        common_params = {'reduceOnly': True, 'tdMode': 'cross', 'posMode': 'oneway'}

        try:
            ex.set_leverage(LEVERAGE, symbol)
            try:
                ex.set_margin_mode('cross', symbol)
            except Exception:
                pass
            try:
                ex.set_position_mode(False, symbol)
            except Exception:
                pass
        except Exception:
            pass

        try:
            FOLLOW_MANUAL_SL_WITH_TRAILING = str(database.get_setting('FOLLOW_MANUAL_SL_WITH_TRAILING', 'true')).lower() == 'true'
        except Exception:
            FOLLOW_MANUAL_SL_WITH_TRAILING = True

        try:
            market = ex.market(symbol) or {}
            tick_size = _bitget_tick_size(market)
        except Exception:
            tick_size = 0.0001

        manual = _apply_manual_override_if_needed(ex, pos, tick_size)
        skip_sl_updates = bool(manual.get('sl_changed') and (not FOLLOW_MANUAL_SL_WITH_TRAILING))

        try:
            df = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
        except Exception:
            df = None
        if df is None or len(df) < 2:
            continue
        last = df.iloc[-1]
        bb20_mid   = float(last['bb20_mid'])
        bb20_up    = float(last['bb20_up'])
        bb20_lo    = float(last['bb20_lo'])
        bb80_up    = float(last['bb80_up'])
        bb80_lo    = float(last['bb80_lo'])
        last_atr   = float(last.get('atr', 0.0))
        last_close = float(last['close'])

        # ---------- TP dynamique ----------
        try:
            regime = pos.get('regime', 'Tendance')
            offset_pct = get_tp_offset_pct()
            try:
                tp_atr_k = float(database.get_setting('TP_ATR_K', 0.50))
            except Exception:
                tp_atr_k = 0.50
            try:
                tp_update_eps = float(database.get_setting('TP_UPDATE_EPS', 0.0005))
            except Exception:
                tp_update_eps = 0.0005

            if regime == 'Tendance':
                ref = bb80_up if is_long else bb80_lo
            else:
                ref = bb20_up if is_long else bb20_lo

            eff_pct = max(offset_pct, (tp_atr_k * last_atr) / ref if ref > 0 else offset_pct)
            target_tp = (ref * (1.0 - eff_pct)) if is_long else (ref * (1.0 + eff_pct))

            current_tp = float(pos['tp_price'])
            improve = (is_long and (target_tp < current_tp * (1.0 - tp_update_eps))) or \
                      ((not is_long) and (target_tp > current_tp * (1.0 + tp_update_eps)))

            if improve:
                try:
                    tkr = ex.fetch_ticker(symbol) or {}
                    last_px = float(tkr.get('last') or tkr.get('close') or pos['entry_price'])
                except Exception:
                    last_px = float(pos['entry_price'])
                try:
                    gap_pct = float(database.get_setting('SL_MIN_GAP_PCT', 0.0003))
                except Exception:
                    gap_pct = 0.0003

                sl_price = float(pos.get('sl_price') or pos['entry_price'])
                if is_long and sl_price >= last_px:
                    sl_price = last_px * (1.0 - gap_pct)
                if (not is_long) and sl_price <= last_px:
                    sl_price = last_px * (1.0 + gap_pct)

                try:
                    side_for_tp = ('buy' if is_long else 'sell')
                    target_tp = _prepare_validated_tp(ex, symbol, side_for_tp, float(target_tp))
                except Exception:
                    if is_long and target_tp <= last_px:
                        target_tp = last_px * (1.0 + gap_pct)
                    if (not is_long) and target_tp >= last_px:
                        target_tp = last_px * (1.0 - gap_pct)

                qty = float(pos['quantity'])
                try:
                    qty = float(ex.amount_to_precision(symbol, qty))
                    target_tp = float(ex.price_to_precision(symbol, target_tp))
                    sl_price  = float(ex.price_to_precision(symbol, sl_price))
                except Exception:
                    pass
                if qty > 0:
                    mark_now_tp = _current_mark_price(ex, symbol)
                    sl_price = _validate_sl_for_side(('buy' if is_long else 'sell'), float(sl_price), mark_now_tp, tick_size)

                    ex.create_order(
                        symbol,
                        'market',
                        close_side,
                        qty,
                        price=None,
                        params={**common_params, 'stopLossPrice': float(sl_price), 'triggerType': 'mark'}
                    )
                    ex.create_order(
                        symbol,
                        'market',
                        close_side,
                        qty,
                        price=None,
                        params={**common_params, 'takeProfitPrice': float(target_tp), 'triggerType': 'mark'}
                    )
                    try:
                        database.update_trade_tp(pos['id'], float(target_tp))
                    except Exception:
                        pass
        except Exception as e:
            print(f"Erreur TP dynamique {symbol}: {e}")

        # ---------- Break-Even & Trailing ----------
        if skip_sl_updates:
            continue

        strategy_mode = str(database.get_setting('STRATEGY_MODE', 'NORMAL')).upper()
        regime = pos.get('regime', 'Tendance')
        be_allowed = False
        if regime == 'Tendance':
            be_allowed = (strategy_mode == 'SPLIT')
        else:
            be_allowed = True

        # ---------- Déclencheur BE : contact BB20_mid depuis l'ENTRÉE ----------
        be_trigger = False
        try:
            # 1) Fenêtre principale : depuis l'ouverture du trade si possible
            scan_df = None
            try:
                ts_open = int(pos.get('open_timestamp') or 0)
            except Exception:
                ts_open = 0

            if ts_open > 0 and isinstance(df.index, pd.DatetimeIndex):
                try:
                    # on tolère timestamp en secondes ou millisecondes
                    if ts_open > 10**12:
                        ts_open_ts = pd.to_datetime(ts_open, unit='ms')
                    else:
                        ts_open_ts = pd.to_datetime(ts_open, unit='s')
                    window_from_entry = df[df.index >= ts_open_ts]
                    if len(window_from_entry) > 0:
                        scan_df = window_from_entry
                except Exception:
                    scan_df = None

            # 2) Fallback : petite fenêtre glissante BE_TOUCH_LOOKBACK (comportement historique)
            if scan_df is None:
                try:
                    be_lookback = int(database.get_setting('BE_TOUCH_LOOKBACK', 2))
                except Exception:
                    be_lookback = 2
                be_lookback = max(1, min(be_lookback, len(df)))
                scan_df = df.iloc[-be_lookback:]

            for _, row_be in scan_df.iterrows():
                try:
                    high_be = float(row_be['high'])
                    low_be = float(row_be['low'])
                    mid_be = float(row_be.get('bb20_mid', bb20_mid))
                except Exception:
                    continue

                if is_long and high_be >= mid_be:
                    be_trigger = True
                    break
                if (not is_long) and low_be <= mid_be:
                    be_trigger = True
                    break

            # 3) Sécurité : si rien détecté mais la clôture actuelle est déjà de l'autre côté de la médiane
            if not be_trigger:
                if is_long and last_close >= bb20_mid:
                    be_trigger = True
                if (not is_long) and last_close <= bb20_mid:
                    be_trigger = True
        except Exception:
            # Fallback extrême : on retombe sur la simple position de la dernière bougie
            try:
                if is_long and float(last['high']) >= bb20_mid:
                    be_trigger = True
                if (not is_long) and float(last['low']) <= bb20_mid:
                    be_trigger = True
            except Exception:
                if is_long and last_close >= bb20_mid:
                    be_trigger = True
                if (not is_long) and last_close <= bb20_mid:
                    be_trigger = True

        try:
            be_price_theo = compute_fee_safe_be_price(
                entry=float(pos['entry_price']),
                side=('long' if is_long else 'short'),
                qty=float(pos['quantity']),
                fee_in_pct=FEE_ENTRY_PCT,
                fee_out_pct=FEE_EXIT_PCT,
                buffer_pct=BE_BUFFER_PCT,
                buffer_usdt=BE_BUFFER_USDT
            )
        except Exception:
            be_price_theo = float(pos['entry_price'])

        sl_current = float(pos.get('sl_price') or pos['entry_price'])
        try:
            prev_be_status = str(pos.get('breakeven_status', '')).upper()
        except Exception:
            prev_be_status = ''
        be_armed = (prev_be_status == 'ACTIVE')

        try:
            qty = float(ex.amount_to_precision(symbol, float(pos['quantity'])))
        except Exception:
            qty = float(pos['quantity'])

        # ----------- 🔸 BE basé sur swing + offset SL ----------
        if be_allowed and be_trigger and qty > 0:
            swing_anchor = _find_last_swing_anchor(df, is_long, max_lookback=15)

            want_sl_from_swing = None
            if swing_anchor is not None:
                try:
                    want_sl_from_swing = adjust_sl_for_offset(
                        raw_sl=float(swing_anchor),
                        side=('buy' if is_long else 'sell'),
                        atr=float(last_atr or 0.0),
                        ref_price=float(swing_anchor)
                    )
                except Exception:
                    want_sl_from_swing = float(swing_anchor)

            if want_sl_from_swing is None:
                want_sl = be_price_theo
            else:
                if is_long:
                    want_sl = max(be_price_theo, float(want_sl_from_swing))
                else:
                    want_sl = min(be_price_theo, float(want_sl_from_swing))

            improve_sl = (is_long and want_sl > sl_current) or ((not is_long) and want_sl < sl_current)
            if improve_sl:
                try:
                    mark_now_be = _current_mark_price(ex, symbol)
                except Exception:
                    mark_now_be = None

                want_sl = _validate_sl_for_side(('buy' if is_long else 'sell'), float(want_sl), mark_now_be, tick_size)
                try:
                    want_sl = float(ex.price_to_precision(symbol, want_sl))
                except Exception:
                    pass

                ex.create_order(
                    symbol,
                    'market',
                    close_side,
                    qty,
                    price=None,
                    params={**common_params, 'stopLossPrice': float(want_sl), 'triggerType': 'mark'}
                )
                try:
                    database.update_trade_to_breakeven(pos['id'], float(qty), float(want_sl))
                    sl_current = float(want_sl)
                    be_armed = True

                    if prev_be_status != 'ACTIVE':
                        try:
                            remaining_qty = float(qty)
                            entry_price_be = float(pos.get('entry_price') or 0.0)

                            curr = mark_now_be
                            if curr is None:
                                try:
                                    curr = _current_mark_price(ex, symbol)
                                except Exception:
                                    curr = None

                            if curr is None or entry_price_be <= 0 or remaining_qty <= 0:
                                pnl_realised = 0.0
                            else:
                                if is_long:
                                    pnl_realised = max(0.0, (curr - entry_price_be) * remaining_qty)
                                else:
                                    pnl_realised = max(0.0, (entry_price_be - curr) * remaining_qty)

                            notifier.send_breakeven_notification(
                                symbol=symbol,
                                pnl_realised=float(pnl_realised),
                                remaining_qty=float(remaining_qty)
                            )
                        except Exception:
                            pass
                except Exception:
                    pass

        # 5) Trailing après BE actif
        if be_armed or ((is_long and sl_current >= be_price_theo) or ((not is_long) and sl_current <= be_price_theo)):
            try:
                min_move_pct = float(database.get_setting('TRAIL_MIN_MOVE_PCT', 0.001))
            except Exception:
                min_move_pct = 0.001
            try:
                min_move_atr_k = float(database.get_setting('TRAIL_MIN_MOVE_ATR_K', 0.25))
            except Exception:
                min_move_atr_k = 0.25

            mark_now = _current_mark_price(ex, symbol)

            try:
                tp_pos = float(pos.get('tp_price') or 0.0)
                entry_px = float(pos.get('entry_price') or 0.0)
            except Exception:
                tp_pos, entry_px = 0.0, 0.0

            progress = _progress_to_tp(entry_px, tp_pos, mark_now, is_long)

            try:
                act_prog = float(database.get_setting('TRAIL_PROGRESS_ACTIVATION', 0.60))
            except Exception:
                act_prog = 0.60
            try:
                tight_prog = float(database.get_setting('TRAIL_PROGRESS_TIGHT', 0.85))
            except Exception:
                tight_prog = 0.85

            if progress < act_prog:
                continue

            try:
                base_pct = float(database.get_setting('TRAIL_PCT', 0.0035))
            except Exception:
                base_pct = 0.0035
            try:
                base_k = float(database.get_setting('TRAIL_ATR_K', 1.0))
            except Exception:
                base_k = 1.0
            try:
                near_pct = float(database.get_setting('TRAIL_NEAR_TP_PCT', 0.0020))
            except Exception:
                near_pct = 0.0020
            try:
                near_k = float(database.get_setting('TRAIL_NEAR_TP_ATR_K', 0.7))
            except Exception:
                near_k = 0.7
            try:
                ultra_pct = float(database.get_setting('TRAIL_ULTRA_NEAR_TP_PCT', 0.0010))
            except Exception:
                ultra_pct = 0.0010
            try:
                ultra_k = float(database.get_setting('TRAIL_ULTRA_NEAR_TP_ATR_K', 0.5))
            except Exception:
                ultra_k = 0.5

            d_pct = base_pct
            k_atr = base_k
            if progress >= tight_prog:
                d_pct = ultra_pct
                k_atr = ultra_k
            elif progress >= act_prog:
                d_pct = near_pct
                k_atr = near_k

            dist_pct = abs(d_pct) * mark_now
            dist_atr = abs(k_atr) * last_atr
            dist = max(dist_pct, dist_atr)

            if is_long:
                want_sl = max(0.0, mark_now - dist)
            else:
                want_sl = max(0.0, mark_now + dist)

            if is_long and want_sl <= sl_current:
                continue
            if (not is_long) and want_sl >= sl_current:
                continue

            min_move_abs = max(min_move_pct * mark_now, min_move_atr_k * last_atr)
            if abs(want_sl - sl_current) < max(min_move_abs, tick_size):
                continue

            want_sl = _validate_sl_for_side(('buy' if is_long else 'sell'), float(want_sl), mark_now, tick_size)
            try:
                want_sl = float(ex.price_to_precision(symbol, want_sl))
            except Exception:
                pass

            ex.create_order(
                symbol,
                'market',
                close_side,
                qty,
                price=None,
                params={**common_params, 'stopLossPrice': float(want_sl), 'triggerType': 'mark'}
            )
            try:
                database.update_trade_sl(pos['id'], float(want_sl))
                pos['sl_price'] = float(want_sl)
                pos['breakeven_status'] = 'ACTIVE'
            except Exception:
                pass



def get_usdt_balance(ex: ccxt.Exchange) -> Optional[float]:
    """
    Retourne le solde USDT (équity portefeuille) en float.
    - Préfère get_portfolio_equity_usdt() pour Bitget/Bybit (équity globale).
    - Fallback sur diverses clés de fetch_balance() si nécessaire.
    - Met à jour settings.CURRENT_BALANCE_USDT si une valeur est trouvée.
    """
    # 1) Source principale : équity globale (Bitget/Bybit, etc.)
    equity = 0.0
    try:
        equity = float(get_portfolio_equity_usdt(ex))
    except Exception:
        equity = 0.0

    if equity > 0.0:
        try:
            database.set_setting("CURRENT_BALANCE_USDT", f"{equity:.6f}")
        except Exception:
            pass
        return equity

    # 2) Fallback direct sur la structure de balance CCXT
    try:
        bal = _fetch_balance_safe(ex)
    except Exception:
        bal = None

    if not bal:
        return None

    candidates: List[float] = []

    # Sections normalisées: total / free / used
    for section in ("total", "free", "used"):
        try:
            sec = bal.get(section) or {}
            if isinstance(sec, dict):
                for k in ("USDT", "USDT:USDT"):
                    v = sec.get(k)
                    if v is not None:
                        try:
                            candidates.append(float(v))
                        except Exception:
                            pass
        except Exception:
            pass

    # Entrées directes par devise (bal['USDT'], bal['USDT:USDT'])
    for k in ("USDT", "USDT:USDT"):
        try:
            coin = bal.get(k)
            if isinstance(coin, dict):
                for sub in ("total", "free", "availableBalance", "available"):
                    v = coin.get(sub)
                    if v is not None:
                        try:
                            candidates.append(float(v))
                        except Exception:
                            pass
        except Exception:
            pass

    if not candidates:
        return None

    balance_usdt = float(max(candidates))

    try:
        database.set_setting("CURRENT_BALANCE_USDT", f"{balance_usdt:.6f}")
    except Exception:
        pass

    return balance_usdt

def calculate_position_size(balance: float, risk_percent: float, entry_price: float, sl_price: float) -> float:
    """Calcule la quantité d'actifs à trader."""
    if balance <= 0 or entry_price == sl_price: return 0.0
    risk_amount_usdt = balance * (risk_percent / 100.0)
    price_diff_per_unit = abs(entry_price - sl_price)
    return risk_amount_usdt / price_diff_per_unit if price_diff_per_unit > 0 else 0.0

def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    """(MODIFIÉ) Clôture manuelle robuste :
    - utilise create_market_order_smart() pour BUY et SELL
    - annule tous les ordres restants (TP/SL/BE) sur le symbole après fermeture.
    """
    _ensure_bitget_mix_options(ex)
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    trade = database.get_trade_by_id(trade_id)
    if not trade or trade.get('status') != 'OPEN':
        return notifier.tg_send(f"Trade #{trade_id} déjà fermé ou invalide.")
    
    symbol = trade['symbol']
    side = trade['side']
    qty_db = float(trade['quantity'])

    try:
        # Contexte marge/levier/position
        try:
            ex.set_leverage(LEVERAGE, symbol)
            try:
                ex.set_margin_mode('cross', symbol)
            except Exception:
                pass
            try:
                ex.set_position_mode(False, symbol)
            except Exception:
                pass
        except Exception:
            pass

        # Quantité réelle côté exchange (sécurise si DB désync)
        real_qty = 0.0
        market = None
        try:
            market = ex.market(symbol)
        except Exception:
            pass

        try:
            positions = _fetch_positions_safe(ex, [symbol])
            for p in positions:
                same = (p.get('symbol') == symbol) or (market and p.get('raw', {}).get('symbol') == market.get('id'))
                if same:
                    # ⚠️ On lit d'abord 'size' (rempli par _fetch_positions_safe), puis fallback
                    contracts = float(p.get('size') or p.get('contracts') or p.get('positionAmt') or 0.0)
                    contracts = abs(contracts)
                    if contracts and contracts > 0:
                        real_qty = contracts
                        break
        except Exception:
            pass

        if real_qty <= 0:
            # Pas de position réelle → on ferme en DB et on nettoie les ordres éventuels par sécurité
            try:
                _cancel_all_orders_safe(ex, symbol)
            except Exception:
                pass
            database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
            return notifier.tg_send(
                f"ℹ️ Aucune position ouverte détectée pour {symbol}. "
                f"Trade #{trade_id} marqué fermé et ordres annulés."
            )

        qty_to_close = min(qty_db, real_qty)
        try:
            qty_to_close = float(ex.amount_to_precision(symbol, qty_to_close))
        except Exception:
            pass
        if qty_to_close <= 0:
            try:
                _cancel_all_orders_safe(ex, symbol)
            except Exception:
                pass
            database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
            return notifier.tg_send(
                f"ℹ️ Quantité nulle à clôturer sur {symbol}. "
                f"Trade #{trade_id} marqué fermé et ordres annulés."
            )

        if not is_paper_mode:
            close_side = 'sell' if side == 'buy' else 'buy'
            params = {'reduceOnly': True, 'tdMode': 'cross', 'posMode': 'oneway'}

            # Prix de référence pour conversion qty→cost si nécessaire (Bitget BUY)
            try:
                t = ex.fetch_ticker(symbol) or {}
                ref_px = float(t.get('last') or t.get('close') or t.get('bid') or t.get('ask') or 0.0)
            except Exception:
                ref_px = 0.0

            # ✅ Unifie le chemin: toujours via create_market_order_smart (BUY & SELL)
            create_market_order_smart(
                ex, symbol, close_side, qty_to_close, ref_price=ref_px, params=params
            )

            # 🧹 Après fermeture de la position, on enlève tous les ordres restants (TP/SL/BE)
            try:
                _cancel_all_orders_safe(ex, symbol)
            except Exception:
                pass

        else:
            # En mode papier, on ne touche pas l'exchange, mais on peut tout de même nettoyer les ordres
            try:
                _cancel_all_orders_safe(ex, symbol)
            except Exception:
                pass

        database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
        notifier.tg_send(f"✅ Position sur {symbol} (Trade #{trade_id}) fermée manuellement (qty={qty_to_close}).")

    except Exception as e:
        notifier.tg_send_error(f"Fermeture manuelle de {symbol}", e)
