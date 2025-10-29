# Fichier: trader.py
import os
import time
import ccxt
import pandas as pd
from typing import Dict, Any, Optional, Tuple
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

# ==============================================================================
# ANALYSE DE LA BOUGIE (Nouvelle Section)
# ==============================================================================

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
    """Validation stricte de la bougie de réaction (patterns + seuils).
    Règles DB:
      - DOJI_BODY_MAX (def 0.10)
      - PINBAR_MAX_BODY (def 0.30)
      - SIMPLE_WICK_MIN (def 0.30)
      - PINBAR_OPP_WICK_MAX (def 0.20)
      - MARUBOZU_MIN_BODY (def 0.30)
      - WICK_HUGE_MAX (def 0.70)
    Ajouts DOCS:
      - GAP + IMPULSION (via prev)
      - DOUBLE MARUBOZU 30 % (via prev)
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

    # 1) Rejet doji
    if body_r < doji_max:
        return False

    # 2) Mèche côté signal « énorme » -> rejet
    if side == 'buy' and w_dn_r > wick_huge_max:
        return False
    if side == 'sell' and w_up_r > wick_huge_max:
        return False

    # 3) Pinbar (bicolore toléré) — seuils DB (par défaut 30 %)
    if body_r <= pinbar_max_body:
        if side == 'buy':
            if w_dn_r >= simple_wick_min and w_up_r <= pinbar_opp_wick_max:
                return True
        else:
            if w_up_r >= simple_wick_min and w_dn_r <= pinbar_opp_wick_max:
                return True

    # 4) Impulsion / marubozu relatif (grand corps de la bougie courante)
    if body_r >= marubozu_min_body:
        return True

    # 5) Cas DOCS dépendant de prev: GAP+IMPULSION ou DOUBLE MARUBOZU 30 %
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
    
def _is_gap_impulse(prev: pd.Series, cur: pd.Series, side: str) -> bool:
    """
    Détection 'Gap + Impulsion' entre prev et cur.
    Lit les seuils en DB:
      - GAP_MIN_PCT (def 0.001 = 0.1%)
      - IMPULSE_MIN_BODY (def 0.30 = 30% du range)
    """
    if prev is None or cur is None:
        return False
    try:
        gap_min_pct      = float(database.get_setting('GAP_MIN_PCT', 0.001))
        impulse_min_body = float(database.get_setting('IMPULSE_MIN_BODY', 0.30))
    except Exception:
        gap_min_pct, impulse_min_body = 0.001, 0.30

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

    is_green = float(cur['close']) > float(cur['open'])
    is_red   = float(cur['close']) < float(cur['open'])
    return (side == 'buy' and is_green) or (side == 'sell' and is_red)


def _is_double_marubozu(prev: pd.Series, cur: pd.Series, side: str) -> bool:
    """
    Détection 'Double marubozu 30%':
      - body_ratio(prev) ≥ DOUBLE_MARUBOZU_MIN (def 0.30)
      - body_ratio(cur)  ≥ DOUBLE_MARUBOZU_MIN (def 0.30)
      - mèches de chaque bougie ≤ DOUBLE_MARUBOZU_WICK_MAX (def 0.10)
      - couleur de 'cur' dans le sens du trade
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

    is_green = float(cur['close']) > float(cur['open'])
    is_red   = float(cur['close']) < float(cur['open'])
    return (side == 'buy' and is_green) or (side == 'sell' and is_red)


def _anchor_sl_from_extreme(df: pd.DataFrame, side: str) -> float:
    """
    Calcule le SL à partir de la bougie d'ancrage (AU-DESSUS/EN-DESSOUS de l'extrême de sa MÈCHE) + ATR*K.
    Règle:
      - Short  : SL = au-dessus du PLUS HAUT de la bougie d'ancrage + ATR*K
      - Long   : SL = au-dessous du PLUS BAS  de la bougie d'ancrage - ATR*K

    Sélection de la bougie d'ancrage:
      - On cherche dans une fenêtre récente (par défaut ANCHOR_WINDOW, def=3).
      - On EXCLUT la bougie de réaction (la toute dernière) pour se caler sur le "contact/anchoring".
      - Pour un Short : on prend la bougie au PLUS HAUT max (tie-break: mèche haute la plus longue).
      - Pour un Long  : on prend la bougie au PLUS BAS  min (tie-break: mèche basse la plus longue).

    K = SL_ATR_K (def 0.125). ATR de la bougie d’ancrage si dispo, sinon fallback sur la dernière.
    """
    if df is None or len(df) < 3:
        return 0.0

    # Fenêtre de recherche (exclut la dernière bougie = réaction)
    try:
        window = int(database.get_setting('ANCHOR_WINDOW', 3))
    except Exception:
        window = 3
    window = max(1, min(window, len(df) - 1))  # -1 car on exclut la dernière
    search = df.iloc[-(window+1):-1].copy()    # [ ... , avant-dernière ]  (exclut la dernière)

    # Coefficient ATR (réduction confirmée à 0.125)
    try:
        atr_k = float(database.get_setting('SL_ATR_K', 0.125))
    except Exception:
        atr_k = 0.125

    if len(search) == 0:
        search = df.iloc[:-1]  # fallback : tout sauf la dernière
        if len(search) == 0:
            return 0.0

    # Helpers mèche
    def wick_high(row):
        o, c, h = float(row['open']), float(row['close']), float(row['high'])
        body_top = max(o, c)
        return max(0.0, h - body_top)

    def wick_low(row):
        o, c, l = float(row['open']), float(row['close']), float(row['low'])
        body_bot = min(o, c)
        return max(0.0, body_bot - l)

    # Sélection de l’ancre
    if side == 'sell':
        # 1) indice du plus haut absolu
        idx_max_high = search['high'].astype(float).idxmax()
        candidate = search.loc[idx_max_high]
        # 2) parmi les égalités éventuelles, on préfère la plus grande mèche haute
        same_high = search[search['high'].astype(float) == float(candidate['high'])]
        if len(same_high) > 1:
            idx = same_high.apply(wick_high, axis=1).astype(float).idxmax()
            anchor = same_high.loc[idx]
        else:
            anchor = candidate

        high_anchor = float(anchor['high'])
        # ATR sur l’ancre, fallback dernière ligne
        try:
            atr_anchor = float(anchor.get('atr', df.iloc[-1].get('atr', 0.0)))
        except Exception:
            atr_anchor = float(df.iloc[-1].get('atr', 0.0))
        atr_anchor = max(0.0, atr_anchor)

        return high_anchor + atr_anchor * atr_k

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
        try:
            atr_anchor = float(anchor.get('atr', df.iloc[-1].get('atr', 0.0)))
        except Exception:
            atr_anchor = float(df.iloc[-1].get('atr', 0.0))
        atr_anchor = max(0.0, atr_anchor)

        return low_anchor - atr_anchor * atr_k


def detect_signal(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Détection avec: contact→réaction (≤N), Option E = entrée sur la bougie suivante (configurable),
    CT = réintégration stricte (configurable) BB20 &/ou BB80, tolérance % + coussin ATR
    UNIQUEMENT sur le contact BB80, neutralité MM80 et anti-excès."""
    if df is None or len(df) < 81:
        return None

    # --- Paramètres ---
    try:
        reaction_max_bars = int(database.get_setting('REACTION_MAX_BARS', 2))
    except Exception:
        reaction_max_bars = 2
    try:
        tol_yellow = float(database.get_setting('YELLOW_BB_CONTACT_TOL_PCT', 0.001))  # 0.10%
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
        ct_tp_atr_k = float(database.get_setting('CT_TP_ATR_K', 1.0))
    except Exception:
        ct_tp_atr_k = 1.0
    try:
        tp_atr_k = float(database.get_setting('TP_ATR_K', 1.0))
    except Exception:
        tp_atr_k = 1.0
    try:
        ct_reintegrate_both = str(database.get_setting('CT_REINTEGRATE_BOTH_BB', 'true')).lower() == 'true'
    except Exception:
        ct_reintegrate_both = True
    try:
        cut_wick_enabled = str(database.get_setting('CUT_WICK_FOR_RR', 'false')).lower() == 'true'
    except Exception:
        cut_wick_enabled = False

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

    # --- Réaction = dernière FERMÉE si Option E active ---
    last_idx = -2 if (enforce_next_bar and len(df) >= 2) else -1
    last = df.iloc[last_idx]
    base_idx = len(df) + last_idx

    # --- Cherche CONTACT (1..reaction_max_bars barres avant la réaction) ---
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

    # --- Sens via la bougie de réaction ---
    side_guess = 'buy' if float(last['close']) > float(last['open']) else 'sell'

    # --- Filtre neutralité MM80 ---
    dead_zone = float(last['bb80_mid']) * (MM_DEAD_ZONE_PERCENT / 100.0)
    if abs(float(last['close']) - float(last['bb80_mid'])) < dead_zone:
        return None

    # --- Réintégration par CLÔTURE à la réaction ---
    inside_bb20 = _inside(float(last['close']), float(last['bb20_lo']), float(last['bb20_up']))
    inside_bb80 = _inside(float(last['close']), float(last['bb80_lo']), float(last['bb80_up']))
    ct_reintegration_ok = inside_bb20 and (inside_bb80 if ct_reintegrate_both else True)

    # --- Réaction du prix: pattern valide requis ---
    if not is_valid_reaction_candle(last, side_guess, prev=contact):
        return None

    signal = None

    # --- Indicateurs pour TP ---
    atr_ref = float(contact.get('atr', last.get('atr', 0.0)))
    tp_offset_trend = atr_ref * tp_atr_k
    tp_offset_ct    = atr_ref * ct_tp_atr_k

    # --- Pattern TENDANCE ---
    is_above_mm80 = float(last['close']) > float(last['bb80_mid'])
    touched_bb20_low  = utils.touched_or_crossed(contact['low'], contact['high'], contact['bb20_lo'], "buy")
    touched_bb20_high = utils.touched_or_crossed(contact['low'], contact['high'], contact['bb20_up'], "sell")

    if is_above_mm80 and touched_bb20_low and inside_bb20:
        regime = "Tendance"
        entry = float(last['close'])
        sl = float(_anchor_sl_from_extreme(df, 'buy'))
        prev = contact

        # TP long: un peu sous BB80_up
        target_band = float(last['bb80_up'])
        tp = target_band - tp_offset_trend
        if tp <= entry:
            tp = target_band
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

    elif (not is_above_mm80) and touched_bb20_high and inside_bb20:
        regime = "Tendance"
        entry = float(last['close'])
        sl = float(_anchor_sl_from_extreme(df, 'sell'))
        prev = contact

        # TP short: un peu au-dessus de BB80_lo (⚠️ correction: + offset)
        target_band = float(last['bb80_lo'])
        tp = target_band + tp_offset_trend
        if tp >= entry:
            tp = target_band
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

    # --- Garde-fou contre-tendance après excès prolongé ---
    if not allow_countertrend:
        if signal:
            signal['bb20_mid'] = last['bb20_mid']
            signal['entry_atr'] = contact.get('atr', 0.0)
            signal['entry_rsi'] = 0.0
            return signal
        return None

    # --- CONTRE-TENDANCE (double extrême + réintégration stricte) ---
    if not signal:
        prev = contact
        atr_contact = float(contact.get('atr', last.get('atr', 0.0)))

        touched_ct_low  = (float(prev['low'])  <= float(prev['bb20_lo'])) and (
            float(prev['low'])  <= float(prev['bb80_lo']) or _near_bb80_with_tolerance(float(prev['low']),  float(prev['bb80_lo']), 'buy',  tol_yellow, atr_contact, atr_tol_k)
        )
        touched_ct_high = (float(prev['high']) >= float(prev['bb20_up'])) and (
            float(prev['high']) >= float(prev['bb80_up']) or _near_bb80_with_tolerance(float(prev['high']), float(prev['bb80_up']), 'sell', tol_yellow, atr_contact, atr_tol_k)
        )

        if touched_ct_low and ct_reintegration_ok:
            regime = "Contre-tendance"
            entry = float(last['close'])
            sl = float(_anchor_sl_from_extreme(df, 'buy'))

            # TP long CT: un peu sous BB20_mid
            target_band = float(last['bb20_mid'])
            tp = target_band - tp_offset_ct
            if tp <= entry:
                tp = target_band
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
            entry = float(last['close'])
            sl = float(_anchor_sl_from_extreme(df, 'sell'))

            # TP short CT: un peu au-dessus de BB20_mid (⚠️ correction: + offset)
            target_band = float(last['bb20_mid'])
            tp = target_band + tp_offset_ct
            if tp >= entry:
                tp = target_band
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
        signal['entry_atr'] = contact.get('atr', 0.0)
        signal['entry_rsi'] = 0.0
        return signal

    return None

# ==============================================================================
# LOGIQUE D'EXÉCUTION (Améliorée)
# ==============================================================================

def sync_positions_with_exchange(ex: ccxt.Exchange) -> Dict[str, Any]:
    """
    Vérifie la cohérence entre les positions ouvertes sur l'exchange (Bitget via CCXT)
    et les trades 'OPEN' en base. Ne supprime rien côté exchange. Peut, en option,
    fermer en DB les orphelins (présents en DB mais absents sur l'exchange) selon
    le paramètre AUTO_CLOSE_DB_ORPHANS (DB, défaut false).
    """
    report = {
        "only_on_exchange": [],
        "only_in_db": [],
        "qty_mismatch": [],
        "matched": [],
    }

    try:
        # --- Récup Exchange (corrigé: wrapper qui force productType/marginCoin sur Bitget) ---
        ex_positions_raw = _fetch_positions_safe(ex)

        # Filtrer les positions réellement ouvertes (> 0)
        ex_open = []
        for p in (ex_positions_raw or []):
            try:
                symbol = p.get('symbol') or (p.get('info', {}) or {}).get('symbol')
                if not symbol:
                    continue
                contracts = float(p.get('contracts') or p.get('positionAmt') or 0.0)
                if contracts <= 0:
                    continue
                side_raw = (p.get('side') or '').lower()
                if side_raw in ('long', 'buy'):
                    side = 'buy'
                elif side_raw in ('short', 'sell'):
                    side = 'sell'
                else:
                    side = 'buy'
                entry = float(p.get('entryPrice') or 0.0)
                ex_open.append({
                    'symbol': symbol,
                    'side': side,
                    'quantity': contracts,
                    'entry_price': entry,
                })
            except Exception:
                continue

        # Indexation exchange par (symbol, side)
        ex_map = {}
        for r in ex_open:
            ex_map[(r['symbol'], r['side'])] = r

        # --- Récup DB ---
        db_open = database.get_open_positions() or []
        db_map = {}
        for d in db_open:
            try:
                key = (d['symbol'], d['side'])
                if key not in db_map:
                    db_map[key] = {
                        'ids': [d['id']],
                        'quantity': float(d['quantity']),
                        'entry_price': float(d.get('entry_price') or 0.0)
                    }
                else:
                    db_map[key]['ids'].append(d['id'])
                    db_map[key]['quantity'] += float(d['quantity'])
            except Exception:
                continue

        # --- Comparaison ---
        tol_qty_pct = float(database.get_setting('SYNC_QTY_TOL_PCT', 0.01))  # 1% par défaut
        only_on_exchange, only_in_db, qty_mismatch, matched = [], [], [], []

        for key, ex_pos in ex_map.items():
            symbol, side = key
            ex_qty = float(ex_pos['quantity'])
            if key not in db_map:
                only_on_exchange.append((symbol, side, ex_qty, ex_pos.get('entry_price', 0.0)))
            else:
                db_qty = float(db_map[key]['quantity'])
                if db_qty == 0 and ex_qty > 0:
                    qty_mismatch.append(((db_map[key]['ids'], symbol, side), db_qty, ex_qty))
                else:
                    diff_pct = abs(ex_qty - db_qty) / max(ex_qty, db_qty)
                    if diff_pct > tol_qty_pct:
                        qty_mismatch.append(((db_map[key]['ids'], symbol, side), db_qty, ex_qty))
                    else:
                        matched.append((db_map[key]['ids'], symbol, side, min(db_qty, ex_qty)))

        for key, d in db_map.items():
            if key not in ex_map:
                symbol, side = key
                only_in_db.append((d['ids'], symbol, side, float(d['quantity'])))

        report["only_on_exchange"] = only_on_exchange
        report["only_in_db"] = only_in_db
        report["qty_mismatch"] = qty_mismatch
        report["matched"] = matched

        # --- Actions automatiques limitées (sûres) ---
        auto_close_orphans = str(database.get_setting('AUTO_CLOSE_DB_ORPHANS', 'false')).lower() == 'true'
        if auto_close_orphans and only_in_db:
            for ids, symbol, side, qty in only_in_db:
                try:
                    for tid in (ids if isinstance(ids, list) else [ids]):
                        database.close_trade(tid, status='SYNC_CLOSED_ORPHAN', pnl=0.0)
                    notifier.tg_send(f"🧹 Sync: trades DB orphelins fermés ({symbol} {side}, qty≈{qty}).")
                except Exception as e:
                    notifier.tg_send_error(f"Sync — fermeture orphelin {symbol}", e)

        # --- Notification (désactivable) ---
        try:
            want_notify = str(database.get_setting('SYNC_NOTIFY', 'false')).lower() == 'true'
        except Exception:
            want_notify = False

        if want_notify:
            lines = ["🧭 Vérification positions (Exchange vs DB)"]
            if only_on_exchange:
                lines.append("• Uniquement sur l'exchange:")
                for symbol, side, q, ep in only_on_exchange:
                    lines.append(f"   - {symbol} {side} qty={q} entry≈{ep}")
            if only_in_db:
                lines.append("• Uniquement en DB (status OPEN, pas de position réelle):")
                for ids, symbol, side, q in only_in_db:
                    lines.append(f"   - {symbol} {side} qtyDB={q} (ids={ids})")
            if qty_mismatch:
                lines.append("• Écarts de quantités:")
                for (ids, symbol, side), qdb, qex in qty_mismatch:
                    lines.append(f"   - {symbol} {side} DB={qdb} vs EX={qex} (ids={ids})")
            if matched and not (only_on_exchange or only_in_db or qty_mismatch):
                lines.append("• OK: toutes les positions concordent.")
            notifier.tg_send("\n".join(lines))

    except Exception as e:
        notifier.tg_send_error("Sync positions — erreur inattendue", e)

    return report


def _bitget_positions_params() -> Dict[str, str]:
    """
    Paramètres requis par Bitget pour les endpoints 'mix' USDT Perp.
    umcbl = USDT-margined perpetual.
    """
    return {"productType": "umcbl", "marginCoin": "USDT"}


def _ensure_bitget_mix_options(ex: ccxt.Exchange) -> None:
    """
    Sécurise la configuration Bitget pour les contrats UM (USDT Perp).
    """
    try:
        if getattr(ex, "id", "") != "bitget":
            return
        if not hasattr(ex, "options") or ex.options is None:
            ex.options = {}
        ex.options.setdefault("defaultType", "swap")
        ex.options.setdefault("productType", "umcbl")
        ex.options.setdefault("marginCoin", "USDT")
    except Exception:
        pass

def _resolve_bitget_route(exchange):
    """
    Route Bitget USDT-M uniquement.
    Priorité v2: productType 'USDT-FUTURES', fallback alias 'umcbl'.
    """
    try:
        if not hasattr(exchange, "options") or not isinstance(exchange.options, dict):
            exchange.options = {}
        exchange.options["defaultType"] = "swap"
        exchange.options["defaultSettle"] = "USDT"
    except Exception:
        pass

    return {
        "marginCoin": "USDT",
        "primary": "USDT-FUTURES",
        "fallback": "umcbl",
    }

def _fetch_positions_safe(exchange, symbols=None):
    """
    Lecture robuste des positions Bitget (USDT-M only):
    - Tente d'abord productType='USDT-FUTURES' (v2) avec marginCoin='USDT'
    - Fallback alias 'umcbl'
    - Fallback par symbole (uniquement marchés swap en USDT)
    """
    try:
        exchange.load_markets()
    except Exception:
        pass

    ex_id = getattr(exchange, "id", "")
    if ex_id == "bitget":
        route = _resolve_bitget_route(exchange)
        mc = route["marginCoin"]
        pt_primary = route["primary"]
        pt_fallback = route["fallback"]

        def _try(params):
            res = exchange.fetch_positions(symbols, params)
            return res if res is not None else []

        last_err = None

        # 1) Agrégé: USDT-FUTURES
        try:
            return _try({"type": "swap", "productType": pt_primary, "marginCoin": mc})
        except Exception as e1:
            last_err = e1

        # 2) Agrégé: umcbl (alias)
        try:
            return _try({"type": "swap", "productType": pt_fallback, "marginCoin": mc})
        except Exception as e2:
            last_err = e2

        # 3) Fallback par symbole (USDT swap uniquement)
        positions = []
        try:
            if symbols is None:
                swap_usdt = [
                    m for m, info in (exchange.markets or {}).items()
                    if info.get("swap") and str(info.get("settle", "")).upper() == "USDT"
                ]
            else:
                swap_usdt = list(symbols)
        except Exception:
            swap_usdt = []

        for sym in swap_usdt[:100]:
            # v2 d'abord
            try:
                p = exchange.fetch_position(sym, {"type": "swap", "productType": pt_primary, "marginCoin": mc})
                if p:
                    positions.append(p)
                    continue
            except Exception:
                pass
            # alias ensuite
            try:
                p = exchange.fetch_position(sym, {"type": "swap", "productType": pt_fallback, "marginCoin": mc})
                if p:
                    positions.append(p)
            except Exception:
                continue

        if positions:
            return positions

        try:
            notifier.tg_send(f"❌ Erreur: Lecture des positions exchange\nbitget {str(last_err)}")
        except Exception:
            pass
        return []
    else:
        # Exchanges non-Bitget: standard
        try:
            res = exchange.fetch_positions(symbols)
            return res if res is not None else []
        except Exception as e:
            try:
                notifier.tg_send(f"❌ Erreur: Lecture des positions exchange\n{getattr(exchange,'id','')} {str(e)}")
            except Exception:
                pass
            return []

            
def _fetch_balance_safe(exchange):
    """
    Récupère le solde/équity de manière robuste.
    - Bitget: force type swap + marginCoin et essaie les variantes de productType v2.
      Essaie dans l'ordre: 'USDT-FUTURES'/'COIN-FUTURES' puis anciens alias 'umcbl'/'dmcbl'.
    - Autres exchanges: appel standard.
    Retourne l'objet balance CCXT (dict normalisé).
    """
    try:
        exchange.load_markets()
    except Exception:
        pass

    try:
        if getattr(exchange, "id", "") == "bitget":
            # Prépare options
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

            # Détermine famille Futures (USDT vs COIN)
            family_is_usdt = (margin_coin in ("USDT", "USDC"))

            # Liste des candidats productType selon Bitget v2 (et anciens alias pour compat)
            pt_candidates = (["USDT-FUTURES", "umcbl"] if family_is_usdt else ["COIN-FUTURES", "dmcbl"])

            last_err = None
            for pt in pt_candidates:
                try:
                    bal = exchange.fetch_balance({"type": "swap", "productType": pt, "marginCoin": margin_coin})
                    if bal:
                        return bal
                except Exception as e:
                    last_err = e
                    continue

            # Si tout a échoué, notifie et renvoie dict vide
            try:
                notifier.tg_send(f"❌ Erreur: Récupération du solde\nbitget {str(last_err)}")
            except Exception:
                pass
            return {}

        # Par défaut pour les autres exchanges
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
    Renvoie l'équity totale convertie en USDT pour l'affichage statistiques.
    - Bitget: tente de lire 'usdtEquity' depuis 'info.data' (v2) ou somme des comptes.
    - Fallback: lit la devise USDT normalisée de CCXT si dispo.
    """
    bal = _fetch_balance_safe(exchange)
    if not bal:
        return 0.0

    # Cas Bitget: extraire usdtEquity depuis payload brut si présent
    try:
        if getattr(exchange, "id", "") == "bitget":
            info = bal.get("info", {})
            data = info.get("data", None)
            total_usdt_equity = 0.0
            if isinstance(data, list):
                for acc in data:
                    # 'usdtEquity' est une string numérique côté API v2
                    v = acc.get("usdtEquity") or acc.get("equity") or "0"
                    try:
                        total_usdt_equity += float(v)
                    except Exception:
                        continue
                if total_usdt_equity > 0:
                    return float(total_usdt_equity)
            elif isinstance(data, dict):
                v = data.get("usdtEquity") or data.get("equity") or "0"
                return float(v)
    except Exception:
        pass

    # Fallback générique CCXT: lire USDT total si présent
    try:
        usdt = bal.get("USDT", None)
        if isinstance(usdt, dict):
            for key in ("total", "free", "used"):
                if key in usdt and usdt[key] is not None:
                    return float(usdt.get("total") or usdt.get("free") or 0.0)
    except Exception:
        pass

    # Dernier recours: total 'total' s'il existe (certains exch renvoient un agrégat)
    try:
        total = bal.get("total", {})
        if isinstance(total, dict) and "USDT" in total:
            return float(total["USDT"])
    except Exception:
        pass

    return 0.0

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame, entry_price: float) -> Tuple[bool, str]:
    """Tente d'exécuter un trade avec toutes les vérifications de sécurité."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv('MAX_OPEN_POSITIONS', 3)))

    # --- Sync/guard optionnels avant toute exécution ---
    try:
        if str(database.get_setting('SYNC_BEFORE_EXECUTE', 'true')).lower() == 'true':
            sync_positions_with_exchange(ex)
    except Exception:
        pass

    # Si une position réelle existe déjà sur l'exchange pour ce symbole en mode oneway,
    # on évite d'ouvrir une nouvelle entrée (risque de sur-exposition).
    try:
        market = None
        try:
            market = ex.market(symbol)
        except Exception:
            pass

        ex_positions = _fetch_positions_safe(ex, [symbol])
        already_open = False
        for p in ex_positions or []:
            same = (p.get('symbol') == symbol) or (market and p.get('info', {}).get('symbol') == market.get('id'))
            if not same:
                continue
            contracts = float(p.get('contracts') or p.get('positionAmt') or 0.0)
            if contracts > 0:
                already_open = True
                break

        # --- Auto-import éventuel si position réelle mais pas en DB ---
        if already_open and not database.is_position_open(symbol):
            try:
                if str(database.get_setting('AUTO_IMPORT_EX_POS', 'false')).lower() == 'true':
                    for p in ex_positions or []:
                        same = (p.get('symbol') == symbol) or (market and p.get('info', {}).get('symbol') == market.get('id'))
                        if not same:
                            continue
                        contracts = float(p.get('contracts') or p.get('positionAmt') or 0.0)
                        if contracts <= 0:
                            continue
                        side_raw = (p.get('side') or '').lower()
                        if side_raw in ('long', 'buy'):
                            side_import = 'buy'
                        elif side_raw in ('short', 'sell'):
                            side_import = 'sell'
                        else:
                            side_import = 'buy' if contracts > 0 else 'sell'
                        entry_px = float(p.get('entryPrice') or 0.0)

                        # Récup ATR pour enrichir le trade importé (optionnel)
                        atr_ref = 0.0
                        try:
                            df_tmp = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
                            if df_tmp is not None and len(df_tmp) > 0:
                                atr_ref = float(df_tmp.iloc[-1].get('atr', 0.0))
                        except Exception:
                            pass

                        database.create_trade(
                            symbol=symbol,
                            side=side_import,
                            regime="Importé",
                            entry_price=entry_px,
                            sl_price=entry_px,
                            tp_price=entry_px,
                            quantity=contracts,
                            risk_percent=RISK_PER_TRADE_PERCENT,
                            management_strategy="NORMAL",
                            entry_atr=atr_ref,
                            entry_rsi=0.0,
                        )
                        notifier.tg_send(f"♻️ Position importée automatiquement depuis l'exchange : {symbol} {side_import} qty={contracts}, entry≈{entry_px}")
                        return True, f"Position {symbol} importée depuis exchange."
                else:
                    return False, f"Rejeté: Position déjà ouverte sur l'exchange pour {symbol} (DB non alignée)."
            except Exception as e:
                notifier.tg_send_error(f"Auto-import {symbol}", e)
                return False, f"Erreur auto-import: {e}"
    except Exception:
        # En cas d'échec de lecture des positions, on continue normalement
        pass

    if len(database.get_open_positions()) >= max_pos:
        return False, f"Rejeté: Max positions ({max_pos}) atteint."
    if database.is_position_open(symbol):
        return False, "Rejeté: Position déjà ouverte (DB)."
    
    balance = get_usdt_balance(ex)
    if balance is None or balance <= 10:
        return False, f"Rejeté: Solde insuffisant ({balance or 0:.2f} USDT) ou erreur API."
    
    quantity = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, entry_price, signal['sl'])
    if quantity <= 0:
        return False, f"Rejeté: Quantité calculée nulle ({quantity})."
        
    notional_value = quantity * entry_price
    if notional_value < MIN_NOTIONAL_VALUE:
        return False, f"Rejeté: Valeur du trade ({notional_value:.2f} USDT) < min requis ({MIN_NOTIONAL_VALUE} USDT)."
    
    final_entry_price = entry_price
    if not is_paper_mode:
        try:
            # 2) S’assurer du contexte (avant ordres/lectures)
            ex.set_leverage(LEVERAGE, symbol)
            try: ex.set_margin_mode('cross', symbol)
            except Exception: pass
            try: ex.set_position_mode(False, symbol)  # False => oneway
            except Exception: pass

            common_params = {'tdMode': 'cross', 'posMode': 'oneway'}

            # Garde-fou SL/TP vs prix d'entrée
            gap_pct = float(database.get_setting('SL_MIN_GAP_PCT', 0.0003))
            price_ref = float(entry_price)
            side = signal['side']

            sl = float(signal['sl'])
            tp = float(signal['tp'])

            if side == 'sell':  # SHORT
                if sl <= price_ref: sl = price_ref * (1.0 + gap_pct)
                if tp >= price_ref: tp = price_ref * (1.0 - gap_pct)
            else:  # BUY (LONG)
                if sl >= price_ref: sl = price_ref * (1.0 - gap_pct)
                if tp <= price_ref: tp = price_ref * (1.0 + gap_pct)

            # Précision prix
            try:
                sl = float(ex.price_to_precision(symbol, sl))
                tp = float(ex.price_to_precision(symbol, tp))
            except Exception:
                pass

            signal['sl'] = sl
            signal['tp'] = tp

            # Recalcule taille avec SL ajusté (puis arrondi quantité)
            qty_adj = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, entry_price, sl)
            quantity = min(quantity, qty_adj)
            try:
                quantity = float(ex.amount_to_precision(symbol, quantity))
            except Exception:
                pass
            if quantity <= 0:
                return False, "Rejeté: quantité arrondie à 0."

            # Recheck notional
            notional_value = quantity * entry_price
            if notional_value < MIN_NOTIONAL_VALUE:
                return False, f"Rejeté: Valeur du trade ({notional_value:.2f} USDT) < min requis ({MIN_NOTIONAL_VALUE} USDT)."

            # 1) Entrée au marché
            order = ex.create_market_order(symbol, signal['side'], quantity, params=common_params)

            # 2) SL/TP triggers en MARKET, reduceOnly
            close_side = 'sell' if signal['side'] == 'buy' else 'buy'
            ex.create_order(symbol, 'market', close_side, quantity, price=None,
                            params={**common_params, 'stopLossPrice': sl, 'reduceOnly': True, 'triggerType': 'mark'})
            ex.create_order(symbol, 'market', close_side, quantity, price=None,
                            params={**common_params, 'takeProfitPrice': tp, 'reduceOnly': True, 'triggerType': 'mark'})

            if order and order.get('price'):
                final_entry_price = float(order['price'])

        except Exception as e:
            try:
                close_side = 'sell' if signal['side'] == 'buy' else 'buy'
                # on tente de réduire ce qui a pu s’ouvrir
                ex.create_market_order(symbol, close_side, quantity, params={'reduceOnly': True, 'tdMode': 'cross', 'posMode': 'oneway'})
            except Exception:
                pass
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            return False, f"Erreur d'exécution: {e}"

    signal['entry'] = final_entry_price
    
    management_strategy = "NORMAL"
    if database.get_setting('STRATEGY_MODE', 'NORMAL').upper() == 'SPLIT':
        management_strategy = "SPLIT"
        
    database.create_trade(
        symbol=symbol,
        side=signal['side'],
        regime=signal['regime'],
        entry_price=final_entry_price,
        sl_price=signal['sl'],
        tp_price=signal['tp'],
        quantity=quantity,
        risk_percent=RISK_PER_TRADE_PERCENT,
        management_strategy=management_strategy,
        entry_atr=signal.get('entry_atr', 0.0) or 0.0,
        entry_rsi=signal.get('entry_rsi', 0.0) or 0.0,
    )
    
    chart_image = charting.generate_trade_chart(symbol, df, signal)
    mode_text = "PAPIER" if is_paper_mode else "RÉEL"
    trade_message = notifier.format_trade_message(symbol, signal, quantity, mode_text, RISK_PER_TRADE_PERCENT)
    notifier.tg_send_with_photo(photo_buffer=chart_image, caption=trade_message)
    
    return True,"Position ouverte avec succès."

def manage_open_positions(ex: ccxt.Exchange):
    """Gère les positions ouvertes : SPLIT (50% + BE), BE auto en NORMALE/CT, trailing après BE.
       Tous les ordres sont MARKET reduceOnly (triggers) + precision quantité + sécurisation modes."""
    if database.get_setting('PAPER_TRADING_MODE', 'true') == 'true':
        return

    try:
        need_sync = str(database.get_setting('SYNC_BEFORE_MANAGE', 'true')).lower() == 'true'
    except Exception:
        need_sync = True
    if need_sync:
        sync_positions_with_exchange(ex)

    open_positions = database.get_open_positions()
    if not open_positions:
        return

    for pos in open_positions:
        symbol = pos['symbol']
        is_long = (pos['side'] == 'buy')
        close_side = 'sell' if is_long else 'buy'
        common_params = {'reduceOnly': True, 'tdMode': 'cross', 'posMode': 'oneway'}

        # 2) Sécuriser les modes AVANT toute action marché sur ce symbole
        try:
            ex.set_leverage(LEVERAGE, symbol)
            try: ex.set_margin_mode('cross', symbol)
            except Exception: pass
            try: ex.set_position_mode(False, symbol)
            except Exception: pass
        except Exception:
            pass

        # --- SPLIT ---
        if pos['management_strategy'] == 'SPLIT' and pos['breakeven_status'] == 'PENDING':
            try:
                current_price = ex.fetch_ticker(symbol)['last']
                df = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
                if df is None or len(df) == 0:
                    continue

                management_trigger_price = df.iloc[-1]['bb20_mid']

                if (is_long and current_price >= management_trigger_price) or (not is_long and current_price <= management_trigger_price):
                    qty_to_close = pos['quantity'] / 2
                    try: qty_to_close = float(ex.amount_to_precision(symbol, qty_to_close))
                    except Exception: pass
                    if qty_to_close <= 0:
                        continue
                    remaining_qty = max(0.0, pos['quantity'] - qty_to_close)

                    # 1) Clôturer 50%
                    ex.create_market_order(symbol, close_side, qty_to_close, params=common_params)

                    # 2) BE sur le restant
                    try:
                        ex.cancel_all_orders(symbol)
                    except Exception as e:
                        if '22001' not in str(e): raise
                    fees_bps = float(database.get_setting('FEES_BPS', 5))
                    fee_factor = (1.0 - fees_bps / 10000.0) if is_long else (1.0 + fees_bps / 10000.0)
                    new_sl_be = pos['entry_price'] * fee_factor

                    # Arrondi quantité restante
                    try: remaining_qty = float(ex.amount_to_precision(symbol, remaining_qty))
                    except Exception: pass
                    if remaining_qty > 0:
                        ex.create_order(symbol, 'market', close_side, remaining_qty, price=None,
                                        params={**common_params, 'stopLossPrice': float(new_sl_be), 'triggerType': 'mark'})
                        ex.create_order(symbol, 'market', close_side, remaining_qty, price=None,
                                        params={**common_params, 'takeProfitPrice': float(pos['tp_price']), 'triggerType': 'mark'})

                    pnl_realised = (current_price - pos['entry_price']) * qty_to_close if is_long else (pos['entry_price'] - current_price) * qty_to_close
                    database.update_trade_to_breakeven(pos['id'], remaining_qty, new_sl_be)
                    notifier.send_breakeven_notification(symbol, pnl_realised, remaining_qty)

            except Exception as e:
                print(f"Erreur de gestion SPLIT pour {symbol}: {e}")

        # --- BE NORMAL (CT) ---
        if pos['management_strategy'] == 'NORMAL' and pos.get('regime') == 'Contre-tendance' and pos.get('breakeven_status') == 'PENDING':
            try:
                df = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
                if df is not None and len(df) > 0:
                    last_close = float(df.iloc[-1]['close'])
                    mm20 = float(df.iloc[-1]['bb20_mid'])

                    crossed = (is_long and last_close >= mm20) or ((not is_long) and last_close <= mm20)
                    if crossed:
                        try:
                            ex.cancel_all_orders(symbol)
                        except Exception as e:
                            if '22001' not in str(e): raise
                        fees_bps = float(database.get_setting('FEES_BPS', 5))
                        fee_factor = (1.0 - fees_bps / 10000.0) if is_long else (1.0 + fees_bps / 10000.0)
                        new_sl_be = float(pos['entry_price']) * fee_factor

                        qty = float(pos['quantity'])
                        try: qty = float(ex.amount_to_precision(symbol, qty))
                        except Exception: pass
                        if qty <= 0:
                            continue

                        ex.create_order(symbol, 'market', close_side, qty, price=None,
                                        params={**common_params, 'stopLossPrice': new_sl_be, 'triggerType': 'mark'})
                        ex.create_order(symbol, 'market', close_side, qty, price=None,
                                        params={**common_params, 'takeProfitPrice': float(pos['tp_price']), 'triggerType': 'mark'})

                        database.update_trade_to_breakeven(pos['id'], qty, new_sl_be)
                        notifier.send_breakeven_notification(symbol, 0.0, qty)

            except Exception as e:
                print(f"Erreur BE NORMAL contre-tendance {symbol}: {e}")

        # --- Trailing après BE (LIVE, 'pro', silencieux) ---
        if pos.get('breakeven_status') in ('ACTIVE', 'DONE', 'BE'):
            try:
                # Paramètres 'pro'
                try:
                    move_min_pct = float(database.get_setting('TRAIL_MOVE_MIN_PCT', 0.0002))  # 0.02%
                except Exception:
                    move_min_pct = 0.0002
                try:
                    atr_k = float(database.get_setting('TRAIL_ATR_K', 1.3))  # un peu large
                except Exception:
                    atr_k = 1.3

                # Données LIVE
                ticker = ex.fetch_ticker(symbol) or {}
                last_price = float(ticker.get('last') or ticker.get('close') or pos['entry_price'])

                df_live = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
                if df_live is None or len(df_live) == 0:
                    continue
                last_row = df_live.iloc[-1]
                bb20_mid = float(last_row['bb20_mid'])
                atr_live = float(last_row.get('atr', 0.0))

                # BE de référence (ne jamais repasser sous/sur BE)
                be_price = float(pos.get('sl_price') or pos['entry_price'])
                current_sl = float(pos.get('sl_price') or pos['entry_price'])

                # Candidat SL :
                # Long  -> max(BB20_mid, last - ATR*K, BE, current_sl)
                # Short -> min(BB20_mid, last + ATR*K, BE, current_sl)
                if is_long:
                    target_atr = last_price - atr_live * atr_k
                    candidate = max(bb20_mid, target_atr, be_price, current_sl)
                    moved = candidate > current_sl and (current_sl <= 0 or (candidate - current_sl) / max(current_sl, 1e-12) >= move_min_pct)
                else:
                    target_atr = last_price + atr_live * atr_k
                    candidate = min(bb20_mid, target_atr, be_price, current_sl)
                    moved = candidate < current_sl and (current_sl <= 0 or (current_sl - candidate) / max(current_sl, 1e-12) >= move_min_pct)

                if not moved:
                    continue

                # Mise à jour côté exchange : on ne bouge QUE le SL (pas de notif)
                try:
                    qty = float(pos['quantity'])
                    try:
                        qty = float(ex.amount_to_precision(symbol, qty))
                    except Exception:
                        pass
                    if qty <= 0:
                        continue

                    try:
                        candidate = float(ex.price_to_precision(symbol, candidate))
                    except Exception:
                        pass

                    try_cancel = str(database.get_setting('TRY_CANCEL_BEFORE_SL_UPDATE', 'false')).lower() == 'true'
                    if try_cancel:
                        try:
                            ex.cancel_all_orders(symbol)
                        except Exception as e:
                            if '22001' not in str(e):
                                raise

                    ex.create_order(
                        symbol, 'market', close_side, qty, price=None,
                        params={'reduceOnly': True, 'tdMode': 'cross', 'posMode': 'oneway',
                                'stopLossPrice': float(candidate), 'triggerType': 'mark'}
                    )

                    try:
                        database.update_trade_sl(pos['id'], candidate)
                    except AttributeError:
                        database.update_trade_to_breakeven(pos['id'], qty, candidate)

                except Exception as e:
                    # Silencieux
                    print(f"Erreur trailing {symbol}: {e}")

            except Exception as e:
                print(f"Erreur trailing {symbol}: {e}")

        # --- TP dynamique (silencieux) ---
        try:
            df = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
            if df is not None and len(df) > 0:
                last = df.iloc[-1]
                regime = pos.get('regime', 'Tendance')

                bb20_mid = float(last['bb20_mid'])
                bb80_up  = float(last['bb80_up'])
                bb80_lo  = float(last['bb80_lo'])

                offset_pct = float(database.get_setting('TP_BB_OFFSET_PCT', 0.0015))
                if regime == 'Tendance':
                    target_tp = (bb80_up * (1.0 - offset_pct)) if is_long else (bb80_lo * (1.0 + offset_pct))
                else:
                    target_tp = (bb20_mid * (1.0 - offset_pct)) if is_long else (bb20_mid * (1.0 + offset_pct))

                current_tp = float(pos['tp_price'])
                improve = (is_long and target_tp > current_tp * 1.0002) or ((not is_long) and target_tp < current_tp * 0.9998)
                if improve:
                    try:
                        ex.cancel_all_orders(symbol)
                    except Exception as e:
                        if '22001' not in str(e): raise
                    sl_price = float(pos.get('sl_price') or pos['entry_price'])

                    qty = float(pos['quantity'])
                    try:
                        qty = float(ex.amount_to_precision(symbol, qty))
                        target_tp = float(ex.price_to_precision(symbol, target_tp))
                        sl_price = float(ex.price_to_precision(symbol, sl_price))
                    except Exception:
                        pass
                    if qty <= 0:
                        continue

                    ex.create_order(symbol, 'market', close_side, qty, price=None,
                                    params={**common_params, 'stopLossPrice': sl_price, 'triggerType': 'mark'})
                    ex.create_order(symbol, 'market', close_side, qty, price=None,
                                    params={**common_params, 'takeProfitPrice': float(target_tp), 'triggerType': 'mark'})

                    try:
                        database.update_trade_tp(pos['id'], float(target_tp))
                    except Exception:
                        pass
                    # Aucune notification ici (mouvements visibles via l'écran Positions)
        except Exception as e:
            print(f"Erreur TP dynamique {symbol}: {e}")

def get_usdt_balance(ex: ccxt.Exchange) -> Optional[float]:
    """Solde USDT/équity robuste (Bitget v2 compatible, évite 'Parameter productType error')."""
    try:
        bal = _fetch_balance_safe(ex)
        if not bal:
            return 0.0

        # Bitget: prioriser l'équity USDT native renvoyée par l'API v2
        if getattr(ex, "id", "") == "bitget":
            try:
                info = bal.get("info", {})
                data = info.get("data", None)
                total_usdt_equity = 0.0
                if isinstance(data, list):
                    for acc in data:
                        v = acc.get("usdtEquity") or acc.get("equity") or "0"
                        total_usdt_equity += float(v)
                    if total_usdt_equity > 0:
                        return float(total_usdt_equity)
                elif isinstance(data, dict):
                    v = data.get("usdtEquity") or data.get("equity") or "0"
                    return float(v)
            except Exception:
                pass  # fallback générique juste dessous

        # Fallback générique CCXT: lire USDT normalisé
        try:
            usdt = bal.get("USDT")
            if isinstance(usdt, dict):
                return float(usdt.get("total") or usdt.get("free") or 0.0)
        except Exception:
            pass

        # Dernier recours: agrégat 'total'
        try:
            total = bal.get("total", {})
            if isinstance(total, dict) and "USDT" in total:
                return float(total["USDT"])
        except Exception:
            pass

        return 0.0

    except Exception as e:
        notifier.tg_send_error("Récupération du solde", e)
        return None

def calculate_position_size(balance: float, risk_percent: float, entry_price: float, sl_price: float) -> float:
    """Calcule la quantité d'actifs à trader."""
    if balance <= 0 or entry_price == sl_price: return 0.0
    risk_amount_usdt = balance * (risk_percent / 100.0)
    price_diff_per_unit = abs(entry_price - sl_price)
    return risk_amount_usdt / price_diff_per_unit if price_diff_per_unit > 0 else 0.0

def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    """Clôture manuelle robuste : vérifie la position réelle, arrondit la quantité, et ferme en MARKET reduceOnly.
       Sécurise leverage/modes avant lecture des positions sur le symbole."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    trade = database.get_trade_by_id(trade_id)
    if not trade or trade.get('status') != 'OPEN':
        return notifier.tg_send(f"Trade #{trade_id} déjà fermé ou invalide.")
    
    symbol = trade['symbol']
    side = trade['side']
    qty_db = float(trade['quantity'])

    try:
        # 2) Contexte du symbole AVANT fetch_positions
        try:
            ex.set_leverage(LEVERAGE, symbol)
            try: ex.set_margin_mode('cross', symbol)
            except Exception: pass
            try: ex.set_position_mode(False, symbol)
            except Exception: pass
        except Exception:
            pass

        # 1) Vérifier la position réelle
        real_qty = 0.0
        market = None
        try:
            market = ex.market(symbol)
        except Exception:
            pass

        try:
            positions = _fetch_positions_safe(ex, [symbol])
            for p in positions:
                same = (p.get('symbol') == symbol) or (market and p.get('info', {}).get('symbol') == market.get('id'))
                if same:
                    contracts = float(p.get('contracts') or p.get('positionAmt') or 0.0)
                    if contracts and contracts > 0:
                        real_qty = contracts
                        break
        except Exception:
            pass

        if real_qty <= 0:
            database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
            return notifier.tg_send(f"ℹ️ Aucune position ouverte détectée pour {symbol}. Trade #{trade_id} marqué fermé.")

        qty_to_close = min(qty_db, real_qty)
        try: qty_to_close = float(ex.amount_to_precision(symbol, qty_to_close))
        except Exception: pass
        if qty_to_close <= 0:
            database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
            return notifier.tg_send(f"ℹ️ Quantité nulle à clôturer sur {symbol}. Trade #{trade_id} marqué fermé.")

        if not is_paper_mode:
            close_side = 'sell' if side == 'buy' else 'buy'
            params = {'reduceOnly': True, 'tdMode': 'cross', 'posMode': 'oneway'}
            ex.create_market_order(symbol, close_side, qty_to_close, params=params)

        database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
        notifier.tg_send(f"✅ Position sur {symbol} (Trade #{trade_id}) fermée manuellement (qty={qty_to_close}).")

    except Exception as e:
        notifier.tg_send_error(f"Fermeture manuelle de {symbol}", e)

