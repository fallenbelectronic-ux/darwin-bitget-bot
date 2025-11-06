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

# --- Filtres de réaction (paternes) ---
REACTION_WINDOW_BARS = 3
PINBAR_MAX_BODY = 0.30      # ≤ 30% du range
IMPULSE_MIN_BODY = 0.30     # ≥ 30% du range
SIMPLE_WICK_MIN = 0.30      # ≥ 30% du range

# --- Frais & BE ---
FEE_ENTRY_PCT   = float(os.getenv("FEE_ENTRY_PCT", "0.0010"))  # 0.1% typique taker
FEE_EXIT_PCT    = float(os.getenv("FEE_EXIT_PCT",  "0.0010"))  # 0.1% typique taker
BE_BUFFER_PCT   = float(os.getenv("BE_BUFFER_PCT", "0.0020"))  # +0.2% au-dessus du VRAI BE
BE_BUFFER_USDT  = float(os.getenv("BE_BUFFER_USDT","0.0"))     # buffer absolu optionnel (USDT). Laisse 0 si tu n’en veux pas.

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


def detect_signal(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """(FINAL) Détection avec offsets hybrides:
       - Option E (entrée bougie suivante) via CT_ENTRY_ON_NEXT_BAR
       - CT: réintégration stricte configurable (BB20 &/ou BB80)
       - Anti-excès BB80
       - Cut-wick seulement si CUT_WICK_FOR_RR = true
       - TP: offset hybride = max(TP_BB_OFFSET_PCT, ATR * TP_ATR_K / ref_band)
       - SL: via _anchor_sl_from_extreme() (déjà hybride)"""
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
        atr_tol_k = float(database.get_setting('BB80_ATR_TOL_K', 0.125))  # tolérance BB80 contact ONLY
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

    # Pourcentage de base (sert de borne basse à l’hybride)
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

    # --- Réaction = dernière FERMÉE si Option E active ---
    last_idx = -2 if (enforce_next_bar and len(df) >= 2) else -1
    last = df.iloc[last_idx]
    base_idx = len(df) + last_idx

    # Prix d'entrée utilisé pour le sizing/RR :
    # - si enforce_next_bar: on entre à l'OPEN de la bougie courante (df.iloc[-1])
    # - sinon: on entre à la CLOSE de la bougie "last" (réaction)
    if enforce_next_bar and len(df) >= 1:
        next_open = float(df.iloc[-1]['open'])
        entry_px_for_rr = next_open
    else:
        entry_px_for_rr = float(last['close'])

    # --- Cherche la bougie CONTACT (≤ reaction_max_bars avant la réaction) ---
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

    # --- Gestion neutralité MM80 avec vague précédente (BB80 only) ---
    bb80_mid_last = float(last['bb80_mid'])
    dead_zone = bb80_mid_last * (MM_DEAD_ZONE_PERCENT / 100.0)
    in_dead_zone = abs(float(last['close']) - bb80_mid_last) < dead_zone

    prev_wave = None
    if in_dead_zone:
        prev_wave = _previous_wave_by_bb80(df, base_idx, MM_DEAD_ZONE_PERCENT)


    # --- Fenêtre 3 barres incluant la bougie de CONTACT ---
    # contact = c1, puis réactions possibles: c2, c3 (si dispo)
    c1 = contact
    c2 = df.iloc[contact_idx + 1] if (contact_idx + 1) < len(df) else None
    c3 = df.iloc[contact_idx + 2] if (contact_idx + 2) < len(df) else None

    # Détermination finale du sens MM80 (après pat_ok / avant tests Tendance/CT)
    if in_dead_zone and prev_wave in ('up', 'down'):
        # en hésitation, on tranche par la vague précédente
        is_above_mm80 = (prev_wave == 'up')
    else:
        is_above_mm80 = float(last['close']) > float(last['bb80_mid'])
    
    def _inside_both(candle):
        return _inside(float(candle['close']), float(candle['bb80_lo']), float(candle['bb80_up'])) and \
               _inside(float(candle['close']), float(candle['bb20_lo']), float(candle['bb20_up']))

    def _inside_bb20_only(candle):
        return _inside(float(candle['close']), float(candle['bb20_lo']), float(candle['bb20_up']))

    # (1) PINBAR sur la bougie de contact (réaction = contact) + inside (BB80 & BB20) — couleur indifférente
    pinbar_contact_ok = False
    try:
        if _body_ratio(c1) <= PINBAR_MAX_BODY and _inside_both(c1):
            o1, c1v = float(c1['open']), float(c1['close'])
            h1, l1 = float(c1['high']), float(c1['low'])
            rng1 = max(1e-12, h1 - l1)
            w_up1 = max(0.0, h1 - max(o1, c1v)) / rng1
            w_dn1 = max(0.0, min(o1, c1v) - l1) / rng1
            if side_guess == 'buy':
                # Long ⇒ grande mèche basse, petite mèche haute
                if w_dn1 >= SIMPLE_WICK_MIN and w_up1 <= PINBAR_OPP_WICK_MAX:
                    pinbar_contact_ok = True
            else:
                # Short ⇒ grande mèche haute, petite mèche basse
                if w_up1 >= SIMPLE_WICK_MIN and w_dn1 <= PINBAR_OPP_WICK_MAX:
                    pinbar_contact_ok = True
    except Exception:
        pinbar_contact_ok = False

    # (2) Réintégration dans la FENÊTRE (barres 2–3) selon le régime
    #     - CT  : inside BB80 ET BB20 (sur c2 OU c3) si ct_reintegrate_both=True
    #             sinon inside BB20 suffit
    #     - Tend: inside BB20 (sur c2 OU c3)
    inside_both_seen = False
    inside_bb20_seen = False

    for cx in (c2, c3):
        if cx is None:
            continue
        if _inside_both(cx):
            inside_both_seen = True
            inside_bb20_seen = True  # inside_both ⇒ inside_bb20
        elif _inside_bb20_only(cx):
            inside_bb20_seen = True

    # Tendance : réintégration BB20 suffit
    trend_reintegration_ok = inside_bb20_seen

    # CT : selon le toggle
    ct_reintegration_ok = inside_both_seen if ct_reintegrate_both else inside_bb20_seen


    # (3) Pattern valide (1→4) dans la fenêtre
    pat_ok = False
    for cx, pv in ((c2, c1), (c3, c2)):
        if cx is None:
            continue
        if is_valid_reaction_candle(cx, side_guess, prev=pv):
            pat_ok = True
            break
    if pinbar_contact_ok:
        pat_ok = True

    # (4) Disqualifiants : pas de pattern validé après 3 barres ⇒ on annule
    if not pat_ok:
        return None

    # ATR courant (pour offset hybride TP)
    last_atr = float(last.get('atr', contact.get('atr', 0.0)))

    signal = None

    touched_bb20_low  = utils.touched_or_crossed(contact['low'], contact['high'], contact['bb20_lo'], "buy")
    touched_bb20_high = utils.touched_or_crossed(contact['low'], contact['high'], contact['bb20_up'], "sell")

    # --- TENDANCE ---
    if is_above_mm80 and touched_bb20_low and trend_reintegration_ok:
        regime = "Tendance"
        entry = entry_px_for_rr
        sl = float(_anchor_sl_from_extreme(df, 'buy'))  # SL hybride

        target_band = float(last['bb80_up'])
        eff_pct = max(tp_pct, (tp_atr_k * last_atr) / target_band if target_band > 0 else tp_pct)
        tp = target_band * (1.0 - eff_pct)
        if tp <= entry:
            return None

        if (entry - sl) > 0:
            rr = (tp - entry) / (entry - sl)
            rr_final = rr
            if rr < MIN_RR and rr >= 2.8 and cut_wick_enabled:
                rr_alt, _ = _maybe_improve_rr_with_cut_wick(contact, entry, sl, tp, 'buy')
                rr_final = max(rr, rr_alt)
            if rr_final >= MIN_RR:
                signal = {"side": "buy", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr_final}

    elif (not is_above_mm80) and touched_bb20_high and trend_reintegration_ok:
        regime = "Tendance"
        entry = entry_px_for_rr
        sl = float(_anchor_sl_from_extreme(df, 'sell'))  # SL hybride

        target_band = float(last['bb80_lo'])
        eff_pct = max(tp_pct, (tp_atr_k * last_atr) / target_band if target_band > 0 else tp_pct)
        tp = target_band * (1.0 + eff_pct)
        if tp >= entry:
            return None

        if (sl - entry) > 0:
            rr = (entry - tp) / (sl - entry)
            rr_final = rr
            if rr < MIN_RR and rr >= 2.8 and cut_wick_enabled:
                rr_alt, _ = _maybe_improve_rr_with_cut_wick(contact, entry, sl, tp, 'sell')
                rr_final = max(rr, rr_alt)
            if rr_final >= MIN_RR:
                signal = {"side": "sell", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr_final}

    # --- Garde-fou CT après excès ---
    if not allow_countertrend:
        if signal:
            signal['bb20_mid'] = last['bb20_mid']
            signal['entry_atr'] = contact.get('atr', 0.0)
            signal['entry_rsi'] = 0.0
            return signal
        return None

    # --- CONTRE-TENDANCE ---
    if not signal:
        prev = contact
        atr_contact = float(contact.get('atr', last.get('atr', 0.0)))  # tolérance BB80 (contact) ONLY

        touched_ct_low  = (float(prev['low'])  <= float(prev['bb20_lo'])) and (
            float(prev['low'])  <= float(prev['bb80_lo']) or _near_bb80_with_tolerance(float(prev['low']),  float(prev['bb80_lo']), 'buy',  tol_yellow, atr_contact, atr_tol_k)
        )
        touched_ct_high = (float(prev['high']) >= float(prev['bb20_up'])) and (
            float(prev['high']) >= float(prev['bb80_up']) or _near_bb80_with_tolerance(float(prev['high']), float(prev['bb80_up']), 'sell', tol_yellow, atr_contact, atr_tol_k)
        )

        if touched_ct_low and ct_reintegration_ok:
            regime = "Contre-tendance"
            entry = entry_px_for_rr
            sl = float(_anchor_sl_from_extreme(df, 'buy'))  # SL hybride

            target_band = float(last['bb20_mid'])
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
            sl = float(_anchor_sl_from_extreme(df, 'sell'))  # SL hybride

            target_band = float(last['bb20_mid'])
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
        signal['entry_atr'] = contact.get('atr', 0.0)
        signal['entry_rsi'] = 0.0
        return signal

    return None


# ==============================================================================
# LOGIQUE D'EXÉCUTION (Améliorée)
# ==============================================================================

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

def sync_positions_with_exchange(ex: ccxt.Exchange) -> Dict[str, Any]:
    """
    Vérifie la cohérence entre les positions ouvertes sur l'exchange (Bitget via CCXT)
    et les trades 'OPEN' en base. Peut importer automatiquement les positions présentes
    sur l'exchange mais absentes en DB (AUTO_IMPORT_EX_POS = true)."""
    _ensure_bitget_mix_options(ex)
    report = {
        "only_on_exchange": [],
        "only_in_db": [],
        "qty_mismatch": [],
        "matched": [],
    }

    try:
        ex_positions_raw = _fetch_positions_safe(ex)

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
                ex_open.append({'symbol': symbol, 'side': side, 'quantity': contracts, 'entry_price': entry})
            except Exception:
                continue

        ex_map = {(r['symbol'], r['side']): r for r in ex_open}

        db_open = database.get_open_positions() or []
        db_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for d in db_open:
            try:
                key = (d['symbol'], d['side'])
                if key not in db_map:
                    db_map[key] = {'ids': [d['id']], 'quantity': float(d['quantity']), 'entry_price': float(d.get('entry_price') or 0.0)}
                else:
                    db_map[key]['ids'].append(d['id'])
                    db_map[key]['quantity'] += float(d['quantity'])
            except Exception:
                continue

        tol_qty_pct = float(database.get_setting('SYNC_QTY_TOL_PCT', 0.01))
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

        report.update({
            "only_on_exchange": only_on_exchange,
            "only_in_db": only_in_db,
            "qty_mismatch": qty_mismatch,
            "matched": matched,
        })

        try:
            do_import = str(database.get_setting('AUTO_IMPORT_EX_POS', 'true')).lower() == 'true'
        except Exception:
            do_import = True

        if do_import and only_on_exchange:
            for symbol, side, q, ep in only_on_exchange:
                try:
                    if database.is_position_open(symbol):
                        continue
                except Exception:
                    pass
                _import_exchange_position_to_db(ex, symbol, side, q, ep)

        # --- NEW: Fermeture auto des orphelins DB (positions disparues côté exchange) ---
        auto_close_orphans = str(database.get_setting('AUTO_CLOSE_DB_ORPHANS', 'true')).lower() == 'true'
        if auto_close_orphans and only_in_db:
            for ids, symbol, side, qty in only_in_db:
                try:
                    for tid in (ids if isinstance(ids, list) else [ids]):
                        database.close_trade(tid, status='CLOSED_BY_EXCHANGE', pnl=0.0)
                    notifier.tg_send(f"🧹 Sync: fermeture auto — {symbol} {side} (qty≈{qty}) disparue côté exchange.")
                except Exception as e:
                    notifier.tg_send_error(f"Sync — fermeture orphelin {symbol}", e)

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
                lines.append("• Uniquement en DB:")
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
    try:
        if getattr(ex, "id", "") != "bitget":
            return
        if not hasattr(ex, "options") or ex.options is None:
            ex.options = {}
        ex.options["defaultType"] = "swap"
        ex.options["defaultSettle"] = "USDT"
        ex.options["productType"] = "USDT-FUTURES"
    except Exception:
        pass


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


def _fetch_positions_safe(exchange, symbols=None):
    try:
        exchange.load_markets()
    except Exception:
        pass

    ex_id = getattr(exchange, "id", "")
    if ex_id == "bitget":
        route = _resolve_bitget_route(exchange)
        mc = route["marginCoin"]

        params_base = {"type": "swap", "productType": "USDT-FUTURES", "marginCoin": mc}
        last_err = None

        try:
            res = exchange.fetch_positions(symbols, params_base)
            return res if res is not None else []
        except Exception as e1:
            last_err = e1

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
            try:
                p = exchange.fetch_position(sym, params_base)
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


def place_order(exchange, symbol: str, side: str, order_type: str, qty: float, price: Optional[float] = None, params: Optional[Dict[str, Any]] = None):
    """
    (MODIFIÉ) Envoi d'ordre avec garde-fous:
      - Cap par marge + respect min_qty/step/min_notional
      - Annulation propre + notif TG si solde insuffisant
      - Ne change PAS le reste de ta logique (pas de refacto)
    """
    try:
        q = abs(float(qty))
        p = float(price) if price is not None else None

        # Cap par marge + filtres marché (uniquement si prix connu ou market avec best bid/ask récupérable)
        ref_price = p
        if ref_price is None:
            # fallback: dernier prix du ticker pour estimer la notional si market order
            try:
                t = exchange.fetch_ticker(symbol)
                ref_price = float(t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0)
            except Exception:
                ref_price = 0.0

        capped_qty, meta = _cap_qty_for_margin_and_filters(exchange, symbol, side, q, ref_price or 0.0)

        if capped_qty <= 0.0 and meta.get("reason") == "INSUFFICIENT_AFTER_CAP":
            # Notif claire et on sort proprement
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
            return None  # on stoppe l'envoi d'ordre

        # Si la taille a été réduite, informer (transparence)
        if meta.get("reason") == "CAPPED_BY_MARGIN":
            try:
                reduced_pct = (1.0 - (capped_qty / q)) * 100.0 if q > 0 else 0.0
                txt = (
                    f"⚠️ Taille réduite par marge\n"
                    f"• {symbol} {side.upper()} {order_type.upper()}\n"
                    f"• Demandée: <code>{q}</code> → Envoyée: <code>{capped_qty}</code> "
                    f"(-{reduced_pct:.2f}%)\n"
                    f"• Levier: <code>{meta.get('leverage')}</code> | Marge dispo: <code>{(meta.get('available_margin') or 0.0):.2f} USDT</code>\n"
                )
                notifier.tg_send(txt)
            except Exception:
                pass

        # Envoi réel de l'ordre (inchangé sinon)
        params = params or {}
        if order_type.lower() == "market":
            return exchange.create_order(symbol, "market", side, capped_qty, None, params)
        else:
            # limit/stop-limit nécessitent un price
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
    (MISE À JOUR) Offset hybride pour le SL : max(pourcentage, ATR*k).
    - ref_price: l’ancre (high/low de la bougie d’ancrage) pour convertir ATR en %.
    - Sans ref_price, on retombe sur l'ancien comportement (pourcentage seul).
    """
    try:
        pct = float(database.get_setting('SL_OFFSET_PCT', '0.006'))  # 0.60%
    except Exception:
        pct = 0.006
    try:
        atr_k = float(database.get_setting('SL_ATR_K', '1.00'))
    except Exception:
        atr_k = 1.00

    eff_pct = pct
    if ref_price and ref_price > 0 and atr > 0:
        eff_pct = max(pct, (atr_k * float(atr)) / float(ref_price))

    s = (side or "").lower()
    if s in ("buy", "long"):
        return float(raw_sl) * (1.0 - eff_pct)
    if s in ("sell", "short"):
        return float(raw_sl) * (1.0 + eff_pct)
    return float(raw_sl)


def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame, entry_price: float) -> Tuple[bool, str]:
    """Tente d'exécuter un trade avec toutes les vérifications de sécurité + cap marge anti-40762."""
    _ensure_bitget_mix_options(ex)
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv('MAX_OPEN_POSITIONS', 3)))

    # --- Sync/guard optionnels avant toute exécution ---
    try:
        if str(database.get_setting('SYNC_BEFORE_EXECUTE', 'true')).lower() == 'true':
            sync_positions_with_exchange(ex)
    except Exception:
        pass

    # … (aucune modification dans les vérifications précédentes) …

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

            # Recalcule taille avec SL ajusté
            qty_adj = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, entry_price, sl)
            quantity = min(quantity, qty_adj)
            if quantity <= 0:
                return False, "Rejeté: quantité recalculée à 0."

            # --- 🔒 CAP marge anti-40762 (avec ticker si besoin pour ref_price) ---
            ref_price = price_ref
            if not ref_price:
                try:
                    t = ex.fetch_ticker(symbol) or {}
                    ref_price = float(t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0)
                except Exception:
                    ref_price = price_ref

            capped_qty, meta = _cap_qty_for_margin_and_filters(ex, symbol, side, abs(float(quantity)), ref_price or price_ref)

            # Insuffisant après cap → on annule proprement (notif déjà claire ici)
            if capped_qty <= 0.0 and meta.get("reason") == "INSUFFICIENT_AFTER_CAP":
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

            # Taille réduite → on informe (transparence)
            if meta.get("reason") == "CAPPED_BY_MARGIN" and capped_qty > 0:
                try:
                    reduced_pct = (1.0 - (capped_qty / max(abs(float(quantity)), 1e-12))) * 100.0
                    notifier.tg_send(
                        f"⚠️ Taille réduite par marge\n"
                        f"• {symbol} {side.upper()} MARKET\n"
                        f"• Demandée: <code>{quantity}</code> → Envoyée: <code>{capped_qty}</code> "
                        f"(-{reduced_pct:.2f}%)\n"
                        f"• Levier: <code>{meta.get('leverage')}</code> | Marge dispo: <code>{(meta.get('available_margin') or 0.0):.2f} USDT</code>"
                    )
                except Exception:
                    pass

            # Recheck notional avec qty cappée
            notional_value = capped_qty * price_ref
            if notional_value < MIN_NOTIONAL_VALUE:
                return False, f"Rejeté: Notional après cap ({notional_value:.2f} USDT) < min requis ({MIN_NOTIONAL_VALUE} USDT)."

            # Arrondi quantité finale
            try:
                quantity = float(ex.amount_to_precision(symbol, capped_qty))
            except Exception:
                quantity = float(capped_qty)
            if quantity <= 0:
                return False, "Rejeté: quantité finale arrondie à 0."

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


def get_tp_offset_pct() -> float:
    """Retourne le pourcentage d'offset (ex: 0.0015 = 0.15%) pour TP/SL depuis la DB,
    clampé pour garantir que le TP se place AVANT la borne (jamais 0)."""
    try:
        v = float(database.get_setting('TP_BB_OFFSET_PCT', 0.0015))
    except Exception:
        v = 0.0015
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


def manage_open_positions(ex: ccxt.Exchange):
    """Gère les positions ouvertes : SPLIT (50% + BE), BE auto en NORMALE/CT, trailing après BE,
    et TP purement dynamique (on ignore tout TP manuel). Le SL manuel (même > BE) peut servir de
    base de trailing si FOLLOW_MANUAL_SL_WITH_TRAILING = true.
    Tous les ordres sont MARKET reduceOnly (triggers) + sécurisation leverage/modes."""
    _ensure_bitget_mix_options(ex)
    if database.get_setting('PAPER_TRADING_MODE', 'true') == 'true':
        return

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

    # Carte des positions réellement ouvertes côté exchange (pour auto-close DB si disparu)
    try:
        symbols = list({p['symbol'] for p in open_positions})
        ex_pos_list = _fetch_positions_safe(ex, symbols)
        live_map = {}
        for p in (ex_pos_list or []):
            try:
                sym = p.get('symbol') or (p.get('info', {}) or {}).get('symbol')
                contracts = float(p.get('contracts') or p.get('positionAmt') or 0.0)
                if sym:
                    live_map[sym] = live_map.get(sym, 0.0) + max(0.0, contracts)
            except Exception:
                continue
    except Exception:
        live_map = {}

    for pos in open_positions:
        symbol = pos['symbol']

        # Auto-close DB si la position n'existe plus côté exchange
        try:
            real_qty = float(live_map.get(symbol, 0.0))
            if real_qty <= 0:
                try:
                    database.close_trade(pos['id'], status='CLOSED_BY_EXCHANGE', pnl=0.0)
                    notifier.tg_send(f"✅ Fermeture auto (exchange) détectée sur {symbol} — Trade #{pos['id']} clôturé en DB.")
                except Exception:
                    pass
                continue
        except Exception:
            pass

        is_long = (pos['side'] == 'buy')
        close_side = 'sell' if is_long else 'buy'
        common_params = {'reduceOnly': True, 'tdMode': 'cross', 'posMode': 'oneway'}

        # Sécurise leverage/modes autour du symbole
        try:
            ex.set_leverage(LEVERAGE, symbol)
            try:
                ex.set_margin_mode('cross', symbol)
            except Exception:
                pass
            try:
                ex.set_position_mode(False, symbol)  # oneway
            except Exception:
                pass
        except Exception:
            pass

        # ---------- Préférences manuel/trailing ----------
        try:
            FOLLOW_MANUAL_SL_WITH_TRAILING = str(database.get_setting('FOLLOW_MANUAL_SL_WITH_TRAILING', 'true')).lower() == 'true'
        except Exception:
            FOLLOW_MANUAL_SL_WITH_TRAILING = True

        # tick_size pour comparaisons fines
        try:
            market = ex.market(symbol) or {}
            tick_size = _bitget_tick_size(market)
        except Exception:
            tick_size = 0.0001

        # Aligne la DB sur l'exchange si SL manuel (TP manuel ignoré, TP reste dynamique)
        manual = _apply_manual_override_if_needed(ex, pos, tick_size)
        skip_sl_updates = bool(manual.get('sl_changed') and (not FOLLOW_MANUAL_SL_WITH_TRAILING))

        # ---------- Mode SPLIT : close 50% + BE sur MM20 ----------
        if pos['management_strategy'] == 'SPLIT' and pos.get('breakeven_status') == 'PENDING':
            try:
                ticker = ex.fetch_ticker(symbol) or {}
                current_price = float(ticker.get('last') or ticker.get('close') or pos['entry_price'])
                df = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
                if df is None or len(df) == 0:
                    continue

                management_trigger_price = float(df.iloc[-1]['bb20_mid'])

                if (is_long and current_price >= management_trigger_price) or ((not is_long) and current_price <= management_trigger_price):
                    qty_to_close = float(pos['quantity']) / 2.0
                    try:
                        qty_to_close = float(ex.amount_to_precision(symbol, qty_to_close))
                    except Exception:
                        pass
                    if qty_to_close <= 0:
                        continue
                    remaining_qty = max(0.0, float(pos['quantity']) - qty_to_close)

                    # Prise de profits partielle
                    ex.create_market_order(symbol, close_side, qty_to_close, params=common_params)

                    # Repose SL/TP sur le restant à BE
                    try:
                        ex.cancel_all_orders(symbol)
                    except Exception as e:
                        # 22001: rien à annuler
                        if '22001' not in str(e):
                            raise

                    be_side = 'long' if is_long else 'short'
                    new_sl_be = compute_fee_safe_be_price(
                        entry=float(pos['entry_price']),
                        side=be_side,
                        qty=float(remaining_qty),
                        fee_in_pct=FEE_ENTRY_PCT,
                        fee_out_pct=FEE_EXIT_PCT,
                        buffer_pct=BE_BUFFER_PCT,
                        buffer_usdt=BE_BUFFER_USDT
                    )
                    try:
                        new_sl_be = float(ex.price_to_precision(symbol, new_sl_be))
                    except Exception:
                        pass

                    if remaining_qty > 0:
                        ex.create_order(
                            symbol, 'market', close_side, remaining_qty, price=None,
                            params={**common_params, 'stopLossPrice': float(new_sl_be), 'triggerType': 'mark'}
                        )
                        # On repose le TP existant en DB (sera amélioré dynamiquement plus bas)
                        ex.create_order(
                            symbol, 'market', close_side, remaining_qty, price=None,
                            params={**common_params, 'takeProfitPrice': float(pos['tp_price']), 'triggerType': 'mark'}
                        )

                    pnl_realised = (current_price - pos['entry_price']) * qty_to_close if is_long else (pos['entry_price'] - current_price) * qty_to_close
                    database.update_trade_to_breakeven(pos['id'], remaining_qty, new_sl_be)
                    notifier.send_breakeven_notification(symbol, pnl_realised, remaining_qty)

            except Exception as e:
                print(f"Erreur de gestion SPLIT pour {symbol}: {e}")

        # ---------- Mode NORMAL + Contre-tendance : BE sur MM20 ----------
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
                            if '22001' not in str(e):
                                raise

                        be_side = 'long' if is_long else 'short'
                        qty = float(pos['quantity'])
                        try:
                            qty = float(ex.amount_to_precision(symbol, qty))
                        except Exception:
                            pass
                        if qty <= 0:
                            continue

                        new_sl_be = compute_fee_safe_be_price(
                            entry=float(pos['entry_price']),
                            side=be_side,
                            qty=qty,
                            fee_in_pct=FEE_ENTRY_PCT,
                            fee_out_pct=FEE_EXIT_PCT,
                            buffer_pct=BE_BUFFER_PCT,
                            buffer_usdt=BE_BUFFER_USDT
                        )
                        try:
                            new_sl_be = float(ex.price_to_precision(symbol, new_sl_be))
                        except Exception:
                            pass

                        ex.create_order(
                            symbol, 'market', close_side, qty, price=None,
                            params={**common_params, 'stopLossPrice': new_sl_be, 'triggerType': 'mark'}
                        )
                        ex.create_order(
                            symbol, 'market', close_side, qty, price=None,
                            params={**common_params, 'takeProfitPrice': float(pos['tp_price']), 'triggerType': 'mark'}
                        )

                        database.update_trade_to_breakeven(pos['id'], qty, new_sl_be)
                        notifier.send_breakeven_notification(symbol, 0.0, qty)

            except Exception as e:
                print(f"Erreur BE NORMAL contre-tendance {symbol}: {e}")

        # ---------- Trailing après BE (ACTIVE/DONE/BE) ----------
        if pos.get('breakeven_status') in ('ACTIVE', 'DONE', 'BE'):
            try:
                try:
                    move_min_pct = float(database.get_setting('TRAIL_MOVE_MIN_PCT', 0.0002))
                except Exception:
                    move_min_pct = 0.0002
                try:
                    atr_k = float(database.get_setting('TRAIL_ATR_K', 1.3))
                except Exception:
                    atr_k = 1.3

                ticker = ex.fetch_ticker(symbol) or {}
                last_price = float(ticker.get('last') or ticker.get('close') or pos['entry_price'])

                df_live = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
                if df_live is None or len(df_live) == 0:
                    continue
                last_row = df_live.iloc[-1]
                bb20_mid = float(last_row['bb20_mid'])
                atr_live = float(last_row.get('atr', 0.0))

                be_price = float(pos.get('sl_price') or pos['entry_price'])
                current_sl = float(pos.get('sl_price') or pos['entry_price'])

                # Candidat trailing : serrer uniquement
                if is_long:
                    target_atr = last_price - atr_live * atr_k
                    candidate = max(bb20_mid, target_atr, be_price, current_sl)
                    moved = candidate > current_sl and (current_sl <= 0 or (candidate - current_sl) / max(current_sl, 1e-12) >= move_min_pct)
                else:
                    target_atr = last_price + atr_live * atr_k
                    candidate = min(bb20_mid, target_atr, be_price, current_sl)
                    moved = candidate < current_sl and (current_sl <= 0 or (current_sl - candidate) / max(current_sl, 1e-12) >= move_min_pct)

                if not moved or skip_sl_updates:
                    # soit pas de mouvement utile, soit SL manuel gelé (si FOLLOW_MANUAL_SL... = false)
                    pass
                else:
                    try:
                        qty = float(pos['quantity'])
                        try:
                            qty = float(ex.amount_to_precision(symbol, qty))
                        except Exception:
                            pass
                        if qty > 0:
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
                        print(f"Erreur trailing {symbol}: {e}")

            except Exception as e:
                print(f"Erreur trailing {symbol}: {e}")

        # ---------- TP dynamique (toujours actif) ----------
        try:
            df = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
            if df is not None and len(df) > 0:
                last = df.iloc[-1]
                regime = pos.get('regime', 'Tendance')

                bb20_mid = float(last['bb20_mid'])
                bb80_up = float(last['bb80_up'])
                bb80_lo = float(last['bb80_lo'])
                last_atr = float(last.get('atr', 0.0))

                offset_pct = get_tp_offset_pct()
                try:
                    tp_atr_k = float(database.get_setting('TP_ATR_K', 0.50))
                except Exception:
                    tp_atr_k = 0.50

                # Référence: Tendance → BB80 opposée ; CT → BB20 mid
                ref = bb80_up if (regime == 'Tendance' and is_long) else (bb80_lo if regime == 'Tendance' else bb20_mid)
                eff_pct = max(offset_pct, (tp_atr_k * last_atr) / ref if ref > 0 else offset_pct)
                target_tp = (ref * (1.0 - eff_pct)) if is_long else (ref * (1.0 + eff_pct))

                current_tp = float(pos['tp_price'])
                improve = (is_long and target_tp > current_tp * 1.0002) or ((not is_long) and target_tp < current_tp * 0.9998)
                if not improve:
                    continue

                # Prépare SL de sécurité vs prix courant
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
                if is_long:
                    if sl_price >= last_px:
                        sl_price = last_px * (1.0 - gap_pct)
                else:
                    if sl_price <= last_px:
                        sl_price = last_px * (1.0 + gap_pct)

                # ✅ Valide TP selon règles Bitget (short < prix courant ; long > prix courant)
                try:
                    side_for_tp = ('buy' if is_long else 'sell')
                    target_tp = _prepare_validated_tp(ex, symbol, side_for_tp, float(target_tp))
                except Exception:
                    # fallback minime si la validation échoue
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
                if qty <= 0:
                    continue

                # Replace propre SL/TP (reduceOnly mark)
                ex.create_order(
                    symbol, 'market', close_side, qty, price=None,
                    params={**common_params, 'stopLossPrice': float(sl_price), 'triggerType': 'mark'}
                )
                ex.create_order(
                    symbol, 'market', close_side, qty, price=None,
                    params={**common_params, 'takeProfitPrice': float(target_tp), 'triggerType': 'mark'}
                )

                try:
                    database.update_trade_tp(pos['id'], float(target_tp))
                except Exception:
                    pass

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
    _ensure_bitget_mix_options(ex)
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

