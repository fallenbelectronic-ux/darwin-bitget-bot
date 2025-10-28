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

def is_valid_reaction_candle(candle: pd.Series, side: str) -> bool:
    """Validation stricte de la bougie de réaction (patterns + seuils).
    Règles (paramétrables via DB) :
      - Doji rejeté : body/range < 10%  (DOJI_BODY_MAX)
      - Pinbar : body/range ≤ 30%, mèche côté signal ≥ 30%, mèche opposée ≤ 20%
      - Impulsion/Marubozu relatif : body/range ≥ 30%
      - Mèche côté signal « énorme » rejetée si > 70% (WICK_HUGE_MAX)
      - Couleur non bloquante si la réintégration est respectée (gérée ailleurs)
    """
    try:
        doji_max              = float(database.get_setting('DOJI_BODY_MAX', 0.10))
        pinbar_max_body       = float(database.get_setting('PINBAR_MAX_BODY', 0.30))
        simple_wick_min       = float(database.get_setting('SIMPLE_WICK_MIN', 0.30))
        pinbar_opp_wick_max   = float(database.get_setting('PINBAR_OPP_WICK_MAX', 0.20))
        marubozu_min_body     = float(database.get_setting('MARUBOZU_MIN_BODY', 0.30))
        wick_huge_max         = float(database.get_setting('WICK_HUGE_MAX', 0.70))
    except Exception:
        doji_max, pinbar_max_body, simple_wick_min = 0.10, 0.30, 0.30
        pinbar_opp_wick_max, marubozu_min_body, wick_huge_max = 0.20, 0.30, 0.70

    o, c = float(candle['open']), float(candle['close'])
    h, l = float(candle['high']), float(candle['low'])
    rng = max(1e-12, h - l)
    body = abs(c - o)
    w_up = max(0.0, h - max(o, c))
    w_dn = max(0.0, min(o, c) - l)

    body_r = body / rng
    w_up_r = w_up / rng
    w_dn_r = w_dn / rng

    # Rejet doji / micro-corps
    if body_r < doji_max:
        return False

    # Mèche côté signal « énorme » -> rejet
    if side == 'buy' and w_dn_r > wick_huge_max:
        return False
    if side == 'sell' and w_up_r > wick_huge_max:
        return False

    # Pinbar (bicolore toléré)
    if body_r <= pinbar_max_body:
        if side == 'buy':
            if w_dn_r >= simple_wick_min and w_up_r <= pinbar_opp_wick_max:
                return True
        else:
            if w_up_r >= simple_wick_min and w_dn_r <= pinbar_opp_wick_max:
                return True

    # Impulsion / marubozu relatif (grand corps)
    if body_r >= marubozu_min_body:
        return True

    return False


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
    """Logique de détection avec fenêtre contact→réaction (≤2), réintégration stricte CT (BB20 & BB80),
    tolérance contact BB80 (jaune), neutralité MM80 et anti-excès BB80."""
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
    ct_reintegrate_both = str(database.get_setting('CT_REINTEGRATE_BOTH_BB', 'true')).lower() == 'true'

    # --- Anti-excès : ignorer la CONTRE-TENDANCE après ≥ N bougies au-delà de la BB80 ---
    skip_threshold = int(database.get_setting('SKIP_AFTER_BB80_STREAK', 5))
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

    # --- Chercher un couple (contact, réaction) dans la fenêtre autorisée ---
    last = df.iloc[-1]  # réaction candidate par défaut
    contact = None
    contact_idx = None

    for back in range(1, reaction_max_bars + 1):  # 1 ou 2 barres avant
        cand = df.iloc[-1 - back]
        # contact BB20 côté pertinent (touche ou traverse)
        touched_lo = utils.touched_or_crossed(cand['low'], cand['high'], cand['bb20_lo'], "buy")
        touched_up = utils.touched_or_crossed(cand['low'], cand['high'], cand['bb20_up'], "sell")
        if touched_lo or touched_up:
            contact = cand
            contact_idx = len(df) - 1 - back
            break

    if contact is None:
        return None  # pas de contact récent

    # Si contact est à -2, vérifier qu'il n'y a pas un nouvel excès sur la barre intermédiaire
    if contact_idx == len(df) - 3:
        mid = df.iloc[-2]
        # nouvel excès = close au-delà d'une borne jaune dans le même sens
        if float(contact['close']) > float(contact['open']):  # contact vert (probable long)
            if float(mid['close']) < float(mid['bb80_lo']):
                pass  # ok (pas un excès contre le long)
        if float(contact['close']) < float(contact['open']):  # contact rouge (probable short)
            if float(mid['close']) > float(mid['bb80_up']):
                pass  # ok
        # sinon, on ne bloque pas—tolérance légère

    # --- Déterminer le sens supposé via la réaction ---
    side_guess = 'buy' if float(last['close']) > float(last['open']) else 'sell'

    # --- Filtre MM80 neutralité ---
    dead_zone = float(last['bb80_mid']) * (MM_DEAD_ZONE_PERCENT / 100.0)
    if abs(float(last['close']) - float(last['bb80_mid'])) < dead_zone:
        return None

    # --- Réintégrations (toujours à la réaction = last) ---
    # Tendance : close inside BB20
    inside_bb20 = _inside(float(last['close']), float(last['bb20_lo']), float(last['bb20_up']))
    # CT : close inside BB20 ET inside BB80 (si activé)
    inside_bb80 = _inside(float(last['close']), float(last['bb80_lo']), float(last['bb80_up']))
    ct_reintegration_ok = inside_bb20 and (inside_bb80 if ct_reintegrate_both else True)

    # --- Valider la bougie de réaction (patterns) ---
    if not is_valid_reaction_candle(last, side_guess):
        return None

    signal = None

    # --- Détection des Patterns (avec contact choisi) ---
    is_above_mm80 = float(last['close']) > float(last['bb80_mid'])
    # contact côté BB20
    touched_bb20_low  = utils.touched_or_crossed(contact['low'], contact['high'], contact['bb20_lo'], "buy")
    touched_bb20_high = utils.touched_or_crossed(contact['low'], contact['high'], contact['bb20_up'], "sell")

    # --- TENDANCE (extrême->réintégration BB20) ---
    if is_above_mm80 and touched_bb20_low and inside_bb20:
        regime = "Tendance"
        entry = float(last['close'])
        sl = float(_anchor_sl_from_extreme(df, 'buy'))
        prev = contact
        tp = float(last['bb80_up']) - max(
            0.12 * float(prev.get('atr', 0.0)),
            0.12 * max(float(last['bb80_up']) - float(last.get('bb80_mid', last['close'])), 0.0)
        )
        if tp <= entry:
            tp = float(last.get('bb20_up', tp))
        if tp <= entry:
            return None
        if (entry - sl) > 0:
            rr = (tp - entry) / (entry - sl)
            rr_final = rr
            if rr < MIN_RR and rr >= 2.8:
                rr_alt, _ = _maybe_improve_rr_with_cut_wick(prev, entry, sl, tp, 'buy')
                rr_final = max(rr, rr_alt)
            if rr_final >= MIN_RR:
                signal = {"side": "buy", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr_final}

    elif (not is_above_mm80) and touched_bb20_high and inside_bb20:
        regime = "Tendance"
        entry = float(last['close'])
        sl = float(_anchor_sl_from_extreme(df, 'sell'))
        prev = contact
        tp = float(last['bb80_lo']) + max(
            0.12 * float(prev.get('atr', 0.0)),
            0.12 * max(float(last.get('bb80_mid', last['close'])) - float(last['bb80_lo']), 0.0)
        )
        if tp >= entry:
            tp = float(last.get('bb20_lo', tp))
        if tp >= entry:
            return None
        if (sl - entry) > 0:
            rr = (entry - tp) / (sl - entry)
            rr_final = rr
            if rr < MIN_RR and rr >= 2.8:
                rr_alt, _ = _maybe_improve_rr_with_cut_wick(prev, entry, sl, tp, 'sell')
                rr_final = max(rr, rr_alt)
            if rr_final >= MIN_RR:
                signal = {"side": "sell", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr_final}

    # --- Garde-fou : si excès prolongé, on saute la contre-tendance ---
    if not allow_countertrend:
        if signal:
            signal['bb20_mid'] = last['bb20_mid']
            signal['entry_atr'] = contact.get('atr', 0.0)
            signal['entry_rsi'] = 0.0
            return signal
        return None

    # --- CONTRE-TENDANCE (double extrême + réintégration stricte) ---
    if not signal:
        # contact avec les 2 bandes (BB20 & BB80) côté pertinent,
        # en acceptant une tolérance de contact sur la jaune (BB80)
        if True:
            prev = contact
            # Long CT : low <= bb20_lo ET (low <= bb80_lo OU tolérance)
            touched_ct_low  = (float(prev['low']) <= float(prev['bb20_lo'])) and (
                float(prev['low']) <= float(prev['bb80_lo']) or _touched_with_tol(float(prev['low']), float(prev['bb80_lo']), 'buy', tol_yellow)
            )
            # Short CT : high >= bb20_up ET (high >= bb80_up OU tolérance)
            touched_ct_high = (float(prev['high']) >= float(prev['bb20_up'])) and (
                float(prev['high']) >= float(prev['bb80_up']) or _touched_with_tol(float(prev['high']), float(prev['bb80_up']), 'sell', tol_yellow)
            )

            if touched_ct_low and ct_reintegration_ok:
                regime = "Contre-tendance"
                entry = float(last['close'])
                sl = float(_anchor_sl_from_extreme(df, 'buy'))
                tp = float(last['bb20_mid']) - max(
                    0.12 * float(prev.get('atr', 0.0)),
                    0.12 * max(float(last.get('bb20_up', float(last['bb20_mid']))) - float(last['bb20_mid']), 0.0)
                )
                if tp <= entry:
                    tp = float(last.get('bb20_up', tp))
                if tp <= entry:
                    return None
                if (entry - sl) > 0:
                    rr = (tp - entry) / (entry - sl)
                    rr_final = rr
                    if rr < MIN_RR and rr >= 2.8:
                        rr_alt, _ = _maybe_improve_rr_with_cut_wick(prev, entry, sl, tp, 'buy')
                        rr_final = max(rr, rr_alt)
                    if rr_final >= MIN_RR:
                        signal = {"side": "buy", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr_final}

            elif touched_ct_high and ct_reintegration_ok:
                regime = "Contre-tendance"
                entry = float(last['close'])
                sl = float(_anchor_sl_from_extreme(df, 'sell'))
                tp = float(last['bb20_mid']) + max(
                    0.12 * float(prev.get('atr', 0.0)),
                    0.12 * max(float(last['bb20_mid']) - float(last.get('bb20_lo', float(last['bb20_mid']))), 0.0)
                )
                if tp >= entry:
                    tp = float(last.get('bb20_lo', tp))
                if tp >= entry:
                    return None
                if (sl - entry) > 0:
                    rr = (entry - tp) / (sl - entry)
                    rr_final = rr
                    if rr < MIN_RR and rr >= 2.8:
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
def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame, entry_price: float) -> Tuple[bool, str]:
    """Tente d'exécuter un trade avec toutes les vérifications de sécurité."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv('MAX_OPEN_POSITIONS', 3)))

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
            ex.set_leverage(LEVERAGE, symbol)

            # Garde-fou SL/TP vs prix d'entrée (Bitget: long -> SL < entry < TP ; short -> TP < entry < SL)
            gap_pct = float(database.get_setting('SL_MIN_GAP_PCT', 0.0003))  # 0.03% par défaut
            price_ref = float(entry_price)
            side = signal['side']

            sl = float(signal['sl'])
            tp = float(signal['tp'])

            if side == 'sell':  # SHORT
                if sl <= price_ref:
                    sl = price_ref * (1.0 + gap_pct)
                if tp >= price_ref:
                    tp = price_ref * (1.0 - gap_pct)
            else:  # BUY (LONG)
                if sl >= price_ref:
                    sl = price_ref * (1.0 - gap_pct)
                if tp <= price_ref:
                    tp = price_ref * (1.0 + gap_pct)

            # Ajuster à la précision de l'exchange
            try:
                sl = float(ex.price_to_precision(symbol, sl))
                tp = float(ex.price_to_precision(symbol, tp))
            except Exception:
                pass

            signal['sl'] = sl
            signal['tp'] = tp

            # recalcul prudent de la taille sur le SL ajusté (sans augmenter le risque prévu)
            qty_adj = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, entry_price, sl)
            quantity = min(quantity, qty_adj)
            
            # Recheck notional après ajustement de la quantité
            notional_value = quantity * entry_price
            if notional_value < MIN_NOTIONAL_VALUE:
                return False, f"Rejeté: Valeur du trade ({notional_value:.2f} USDT) < min requis ({MIN_NOTIONAL_VALUE} USDT)."

            # 1) Entrée au marché (AUCUN stopLossPrice / takeProfitPrice ici)
            order = ex.create_market_order(symbol, signal['side'], quantity)

            # 2) Créer les 2 ordres reduceOnly SÉPARÉS (Bitget n'accepte qu'UNE clé par appel)
            close_side = 'sell' if signal['side'] == 'buy' else 'buy'

            # SL
            ex.create_order(
                symbol, 'limit', close_side, quantity, price=None,
                params={'stopLossPrice': sl, 'reduceOnly': True}
            )
            # TP
            ex.create_order(
                symbol, 'limit', close_side, quantity, price=None,
                params={'takeProfitPrice': tp, 'reduceOnly': True}
            )

            if order and order.get('price'):
                final_entry_price = float(order['price'])

        except Exception as e:
            # Tentative de clôture d'urgence si l'un des ordres a échoué
            try:
                close_side = 'sell' if signal['side'] == 'buy' else 'buy'
                ex.create_market_order(symbol, close_side, quantity, params={'reduceOnly': True})
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
    """Gère les positions ouvertes : SPLIT (50% + BE), BE auto en NORMALE/Contre-tendance, puis trailing après BE."""
    if database.get_setting('PAPER_TRADING_MODE', 'true') == 'true':
        return

    open_positions = database.get_open_positions()
    if not open_positions:
        return

    for pos in open_positions:
        # --- SPLIT : demi-sortie + passage BE sur franchissement MM20/BB20_mid ---
        if pos['management_strategy'] == 'SPLIT' and pos['breakeven_status'] == 'PENDING':
            try:
                current_price = ex.fetch_ticker(pos['symbol'])['last']
                df = utils.fetch_and_prepare_df(ex, pos['symbol'], TIMEFRAME)
                if df is None or len(df) == 0:
                    continue

                management_trigger_price = df.iloc[-1]['bb20_mid']
                is_long = (pos['side'] == 'buy')

                # Déclencheur atteint ?
                if (is_long and current_price >= management_trigger_price) or (not is_long and current_price <= management_trigger_price):
                    print(f"✅ Gestion SPLIT: Déclencheur MM20 atteint pour {pos['symbol']}!")

                    qty_to_close = pos['quantity'] / 2
                    remaining_qty = pos['quantity'] - qty_to_close
                    close_side = 'sell' if is_long else 'buy'

                    # 1) Clôturer 50% (reduceOnly)
                    ex.create_market_order(pos['symbol'], close_side, qty_to_close, params={'reduceOnly': True})

                    # 2) Passer le reste à BE: annule anciens ordres puis recrée SL et TP séparément
                    ex.cancel_all_orders(pos['symbol'])
                    fees_bps = float(database.get_setting('FEES_BPS', 5))  # 5 bps = 0.05%
                    fee_factor = (1.0 - fees_bps / 10000.0) if is_long else (1.0 + fees_bps / 10000.0)
                    new_sl_be = pos['entry_price'] * fee_factor

                    # SL seul
                    ex.create_order(pos['symbol'], 'limit', close_side, remaining_qty, price=None,
                                    params={'stopLossPrice': new_sl_be, 'reduceOnly': True})
                    # TP inchangé seul
                    ex.create_order(pos['symbol'], 'limit', close_side, remaining_qty, price=None,
                                    params={'takeProfitPrice': pos['tp_price'], 'reduceOnly': True})

                    # 3) DB + notif
                    pnl_realised = (current_price - pos['entry_price']) * qty_to_close if is_long else (pos['entry_price'] - current_price) * qty_to_close
                    database.update_trade_to_breakeven(pos['id'], remaining_qty, new_sl_be)
                    notifier.send_breakeven_notification(pos['symbol'], pnl_realised, remaining_qty)

            except Exception as e:
                print(f"Erreur de gestion SPLIT pour {pos['symbol']}: {e}")

        # --- NORMALE / Contre-tendance : passage BE sur franchissement MM20/BB20_mid ---
        if pos['management_strategy'] == 'NORMAL' and pos.get('regime') == 'Contre-tendance' and pos.get('breakeven_status') == 'PENDING':
            try:
                df = utils.fetch_and_prepare_df(ex, pos['symbol'], TIMEFRAME)
                if df is not None and len(df) > 0:
                    last_close = float(df.iloc[-1]['close'])
                    mm20 = float(df.iloc[-1]['bb20_mid'])
                    is_long = (pos['side'] == 'buy')

                    crossed = (is_long and last_close >= mm20) or ((not is_long) and last_close <= mm20)
                    if crossed:
                        ex.cancel_all_orders(pos['symbol'])
                        fees_bps = float(database.get_setting('FEES_BPS', 5))
                        fee_factor = (1.0 - fees_bps / 10000.0) if is_long else (1.0 + fees_bps / 10000.0)
                        new_sl_be = float(pos['entry_price']) * fee_factor

                        close_side = 'sell' if is_long else 'buy'
                        # SL BE seul
                        ex.create_order(pos['symbol'], 'limit', close_side, pos['quantity'], price=None,
                                        params={'stopLossPrice': new_sl_be, 'reduceOnly': True})
                        # TP inchangé seul
                        ex.create_order(pos['symbol'], 'limit', close_side, pos['quantity'], price=None,
                                        params={'takeProfitPrice': pos['tp_price'], 'reduceOnly': True})

                        database.update_trade_to_breakeven(pos['id'], pos['quantity'], new_sl_be)
                        notifier.send_breakeven_notification(pos['symbol'], 0.0, pos['quantity'])

            except Exception as e:
                print(f"Erreur BE NORMAL contre-tendance {pos['symbol']}: {e}")

        # --- Trailing après BE (NORMAL & SPLIT) en suivant BB20_mid ---
        if pos.get('breakeven_status') in ('ACTIVE', 'DONE', 'BE'):
            try:
                df = utils.fetch_and_prepare_df(ex, pos['symbol'], TIMEFRAME)
                if df is None or len(df) == 0:
                    continue

                # Garde-fou BE dynamique : activer le trailing seulement après K clôtures au-delà du BE, sans réintégration
                try:
                    k_needed = int(database.get_setting('BE_ACTIVATION_K', 2))
                    m_window = int(database.get_setting('BE_NO_REENTRY_M', 3))
                except Exception:
                    k_needed, m_window = 2, 3

                is_long = (pos['side'] == 'buy')
                entry_px = float(pos.get('sl_price') or pos['entry_price'])

                # 1) K clôtures consécutives au-delà du BE
                closes = df['close'].astype(float).iloc[-max(k_needed, 1):]
                if len(closes) < k_needed:
                    continue
                ok_streak = all(c > entry_px for c in closes) if is_long else all(c < entry_px for c in closes)
                if not ok_streak:
                    continue

                # 2) Pas de réintégration côté opposé dans la fenêtre M
                window = df['close'].astype(float).iloc[-max(m_window, 1):]
                reentered = any(c <= entry_px for c in window) if is_long else any(c >= entry_px for c in window)
                if reentered:
                    continue

                trail_ref = float(df.iloc[-1]['bb20_mid'])  # rail de trailing (MM20)
                current_sl = float(pos.get('sl_price') or pos['entry_price'])

                # Pousser le SL seulement dans le bon sens (jamais le reculer)
                new_sl = max(current_sl, trail_ref) if is_long else min(current_sl, trail_ref)

                # Seuil anti-spam (~0.02%)
                moved = (is_long and new_sl > current_sl * 1.0002) or ((not is_long) and new_sl < current_sl * 0.9998)
                if not moved:
                    continue

                # Recharger la quantité restante depuis la DB (après éventuels splits)
                try:
                    pos_ref = database.get_trade_by_id(pos['id'])
                    if pos_ref and float(pos_ref.get('quantity', 0)) > 0:
                        pos['quantity'] = float(pos_ref['quantity'])
                except Exception:
                    pass

                # Remplacer les ordres existants par 2 ordres séparés : SL trailé + TP courant
                ex.cancel_all_orders(pos['symbol'])
                close_side = 'sell' if is_long else 'buy'
                # SL trailé seul
                ex.create_order(pos['symbol'], 'limit', close_side, pos['quantity'], price=None,
                                params={'stopLossPrice': new_sl, 'reduceOnly': True})
                # TP inchangé seul
                ex.create_order(pos['symbol'], 'limit', close_side, pos['quantity'], price=None,
                                params={'takeProfitPrice': pos['tp_price'], 'reduceOnly': True})

                # DB + notif
                try:
                    database.update_trade_sl(pos['id'], new_sl)
                except AttributeError:
                    database.update_trade_to_breakeven(pos['id'], pos['quantity'], new_sl)

                notifier.tg_send(f"🔁 Trailing SL mis à jour sur {pos['symbol']} → {new_sl:.6f}")

            except Exception as e:
                print(f"Erreur trailing {pos['symbol']}: {e}")

        # --- TP dynamique proche des bornes de Bollinger (NORMAL & SPLIT) ---
        try:
            df = utils.fetch_and_prepare_df(ex, pos['symbol'], TIMEFRAME)
            if df is None or len(df) == 0:
                pass
            else:
                last = df.iloc[-1]
                is_long = (pos['side'] == 'buy')
                regime = pos.get('regime', 'Tendance')

                bb20_mid = float(last['bb20_mid'])
                bb80_up  = float(last['bb80_up'])
                bb80_lo  = float(last['bb80_lo'])

                # Décalage avant la borne (ex: 0.15%) configurable
                offset_pct = float(database.get_setting('TP_BB_OFFSET_PCT', 0.0015))

                # Cible “un peu avant” la borne pertinente
                if regime == 'Tendance':
                    target_tp = bb80_up * (1.0 - offset_pct) if is_long else bb80_lo * (1.0 + offset_pct)
                else:  # Contre-tendance
                    target_tp = bb20_mid * (1.0 - offset_pct) if is_long else bb20_mid * (1.0 + offset_pct)

                # N'améliorer que dans le bon sens (jamais réduire le TP)
                current_tp = float(pos['tp_price'])
                improve = (is_long and target_tp > current_tp * 1.0002) or ((not is_long) and target_tp < current_tp * 0.9998)
                if improve:
                    # Ajuster la précision (optionnel)
                    try:
                        target_tp = float(ex.price_to_precision(pos['symbol'], target_tp))
                    except Exception:
                        pass

                    # Recharger quantité restante au cas où (post-split)
                    try:
                        pos_ref = database.get_trade_by_id(pos['id'])
                        if pos_ref and float(pos_ref.get('quantity', 0)) > 0:
                            pos['quantity'] = float(pos_ref['quantity'])
                    except Exception:
                        pass

                    # Remplacer l'existant par 2 ordres : SL courant + nouveau TP
                    ex.cancel_all_orders(pos['symbol'])
                    close_side = 'sell' if is_long else 'buy'
                    sl_price = float(pos.get('sl_price') or pos['entry_price'])
                    # SL (inchangé) seul
                    ex.create_order(pos['symbol'], 'limit', close_side, pos['quantity'], price=None,
                                    params={'stopLossPrice': sl_price, 'reduceOnly': True})
                    # TP (nouveau) seul
                    ex.create_order(pos['symbol'], 'limit', close_side, pos['quantity'], price=None,
                                    params={'takeProfitPrice': target_tp, 'reduceOnly': True})

                    # DB + notif (si update_tp absent, on ignore)
                    try:
                        database.update_trade_tp(pos['id'], target_tp)
                    except Exception:
                        pass
                    notifier.tg_send(f"🎯 TP ajusté dynamiquement sur {pos['symbol']} → {target_tp:.6f}")
        except Exception as e:
            print(f"Erreur TP dynamique {pos['symbol']}: {e}")


def get_usdt_balance(ex: ccxt.Exchange) -> Optional[float]:
    """Récupère le solde USDT."""
    try:
        ex.options['recvWindow'] = 10000
        balance = ex.fetch_balance(params={'type': 'swap', 'code': 'USDT'})
        return float(balance['total'].get('USDT', 0.0))
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
    """Clôture manuellement une position."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    trade = database.get_trade_by_id(trade_id)
    if not trade or trade.get('status') != 'OPEN':
        return notifier.tg_send(f"Trade #{trade_id} déjà fermé ou invalide.")
    
    try:
        if not is_paper_mode:
            ex.create_market_order(trade['symbol'], 'sell' if trade['side'] == 'buy' else 'buy', trade['quantity'], params={'reduceOnly': True})
        
        # Calcule un PNL approximatif, mais l'idéal serait d'avoir le prix de sortie réel
        database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
        notifier.tg_send(f"✅ Position sur {trade['symbol']} (Trade #{trade_id}) fermée manuellement.")
    except Exception as e:
        notifier.tg_send_error(f"Fermeture manuelle de {trade['symbol']}", e)
