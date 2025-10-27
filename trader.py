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
    """Analyse la forme de la bougie de réaction pour valider le signal."""
    body = abs(candle['close'] - candle['open'])
    total_range = candle['high'] - candle['low']
    if total_range == 0 or body < total_range * 0.15: # Ignorer les Dojis / corps trop petits
        return False

    wick_high = candle['high'] - max(candle['open'], candle['close'])
    wick_low = min(candle['open'], candle['close']) - candle['low']

    if side == 'buy':
        # Bougie verte, petite mèche haute (pas un "pinbar inversé")
        return candle['close'] > candle['open'] and wick_high < body * 1.5
    
    if side == 'sell':
        # Bougie rouge, petite mèche basse
        return candle['close'] < candle['open'] and wick_low < body * 1.5
        
    return False

def detect_signal(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Logique de détection complète avec les règles avancées."""
    if df is None or len(df) < 81:
        return None
    
    last, prev = df.iloc[-1], df.iloc[-2]  # last=réaction, prev=contact

    # --- Filtre 0: Analyse de la bougie de réaction ---
    side_guess = 'buy' if last['close'] > last['open'] else 'sell'
    if not is_valid_reaction_candle(last, side_guess):
        return None

    # --- Filtre 1: Réintégration BB20 ---
    if not utils.close_inside_bb20(last['close'], last['bb20_lo'], last['bb20_up']):
        return None
    
    # --- Filtre 2: Zone neutre MM80 ---
    dead_zone = float(last['bb80_mid']) * (MM_DEAD_ZONE_PERCENT / 100.0)
    if abs(float(last['close']) - float(last['bb80_mid'])) < dead_zone:
        return None

    # --- Anti-excès : ignorer la CONTRE-TENDANCE après ≥ N bougies au-delà de la BB80 ---
    skip_threshold = int(database.get_setting('SKIP_AFTER_BB80_STREAK', 5))
    lookback = max(skip_threshold, 8)
    allow_countertrend = True
    if len(df) >= lookback:
        recent = df.iloc[-lookback:]

        # Streak haussier (clôtures >= bb80_up consécutives en partant de la plus récente)
        streak_up = 0
        for i in range(len(recent)):
            row = recent.iloc[-1 - i]
            c = float(row['close'])
            b_up = float(row['bb80_up'])
            if c >= b_up:
                streak_up += 1
            else:
                break

        # Streak baissier (clôtures <= bb80_lo consécutives)
        streak_down = 0
        for i in range(len(recent)):
            row = recent.iloc[-1 - i]
            c = float(row['close'])
            b_lo = float(row['bb80_lo'])
            if c <= b_lo:
                streak_down += 1
            else:
                break

        if streak_up >= skip_threshold or streak_down >= skip_threshold:
            allow_countertrend = False

    signal = None
    
    # --- Détection des Patterns ---
    is_above_mm80 = float(last['close']) > float(last['bb80_mid'])
    touched_bb20_low = utils.touched_or_crossed(prev['low'], prev['high'], prev['bb20_lo'], "buy")
    touched_bb20_high = utils.touched_or_crossed(prev['low'], prev['high'], prev['bb20_up'], "sell")

    # Pattern 1: Tendance (Extrême Correction)
    if is_above_mm80 and touched_bb20_low:
        regime = "Tendance"
        entry = float(last['close'])
        sl = float(prev['low']) - (float(prev['atr']) * 0.25)
        tp = float(last['bb80_up']) - max(
            0.25 * float(prev.get('atr', 0.0)),
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

    elif (not is_above_mm80) and touched_bb20_high:
        regime = "Tendance"
        entry = float(last['close'])
        sl = float(prev['high']) + (float(prev['atr']) * 0.25)
        tp = float(last['bb80_lo']) + max(
            0.25 * float(prev.get('atr', 0.0)),
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

    # Garde-fou : si excès prolongé, on saute la contre-tendance (on conserve un éventuel signal de tendance)
    if not allow_countertrend:
        if signal:
            signal['bb20_mid'] = last['bb20_mid']
            signal['entry_atr'] = prev.get('atr', 0.0)
            signal['entry_rsi'] = 0.0
            return signal
        return None

    # Pattern 2: Contre-Tendance (Double Extrême)
    if not signal:
        touched_double_low = float(prev['low']) <= min(float(prev['bb20_lo']), float(prev['bb80_lo']))
        touched_double_high = float(prev['high']) >= max(float(prev['bb20_up']), float(prev['bb80_up']))

        if touched_double_low:
            regime = "Contre-tendance"
            entry = float(last['close'])
            sl = float(prev['low']) - (float(prev['atr']) * 0.25)
            tp = float(last['bb20_mid']) - max(
                0.25 * float(prev.get('atr', 0.0)),
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

        elif touched_double_high:
            regime = "Contre-tendance"
            entry = float(last['close'])
            sl = float(prev['high']) + (float(prev['atr']) * 0.25)
            tp = float(last['bb20_mid']) + max(
                0.25 * float(prev.get('atr', 0.0)),
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
        signal['entry_atr'] = prev.get('atr', 0.0)
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

            # Utiliser ces valeurs corrigées dans params (convention Bitget)
            params = {
                'stopLossPrice': sl,
                'takeProfitPrice': tp
            }

            order = ex.create_market_order(symbol, signal['side'], quantity, params=params)
            
            time.sleep(3)
            position = ex.fetch_position(symbol)
            if not position or float(position.get('stopLossPrice', 0)) == 0:
                print("🚨 ALERTE SÉCURITÉ : SL non détecté ! Clôture d'urgence.")
                ex.create_market_order(symbol, 'sell' if signal['side'] == 'buy' else 'buy', quantity, params={'reduceOnly': True})
                return False, "ERREUR CRITIQUE: Stop Loss non placé. Position clôturée."
            
            if order and order.get('price'):
                final_entry_price = float(order['price'])

        except Exception as e:
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

                    # 2) Passer le reste à BE (annule anciens ordres puis recrée OCO)
                    ex.cancel_all_orders(pos['symbol'])
                    fees_bps = float(database.get_setting('FEES_BPS', 5))  # 5 bps = 0.05%
                    fee_factor = (1.0 - fees_bps / 10000.0) if is_long else (1.0 + fees_bps / 10000.0)
                    new_sl_be = pos['entry_price'] * fee_factor

                    params = {'stopLossPrice': new_sl_be, 'takeProfitPrice': pos['tp_price'], 'reduceOnly': True}
                    ex.create_order(pos['symbol'], 'limit', close_side, remaining_qty, price=None, params=params)

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
                        params = {'stopLossPrice': new_sl_be, 'takeProfitPrice': pos['tp_price'], 'reduceOnly': True}
                        ex.create_order(pos['symbol'], 'limit', close_side, pos['quantity'], price=None, params=params)

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

                trail_ref = float(df.iloc[-1]['bb20_mid'])  # rail de trailing (MM20)
                is_long = (pos['side'] == 'buy')
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

                # Remplacer l’OCO existant par un nouveau avec SL trailé
                ex.cancel_all_orders(pos['symbol'])
                close_side = 'sell' if is_long else 'buy'
                params = {'stopLossPrice': new_sl, 'takeProfitPrice': pos['tp_price'], 'reduceOnly': True}
                ex.create_order(pos['symbol'], 'limit', close_side, pos['quantity'], price=None, params=params)

                # DB + notif
                try:
                    database.update_trade_sl(pos['id'], new_sl)
                except AttributeError:
                    database.update_trade_to_breakeven(pos['id'], pos['quantity'], new_sl)

                notifier.tg_send(f"🔁 Trailing SL mis à jour sur {pos['symbol']} → {new_sl:.6f}")

            except Exception as e:
                print(f"Erreur trailing {pos['symbol']}: {e}")

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
