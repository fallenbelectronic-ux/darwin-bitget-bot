# Fichier: trader.py
import os
import time
import ccxt
import pandas as pd
from ta.volatility import BollingerBands, AverageTrueRange
from ta.momentum import RSIIndicator
from typing import Dict, Any, Optional, Tuple

import database
import notifier
import charting
from utils import fetch_ohlcv_df

# --- PARAMÈTRES ---
RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_PER_TRADE_PERCENT", "1.0"))
LEVERAGE = int(os.getenv("LEVERAGE", "2"))
TIMEFRAME = os.getenv("TIMEFRAME", "1h")
TP_UPDATE_THRESHOLD_PERCENT = 0.05
MIN_NOTIONAL_VALUE = 5.0
MIN_RR = float(os.getenv("MIN_RR", "3.0"))
ATR_TRAILING_MULTIPLIER = 2.0

def _get_indicators(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Calcule et ajoute tous les indicateurs techniques nécessaires."""
    if df is None or len(df) < 81: return None
    bb_20 = BollingerBands(close=df['close'], window=20, window_dev=2); df['bb20_up'], df['bb20_mid'], df['bb20_lo'] = bb_20.bollinger_hband(), bb_20.bollinger_mavg(), bb_20.bollinger_lband()
    bb_80 = BollingerBands(close=df['close'], window=80, window_dev=2); df['bb80_up'], df['bb80_mid'], df['bb80_lo'] = bb_80.bollinger_hband(), bb_80.bollinger_mavg(), bb_80.bollinger_lband()
    df['atr'] = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14).average_true_range()
    df['rsi'] = RSIIndicator(close=df['close'], window=14).rsi()
    return df

def is_valid_reaction_candle(candle: pd.Series, side: str) -> bool:
    """Vérifie si la bougie de réaction est une bougie de décision valide."""
    body = abs(candle['close'] - candle['open'])
    if body == 0: return False
    wick_high = candle['high'] - max(candle['open'], candle['close']); wick_low = min(candle['open'], candle['close']) - candle['low']; total_size = candle['high'] - candle['low']
    if body < total_size * 0.15: return False
    if side == 'buy':
        if candle['close'] <= candle['open']: return False
        if wick_high > body * 2.0: return False
    if side == 'sell':
        if candle['close'] >= candle['open']: return False
        if wick_low > body * 2.0: return False
    return True

def detect_signal(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Logique de détection de signal qui analyse les 2 bougies précédentes pour un contact."""
    if df is None or len(df) < 83: return None
    df_with_indicators = _get_indicators(df.copy())
    if df_with_indicators is None: return None
    
    last_candle = df_with_indicators.iloc[-1]
    
    for i in range(2, 4):
        contact_candle = df_with_indicators.iloc[-i]
        signal = None
        
        is_uptrend = contact_candle['close'] > contact_candle['bb80_mid']
        is_downtrend = contact_candle['close'] < contact_candle['bb80_mid']

        # Achat
        buy_tendance = is_uptrend and contact_candle['low'] <= contact_candle['bb20_lo']
        buy_ct = (contact_candle['low'] <= contact_candle['bb20_lo'] and contact_candle['low'] <= contact_candle['bb80_lo'])
        
        if buy_tendance or buy_ct:
            if is_valid_reaction_candle(last_candle, 'buy'):
                reintegration_ok = last_candle['close'] > contact_candle['bb20_lo']
                if buy_ct: reintegration_ok = reintegration_ok and last_candle['close'] > contact_candle['bb80_lo']
                if reintegration_ok:
                    regime = "Tendance" if buy_tendance else "Contre-tendance"
                    entry = (last_candle['open'] + last_candle['close']) / 2
                    sl = contact_candle['low'] - (contact_candle['atr'] * 0.25)
                    tp = last_candle['bb80_up'] if regime == 'Tendance' else last_candle['bb20_up']
                    rr = (tp - entry) / (entry - sl) if (entry - sl) > 0 else 0
                    if rr >= MIN_RR: signal = {"side": "buy", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}

        # Vente
        sell_tendance = is_downtrend and contact_candle['high'] >= contact_candle['bb20_up']
        sell_ct = (contact_candle['high'] >= contact_candle['bb20_up'] and contact_candle['high'] >= contact_candle['bb80_up'])

        if not signal and (sell_tendance or sell_ct):
            if is_valid_reaction_candle(last_candle, 'sell'):
                reintegration_ok = last_candle['close'] < contact_candle['bb20_up']
                if sell_ct: reintegration_ok = reintegration_ok and last_candle['close'] < contact_candle['bb80_up']
                if reintegration_ok:
                    regime = "Tendance" if sell_tendance else "Contre-tendance"
                    entry = (last_candle['open'] + last_candle['close']) / 2
                    sl = contact_candle['high'] + (contact_candle['atr'] * 0.25)
                    tp = last_candle['bb80_lo'] if regime == 'Tendance' else last_candle['bb20_lo']
                    rr = (entry - tp) / (sl - entry) if (sl - entry) > 0 else 0
                    if rr >= MIN_RR: signal = {"side": "sell", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}

        if signal:
            signal['bb20_mid'] = last_candle['bb20_mid']
            signal['entry_atr'] = contact_candle['atr']
            signal['entry_rsi'] = contact_candle['rsi']
            return signal
    return None

def close_position_programmatically(ex: ccxt.Exchange, trade: Dict, reason: str, exit_price: Optional[float] = None):
    """Clôture une position pour une raison spécifique."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    
    print(f"Clôture programmée du trade #{trade['id']} sur {trade['symbol']}. Raison: {reason}")
    if exit_price is None:
        try:
            exit_price = ex.fetch_ticker(trade['symbol'])['last']
        except Exception as e:
            print(f"Impossible de récupérer le prix de sortie pour {trade['symbol']}: {e}")
            return

    if not is_paper_mode:
        try:
            ex.create_market_order(trade['symbol'], 'sell' if trade['side'] == 'buy' else 'buy', trade['quantity'], params={'reduceOnly': True})
        except Exception as e:
            notifier.tg_send_error(f"Clôture programmée {trade['symbol']}", e)
            return
    
    database.close_trade(trade['id'], status=f'CLOSED_{reason.upper()}', exit_price=exit_price)
    notifier.send_programmatic_closure_notification(trade['symbol'], trade['side'], reason, exit_price)

def manage_open_positions(ex: ccxt.Exchange):
    """Gère les positions ouvertes, incluant la clôture sur signal inverse et le Trailing Stop."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    
    for pos in database.get_open_positions():
        df = fetch_ohlcv_df(ex, pos['symbol'], TIMEFRAME)
        if df is None or len(df) < 83: continue
        
        signal = detect_signal(pos['symbol'], df)
        if signal:
            is_long_position = pos['side'] == 'buy'
            is_short_signal = signal['side'] == 'sell'
            if (is_long_position and is_short_signal) or (not is_long_position and not is_short_signal):
                close_position_programmatically(ex, pos, "REVERSE_SIGNAL")
                continue

        if is_paper_mode: continue
        
        last_indicators = _get_indicators(df).iloc[-1]
        is_long = pos['side'] == 'buy'

        if pos['management_strategy'] == 'SPLIT' and pos['breakeven_status'] == 'PENDING':
            try:
                current_price = ex.fetch_ticker(pos['symbol'])['last']
                management_trigger_price = last_indicators['bb20_mid']
                if (is_long and current_price >= management_trigger_price) or (not is_long and current_price <= management_trigger_price):
                    print(f"Gestion SPLIT: Déclencheur MM20 atteint pour {pos['symbol']}!")
                    qty_to_close, remaining_qty = pos['quantity'] / 2, pos['quantity'] / 2
                    ex.create_market_order(pos['symbol'], 'sell' if is_long else 'buy', qty_to_close)
                    pnl_realised = (current_price - pos['entry_price']) * qty_to_close if is_long else (pos['entry_price'] - current_price) * qty_to_close
                    new_sl_be = pos['entry_price']
                    params = {'symbol': pos['symbol'], 'stopLossPrice': f"{new_sl_be:.5f}", 'takeProfitPrice': f"{pos['tp_price']:.5f}"}
                    ex.private_post_mix_position_modify_position(params)
                    database.update_trade_to_breakeven(pos['id'], remaining_qty, new_sl_be)
                    notifier.send_breakeven_notification(pos['symbol'], pnl_realised, remaining_qty)
            except Exception as e:
                print(f"Erreur de gestion SPLIT pour {pos['symbol']}: {e}")
                notifier.tg_send_error(f"Gestion SPLIT {pos['symbol']}", e)
        
        elif pos['regime'] == 'Tendance': # Trailing Stop ATR
            new_trailing_sl = 0
            current_price = last_indicators['close']
            if is_long:
                new_trailing_sl = current_price - (last_indicators['atr'] * ATR_TRAILING_MULTIPLIER)
                if new_trailing_sl > pos['sl_price']:
                    try:
                        print(f"Trailing SL pour {pos['symbol']}: {pos['sl_price']:.5f} -> {new_trailing_sl:.5f}")
                        params = {'symbol': pos['symbol'], 'stopLossPrice': f"{new_trailing_sl:.5f}"}
                        ex.private_post_mix_position_modify_position(params)
                        database.update_trade_sl(pos['id'], new_trailing_sl)
                    except Exception as e: print(f"Erreur Trailing SL pour {pos['symbol']}: {e}")
            else: # Short
                new_trailing_sl = current_price + (last_indicators['atr'] * ATR_TRAILING_MULTIPLIER)
                if new_trailing_sl < pos['sl_price']:
                    try:
                        print(f"Trailing SL pour {pos['symbol']}: {pos['sl_price']:.5f} -> {new_trailing_sl:.5f}")
                        params = {'symbol': pos['symbol'], 'stopLossPrice': f"{new_trailing_sl:.5f}"}
                        ex.private_post_mix_position_modify_position(params)
                        database.update_trade_sl(pos['id'], new_trailing_sl)
                    except Exception as e: print(f"Erreur Trailing SL pour {pos['symbol']}: {e}")
        else: # TP Dynamique standard
            new_dynamic_tp = last_indicators['bb20_up'] if is_long else last_indicators['bb20_lo']
            if new_dynamic_tp and (abs(new_dynamic_tp - pos['tp_price']) / pos['tp_price']) * 100 >= TP_UPDATE_THRESHOLD_PERCENT:
                try:
                    print(f"Gestion Dynamique: Mise à jour du TP pour {pos['symbol']} -> {new_dynamic_tp:.5f}")
                    params = {'symbol': pos['symbol'], 'takeProfitPrice': f"{new_dynamic_tp:.5f}", 'stopLossPrice': f"{pos['sl_price']:.5f}"}
                    ex.private_post_mix_position_modify_position(params)
                    database.update_trade_tp(pos['id'], new_dynamic_tp)
                except Exception as e:
                    print(f"Erreur de mise à jour TP (Dynamique) pour {pos['symbol']}: {e}")

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame, entry_price: float) -> Tuple[bool, str]:
    """Tente d'exécuter un trade avec toutes les vérifications de sécurité."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv('MAX_OPEN_POSITIONS', 3)))
    
    if len(database.get_open_positions()) >= max_pos: return False, f"Rejeté: Max positions ({max_pos}) atteint."
    if database.is_position_open(symbol): return False, "Rejeté: Position déjà ouverte (DB)."
    try:
        positions = ex.fetch_positions([symbol])
        if any(p for p in positions if p.get('contracts') and float(p['contracts']) > 0):
            return False, "Rejeté: Position déjà ouverte (Exchange)."
    except Exception as e:
        return False, f"Rejeté: Erreur de vérification de position ({e})."

    balance = get_usdt_balance(ex)
    if balance is None: return False, "Rejeté: Erreur de solde (Clés API?)."
    if balance <= 10: return False, f"Rejeté: Solde insuffisant ({balance:.2f} USDT)."
    
    quantity = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, entry_price, signal['sl'])
    if quantity <= 0: return False, f"Rejeté: Quantité calculée nulle ({quantity})."
    
    notional_value = quantity * entry_price
    if notional_value < MIN_NOTIONAL_VALUE: return False, f"Rejeté: Valeur du trade ({notional_value:.2f} USDT) < minimum requis ({MIN_NOTIONAL_VALUE} USDT)."
    
    final_entry_price = entry_price
    if not is_paper_mode:
        try:
            ex.set_leverage(LEVERAGE, symbol)
            market = ex.market(symbol)
            params = { 'symbol': market['id'], 'marginCoin': market['quote'], 'side': signal['side'], 'orderType': 'market', 'size': ex.amount_to_precision(symbol, quantity), 'presetTakeProfitPrice': ex.price_to_precision(symbol, signal['tp']), 'presetStopLossPrice': ex.price_to_precision(symbol, signal['sl']), }
            ex.private_post_mix_order_place_order(params)
            time.sleep(3)
            position = ex.fetch_position(symbol)
            if not position or float(position.get('stopLossPrice', 0)) == 0:
                ex.create_market_order(symbol, 'sell' if signal['side'] == 'buy' else 'buy', quantity, params={'reduceOnly': True})
                return False, "ERREUR CRITIQUE: Stop Loss non placé. Position clôturée."
            final_entry_price = entry_price
        except Exception as e:
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            return False, f"Erreur d'exécution: {e}"

    signal['entry'] = final_entry_price
    
    current_strategy_mode = database.get_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper())
    management_strategy = "NORMAL"
    if current_strategy_mode == 'SPLIT' and signal['regime'] == 'Contre-tendance':
        management_strategy = "SPLIT"
        
    database.create_trade(symbol, signal['side'], signal['regime'], final_entry_price, signal['sl'], signal['tp'], quantity, RISK_PER_TRADE_PERCENT, management_strategy, signal['entry_atr'], signal['entry_rsi'])
    
    notifier.send_validated_signal_report(symbol, signal, True, "Position ouverte avec succès.")
    
    chart_image = charting.generate_trade_chart(symbol, df, signal)
    mode_text = "PAPIER" if is_paper_mode else "RÉEL"
    trade_message = notifier.format_trade_message(symbol, signal, quantity, mode_text, RISK_PER_TRADE_PERCENT)
    notifier.tg_send_with_photo(photo_buffer=chart_image, caption=trade_message, chat_id=notifier.TG_ALERTS_CHAT_ID or notifier.TG_CHAT_ID)
    
    return True, "Position ouverte avec succès."

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
    """Calcule la taille de la position."""
    if balance <= 0 or entry_price == sl_price: return 0.0
    risk_amount_usdt = balance * (risk_percent / 100.0)
    price_diff_per_unit = abs(entry_price - sl_price)
    return risk_amount_usdt / price_diff_per_unit if price_diff_per_unit > 0 else 0.0

def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    """Clôture une position manuellement."""
    trade = database.get_trade_by_id(trade_id)
    if not trade or trade.get('status') != 'OPEN': return notifier.tg_send(f"Trade #{trade_id} déjà fermé ou invalide.")
    
    exit_price = ex.fetch_ticker(trade['symbol'])['last']
    close_position_programmatically(ex, trade, "MANUAL", exit_price)
