# Fichier: trader.py
import os
import time
import ccxt
import pandas as pd
from ta.volatility import BollingerBands, AverageTrueRange
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

def _get_indicators(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Calcule et ajoute tous les indicateurs techniques nécessaires."""
    if df is None or len(df) < 81:
        return None
    
    bb_20 = BollingerBands(close=df['close'], window=20, window_dev=2)
    df['bb20_up'], df['bb20_mid'], df['bb20_lo'] = bb_20.bollinger_hband(), bb_20.bollinger_mavg(), bb_20.bollinger_lband()
    
    bb_80 = BollingerBands(close=df['close'], window=80, window_dev=2)
    df['bb80_up'], df['bb80_mid'], df['bb80_lo'] = bb_80.bollinger_hband(), bb_80.bollinger_mavg(), bb_80.bollinger_lband()
    
    df['atr'] = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14).average_true_range()
    return df

def is_valid_reaction_candle(candle: pd.Series, side: str) -> bool:
    """Vérifie si la bougie de réaction est une bougie de décision valide."""
    body = abs(candle['close'] - candle['open'])
    if body == 0: return False
    
    wick_high = candle['high'] - max(candle['open'], candle['close'])
    wick_low = min(candle['open'], candle['close']) - candle['low']
    total_size = candle['high'] - candle['low']

    if body < total_size * 0.15:
        print(f"  -> INFO: Réaction ignorée (Doji / Corps trop petit).")
        return False

    if side == 'buy':
        if candle['close'] <= candle['open']:
            print(f"  -> INFO: Réaction d'achat ignorée (bougie rouge).")
            return False
        if wick_high > body * 2.0:
            print(f"  -> INFO: Réaction d'achat ignorée (Mèche haute trop grande).")
            return False
            
    if side == 'sell':
        if candle['close'] >= candle['open']:
            print(f"  -> INFO: Réaction de vente ignorée (bougie verte).")
            return False
        if wick_low > body * 2.0:
            print(f"  -> INFO: Réaction de vente ignorée (Mèche basse trop grande).")
            return False
            
    return True

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame, entry_price: float):
    """Tente d'exécuter un trade et envoie les notifications appropriées."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv('MAX_OPEN_POSITIONS', 3)))
    
    rejection_reason = None
    if len(database.get_open_positions()) >= max_pos: rejection_reason = f"Rejeté: Max positions ({max_pos}) atteint."
    elif database.is_position_open(symbol): rejection_reason = "Rejeté: Position déjà ouverte (DB)."
    else:
        try:
            positions = ex.fetch_positions([symbol])
            if any(float(p.get('contracts', 0)) > 0 for p in positions):
                rejection_reason = "Rejeté: Position déjà ouverte (Exchange)."
        except Exception as e:
            rejection_reason = f"Rejeté: Erreur de vérification de position ({e})."

    if rejection_reason:
        notifier.send_validated_signal_report(symbol, signal, False, rejection_reason)
        return

    balance = get_usdt_balance(ex)
    if balance is None: 
        notifier.send_validated_signal_report(symbol, signal, False, "Rejeté: Erreur de solde (Clés API?).")
        return
    if balance <= 10: 
        notifier.send_validated_signal_report(symbol, signal, False, f"Rejeté: Solde insuffisant ({balance:.2f} USDT).")
        return
    
    quantity = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, entry_price, signal['sl'])
    if quantity <= 0:
        notifier.send_validated_signal_report(symbol, signal, False, f"Rejeté: Quantité calculée nulle ({quantity}).")
        return
        
    notional_value = quantity * entry_price
    if notional_value < MIN_NOTIONAL_VALUE:
        notifier.send_validated_signal_report(symbol, signal, False, f"Rejeté: Valeur du trade ({notional_value:.2f} USDT) < minimum requis ({MIN_NOTIONAL_VALUE} USDT).")
        return
        
    final_entry_price = entry_price
    if not is_paper_mode:
        try:
            ex.set_leverage(LEVERAGE, symbol)
            params = {'stopLoss': {'triggerPrice': signal['sl']}, 'takeProfit': {'triggerPrice': signal['tp']}}
            order = ex.create_order(symbol, 'market', signal['side'], quantity, params=params)
            
            time.sleep(3)
            position = ex.fetch_position(symbol)
            if not position or float(position.get('stopLossPrice', 0)) == 0:
                print("🚨 ALERTE SÉCURITÉ : SL non détecté ! Clôture d'urgence.")
                ex.create_market_order(symbol, 'sell' if signal['side'] == 'buy' else 'buy', quantity, params={'reduceOnly': True})
                notifier.send_validated_signal_report(symbol, signal, False, "ERREUR CRITIQUE: Stop Loss non placé. Position clôturée.")
                return
            
            if order and 'price' in order and order['price']: final_entry_price = float(order['price'])
        except Exception as e:
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            notifier.send_validated_signal_report(symbol, signal, False, f"Erreur d'exécution: {e}")
            return

    signal['entry'] = final_entry_price
    
    current_strategy_mode = database.get_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper())
    management_strategy = "NORMAL"
    if current_strategy_mode == 'SPLIT' and signal['regime'] == 'Contre-tendance':
        management_strategy = "SPLIT"
        
    database.create_trade(symbol, signal['side'], signal['regime'], final_entry_price, signal['sl'], signal['tp'], quantity, RISK_PER_TRADE_PERCENT, int(time.time()), signal.get('bb20_mid'), management_strategy)
    
    notifier.send_validated_signal_report(symbol, signal, True, "Position ouverte avec succès.")
    
    chart_image = charting.generate_trade_chart(symbol, df, signal)
    mode_text = "PAPIER" if is_paper_mode else "RÉEL"
    trade_message = notifier.format_trade_message(symbol, signal, quantity, mode_text, RISK_PER_TRADE_PERCENT)
    notifier.tg_send_with_photo(photo_buffer=chart_image, caption=trade_message, chat_id=notifier.TG_ALERTS_CHAT_ID or notifier.TG_CHAT_ID)

def manage_open_positions(ex: ccxt.Exchange):
    """Gère les positions ouvertes selon leur stratégie."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    if is_paper_mode:
        return

    for pos in database.get_open_positions():
        df = _get_indicators(fetch_ohlcv_df(ex, pos['symbol'], TIMEFRAME))
        if df is None: continue
        
        last_indicators = df.iloc[-1]; is_long = pos['side'] == 'buy'

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
        else:
            new_dynamic_tp = last_indicators['bb80_mid'] if pos['regime'] == 'Tendance' else \
                             last_indicators['bb20_up'] if is_long else last_indicators['bb20_lo']
            if new_dynamic_tp and (abs(new_dynamic_tp - pos['tp_price']) / pos['tp_price']) * 100 >= TP_UPDATE_THRESHOLD_PERCENT:
                try:
                    print(f"Gestion Dynamique: Mise à jour du TP pour {pos['symbol']} -> {new_dynamic_tp:.5f}")
                    params = {'symbol': pos['symbol'], 'takeProfitPrice': f"{new_dynamic_tp:.5f}", 'stopLossPrice': f"{pos['sl_price']:.5f}"}
                    ex.private_post_mix_position_modify_position(params)
                    database.update_trade_tp(pos['id'], new_dynamic_tp)
                except Exception as e:
                    print(f"Erreur de mise à jour TP (Dynamique) pour {pos['symbol']}: {e}")

def get_usdt_balance(ex: ccxt.Exchange) -> Optional[float]:
    """Récupère le solde USDT. Retourne None en cas d'erreur."""
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
        
        database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
        notifier.tg_send(f"✅ Position sur {trade['symbol']} (Trade #{trade_id}) fermée manuellement.")
    except Exception as e:
        notifier.tg_send_error(f"Fermeture manuelle de {trade['symbol']}", e)
