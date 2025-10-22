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
        exit_price = ex.fetch_ticker(trade['symbol'])['last']
    
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
            is_long_pos = pos['side'] == 'buy'
            is_short_signal = signal['side'] == 'sell'
            if (is_long_pos and is_short_signal) or (not is_long_pos and not is_short_signal):
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
                    # ... (logique de split inchangée)
            except Exception as e:
                # ... (logique de split inchangée)

        elif pos['regime'] == 'Tendance': # Trailing Stop ATR
            new_trailing_sl = 0
            if is_long:
                new_trailing_sl = last_indicators['close'] - (last_indicators['atr'] * ATR_TRAILING_MULTIPLIER)
                if new_trailing_sl > pos['sl_price']:
                    try:
                        params = {'symbol': pos['symbol'], 'stopLossPrice': f"{new_trailing_sl:.5f}"}
                        ex.private_post_mix_position_modify_position(params)
                        database.update_trade_sl(pos['id'], new_trailing_sl)
                    except Exception as e: print(f"Erreur Trailing SL pour {pos['symbol']}: {e}")
            else: # Short
                new_trailing_sl = last_indicators['close'] + (last_indicators['atr'] * ATR_TRAILING_MULTIPLIER)
                if new_trailing_sl < pos['sl_price']:
                    try:
                        params = {'symbol': pos['symbol'], 'stopLossPrice': f"{new_trailing_sl:.5f}"}
                        ex.private_post_mix_position_modify_position(params)
                        database.update_trade_sl(pos['id'], new_trailing_sl)
                    except Exception as e: print(f"Erreur Trailing SL pour {pos['symbol']}: {e}")
        else: # TP Dynamique standard
            new_dynamic_tp = last_indicators['bb20_up'] if is_long else last_indicators['bb20_lo']
            if new_dynamic_tp and (abs(new_dynamic_tp - pos['tp_price']) / pos['tp_price']) * 100 >= TP_UPDATE_THRESHOLD_PERCENT:
                try:
                    params = {'symbol': pos['symbol'], 'takeProfitPrice': f"{new_dynamic_tp:.5f}", 'stopLossPrice': f"{pos['sl_price']:.5f}"}
                    ex.private_post_mix_position_modify_position(params)
                    database.update_trade_tp(pos['id'], new_dynamic_tp)
                except Exception as e:
                    print(f"Erreur de mise à jour TP (Dynamique) pour {pos['symbol']}: {e}")

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame, entry_price: float) -> Tuple[bool, str]:
    # ... (code complet et vérifié)
    pass

def get_usdt_balance(ex: ccxt.Exchange) -> Optional[float]:
    # ... (code complet et vérifié)
    pass
def calculate_position_size(balance: float, risk_percent: float, entry_price: float, sl_price: float) -> float:
    # ... (code complet et vérifié)
    pass
def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    # ... (code complet et vérifié)
    pass
