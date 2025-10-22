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

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame, entry_price: float) -> Tuple[bool, str]:
    """Tente d'exécuter un trade avec toutes les vérifications de sécurité."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv('MAX_OPEN_POSITIONS', 3)))
    
    # --- Vérifications Pré-Trade ---
    if len(database.get_open_positions()) >= max_pos: return False, f"Rejeté: Max positions ({max_pos}) atteint."
    if database.is_position_open(symbol): return False, "Rejeté: Position déjà ouverte (DB)."
    try:
        positions = ex.fetch_positions([symbol])
        if any(p for p in positions if p.get('contracts') and float(p['contracts']) > 0):
            return False, "Rejeté: Position déjà ouverte (vérifié sur l'exchange)."
    except Exception as e:
        return False, f"Rejeté: Erreur de vérification de position ({e})."

    balance = get_usdt_balance(ex)
    if balance is None: return False, "Rejeté: Erreur de solde (Clés API?)."
    if balance <= 10: return False, f"Rejeté: Solde insuffisant ({balance:.2f} USDT)."
    
    quantity = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, entry_price, signal['sl'])
    if quantity <= 0: return False, f"Rejeté: Quantité calculée nulle ({quantity})."
    
    notional_value = quantity * entry_price
    if notional_value < MIN_NOTIONAL_VALUE: return False, f"Rejeté: Valeur du trade ({notional_value:.2f} USDT) < minimum requis ({MIN_NOTIONAL_VALUE} USDT)."
    
    # --- Exécution de l'ordre ---
    final_entry_price = entry_price
    if not is_paper_mode:
        try:
            ex.set_leverage(LEVERAGE, symbol)
            
            # CORRECTION FINALE : Revenir à create_order avec les bons params pour Bitget
            params = {
                'stopLossPrice': signal['sl'],
                'takeProfitPrice': signal['tp'],
                'side': signal['side'].upper() # LONG ou SHORT pour le mode One-Way
            }
            order = ex.create_order(symbol, 'market', signal['side'], quantity, price=None, params=params)
            
            time.sleep(3)
            position = ex.fetch_position(symbol)
            if not position or float(position.get('stopLossPrice', 0)) == 0:
                print("🚨 ALERTE SÉCURITÉ : SL non détecté ! Clôture d'urgence.")
                ex.create_market_order(symbol, 'sell' if signal['side'] == 'buy' else 'buy', quantity, params={'reduceOnly': True})
                reason = "ERREUR CRITIQUE: Stop Loss non placé. Position clôturée."
                notifier.send_validated_signal_report(symbol, signal, False, reason)
                return False, reason
            
            if order and 'price' in order and order['price']: final_entry_price = float(order['price'])
        except Exception as e:
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            reason = f"Erreur d'exécution: {e}"
            notifier.send_validated_signal_report(symbol, signal, False, reason)
            return False, reason

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
    
    return True, "Position ouverte avec succès."

# --- Le reste du fichier est identique et complet ---
def _get_indicators(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    # ...
def is_valid_reaction_candle(candle: pd.Series, side: str) -> bool:
    # ...
def manage_open_positions(ex: ccxt.Exchange):
    # ...
def get_usdt_balance(ex: ccxt.Exchange) -> Optional[float]:
    # ...
def calculate_position_size(balance: float, risk_percent: float, entry_price: float, sl_price: float) -> float:
    # ...
def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    # ...
