# Fichier: trader.py
import os
import time
import ccxt
import pandas as pd
from ta.volatility import BollingerBands
from typing import Dict, Any, Optional, Tuple

import database
import notifier
import charting
from utils import fetch_ohlcv_df

# --- PARAMÈTRES ---
PAPER_TRADING_MODE = os.getenv("PAPER_TRADING_MODE", "true").lower() in ("1", "true", "yes")
RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_PER_TRADE_PERCENT", "1.0"))
LEVERAGE = int(os.getenv("LEVERAGE", "2"))
TIMEFRAME = os.getenv("TIMEFRAME", "1h")
TP_UPDATE_THRESHOLD_PERCENT = 0.05

def _get_indicators(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Calcule et ajoute tous les indicateurs techniques nécessaires."""
    if df is None or len(df) < 81:
        return None
    bb_20 = BollingerBands(close=df['close'], window=20, window_dev=2)
    df['bb20_up'] = bb_20.bollinger_hband()
    df['bb20_mid'] = bb_20.bollinger_mavg()
    df['bb20_lo'] = bb_20.bollinger_lband()
    
    bb_80 = BollingerBands(close=df['close'], window=80, window_dev=2)
    df['bb80_up'] = bb_80.bollinger_hband()
    df['bb80_mid'] = bb_80.bollinger_mavg()
    df['bb80_lo'] = bb_80.bollinger_lband()
    
    return df

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame, entry_price: float) -> Tuple[bool, str]:
    """Tente d'exécuter un trade avec un prix d'entrée spécifié."""
    
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv('MAX_OPEN_POSITIONS', 3)))
    if len(database.get_open_positions()) >= max_pos:
        return False, f"Rejeté: Max positions ({max_pos}) atteint."
    if database.is_position_open(symbol):
        return False, "Rejeté: Position déjà ouverte."

    balance = get_usdt_balance(ex)
    if balance is None:
        return False, "Rejeté: Erreur de solde (Clés API?)."
    if balance <= 10:
        return False, f"Rejeté: Solde insuffisant ({balance:.2f} USDT)."
    
    # On recalcule la quantité avec le nouveau prix d'entrée
    quantity = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, entry_price, signal['sl'])
    if quantity <= 0:
        return False, f"Rejeté: Quantité calculée nulle ({quantity})."
    
    final_entry_price = entry_price
    if not PAPER_TRADING_MODE:
        try:
            ex.set_leverage(LEVERAGE, symbol)
            # On passe un ordre au marché, qui sera exécuté proche de `entry_price`
            order = ex.create_market_order(symbol, signal['side'], quantity)
            # On met à jour le prix d'entrée avec le prix réel d'exécution si disponible
            if order and 'price' in order and order['price']:
                final_entry_price = float(order['price'])
        except Exception as e:
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            return False, f"Erreur d'exécution: {e}"

    # On met à jour le signal avec le prix d'entrée final pour l'enregistrement et la notification
    signal['entry'] = final_entry_price
    
    # Déterminer la stratégie de gestion
    current_strategy_mode = database.get_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper())
    management_strategy = "NORMAL"
    if current_strategy_mode == 'SPLIT' and signal['regime'] == 'Contre-tendance':
        management_strategy = "SPLIT"
        
    database.create_trade(symbol, signal['side'], signal['regime'], final_entry_price, signal['sl'], signal['tp'], quantity, RISK_PER_TRADE_PERCENT, int(time.time()), signal.get('bb20_mid'), management_strategy)
    
    chart_image = charting.generate_trade_chart(symbol, df, signal)
    mode_text = "PAPIER" if PAPER_TRADING_MODE else "RÉEL"
    trade_message = notifier.format_trade_message(symbol, signal, quantity, mode_text, RISK_PER_TRADE_PERCENT)
    notifier.tg_send_with_photo(photo_buffer=chart_image, caption=trade_message)
    
    return True, "Position ouverte avec succès."

def manage_open_positions(ex: ccxt.Exchange):
    # ... (le code de cette fonction est correct et ne change pas)
    pass
def get_usdt_balance(ex: ccxt.Exchange) -> Optional[float]:
    # ... (le code de cette fonction est correct et ne change pas)
    pass
def calculate_position_size(balance: float, risk_percent: float, entry_price: float, sl_price: float) -> float:
    # ... (le code de cette fonction est correct et ne change pas)
    pass
def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    # ... (le code de cette fonction est correct et ne change pas)
    pass
