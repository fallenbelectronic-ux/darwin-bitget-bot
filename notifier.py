# Fichier: trader.py
import os, time, ccxt, pandas as pd, database, notifier, charting
from ta.volatility import BollingerBands
from typing import Dict, Any, Optional, Tuple
from utils import fetch_ohlcv_df

# --- PARAMÈTRES ---
PAPER_TRADING_MODE = os.getenv("PAPER_TRADING_MODE", "true").lower() in ("1", "true", "yes")
RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_PER_TRADE_PERCENT", "1.0"))
LEVERAGE, TIMEFRAME = int(os.getenv("LEVERAGE", "2")), os.getenv("TIMEFRAME", "1h")
TP_UPDATE_THRESHOLD_PERCENT = 0.05

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame) -> Tuple[bool, str]:
    """Tente d'exécuter un trade et retourne un statut (succès/échec) et un message."""
    
    # MODIFICATION : Lit le paramètre depuis la base de données
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv('MAX_OPEN_POSITIONS', 3)))
    
    if len(database.get_open_positions()) >= max_pos:
        return False, f"Rejeté: Nombre maximum de positions ({max_pos}) atteint."

    if database.is_position_open(symbol):
        return False, "Rejeté: Une position est déjà ouverte sur ce symbole."

    current_strategy_mode = database.get_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper())
    management_strategy = "NORMAL"
    if current_strategy_mode == 'SPLIT' and signal['regime'] == 'Contre-tendance':
        management_strategy = "SPLIT"
    
    balance = get_usdt_balance(ex)
    if balance is None:
        return False, "Rejeté: Impossible de récupérer le solde (Clés API?)."
    if balance <= 10:
        return False, f"Rejeté: Solde insuffisant ({balance:.2f} USDT)."
    
    quantity = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, signal['entry'], signal['sl'])
    if quantity <= 0:
        return False, f"Rejeté: Quantité calculée nulle ({quantity})."
    
    if not PAPER_TRADING_MODE:
        try:
            ex.set_leverage(LEVERAGE, symbol)
            params = {'stopLoss': {'triggerPrice': signal['sl']}, 'takeProfit': {'triggerPrice': signal['tp']}}
            ex.create_market_order(symbol, signal['side'], quantity, params=params)
        except Exception as e:
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            return False, f"Erreur d'exécution: {e}"

    database.create_trade(symbol, signal['side'], signal['regime'], signal['entry'], signal['sl'], signal['tp'], quantity, RISK_PER_TRADE_PERCENT, int(time.time()), signal.get('bb20_mid'), management_strategy)
    
    chart_image = charting.generate_trade_chart(symbol, df, signal)
    mode_text = "PAPIER" if PAPER_TRADING_MODE else "RÉEL"
    trade_message = notifier.format_trade_message(symbol, signal, quantity, mode_text, RISK_PER_TRADE_PERCENT)
    notifier.tg_send_with_photo(photo_buffer=chart_image, caption=trade_message)
    
    return True, "Position ouverte avec succès."

# --- Le reste du fichier est identique ---
def _get_indicators(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    # ...
def manage_open_positions(ex: ccxt.Exchange):
    # ...
def get_usdt_balance(ex: ccxt.Exchange) -> Optional[float]:
    # ...
def calculate_position_size(balance: float, risk_percent: float, entry_price: float, sl_price: float) -> float:
    # ...
def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    # ...
