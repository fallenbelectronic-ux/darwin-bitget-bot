# Fichier: trader.py
import os, time, ccxt
from typing import Dict, Any
import pandas as pd
import database, notifier, charting

PAPER_TRADING_MODE = os.getenv("PAPER_TRADING_MODE", "true").lower() in ("1", "true", "yes")
RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_PER_TRADE_PERCENT", "1.0"))
LEVERAGE = 2

def get_usdt_balance(ex: ccxt.Exchange) -> float:
    try:
        return float(ex.fetch_balance()['total'].get('USDT', 0.0))
    except Exception as e:
        notifier.tg_send_error("Récupération du solde", e)
        return 0.0

def calculate_position_size(balance: float, risk_percent: float, entry_price: float, sl_price: float) -> float:
    if balance <= 0 or entry_price == sl_price: return 0.0
    risk_amount_usdt = balance * (risk_percent / 100.0)
    price_diff_per_unit = abs(entry_price - sl_price)
    return risk_amount_usdt / price_diff_per_unit if price_diff_per_unit > 0 else 0.0

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame):
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', 3))
    if len(database.get_open_positions()) >= max_pos: return
    if symbol in database.get_setting('BLACKLIST', []): return
    if database.is_position_open(symbol): return

    current_risk = RISK_PER_TRADE_PERCENT
    if database.get_setting('DYNAMIC_RISK_ENABLED', False): pass

    balance = get_usdt_balance(ex)
    if balance <= 10: return
    
    quantity = calculate_position_size(balance, current_risk, signal['entry'], signal['sl'])
    if quantity <= 0: return

    mode_text = "PAPIER" if PAPER_TRADING_MODE else "RÉEL"
    trade_message = notifier.format_trade_message(symbol, signal, quantity, mode_text, current_risk)
    chart_image = charting.generate_trade_chart(symbol, df, signal)

    if not PAPER_TRADING_MODE:
        try:
            ex.set_leverage(LEVERAGE, symbol)
            params = {'stopLoss': {'triggerPrice': signal['sl']}, 'takeProfit': {'triggerPrice': signal['tp']}}
            ex.create_market_order(symbol, signal['side'], quantity, params=params)
        except Exception as e:
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            return

    notifier.tg_send_with_photo(photo_buffer=chart_image, caption=trade_message)
    
    database.create_trade(
        symbol=symbol, side=signal['side'], regime=signal['regime'], status='OPEN',
        entry_price=signal['entry'], sl_price=signal['sl'], tp_price=signal['tp'],
        quantity=quantity, risk_percent=current_risk, open_timestamp=int(time.time()),
        bb20_mid_at_entry=signal.get('bb20_mid')
    )

def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    trade = database.get_trade_by_id(trade_id)
    if not trade or trade.get('status') != 'OPEN':
        return notifier.tg_send(f"Trade #{trade_id} déjà fermé ou invalide.")

    symbol, side, quantity = trade['symbol'], trade['side'], trade['quantity']
    try:
        close_side = 'sell' if side == 'buy' else 'buy'
        ex.create_market_order(symbol, close_side, quantity, params={'reduceOnly': True})
        database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0) # PNL à calculer
        notifier.tg_send(f"✅ Position sur {symbol} fermée manuellement.")
    except Exception as e:
        notifier.tg_send_error(f"Fermeture manuelle de {symbol}", e)
