# Fichier: trader.py
import os
import time
import ccxt
import pandas as pd
from ta.volatility import BollingerBands
from typing import Dict, Any, Optional

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

# ==============================================================================
# CORRECTION DE LA KEYERROR
# ==============================================================================
def _get_indicators(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Calcule et ajoute tous les indicateurs techniques nécessaires au DataFrame."""
    if df is None or len(df) < 81:
        return None
    
    # Calcul des Bandes de Bollinger 20 périodes
    bb_20 = BollingerBands(close=df['close'], window=20, window_dev=2)
    df['bb20_up'] = bb_20.bollinger_hband()
    df['bb20_mid'] = bb_20.bollinger_mavg()
    df['bb20_lo'] = bb_20.bollinger_lband()
    
    # Calcul COMPLET des Bandes de Bollinger 80 périodes
    bb_80 = BollingerBands(close=df['close'], window=80, window_dev=2)
    df['bb80_up'] = bb_80.bollinger_hband()   # <-- LIGNE MANQUANTE
    df['bb80_mid'] = bb_80.bollinger_mavg()
    df['bb80_lo'] = bb_80.bollinger_lband()   # <-- LIGNE MANQUANTE
    
    return df
# ==============================================================================

def manage_open_positions(ex: ccxt.Exchange):
    """Gère les positions ouvertes selon leur stratégie de gestion (NORMAL ou SPLIT)."""
    if PAPER_TRADING_MODE:
        return

    open_positions = database.get_open_positions()
    if not open_positions:
        return

    for pos in open_positions:
        df = _get_indicators(fetch_ohlcv_df(ex, pos['symbol'], TIMEFRAME))
        if df is None:
            continue
        
        last_indicators = df.iloc[-1]
        is_long = pos['side'] == 'buy'

        # --- GESTION DE LA STRATÉGIE "SPLIT" (Breakeven) ---
        if pos['management_strategy'] == 'SPLIT' and pos['breakeven_status'] == 'PENDING':
            try:
                current_price = ex.fetch_ticker(pos['symbol'])['last']
                management_trigger_price = last_indicators['bb20_mid']

                if (is_long and current_price >= management_trigger_price) or \
                   (not is_long and current_price <= management_trigger_price):
                    
                    print(f"Gestion SPLIT: Déclencheur MM20 atteint pour {pos['symbol']}!")
                    qty_to_close = pos['quantity'] / 2
                    remaining_qty = pos['quantity'] - qty_to_close

                    ex.create_market_order(pos['symbol'], 'sell' if is_long else 'buy', qty_to_close)
                    
                    pnl_realised = (current_price - pos['entry_price']) * qty_to_close if is_long else (pos['entry_price'] - current_price) * qty_to_close
                    new_sl_be = pos['entry_price']
                    
                    params = {
                        'symbol': pos['symbol'],
                        'stopLossPrice': f"{new_sl_be:.5f}",
                        'takeProfitPrice': f"{pos['tp_price']:.5f}"
                    }
                    ex.private_post_mix_position_modify_position(params)
                    
                    database.update_trade_to_breakeven(pos['id'], remaining_qty, new_sl_be)
                    notifier.send_breakeven_notification(pos['symbol'], pnl_realised, remaining_qty)
            
            except Exception as e:
                print(f"Erreur de gestion SPLIT pour {pos['symbol']}: {e}")
                notifier.tg_send_error(f"Gestion SPLIT {pos['symbol']}", e)

        # --- GESTION DU TAKE PROFIT DYNAMIQUE ---
        else:
            new_dynamic_tp = last_indicators['bb80_mid'] if pos['regime'] == 'Tendance' else \
                             last_indicators['bb20_up'] if is_long else last_indicators['bb20_lo']

            if new_dynamic_tp and (abs(new_dynamic_tp - pos['tp_price']) / pos['tp_price']) * 100 >= TP_UPDATE_THRESHOLD_PERCENT:
                try:
                    print(f"Gestion Dynamique: Mise à jour du TP pour {pos['symbol']} -> {new_dynamic_tp:.5f}")
                    params = {
                        'symbol': pos['symbol'],
                        'takeProfitPrice': f"{new_dynamic_tp:.5f}",
                        'stopLossPrice': f"{pos['sl_price']:.5f}"
                    }
                    ex.private_post_mix_position_modify_position(params)
                    database.update_trade_tp(pos['id'], new_dynamic_tp)
                except Exception as e:
                    print(f"Erreur de mise à jour TP (Dynamique) pour {pos['symbol']}: {e}")

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame):
    """Valide et exécute un trade après détection d'un signal."""
    if len(database.get_open_positions()) >= int(os.getenv('MAX_OPEN_POSITIONS', 3)) or \
       database.is_position_open(symbol):
        return

    current_strategy_mode = database.get_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper())
    
    management_strategy = "NORMAL"
    if current_strategy_mode == 'SPLIT' and signal['regime'] == 'Contre-tendance':
        management_strategy = "SPLIT"
    
    risk = RISK_PER_TRADE_PERCENT
    balance = get_usdt_balance(ex)
    if balance <= 10: return
    
    quantity = calculate_position_size(balance, risk, signal['entry'], signal['sl'])
    if quantity <= 0: return

    mode_text = "PAPIER" if PAPER_TRADING_MODE else "RÉEL"
    trade_message = notifier.format_trade_message(symbol, signal, quantity, mode_text, risk)
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
        symbol=symbol, side=signal['side'], regime=signal['regime'],
        entry_price=signal['entry'], sl_price=signal['sl'], tp_price=signal['tp'],
        quantity=quantity, risk_percent=risk, open_timestamp=int(time.time()),
        bb20_mid_at_entry=signal.get('bb20_mid'),
        management_strategy=management_strategy
    )

def get_usdt_balance(ex: ccxt.Exchange) -> float:
    """Récupère le solde USDT total du compte."""
    try:
        balance = ex.fetch_balance(params={'type': 'swap', 'code': 'USDT'})
        return float(balance['total'].get('USDT', 0.0))
    except Exception as e:
        notifier.tg_send_error("Récupération du solde", e)
        return 0.0

def calculate_position_size(balance: float, risk_percent: float, entry_price: float, sl_price: float) -> float:
    """Calcule la quantité d'actifs à trader en fonction du risque."""
    if balance <= 0 or entry_price == sl_price: return 0.0
    risk_amount_usdt = balance * (risk_percent / 100.0)
    price_diff_per_unit = abs(entry_price - sl_price)
    return risk_amount_usdt / price_diff_per_unit if price_diff_per_unit > 0 else 0.0

def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    """Clôture manuellement une position ouverte via une commande Telegram."""
    trade = database.get_trade_by_id(trade_id)
    if not trade or trade.get('status') != 'OPEN':
        return notifier.tg_send(f"Trade #{trade_id} déjà fermé ou invalide.")
    
    try:
        if not PAPER_TRADING_MODE:
            close_side = 'sell' if trade['side'] == 'buy' else 'buy'
            ex.create_market_order(trade['symbol'], close_side, trade['quantity'], params={'reduceOnly': True})
        
        database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
        notifier.tg_send(f"✅ Position sur {trade['symbol']} (Trade #{trade_id}) fermée manuellement.")
    except Exception as e:
        notifier.tg_send_error(f"Fermeture manuelle de {trade['symbol']}", e)
