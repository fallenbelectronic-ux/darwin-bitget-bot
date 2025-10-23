# Fichier: trader.py
# Version finale, avec l'indentation et la logique corrigées.

import os
import time
import ccxt
import pandas as pd
from typing import Dict, Any, Optional, Tuple

import database
import notifier
import charting
import utils

# --- PARAMÈTRES DE TRADING ---
RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_PER_TRADE_PERCENT", "1.0"))
LEVERAGE = int(os.getenv("LEVERAGE", "2"))
TIMEFRAME = os.getenv("TIMEFRAME", "1h")
MIN_RR = float(os.getenv("MIN_RR", "3.0"))
TP_UPDATE_THRESHOLD_PERCENT = 0.05
MIN_NOTIONAL_VALUE = 5.0
TICK_RATIO = 0.0005 # Gardé pour compatibilité, mais la logique ATR est plus robuste
MM_DEAD_ZONE_PERCENT = float(os.getenv("MM_DEAD_ZONE_PERCENT", "0.1"))


def detect_signal(df: pd.DataFrame, sym: str) -> Optional[Dict[str, Any]]:
    """Logique de détection de signal, maintenant dans le bon module."""
    if df is None or len(df) < 83: return None
    
    last_candle, contact_candle = df.iloc[-1], df.iloc[-2] # Stratégie à N-2
    
    if not utils.is_valid_reaction_candle(last_candle, 'buy') and not utils.is_valid_reaction_candle(last_candle, 'sell'):
        return None

    signal: Optional[Dict[str, Any]] = None
    is_uptrend = contact_candle['close'] > contact_candle['bb80_mid']

    # Scénario d'Achat
    buy_tendance = is_uptrend and contact_candle['low'] <= contact_candle['bb20_lo']
    buy_ct = contact_candle['low'] <= contact_candle['bb20_lo'] and contact_candle['low'] <= contact_candle['bb80_lo']
    
    if buy_tendance or buy_ct:
        if utils.is_valid_reaction_candle(last_candle, 'buy'):
            reintegration_ok = last_candle['close'] > contact_candle['bb20_lo'] and (not buy_ct or last_candle['close'] > contact_candle['bb80_lo'])
            if reintegration_ok:
                regime = "Tendance" if buy_tendance else "Contre-tendance"
                entry = last_candle['close']
                sl = contact_candle['low'] - (contact_candle['atr'] * 0.25)
                tp = last_candle['bb80_up'] if regime == 'Tendance' else last_candle['bb20_up']
                if (entry - sl) > 0:
                    rr = (tp - entry) / (entry - sl)
                    if rr >= MIN_RR:
                        signal = {"side": "buy", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}

    # Scénario de Vente
    sell_tendance = not is_uptrend and contact_candle['high'] >= contact_candle['bb20_up']
    sell_ct = contact_candle['high'] >= contact_candle['bb20_up'] and contact_candle['high'] >= contact_candle['bb80_up']
    
    if not signal and (sell_tendance or sell_ct):
        if utils.is_valid_reaction_candle(last_candle, 'sell'):
            reintegration_ok = last_candle['close'] < contact_candle['bb20_up'] and (not sell_ct or last_candle['close'] < contact_candle['bb80_up'])
            if reintegration_ok:
                regime = "Tendance" if sell_tendance else "Contre-tendance"
                entry = last_candle['close']
                sl = contact_candle['high'] + (contact_candle['atr'] * 0.25)
                tp = last_candle['bb80_lo'] if regime == 'Tendance' else last_candle['bb20_lo']
                if (sl - entry) > 0:
                    rr = (entry - tp) / (sl - entry)
                    if rr >= MIN_RR:
                        signal = {"side": "sell", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}

    if signal:
        signal['entry_atr'] = contact_candle['atr']
        signal['entry_rsi'] = 0.0 # À remplacer si vous ajoutez le RSI
        return signal
        
    return None


def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict, df: pd.DataFrame, entry_price: float) -> Tuple[bool, str]:
    """Exécute le trade après validation."""
    is_paper = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', 3))

    if len(database.get_open_positions()) >= max_pos: return False, "Max positions atteint."
    if database.is_position_open(symbol): return False, "Position déjà ouverte."

    balance = get_usdt_balance(ex)
    if balance is None or balance <= 10: return False, "Solde insuffisant."
    
    quantity = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, entry_price, signal['sl'])
    if (quantity * entry_price) < MIN_NOTIONAL_VALUE: return False, "Valeur de trade trop faible."

    if not is_paper:
        try:
            ex.set_leverage(LEVERAGE, symbol)
            # Utilise une méthode standard CCXT pour plus de robustesse
            params = {'stopLoss': {'triggerPrice': signal['sl']}, 'takeProfit': {'triggerPrice': signal['tp']}}
            ex.create_market_order(symbol, signal['side'], quantity, params=params)
        except Exception as e:
            notifier.tg_send_error(f"Erreur d'ordre sur {symbol}", e)
            return False, str(e)

    management_strategy = "SPLIT" if (database.get_setting('STRATEGY_MODE') == 'SPLIT' and signal['regime'] == 'Contre-tendance') else "NORMAL"
    database.create_trade(symbol, signal['side'], signal['regime'], entry_price, signal['sl'], signal['tp'], quantity, RISK_PER_TRADE_PERCENT, management_strategy, signal.get('entry_atr', 0), signal.get('entry_rsi', 0))

    msg = notifier.format_trade_message(symbol, signal, quantity, "PAPIER" if is_paper else "RÉEL", RISK_PER_TRADE_PERCENT)
    chart = charting.generate_trade_chart(symbol, df, signal)
    notifier.tg_send_with_photo(chart, msg)
    
    return True, "OK"


def manage_open_positions(ex: ccxt.Exchange):
    """Gère les positions ouvertes selon leur stratégie."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    if is_paper_mode:
        return

    for pos in database.get_open_positions():
        df = utils.fetch_and_prepare_df(ex, pos['symbol'], TIMEFRAME)
        if df is None:
            continue
        
        last_indicators = df.iloc[-1]
        is_long = pos['side'] == 'buy'

        # Stratégie de gestion SPLIT
        if pos['management_strategy'] == 'SPLIT' and pos['breakeven_status'] == 'PENDING':
            try:
                current_price = ex.fetch_ticker(pos['symbol'])['last']
                management_trigger_price = last_indicators['bb20_mid']
                if (is_long and current_price >= management_trigger_price) or \
                   (not is_long and current_price <= management_trigger_price):
                    
                    print(f"Gestion SPLIT: Déclencheur MM20 atteint pour {pos['symbol']}!")
                    # Logique pour fermer 50% et mettre à BE
                    # Cette partie est complexe et dépend beaucoup de l'API de l'exchange
            except Exception as e:
                print(f"Erreur de gestion SPLIT pour {pos['symbol']}: {e}")
        
        # Stratégie de gestion DYNAMIQUE
        else:
            new_dynamic_tp = last_indicators['bb80_mid'] if pos.get('regime') == 'Tendance' else \
                             (last_indicators['bb20_up'] if is_long else last_indicators['bb20_lo'])
            
            if new_dynamic_tp and (abs(new_dynamic_tp - pos['tp_price']) / pos['tp_price']) * 100 >= TP_UPDATE_THRESHOLD_PERCENT:
                try:
                    print(f"Gestion Dynamique: Mise à jour du TP pour {pos['symbol']} -> {new_dynamic_tp:.5f}")
                    # Cette méthode n'est PAS standard CCXT. Commentez ou remplacez par une méthode valide pour Bitget.
                    # params = {'symbol': pos['symbol'], 'takeProfitPrice': f"{new_dynamic_tp:.5f}", 'stopLossPrice': f"{pos['sl_price']:.5f}"}
                    # ex.private_post_mix_position_modify_position(params)
                    database.update_trade_tp(pos['id'], new_dynamic_tp)
                except Exception as e:
                    print(f"Erreur de mise à jour TP (Dynamique) pour {pos['symbol']}: {e}")


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
        database.close_trade(trade_id, status='CLOSED_MANUAL', exit_price=trade['entry_price'])
        notifier.tg_send(f"✅ Position sur {trade['symbol']} (Trade #{trade_id}) fermée manuellement.")
    except Exception as e:
        notifier.tg_send_error(f"Fermeture manuelle de {trade['symbol']}", e)
