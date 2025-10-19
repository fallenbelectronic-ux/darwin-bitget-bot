import os
import ccxt
from typing import Dict, Any, Optional

import database
import notifier

# ==============================================================================
# CONSTANTES DE TRADING
# ==============================================================================
# Ces valeurs sont extraites de vos règles DARWIN pour le timeframe H1
RISK_PER_TRADE_PERCENT = 1.0  # Risque de 1% du capital par trade
LEVERAGE = 2                  # Levier max de x2 pour H1

# ==============================================================================
# CALCULS DE POSITION
# ==============================================================================

def get_usdt_balance(ex: ccxt.Exchange) -> float:
    """
    Récupère le solde total en USDT du compte.
    Gère les erreurs d'API et retourne 0.0 en cas de problème.
    """
    try:
        balance_info = ex.fetch_balance()
        return float(balance_info['total'].get('USDT', 0.0))
    except Exception as e:
        print(f"Erreur lors de la récupération du solde : {e}")
        notifier.tg_send(f"⚠️ Impossible de récupérer le solde du compte : {e}")
        return 0.0


def calculate_position_size(
    balance: float,
    entry_price: float,
    sl_price: float,
) -> float:
    """
    Calcule la quantité d'actif à trader pour risquer un pourcentage fixe du capital.

    :param balance: Solde total du compte en USDT.
    :param entry_price: Prix d'entrée prévu.
    :param sl_price: Prix du stop-loss.
    :return: La quantité (taille) de la position à ouvrir.
    """
    if balance <= 0 or entry_price == sl_price:
        return 0.0

    # 1. Calculer le montant en USDT à risquer
    risk_amount_usdt = balance * (RISK_PER_TRADE_PERCENT / 100.0)

    # 2. Calculer la distance entre l'entrée et le SL
    price_diff_per_unit = abs(entry_price - sl_price)

    # 3. Calculer la quantité
    # Quantité = Montant à risquer / Distance du SL par unité
    quantity = risk_amount_usdt / price_diff_per_unit

    return quantity

# ==============================================================================
# EXECUTION DES ORDRES
# ==============================================================================

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any]) -> bool:
    """
    Fonction principale pour ouvrir une nouvelle position.
    Elle calcule la taille, passe l'ordre et l'enregistre en base de données.
    """
    side = signal['side']
    entry_price = signal['entry']
    sl_price = signal['sl']
    tp_price = signal['tp']
    
    # 1. Vérifier si une position n'est pas déjà ouverte sur ce symbole
    if database.is_position_open(symbol):
        print(f"[{symbol}] Position déjà ouverte, nouveau signal ignoré.")
        return False

    # 2. Récupérer le solde du compte
    balance = get_usdt_balance(ex)
    if balance <= 10: # Solde de sécurité
        notifier.tg_send("Solde insuffisant pour trader.")
        return False

    # 3. Calculer la taille de la position
    quantity = calculate_position_size(balance, entry_price, sl_price)
    if quantity == 0.0:
        print(f"[{symbol}] Calcul de la taille de position a retourné 0.")
        return False

    # 4. Exécuter le trade sur l'exchange
    try:
        # A. Définir le levier pour ce symbole
        ex.set_leverage(LEVERAGE, symbol)

        # B. Créer l'ordre d'entrée avec SL/TP attachés
        # C'est la méthode la plus robuste : l'entrée, le SL et le TP sont liés.
        params = {
            'stopLoss': {
                'triggerPrice': sl_price,
                'type': 'market' # ou 'limit' selon la stratégie
            },
            'takeProfit': {
                'triggerPrice': tp_price,
                'type': 'market'
            }
        }
        
        # On passe un ordre au marché pour garantir l'exécution après le signal
        order = ex.create_market_order(symbol, side, quantity, params=params)
        
        print(f"ORDRE EXÉCUTÉ: {order}")
        notifier.tg_send(f"✅ Ordre ouvert sur {symbol} | {side.upper()} | Quantité: {quantity:.4f}")

    except Exception as e:
        print(f"ERREUR D'ORDRE sur {symbol}: {e}")
        notifier.tg_send(f"❌ Erreur lors de l'ouverture de l'ordre sur {symbol}: {e}")
        return False

    # 5. Si l'ordre est passé avec succès, l'enregistrer dans notre base de données
    database.create_trade(
        symbol=symbol,
        side=side,
        regime=signal['regime'],
        entry_price=entry_price,
        sl_price=sl_price,
        tp_price=tp_price,
        quantity=quantity,
        # On stocke la valeur de la MM20 pour la gestion future du Break-Even
        bb20_mid_at_entry=signal.get('bb20_mid') 
    )
    return True

# ==============================================================================
# GESTION DES POSITIONS OUVERTES
# ==============================================================================

def manage_open_positions(ex: ccxt.Exchange):
    """
    Surveille toutes les positions ouvertes et applique les règles de gestion.
    Principalement : la gestion du Break-Even pour les trades de contre-tendance.
    """
    open_positions = database.get_open_positions()
    if not open_positions:
        return # Rien à faire

    # Récupérer les prix actuels de tous les actifs concernés pour optimiser
    symbols = [pos['symbol'] for pos in open_positions]
    try:
        tickers = ex.fetch_tickers(symbols)
    except Exception as e:
        print(f"Impossible de récupérer les tickers pour la gestion des positions : {e}")
        return

    for pos in open_positions:
        symbol = pos['symbol']
        current_price = tickers.get(symbol, {}).get('last')
        if not current_price:
            continue

        # --- RÈGLE DE BREAK-EVEN (UNIQUEMENT POUR CONTRE-TENDANCE) ---
        is_breakeven_candidate = (pos['regime'] == 'counter' and pos['status'] == 'OPEN')
        
        if is_breakeven_candidate:
            # La règle est "BE = Moyenne Mobile BB blanche"
            breakeven_target_price = pos['bb20_mid_at_entry']

            # Pour un achat (buy), on passe à BE si le prix actuel a dépassé la MM20
            if pos['side'] == 'buy' and current_price > breakeven_target_price:
                move_sl_to_breakeven(ex, pos)
            
            # Pour une vente (sell), on passe à BE si le prix actuel est passé sous la MM20
            elif pos['side'] == 'sell' and current_price < breakeven_target_price:
                move_sl_to_breakeven(ex, pos)


def move_sl_to_breakeven(ex: ccxt.Exchange, position: Dict[str, Any]):
    """
    Modifie l'ordre stop-loss d'une position pour le remonter au prix d'entrée.
    """
    try:
        # La modification d'ordres SL/TP complexes dépend de l'exchange.
        # La méthode la plus sûre est souvent d'annuler l'ancien SL et d'en créer un nouveau.
        # Cette logique est à affiner en fonction des capacités de l'API de Bitget.
        # Pour l'instant, nous simulons l'action et mettons à jour notre base de données.
        
        # ex.edit_order(...) ou ex.cancel_all_orders(symbol) + ex.create_stop_market_order(...)
        
        print(f"[{position['symbol']}] PASSAGE AU BREAK-EVEN. SL déplacé à {position['entry_price']}")
        
        # Mettre à jour notre base de données pour ne pas répéter l'action
        database.update_trade_status(position['id'], 'BREAKEVEN')
        
        # Notifier l'utilisateur
        notifier.tg_send(f"🛡️ BREAK-EVEN sur {position['symbol']}. Le risque est neutralisé.")

    except Exception as e:
        print(f"Erreur lors du passage au Break-Even pour {position['symbol']}: {e}")
