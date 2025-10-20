# Fichier: main.py
import os
import time
import ccxt
import pandas as pd
import traceback
from ta.volatility import BollingerBands
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import pytz

import database
import trader
import notifier
import utils
import state  # Import du nouveau module d'état

# --- PARAMÈTRES GLOBAUX ---
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY, API_SECRET, PASSPHRASSE = os.getenv("BITGET_API_KEY", ""), os.getenv("BITGET_API_SECRET", ""), os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASSE", "")
TIMEFRAME, UNIVERSE_SIZE, MIN_RR = os.getenv("TIMEFRAME", "1h"), int(os.getenv("UNIVERSE_SIZE", "30")), float(os.getenv("MIN_RR", "3.0"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", 3))
LOOP_DELAY, TIMEZONE, REPORT_HOUR, REPORT_WEEKDAY = int(os.getenv("LOOP_DELAY", "5")), os.getenv("TIMEZONE", "Europe/Lisbon"), int(os.getenv("REPORT_HOUR", "21")), int(os.getenv("REPORT_WEEKDAY", "6"))

# --- VARIABLES D'ÉTAT ---
_last_update_id: Optional[int] = None; _paused = False; _last_daily_report_day, _last_weekly_report_day = -1, -1
_recent_signals: List[Dict] = []

# --- NOUVELLE FONCTION POUR GÉRER LES SIGNAUX EN ATTENTE ---
def check_pending_signals(ex: ccxt.Exchange, symbol: str, df: pd.DataFrame):
    """Vérifie si un signal en attente doit être exécuté au début d'une nouvelle bougie."""
    if symbol in state.pending_signals:
        pending = state.pending_signals[symbol]
        signal_candle_timestamp = pending['candle_timestamp']
        current_candle_timestamp = df.index[-1]

        # Si nous sommes sur une nouvelle bougie (la bougie qui suit celle du signal)
        if current_candle_timestamp > signal_candle_timestamp:
            print(f"  -> NOUVELLE BOUGIE détectée pour {symbol}. Exécution du signal en attente...")
            
            # Le prix d'entrée est le prix d'ouverture de la nouvelle bougie
            entry_price = df['open'].iloc[-1]
            
            # On tente d'exécuter le trade avec le nouveau prix d'entrée
            is_taken, reason = trader.execute_trade(ex, symbol, pending['signal'], pending['df'], entry_price)
            
            if is_taken:
                print(f"   -> SUCCÈS: Le trade en attente pour {symbol} a été pris.")
            else:
                # Si le trade n'est pas pris, on envoie une notification de rejet
                notifier.send_validated_signal_report(symbol, pending['signal'], is_taken, reason)
                print(f"   -> ÉCHEC: Le trade en attente pour {symbol} n'a pas pu être pris. Raison: {reason}")
            
            # On supprime le signal en attente, qu'il soit pris ou non
            del state.pending_signals[symbol]

def detect_signal(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Logique de détection de signal sur la bougie en cours."""
    # Le reste de cette fonction est identique à la version stable
    # ...
    pass

def main():
    """Boucle principale du bot."""
    ex = create_exchange()
    database.setup_database()
    # ... (initialisation des settings)
    
    notifier.send_start_banner("TESTNET" if BITGET_TESTNET else "LIVE", "PAPIER" if trader.PAPER_TRADING_MODE else "RÉEL", trader.RISK_PER_TRADE_PERCENT)
    universe = build_universe(ex)
    if not universe:
        notifier.tg_send("❌ Impossible de construire l'univers de trading.")
        return
    print(f"Univers de trading chargé avec {len(universe)} paires.")
    
    while True:
        print(f"\n--- [{time.strftime('%Y-%m-%d %H:%M:%S')}] Nouveau cycle de la boucle ---")
        try:
            # ... (gestion Telegram, rapports, pause, positions ouvertes)
            
            print(f"4. Début du scan de l'univers ({len(universe)} paires)...")
            for symbol in universe:
                df = utils.fetch_ohlcv_df(ex, symbol, TIMEFRAME)
                if df is None or len(df) < 81:
                    continue
                
                # Étape 1: Vérifier s'il y a un signal en attente à exécuter pour ce symbole
                check_pending_signals(ex, symbol, df)
                
                # Étape 2: Détecter de nouveaux signaux sur la bougie en cours
                signal = detect_signal(symbol, df)
                
                if signal:
                    # Si un nouveau signal est trouvé et qu'il n'y en a pas déjà un en attente
                    if symbol not in state.pending_signals:
                        print(f"✅✅✅ Signal '{signal['regime']}' DÉTECTÉ pour {symbol}. MISE EN ATTENTE pour la prochaine bougie.")
                        
                        # On stocke le signal, le dataframe, et le timestamp de la bougie actuelle
                        state.pending_signals[symbol] = {
                            'signal': signal,
                            'df': df.copy(), # On stocke une copie du df au moment du signal
                            'candle_timestamp': df.index[-1]
                        }
                        
                        # On envoie une notification pour dire que le signal est en attente
                        notifier.send_validated_signal_report(symbol, signal, False, "En attente de la clôture horaire pour confirmation.")
                        
                        # On l'ajoute aussi à l'historique des signaux récents
                        _recent_signals.append({'timestamp': time.time(), 'symbol': symbol, 'signal': signal})

            print(f"--- Fin du cycle. Attente de {LOOP_DELAY} secondes. ---")
            time.sleep(LOOP_DELAY)
        
        except KeyboardInterrupt:
            # ...
            break
        except Exception:
            # ...

# --- Le reste des fonctions (create_exchange, build_universe, etc.) reste inchangé ---
if __name__ == "__main__":
    main()
