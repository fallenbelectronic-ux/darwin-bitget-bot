# Fichier: state.py

# Dictionnaire pour stocker les signaux en se forme à 14:43, mais on veut s'assurer d'entrer sur le marché uniquement à attente de confirmation
# La clé sera le symbole (ex: 'BTC/USDT:USDT')
 15:00:00, au début de la bougie suivante, pour confirmer le signal avec la# La valeur sera un dictionnaire contenant les détails du signal et le timestamp de la bougie
pending_signals = {}
 clôture.

J'ai implémenté cette logique complexe. Elle transforme votre bot d'un simple "scanner```

---

### **Action 2 : Mettre à Jour `trader.py`**

Nous allons modifier `execute" en un véritable "gestionnaire de setups".

### **La Nouvelle Logique Expliquée**

1.  **Mé_trade` pour qu'elle puisse prendre un prix d'entrée différent de celui calculé dans le signal (lemoire des Setups :** Le bot a maintenant une nouvelle mémoire (une simple variable) qui stocke les "setups en prix d'ouverture de la nouvelle bougie).

**Remplacez votre fichier `trader.py` par attente".
2.  **Scan en Temps Réel :** À chaque cycle (par exemple à 14: ceci :**
```python
# Fichier: trader.py
import os, time, ccxt, pandas43), le bot analyse la **bougie en cours**. S'il détecte un signal valide (contact, ré as pd, database, notifier, charting
from ta.volatility import BollingerBands
from typing import Dict, Anyintégration, R/R > 3), il **ne prend PAS le trade**. À la place, il sauvegarde toutes, Optional, Tuple
from utils import fetch_ohlcv_df

# --- PARAMÈTRES ---
PAPER_TR les informations du signal (symbole, entrée, SL, TP) dans la liste des "setups en attente".ADING_MODE = os.getenv("PAPER_TRADING_MODE", "true").lower() in ("1",
3.  **Le Déclencheur Horaire :** Au début de chaque nouvelle heure (15:00 "true", "yes")
RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_, 16:00, etc.), une nouvelle fonction se déclenche. Elle parcourt la liste des "PER_TRADE_PERCENT", "1.0"))
LEVERAGE, TIMEFRAME = int(os.getenvsetups en attente".
4.  **Exécution :** Pour chaque setup en attente, elle exécute le("LEVERAGE", "2")), os.getenv("TIMEFRAME", "1h")
TP_UPDATE trade en utilisant les paramètres qui ont été sauvegardés. La liste est ensuite vidée, prête pour la nouvelle heure.

_THRESHOLD_PERCENT = 0.05

# --- FONCTION MODIFIÉE POUR ACCEPTER UN PCette méthode combine le meilleur des deux mondes : la réactivité de la détection en temps réel et la sécurité dRIX D'ENTRÉE EXTERNE ---
def execute_trade(ex: ccxt.Exchange, symbol: str,'une exécution synchronisée sur la clôture.

Cette fonctionnalité majeure ne touche qu'**un seul fichier** signal: Dict[str, Any], df: pd.DataFrame, entry_price: float) -> Tuple[bool, str]:
    """Tente d'exécuter un trade avec un prix d'entrée spécifié.""" : `main.py`.

---

### **Action Requise : Remplacer `main.py`**

Rem
    
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv('placez l'intégralité de votre fichier `main.py` par cette version finale et complète.

MAX_OPEN_POSITIONS', 3)))
    if len(database.get_open_positions()) >= max_pos:
        return False, f"Rejeté: Max positions ({max_pos}) atteint."
    if database```python
# Fichier: main.py
import os
import time
import ccxt
import pandas as pd
import traceback.is_position_open(symbol):
        return False, "Rejeté: Position déjà ouverte."

    
from ta.volatility import BollingerBands
from typing import List, Dict, Any, Optional
from datetime import datetimebalance = get_usdt_balance(ex)
    if balance is None:
        return False, "Re, timezone
import pytz

import database
import trader
import notifier
import utils

# --- PARAMÈTRES Gjeté: Erreur de solde (Clés API?)."
    if balance <= 10:
        return False, f"Rejeté: Solde insuffisant ({balance:.2f} USDT)."
    
    #LOBAUX ---
BITGET_TESTNET   = os.getenv("BITGET_TESTNET", "true").lower On recalcule la quantité avec le nouveau prix d'entrée
    quantity = calculate_position_size(balance, RISK() in ("1", "true", "yes")
API_KEY, API_SECRET, PASSPHRASSE_PER_TRADE_PERCENT, entry_price, signal['sl'])
    if quantity <= 0:
 = os.getenv("BITGET_API_KEY", ""), os.getenv("BITGET_API_SECRET", ""),        return False, f"Rejeté: Quantité calculée nulle ({quantity})."
    
    if not os.getenv("BITGET_API_PASSWORD", "") or os.getenv("BITGET_PASSPHRASSE PAPER_TRADING_MODE:
        try:
            ex.set_leverage(LEVERAGE,", "")
TIMEFRAME        = os.getenv("TIMEFRAME", "1h")
UN symbol)
            # On passe un ordre au marché, qui sera exécuté proche de `entry_price`
            orderIVERSE_SIZE    = int(os.getenv("UNIVERSE_SIZE", "30"))
MIN = ex.create_market_order(symbol, signal['side'], quantity)
            # On met à jour le_RR           = float(os.getenv("MIN_RR", "3.0"))
MAX_OPEN_POSITIONS prix d'entrée avec le prix réel d'exécution
            final_entry_price = float(order['price'])
        except Exception as e:
            notifier.tg_send_error(f"Exécution d'ordre = int(os.getenv("MAX_OPEN_POSITIONS", 3))
LOOP_DELAY       = int(os sur {symbol}", e)
            return False, f"Erreur d'exécution: {e}"
    else.getenv("LOOP_DELAY", "5"))
TIMEZONE         = os.getenv("TIMEZONE", "Europe/Lisbon")
REPORT_HOUR      = int(os.getenv("REPORT_HOUR", "21"))
:
        final_entry_price = entry_price # En mode papier, on simule l'entrée

    # OnREPORT_WEEKDAY   = int(os.getenv("REPORT_WEEKDAY", "6"))

# --- VARIABLES D'ÉTAT ---
_last_update_id: Optional[int] = None
_paused = False
_last met à jour le signal avec le prix d'entrée final pour l'enregistrement et la notification
    signal['entry'] = final_entry_price
    
    database.create_trade(symbol, signal['side'], signal['regime'], final_entry_price, signal['sl'], signal['tp'], quantity, RISK_PER_TRADE_PERCENT, int_execution_hour = -1 # NOUVEAU: Pour suivre la dernière heure d'exécution
_pending_setups: Dict[str, Dict] = {} # NOUVEAU: Dictionnaire pour les setups en attente(time.time()), signal.get('bb20_mid'), "NORMAL")
    
    chart_ {symbol: signal_data}

# --- FONCTIONS PRINCIPALES ---

def detect_signal(symbol: strimage = charting.generate_trade_chart(symbol, df, signal)
    mode_text = "P, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Logique de détection de signalAPIER" if PAPER_TRADING_MODE else "RÉEL"
    trade_message = notifier.format sur la bougie en cours."""
    if df is None or len(df) < 81: return None
    _trade_message(symbol, signal, quantity, mode_text, RISK_PER_TRADE_PERCENT)

    df_with_indicators = trader._get_indicators(df.copy())
    if df_with_indicators    notifier.tg_send_with_photo(photo_buffer=chart_image, caption=trade_message)
     is None: return None
    
    last_candle = df_with_indicators.iloc[-1]
    
    return True, "Position ouverte avec succès."

# --- Le reste du fichier est inchangé ---
def
    is_uptrend = last_candle['close'] > last_candle['bb80_mid']
     _get_indicators(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    # ...
def manageis_downtrend = last_candle['close'] < last_candle['bb80_mid']
    _open_positions(ex: ccxt.Exchange):
    # ...
def get_usdt_balance(ex: ccxt.Exchange) -> Optional[float]:
    # ...
def calculate_position_size(balancesignal = None

    # Tendance
    if is_uptrend and last_candle['low'] <= last_candle['bb20_lo'] and last_candle['close'] > last_candle['bb20_lo']:: float, risk_percent: float, entry_price: float, sl_price: float) -> float:
    # ...
def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    
        entry = (last_candle['open'] + last_candle['close']) / 2
        sl, tp# ...
