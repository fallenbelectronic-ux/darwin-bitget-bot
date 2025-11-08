# Fichier: state.py

# Dictionnaire pour stocker les signaux en attente de confirmation
# La clé sera le symbole (ex: 'BTC/USDT:USDT')
# La valeur sera un dictionnaire contenant les détails du signal et le timestamp de la bougie
import threading
from typing import Dict, Any, Optional

_pending_signals: Dict[str, Any] = {}
_lock = threading.Lock()

def set_pending_signal(symbol: str, payload: Dict[str, Any]) -> None:
    """Ajoute ou met à jour un signal en attente pour un symbole."""
    with _lock:
        _pending_signals[symbol] = payload

def get_pending_signals() -> Dict[str, Any]:
    """Retourne une COPIE des signaux en attente (thread-safe)."""
    with _lock:
        return dict(_pending_signals)

def pop_pending_signal(symbol: str) -> Optional[Dict[str, Any]]:
    """Retire et retourne le signal en attente pour ce symbole (ou None)."""
    with _lock:
        return _pending_signals.pop(symbol, None)

def clear_pending_signals() -> None:
    """Vide tous les signaux en attente."""
    with _lock:
        _pending_signals.clear()

def count_pending_signals() -> int:
    """Nombre de signaux en attente."""
    with _lock:
        return len(_pending_signals)

