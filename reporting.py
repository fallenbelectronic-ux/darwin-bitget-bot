# Fichier: reporting.py
from typing import List, Dict, Any

def get_report_stats(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calcule les statistiques de performance à partir d'une liste de trades."""
    total_trades = len(trades)
    if total_trades == 0:
        return {"total_trades": 0}

    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] < 0]
    
    nb_wins = len(wins)
    nb_losses = len(losses)
    
    win_rate = (nb_wins / total_trades) * 100 if total_trades > 0 else 0
    
    total_pnl = sum(t['pnl'] for t in trades)
    gross_profit = sum(t['pnl'] for t in wins)
    gross_loss = abs(sum(t['pnl'] for t in losses))
    
    avg_win = gross_profit / nb_wins if nb_wins > 0 else 0
    avg_loss = gross_loss / nb_losses if nb_losses > 0 else 0
    
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

    return {
        "total_trades": total_trades, "nb_wins": nb_wins, "nb_losses": nb_losses,
        "win_rate": win_rate, "total_pnl": total_pnl, "profit_factor": profit_factor,
        "avg_win": avg_win, "avg_loss": avg_loss
    }

def format_report_message(title: str, stats: Dict[str, Any], balance: float) -> str:
    """Met en forme le message de rapport pour Telegram, en incluant le solde."""
    balance_str = f"<code>{balance:.2f} USDT</code>" if balance is not None else "<i>(non disponible)</i>"
    
    header = f"<b>{title}</b>\n\n💰 <b>Solde Actuel:</b> {balance_str}\n"
    
    if stats.get("total_trades", 0) == 0:
        return header + "\n- Pas de trades clôturés durant cette période."

    pf_str = "Infini" if stats['profit_factor'] == float('inf') else f"{stats['profit_factor']:.2f}"

    return (
        header + "\n"
        f"📈 <b>Trades Total :</b> <code>{stats['total_trades']}</code>\n"
        f"✅ <b>Gagnants :</b> <code>{stats['nb_wins']}</code> | ❌ <b>Perdants :</b> <code>{stats['nb_losses']}</code>\n"
        f"🎯 <b>Taux de réussite :</b> <code>{stats['win_rate']:.2f}%</code>\n\n"
        f"💰 <b>Profit & Loss Net :</b> <code>{stats['total_pnl']:.2f} USDT</code>\n"
        f"🏆 <b>Profit Factor :</b> <code>{pf_str}</code>"
    )```

#### **3. Fichier `notifier.py` (Version Finale Professionnelle)**

```python
# Fichier: notifier.py
import os
import time
import html
import requests
import io
from typing import List, Dict, Any, Optional

import reporting

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API = f"https://api.telegram.org/bot{TG_TOKEN}"

def tg_send(text: str, reply_markup: Optional[Dict] = None):
    """Envoie un message texte simple sur Telegram."""
    if not TG_TOKEN or not TG_CHAT_ID: return
    try:
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
        if reply_markup: payload['reply_markup'] = reply_markup
        requests.post(f"{TELEGRAM_API}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        print(f"Erreur d'envoi Telegram: {e}")

def send_report(title: str, trades: List[Dict[str, Any]], balance: float):
    """Calcule les stats et envoie un rapport."""
    stats = reporting.get_report_stats(trades)
    message = reporting.format_report_message(title, stats, balance)
    tg_send(message)

def send_validated_signal_report(symbol: str, signal: Dict, is_taken: bool, reason: str):
    """Envoie un rapport de signal validé, avec le statut d'exécution."""
    side_icon = "📈" if signal['side'] == 'buy' else "📉"
    status_icon = "✅" if is_taken else "❌"
    status_text = "<b>Position Ouverte</b>" if is_taken else f"<b>Position NON Ouverte</b>\n   - Raison: <i>{html.escape(reason)}</i>"
    message = ( f"<b>{status_icon} Signal Validé {side_icon}</b>\n\n"
               f" paire: <code>{html.escape(symbol)}</code>\n Type: <b>{html.escape(signal['regime'].capitalize())}</b>\n\n"
               f" Entrée: <code>{signal['entry']:.5f}</code>\n SL: <code>{signal['sl']:.5f}</code>\n TP: <code>{signal['tp']:.5f}</code>\n"
               f" RR: <b>x{signal['rr']:.2f}</b>\n\n{status_text}" )
    tg_send(message)

# --- Le reste du fichier est identique à la version stable et propre précédente ---
def send_config_message(min_rr: float, risk: float, max_pos: int, leverage: int):
    # ...
def send_mode_message(is_testnet: bool, is_paper: bool):
    # ...
def get_main_menu_keyboard(is_paused: bool) -> Dict:
    # ...
def send_breakeven_notification(symbol: str, pnl_realised: float, remaining_qty: float):
    # ...
def tg_send_with_photo(photo_buffer: io.BytesIO, caption: str):
    # ...
def tg_get_updates(offset: Optional[int] = None) -> List[Dict[str, Any]]:
    # ...
def get_strategy_menu_keyboard(current_strategy: str) -> Dict:
    # ...
def get_positions_keyboard(positions: List[Dict[str, Any]]) -> Optional[Dict]:
    # ...
def send_start_banner(platform: str, trading: str, risk: float):
    # ...
def send_main_menu(is_paused: bool):
    # ...
def send_strategy_menu(current_strategy: str):
    # ...
def format_open_positions(positions: List[Dict[str, Any]]):
    # ...
def tg_send_error(title: str, error: Any):
    # ...
def format_trade_message(symbol, signal, quantity, mode, risk) -> str:
    # ...
