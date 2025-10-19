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
        "total_trades": total_trades,
        "nb_wins": nb_wins,
        "nb_losses": nb_losses,
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "profit_factor": profit_factor,
        "avg_win": avg_win,
        "avg_loss": avg_loss
    }

def format_report_message(title: str, stats: Dict[str, Any]) -> str:
    """Met en forme le message de rapport pour Telegram."""
    if stats.get("total_trades", 0) == 0:
        return f"<b>{title}</b>\n\n- Pas de trades clôturés durant cette période."

    # Formatage du Profit Factor
    if stats['profit_factor'] == float('inf'):
        pf_str = "Infini (aucune perte)"
    else:
        pf_str = f"{stats['profit_factor']:.2f}"

    return (
        f"<b>{title}</b>\n\n"
        f"📈 <b>Trades Total :</b> <code>{stats['total_trades']}</code>\n"
        f"✅ <b>Gagnants :</b> <code>{stats['nb_wins']}</code>\n"
        f"❌ <b>Perdants :</b> <code>{stats['nb_losses']}</code>\n"
        f"🎯 <b>Taux de réussite :</b> <code>{stats['win_rate']:.2f}%</code>\n\n"
        f"💰 <b>Profit & Loss Net :</b> <code>{stats['total_pnl']:.2f} USDT</code>\n"
        f"🏆 <b>Profit Factor :</b> <code>{pf_str}</code>\n\n"
        f"👍 <b>Gain moyen :</b> <code>{stats['avg_win']:.2f} USDT</code>\n"
        f"👎 <b>Perte moyenne :</b> <code>{stats['avg_loss']:.2f} USDT</code>"
    )
