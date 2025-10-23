# Fichier: reporting.py
from typing import List, Dict, Any, Optional
from tabulate import tabulate
import numpy as np
from scipy import stats as st

def get_report_stats(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calcule les statistiques de performance à partir d'une liste de trades."""
def calculate_performance_stats(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calcule les statistiques de performance avancées à partir d'une liste de trades."""
    total_trades = len(trades)
    if total_trades == 0:
    if total_trades < 1:
        return {"total_trades": 0}

    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] < 0]
    pnls = np.array([t['pnl'] for t in trades])
    pnl_percents = np.array([t['pnl_percent'] for t in trades])
    
    wins = pnls[pnls > 0]
    losses = pnls[pnls < 0]

    nb_wins = len(wins)
    nb_losses = len(losses)

    win_rate = (nb_wins / total_trades) * 100 if total_trades > 0 else 0
    
    total_pnl = sum(t['pnl'] for t in trades)
    gross_profit = sum(t['pnl'] for t in wins)
    gross_loss = abs(sum(t['pnl'] for t in losses))
    
    avg_win = gross_profit / nb_wins if nb_wins > 0 else 0
    avg_loss = gross_loss / nb_losses if nb_losses > 0 else 0
    total_pnl = np.sum(pnls)
    gross_profit = np.sum(wins)
    gross_loss = abs(np.sum(losses))

    win_rate = (nb_wins / total_trades) * 100 if total_trades > 0 else 0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
    avg_trade_pnl_percent = np.mean(pnl_percents) if total_trades > 0 else 0
    sharpe_ratio = (np.mean(pnl_percents) / np.std(pnl_percents)) * np.sqrt(365*24) if np.std(pnl_percents) > 0 else 0 # Approximation pour H1

    # Calcul du Drawdown
    equity_curve = np.cumsum(pnls)
    peak = np.maximum.accumulate(equity_curve)
    drawdowns = (peak - equity_curve)
    max_drawdown_value = np.max(drawdowns) if len(drawdowns) > 0 else 0
    
    # Pourcentage de Drawdown par rapport au pic le plus haut atteint
    total_peak = np.max(peak) if len(peak) > 0 else 0
    max_drawdown_percent = (max_drawdown_value / total_peak) * 100 if total_peak > 0 else 0

    return {
        "total_trades": total_trades, "nb_wins": nb_wins, "nb_losses": nb_losses,
        "win_rate": win_rate, "total_pnl": total_pnl, "profit_factor": profit_factor,
        "avg_win": avg_win, "avg_loss": avg_loss
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "profit_factor": profit_factor,
        "avg_trade_pnl_percent": avg_trade_pnl_percent,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown_percent": max_drawdown_percent
    }

def format_report_message(title: str, stats: Dict[str, Any], balance: Optional[float]) -> str:
def format_report_message(title: str, stats: Dict[str, Any], balance: float) -> str:
    """Met en forme le message de rapport pour Telegram, en incluant le solde."""
    balance_str = f"<code>{balance:.2f} USDT</code>" if balance is not None else "<i>(non disponible)</i>"

    header = f"<b>{title}</b>\n\n💰 <b>Solde Actuel:</b> {balance_str}\n"

    if stats.get("total_trades", 0) == 0:
        return header + "\n- Pas de trades clôturés durant cette période."
    if stats.get("total_trades", 0) < 1:
        return header + "\n- Pas assez de données de trades pour générer un rapport."

    pf_str = "Infini" if stats['profit_factor'] == float('inf') else f"{stats['profit_factor']:.2f}"

    return (
        header + "\n"
        f"📈 <b>Trades Total :</b> <code>{stats['total_trades']}</code>\n"
        f"✅ <b>Gagnants :</b> <code>{stats['nb_wins']}</code> | ❌ <b>Perdants :</b> <code>{stats['nb_losses']}</code>\n"
        f"🎯 <b>Taux de réussite :</b> <code>{stats['win_rate']:.2f}%</code>\n\n"
        f"💰 <b>Profit & Loss Net :</b> <code>{stats['total_pnl']:.2f} USDT</code>\n"
        f"🏆 <b>Profit Factor :</b> <code>{pf_str}</code>"
    )
    
    headers = ["Statistique", "Valeur"]
    table_data = [
        ["Trades Total", f"{stats['total_trades']}"],
        ["Taux de Réussite", f"{stats['win_rate']:.2f}%"],
        ["PNL Net Total", f"{stats['total_pnl']:.2f} USDT"],
        ["Profit Factor", pf_str],
        ["Gain Moyen / Trade", f"{stats['avg_trade_pnl_percent']:.2f}%"],
        ["Ratio de Sharpe (approx.)", f"{stats['sharpe_ratio']:.2f}"],
        ["Drawdown Max", f"{stats['max_drawdown_percent']:.2f}%"]
    ]
    
    table = tabulate(table_data, headers=headers, tablefmt="simple")
    
    return f"{header}\n<pre>{table}</pre>"
