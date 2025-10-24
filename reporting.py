# Fichier: reporting.py
from typing import List, Dict, Any, Optional
from tabulate import tabulate
import numpy as np

def calculate_performance_stats(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calcule les statistiques de performance avancées à partir d'une liste de trades."""
    total_trades = len(trades)
    if total_trades < 1:
        return {"total_trades": 0}

    pnls = np.array([t['pnl'] for t in trades if t['pnl'] is not None])
    pnl_percents = np.array([t['pnl_percent'] for t in trades if t['pnl_percent'] is not None])
    
    if len(pnls) == 0:
        return {"total_trades": 0, "nb_wins": 0, "nb_losses": 0}

    wins = pnls[pnls > 0]
    losses = pnls[pnls < 0]
    
    nb_wins = len(wins)
    nb_losses = len(losses)
    
    total_pnl = np.sum(pnls)
    gross_profit = np.sum(wins)
    gross_loss = abs(np.sum(losses))
    
    win_rate = (nb_wins / total_trades) * 100
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
    avg_trade_pnl_percent = np.mean(pnl_percents) if len(pnl_percents) > 0 else 0
    sharpe_ratio = 0.0 
    if np.std(pnl_percents) > 0:
        sharpe_ratio = (np.mean(pnl_percents) / np.std(pnl_percents)) * np.sqrt(365*24)

    equity_curve = np.cumsum(pnls)
    peak = np.maximum.accumulate(equity_curve)
    drawdowns = peak - equity_curve
    max_drawdown_value = np.max(drawdowns) if len(drawdowns) > 0 else 0
    total_peak = np.max(peak) if len(peak) > 0 else 0
    max_drawdown_percent = (max_drawdown_value / total_peak) * 100 if total_peak > 0 else 0

    return {
        "total_trades": total_trades, "nb_wins": nb_wins, "nb_losses": nb_losses,
        "win_rate": win_rate, "total_pnl": total_pnl, "profit_factor": profit_factor,
        "avg_trade_pnl_percent": avg_trade_pnl_percent, "sharpe_ratio": sharpe_ratio,
        "max_drawdown_percent": max_drawdown_percent
    }

def format_report_message(title: str, stats: Dict[str, Any], balance: Optional[float]) -> str:
    """Met en forme le message de rapport pour Telegram."""
    balance_str = f"<code>{balance:.2f} USDT</code>" if balance is not None else "<i>(non disponible)</i>"
    header = f"<b>{title}</b>\n\n💰 <b>Solde Actuel:</b> {balance_str}\n"
    
    if stats.get("total_trades", 0) < 1:
        return header + "\n- Pas assez de données de trades pour générer un rapport."

    pf_str = "Infini" if stats.get('profit_factor', 0) == float('inf') else f"{stats.get('profit_factor', 0):.2f}"
    
    headers = ["Statistique", "Valeur"]
    table_data = [
        ["Trades Total", f"{stats.get('total_trades', 0)}"],
        ["Taux de Réussite", f"{stats.get('win_rate', 0):.2f}%"],
        ["PNL Net Total", f"{stats.get('total_pnl', 0):.2f} USDT"],
        ["Profit Factor", pf_str],
        ["Gain Moyen / Trade", f"{stats.get('avg_trade_pnl_percent', 0):.2f}%"],
        ["Ratio de Sharpe (approx.)", f"{stats.get('sharpe_ratio', 0):.2f}"],
        ["Drawdown Max", f"{stats.get('max_drawdown_percent', 0):.2f}%"]
    ]
    
    table = tabulate(table_data, headers=headers, tablefmt="simple")
    return f"{header}\n<pre>{table}</pre>"
