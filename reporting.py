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

def _compute_upnl_rpnl(pos: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    """
    Calcule l'UPnL (non réalisé) et l'RPnL (réalisé) en USDT pour une position.
    - UPnL utilise le dernier prix si disponible dans la position:
      keys possibles: 'last', 'lastPrice', 'mark_price', 'markPrice'
      Sinon, retourne (None, rpnl) sans lever d'erreur.
    - RPnL est cherché dans plusieurs clés usuelles.
    Aucun frais inclus.
    """
    try:
        entry = float(pos.get('entryPrice') or pos.get('entry_price') or pos.get('entry') or pos.get('avgEntryPrice') or 0.0)

        # qty peut être négative (short) selon l'exchange → on prend la valeur absolue
        raw_qty = (pos.get('contracts') or pos.get('amount') or pos.get('size') or pos.get('qty') or 0.0)
        qty = abs(float(raw_qty))

        side  = (pos.get('side') or pos.get('positionSide') or '').lower()  # 'long' | 'short'

        # dernier prix
        last = pos.get('last')
        if last is None: last = pos.get('lastPrice')
        if last is None: last = pos.get('mark_price')
        if last is None: last = pos.get('markPrice')
        last_price = float(last) if last is not None else None

        # RPnL (réalisé)
        rpnl = pos.get('realizedPnl')
        if rpnl is None: rpnl = pos.get('realized_pnl')
        if rpnl is None: rpnl = pos.get('rpnl')
        realized = float(rpnl) if rpnl is not None else 0.0

        if last_price is None or entry == 0.0 or qty == 0.0 or side not in ('long', 'short'):
            return (None, realized)

        if side == 'long':
            upnl = (last_price - entry) * qty
        else:
            upnl = (entry - last_price) * qty

        return (float(upnl), float(realized))
    except Exception:
        # sécurité: ne jamais casser l'affichage si une clé manque
        return (None, 0.0)

def _fmt_pnl_line(pos: Dict[str, Any]) -> str:
    """
    Affiche UNIQUEMENT le PnL en pourcentage (sans fees), basé sur le dernier prix.
    - Long : ((last / entry) - 1) * 100
    - Short: ((entry / last) - 1) * 100
    Si une info manque → 'PnL: n/a%'.
    """
    try:
        entry = float(pos.get('entryPrice') or pos.get('entry_price') or pos.get('entry') or pos.get('avgEntryPrice') or 0.0)
        side  = (pos.get('side') or pos.get('positionSide') or '').lower()
        last  = pos.get('last') or pos.get('lastPrice') or pos.get('mark_price') or pos.get('markPrice')
        last_price = float(last) if last is not None else None

        if entry <= 0 or last_price is None or side not in ('long', 'short', 'buy', 'sell'):
            return "PnL: n/a%"

        if side in ('long', 'buy'):
            pct = (last_price / entry - 1.0) * 100.0
        else:
            pct = (entry / last_price - 1.0) * 100.0

        return f"PnL: {pct:+.2f}%"
    except Exception:
        return "PnL: n/a%"

def format_position_row(idx: int, pos: Dict[str, Any]) -> str:
    """
    Formate une position pour le bloc 'Positions Ouvertes (DB)'.
    Ajout: ligne PnL% (sans fees) juste sous 'SL | TP'.
    """
    symbol = str(pos.get('symbol') or pos.get('market') or "").upper()
    entry  = pos.get('entryPrice') or pos.get('entry_price') or pos.get('entry') or pos.get('avgEntryPrice')
    sl     = pos.get('stopLoss') or pos.get('sl') or pos.get('stop_loss') or pos.get('sl_price')
    tp     = pos.get('takeProfit') or pos.get('tp') or pos.get('take_profit') or pos.get('tp_price')

    entry_s = f"{float(entry):.4f}" if entry is not None else "n/a"
    sl_s    = f"{float(sl):.4f}"    if sl    is not None else "n/a"
    tp_s    = f"{float(tp):.4f}"    if tp    is not None else "n/a"

    header = f"{idx}. {symbol}"
    line1  = f"Entrée: <u>{entry_s}</u>"
    line2  = f"SL: <u>{sl_s}</u> | TP: <u>{tp_s}</u>"
    line3  = _fmt_pnl_line(pos)  # uniquement le %

    return f"{header}\n{line1}\n{line2}\n{line3}"


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
