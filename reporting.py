# Fichier: reporting.py
from typing import List, Dict, Any, Optional
from tabulate import tabulate
import numpy as np
import math 

def calculate_performance_stats(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calcule les statistiques de performance à partir d'une liste de trades."""
    total_trades = len(trades)
    if total_trades < 1:
        return {"total_trades": 0}

    # Récupération des séries (inclut les 0 pour bien traiter les BE)
    pnls = np.array([float(t.get('pnl', 0.0)) for t in trades if t.get('pnl') is not None], dtype=float)
    pnl_percents = np.array([float(t.get('pnl_percent', 0.0)) for t in trades if t.get('pnl_percent') is not None], dtype=float)

    if pnls.size == 0:
        return {"total_trades": 0, "nb_wins": 0, "nb_losses": 0}

    effective_trades = int(pnls.size)

    wins = pnls[pnls > 0.0]
    losses = pnls[pnls < 0.0]
    # breakeven = pnls[pnls == 0.0]

    nb_wins = int(wins.size)
    nb_losses = int(losses.size)

    total_pnl = float(np.sum(pnls))
    gross_profit = float(np.sum(wins)) if wins.size else 0.0
    gross_loss = abs(float(np.sum(losses))) if losses.size else 0.0  # positif

    # Winrate (les BE restent dans le dénominateur des trades effectivement renseignés)
    win_rate = (nb_wins / effective_trades) * 100.0 if effective_trades else 0.0

    # Profit Factor robuste
    if gross_profit == 0.0 and gross_loss == 0.0:
        profit_factor = None                 # indéfini (0/0) => affichage "—"
    elif gross_loss == 0.0 and gross_profit > 0.0:
        profit_factor = math.inf             # aucune perte mais du profit => ∞
    else:
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0.0 else 0.0

    # Gain moyen par trade (%)
    avg_trade_pnl_percent = float(np.mean(pnl_percents)) if pnl_percents.size > 0 else 0.0

    # Sharpe approx par trade: mean/std * sqrt(n). Protège variance nulle et n<2.
    if pnl_percents.size > 1:
        sigma = float(np.std(pnl_percents, ddof=1))
        if sigma > 0.0:
            mu = float(np.mean(pnl_percents))
            sharpe_ratio = (mu / sigma) * math.sqrt(pnl_percents.size)
        else:
            sharpe_ratio = 0.0
    else:
        sharpe_ratio = 0.0

    # Max Drawdown (%) sur l'equity cumulée (en USDT), normalisée par le pic courant
    equity_curve = np.cumsum(pnls).astype(float)
    running_max = np.maximum.accumulate(equity_curve)
    drawdown = equity_curve - running_max  # <= 0
    with np.errstate(divide='ignore', invalid='ignore'):
        dd_pct = np.where(running_max != 0.0, drawdown / running_max, 0.0)
    max_drawdown_percent = float(abs(np.min(dd_pct)) * 100.0) if dd_pct.size > 0 else 0.0

    return {
        "total_trades": total_trades,
        "nb_wins": nb_wins,
        "nb_losses": nb_losses,
        "win_rate": round(win_rate, 2),
        "total_pnl": round(total_pnl, 2),
        "profit_factor": profit_factor,  # laissé tel quel pour être formaté à l'affichage
        "avg_trade_pnl_percent": round(avg_trade_pnl_percent, 2),
        "sharpe_ratio": round(sharpe_ratio, 2),
        "max_drawdown_percent": round(max_drawdown_percent, 2),
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
    """Met en forme le message de rapport pour Telegram.
    Si balance est None, tente de le récupérer automatiquement (exchange puis DB)."""
    # Tentative de récupération automatique du solde si non fourni
    if balance is None:
        try:
            import trader  # import local pour éviter de casser les autres modules
            import database

            ex = None
            live_balance: Optional[float] = None

            # 1) Essayer de créer une instance d'exchange
            try:
                if hasattr(trader, "create_exchange"):
                    ex = trader.create_exchange()  # type: ignore[attr-defined]
            except Exception:
                ex = None

            # 2) Lire le solde via les helpers dispo dans trader
            if ex is not None:
                try:
                    if hasattr(trader, "get_portfolio_equity_usdt"):
                        # Helper d'équity globale si présent
                        live_balance = float(trader.get_portfolio_equity_usdt(ex))  # type: ignore[attr-defined]
                    elif hasattr(trader, "get_usdt_balance"):
                        # Fallback sur la fonction de solde USDT que tu utilises déjà
                        live_balance = float(trader.get_usdt_balance(ex))  # type: ignore[attr-defined]
                except Exception:
                    live_balance = None

            # 3) Fallback : dernière valeur connue en DB (CURRENT_BALANCE_USDT)
            if live_balance is None:
                try:
                    raw = database.get_setting("CURRENT_BALANCE_USDT", None)
                    if raw is not None:
                        live_balance = float(raw)
                except Exception:
                    live_balance = None

            # 4) Si on a réussi à récupérer quelque chose, on l'utilise
            if live_balance is not None:
                balance = live_balance
        except Exception:
            # En cas de problème, on laisse balance à None pour afficher "non disponible"
            pass

    balance_str = f"<code>{balance:.2f} USDT</code>" if balance is not None else "<i>(non disponible)</i>"
    header = f"<b>{title}</b>\n\n💰 <b>Solde Actuel:</b> {balance_str}\n"
    
    if stats.get("total_trades", 0) < 1:
        return header + "\n- Pas assez de données de trades pour générer un rapport."

    # >>> PF: formatage robuste (— pour indéfini, ∞ si aucune perte)
    pf = stats.get('profit_factor', None)
    if pf is None:
        pf_str = "—"
    elif pf == float('inf') or pf == math.inf:
        pf_str = "∞"
    else:
        pf_str = f"{pf:.2f}"

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

