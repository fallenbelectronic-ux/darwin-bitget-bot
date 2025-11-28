# Fichier: reporting.py
from typing import List, Dict, Any, Optional
from tabulate import tabulate
import numpy as np
import math 
import io

def calculate_performance_stats(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calcule les statistiques de performance à partir d'une liste de trades.
    - Utilise 'pnl' (USDT) et 'pnl_percent' (%).
    - Si 'pnl_percent' est manquant, il est recalculé à la volée:
        pnl_percent = pnl / (entry_price * quantity) * 100
    """
    total_trades = len(trades)
    if total_trades < 1:
        return {"total_trades": 0}

    # Série des PnL en USDT (None -> 0.0)
    pnls_list = []
    for t in trades:
        try:
            v = t.get('pnl', 0.0)
            pnls_list.append(float(v if v is not None else 0.0))
        except Exception:
            pnls_list.append(0.0)
    pnls = np.array(pnls_list, dtype=float)

    # Série des PnL en % (DB ou recalcul à la volée)
    pnl_percents_list = []
    for t in trades:
        raw_pct = t.get('pnl_percent', None)

        if raw_pct is None:
            # Fallback: on essaie de recalculer à partir de pnl / (entry_price * quantity)
            try:
                pnl_val = float(t.get('pnl') or 0.0)
                entry_price = float(t.get('entry_price') or 0.0)
                quantity = float(t.get('quantity') or 0.0)
                notional = abs(entry_price * quantity)
                if notional > 0.0:
                    raw_pct = (pnl_val / notional) * 100.0
                else:
                    raw_pct = 0.0
            except Exception:
                raw_pct = 0.0

        try:
            pnl_percents_list.append(float(raw_pct if raw_pct is not None else 0.0))
        except Exception:
            pnl_percents_list.append(0.0)

    pnl_percents = np.array(pnl_percents_list, dtype=float)

    effective_trades = int(pnls.size)
    if effective_trades < 1:
        return {"total_trades": 0, "nb_wins": 0, "nb_losses": 0}

    wins = pnls[pnls > 0.0]
    losses = pnls[pnls < 0.0]

    nb_wins = int(wins.size)
    nb_losses = int(losses.size)

    total_pnl = float(np.sum(pnls))
    gross_profit = float(np.sum(wins)) if wins.size else 0.0
    gross_loss = abs(float(np.sum(losses))) if losses.size else 0.0  # positif

    # Winrate sur tous les trades considérés (BE inclus dans le dénominateur)
    win_rate = (nb_wins / effective_trades) * 100.0 if effective_trades else 0.0

    # Profit Factor robuste
    if gross_profit == 0.0 and gross_loss == 0.0:
        profit_factor = None                 # indéfini (0/0) => affichage "—"
    elif gross_loss == 0.0 and gross_profit > 0.0:
        profit_factor = math.inf             # aucune perte mais du profit => ∞
    else:
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0.0 else 0.0

    # Gain moyen par trade en %
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
    Solde :
      - utilise en priorité la valeur passée en argument si valide,
      - sinon lit CURRENT_BALANCE_USDT en DB,
      - en dernier recours tente un appel live à get_usdt_balance(ex).
    """
    # Normalisation de la valeur reçue
    try:
        if balance is not None:
            balance = float(balance)
    except Exception:
        balance = None

    # 1) Si balance n'est pas fournie ou invalide → tenter la DB d'abord
    if balance is None:
        try:
            import database
            raw = database.get_setting("CURRENT_BALANCE_USDT", None)
            if raw is not None:
                try:
                    balance = float(raw)
                except Exception:
                    balance = None
        except Exception:
            balance = None

    # 2) Si toujours None → tentative live via l'exchange (get_usdt_balance)
    if balance is None:
        try:
            import trader
            import database  # re-import safe

            ex = None
            live_balance: Optional[float] = None

            # Créer un exchange via main.create_exchange si dispo
            try:
                from main import create_exchange as _create_ex  # type: ignore
                ex = _create_ex()
            except Exception:
                ex = None

            if ex is not None:
                try:
                    # On se base sur la même logique que le bot pour le solde USDT
                    live_balance = float(trader.get_usdt_balance(ex))  # type: ignore[attr-defined]
                except Exception:
                    live_balance = None

            if live_balance is not None:
                balance = live_balance
                # Mémorisation en DB pour les prochains rapports
                try:
                    database.set_setting("CURRENT_BALANCE_USDT", f"{balance:.2f}")
                except Exception:
                    pass
        except Exception:
            balance = None

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


def build_equity_history(trades: List[Dict[str, Any]]) -> List[tuple]:
    """
    Construit l'historique equity en cumulant les PNL des trades fermés.
    Retourne une liste triée de tuples: (timestamp, equity).
    """
    history = []
    equity = 0.0

    # tri par timestamp
    try:
        trades_sorted = sorted(trades, key=lambda t: float(t.get("close_timestamp", 0)))
    except Exception:
        trades_sorted = trades

    for t in trades_sorted:
        try:
            pnl = float(t.get("pnl", 0.0) or 0.0)
        except Exception:
            pnl = 0.0

        try:
            ts = float(t.get("close_timestamp") or t.get("ts") or 0.0)
        except Exception:
            ts = 0.0

        # corriger format si millisecondes
        if ts > 10_000_000_000:
            ts /= 1000.0

        equity += pnl
        history.append((ts, equity))

    return history


def generate_equity_chart(trades: List[Dict[str, Any]]) -> Optional[io.BytesIO]:
    """
    Génère une courbe d'équité (évolution cumulée du PnL) au format PNG dans un buffer mémoire.

    - Cherche automatiquement un champ temporel dans chaque trade :
      ('close_time', 'closed_at', 'exit_time', 'timestamp', 'time', 'open_time', 'opened_at')
    - Convertit de façon robuste les timestamps (s / ms / µs / ns) en datetime.
    - Classe les trades par temps croissant.
    - Calcule la courbe d'équité comme somme cumulée du PnL (champ 'pnl').
    - Retourne un io.BytesIO contenant l'image PNG, ou None si pas assez de données.
    """
    import math
    import datetime
    import matplotlib.pyplot as plt
    from matplotlib import dates as mdates

    # ---------- Cas trivial : pas (ou trop peu) de trades ----------
    if not trades or len(trades) < 1:
        return None

    # ---------- Helpers internes ----------

    def _extract_time_field(trade: Dict[str, Any]) -> Any:
        """
        Tente de récupérer un champ temporel parmi plusieurs clés possibles.
        Retourne la valeur brute (int/float/str/datetime) ou None.
        """
        for key in (
            "close_time",
            "closed_at",
            "exit_time",
            "timestamp",
            "time",
            "open_time",
            "opened_at",
        ):
            if key in trade and trade[key] is not None:
                return trade[key]
        return None

    def _to_datetime_safe(raw: Any) -> Optional[datetime.datetime]:
        """
        Convertit de manière robuste un champ 'temps' en datetime timezone-aware (UTC).

        Gère les cas fréquents :
        - int / float en secondes, millisecondes, microsecondes, nanosecondes.
        - chaîne ISO-8601.
        - datetime déjà prêt.

        Si la valeur est incohérente (int énorme, etc.), renvoie None.
        """
        if raw is None:
            return None

        # Déjà un datetime
        if isinstance(raw, datetime.datetime):
            # Normalise en UTC aware si possible
            if raw.tzinfo is None:
                return raw.replace(tzinfo=datetime.timezone.utc)
            return raw.astimezone(datetime.timezone.utc)

        # Numérique : timestamp
        if isinstance(raw, (int, float)):
            try:
                v = float(raw)

                # Si la valeur est vraiment gigantesque, on réduit par palier
                # jusqu'à obtenir un ordre de grandeur cohérent.
                # - secondes ~ 1e9 (31 ans)
                # - ms      ~ 1e12
                # - µs      ~ 1e15
                # - ns      ~ 1e18
                # On divise par 1000 tant que c'est trop grand.
                # Limite de sécurité : on s'arrête avant que ça ne devienne absurde.
                steps = 0
                while abs(v) > 1e11 and steps < 5:
                    v /= 1000.0
                    steps += 1

                # Ultime garde-fou : si c'est encore trop grand, on abandonne
                if abs(v) > 1e11:
                    return None

                return datetime.datetime.fromtimestamp(v, tz=datetime.timezone.utc)
            except Exception:
                return None

        # Chaîne de caractères
        if isinstance(raw, str):
            raw_str = raw.strip()
            if not raw_str:
                return None

            # Essai ISO-8601 direct
            try:
                dt = datetime.datetime.fromisoformat(raw_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                else:
                    dt = dt.astimezone(datetime.timezone.utc)
                return dt
            except Exception:
                pass

            # Essai : chaîne numérique (timestamp)
            try:
                v = float(raw_str)
                steps = 0
                while abs(v) > 1e11 and steps < 5:
                    v /= 1000.0
                    steps += 1
                if abs(v) > 1e11:
                    return None
                return datetime.datetime.fromtimestamp(v, tz=datetime.timezone.utc)
            except Exception:
                return None

        # Type non géré
        return None

    def _get_pnl(trade: Dict[str, Any]) -> float:
        """
        Récupère le PnL du trade (USDT).
        Si le champ 'pnl' n'existe pas, renvoie 0.0.
        """
        try:
            return float(trade.get("pnl", 0.0) or 0.0)
        except Exception:
            return 0.0

    # ---------- Extraction & préparation des séries temps / équité ----------

    time_equity_points = []

    for tr in trades:
        raw_t = _extract_time_field(tr)
        dt = _to_datetime_safe(raw_t)
        if dt is None:
            continue  # on ignore les trades avec timestamp illisible

        pnl = _get_pnl(tr)
        time_equity_points.append((dt, pnl))

    # Si après filtrage il ne reste rien, on ne fait pas de graphe
    if not time_equity_points:
        return None

    # Tri par date croissante
    time_equity_points.sort(key=lambda x: x[0])

    # Construction de la courbe d'équité : somme cumulée des PnL
    dates = [t for (t, _) in time_equity_points]
    pnls = [p for (_, p) in time_equity_points]

    equity = []
    cum = 0.0
    for p in pnls:
        cum += p
        equity.append(cum)

    # ---------- Génération du graphique ----------

    # Conversion en format numérique matplotlib
    x = mdates.date2num(dates)
    y = equity

    fig, ax = plt.subplots(figsize=(8, 4))

    ax.plot(x, y, linewidth=1.5)
    ax.set_xlabel("Temps")
    ax.set_ylabel("Équité (PnL cumulé)")
    ax.set_title("Courbe d'équité")

    # Formatage de l'axe des dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d\n%H:%M"))
    fig.autofmt_xdate()

    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)

    # ---------- Export en PNG vers un buffer mémoire ----------
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=120)
    plt.close(fig)
    buf.seek(0)

    return buf


    except Exception as e:
        print(f"[generate_equity_chart] erreur: {e}")
        return None

