# Charting.py
import io
import pandas as pd
import mplfinance as mpf
from typing import Dict, Any, Optional

def generate_trade_chart(symbol: str, df: pd.DataFrame, signal: Dict[str, Any]) -> Optional[io.BytesIO]:
    """Génère une image PNG (fond sombre) pour un trade.

    - Si df est None : fetch auto via utils.fetch_and_prepare_df
    - Utilise BB20 / BB80 déjà présents dans df
    - Affiche un bloc vert/rouge type 'position' (TP / SL autour de l'entrée)
      dont la zone commence SUR la bougie d'entrée et s'étend jusqu'à la
      dernière bougie visible (jamais dans le futur).
    - Marque les bougies de contact / réaction / entrée par des traits
      verticaux + labels avec flèches.
    """
    try:
        import os
        import ccxt
        import utils
        import matplotlib.pyplot as plt
        from matplotlib import dates as mdates
        from matplotlib.patches import Rectangle

        # 1) Si aucun DF fourni, on le prépare (OHLCV + BB requis par utils.fetch_and_prepare_df)
        if df is None:
            timeframe = os.getenv("TIMEFRAME", "1h")
            try:
                ex = ccxt.bitget({"enableRateLimit": True, "options": {"defaultType": "swap"}})
                if os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes"):
                    try:
                        ex.set_sandbox_mode(True)
                    except Exception:
                        pass
                df = utils.fetch_and_prepare_df(ex, symbol, timeframe)
            except Exception:
                df = None
            if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                return None

        # 2) Vérifs colonnes nécessaires BB
        bb_cols = ['bb20_up', 'bb20_mid', 'bb20_lo', 'bb80_up', 'bb80_mid', 'bb80_lo']
        if not all(c in df.columns for c in bb_cols):
            return None

        # 3) S'assurer d'un DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df = df.copy()
                df.index = pd.to_datetime(df.index, utc=True)
            except Exception:
                return None

        # 4) Sous-DF pour mplfinance (OHLCV)
        base_cols = ['open', 'high', 'low', 'close']
        if not all(c in df.columns for c in base_cols):
            return None

        df_plot = df[base_cols].copy()
        # Volume facultatif → colonne neutre si absente
        if 'volume' in df.columns:
            df_plot['volume'] = df['volume']
        else:
            df_plot['volume'] = 0.0

        # On ne garde que les 20 dernières bougies pour le visuel
        df_plot = df_plot.tail(20)
        if df_plot.empty:
            return None
        df_plot.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        # 5) Thème sombre & couleurs
        bg_fig, bg_ax, grid_col = '#121417', '#0f1215', '#1f2937'
        mc = mpf.make_marketcolors(
            up='#22c55e',
            down='#ef4444',
            edge='inherit',
            wick='inherit',
            volume='in'
        )
        style = mpf.make_mpf_style(
            base_mpf_style='nightclouds',
            marketcolors=mc,
            gridstyle='-',
            gridcolor=grid_col,
            facecolor=bg_ax,
            figcolor=bg_fig,
            edgecolor=bg_ax
        )
        col_turq, col_blue = '#2dd4bf', '#60a5fa'

        # 6) Séries supplémentaires (BB20/BB80) alignées sur df_plot
        seg = slice(-len(df_plot), None)
        df_bb = df[bb_cols].iloc[seg].copy()
        df_bb = df_bb.loc[df_plot.index]  # sécurité d’alignement

        plots = [
            mpf.make_addplot(df_bb['bb20_up'],  color=col_turq, linestyle='-',  width=1.2),
            mpf.make_addplot(df_bb['bb20_lo'],  color=col_turq, linestyle='-',  width=1.2),
            mpf.make_addplot(df_bb['bb20_mid'], color=col_turq, linestyle='--', width=1.2),
            mpf.make_addplot(df_bb['bb80_up'],  color=col_blue, linestyle='-',  width=1.6),
            mpf.make_addplot(df_bb['bb80_lo'],  color=col_blue, linestyle='-',  width=1.6),
            mpf.make_addplot(df_bb['bb80_mid'], color=col_blue, linestyle='--', width=1.2),
        ]

        # Helpers pour récupérer prix et positions de bougies
        def _get_price(keys) -> Optional[float]:
            for k in keys:
                try:
                    v = signal.get(k, None)
                except Exception:
                    v = None
                if v is None:
                    continue
                try:
                    return float(v)
                except Exception:
                    continue
            return None

        def _resolve_bar_position(base_key: str) -> Optional[int]:
            """Retourne la position (0..len(df_plot)-1) de la bougie
            base_key (contact / reaction / entry) si possible."""
            # 1) Index sur DF complet
            idx_keys = [f"{base_key}_index", f"{base_key}_idx", f"{base_key}_bar"]
            for k in idx_keys:
                if k in signal and signal[k] is not None:
                    try:
                        idx_full = int(signal[k])
                    except Exception:
                        continue
                    if idx_full < 0 or idx_full >= len(df):
                        continue
                    ts_full = df.index[idx_full]
                    try:
                        loc = df_plot.index.get_loc(ts_full)
                        if isinstance(loc, slice):
                            loc = loc.start
                        if isinstance(loc, (list, tuple)):
                            loc = loc[0]
                        return int(loc)
                    except Exception:
                        # si la bougie n'est pas dans la fenêtre df_plot,
                        # on tente la résolution par timestamp
                        continue

            # 2) Timestamp
            ts_keys = [f"{base_key}_ts", f"{base_key}_time", f"{base_key}_timestamp"]
            for k in ts_keys:
                if k in signal and signal[k] is not None:
                    raw = signal[k]
                    try:
                        if isinstance(raw, (int, float)):
                            # heuristique ms / s
                            if raw > 1e12:
                                ts = pd.to_datetime(int(raw), unit='ms', utc=True)
                            else:
                                ts = pd.to_datetime(int(raw), unit='s', utc=True)
                        else:
                            ts = pd.to_datetime(raw, utc=True)
                    except Exception:
                        continue
                    try:
                        # si le timestamp est clairement hors de la fenêtre affichée,
                        # on ne le force pas sur la dernière bougie
                        if ts < df_plot.index[0] or ts > df_plot.index[-1]:
                            continue
                        deltas = (df_plot.index - ts)
                        pos = int(deltas.abs().argmin())
                        return pos
                    except Exception:
                        continue

            return None

        # 7) Plot principal (sans hlines globales)
        regime = (signal.get('regime') or '').upper()
        fig, axes = mpf.plot(
            df_plot,
            type='candle',
            style=style,
            title=f"Setup de Trade: {symbol} ({regime})",
            ylabel='Prix (USDT)',
            addplot=plots,
            returnfig=True,
            figsize=(12, 7)
        )

        try:
            ax = axes[0]  # cas figure + volume
        except TypeError:
            ax = axes      # cas figure sans volume

        # 8) Bloc risque/rendement type "position"
        entry_price = _get_price(['entry', 'entry_price'])
        sl_price    = _get_price(['sl', 'sl_price', 'stop_loss'])
        tp_price    = _get_price(['tp', 'tp_price', 'take_profit'])

        if entry_price is not None and sl_price is not None and tp_price is not None:
            # position de la bougie d'entrée dans df_plot
            entry_pos = _resolve_bar_position('entry')

            # Si on n'a pas réussi à résoudre la bougie d'entrée,
            # on prend par défaut la dernière bougie visible
            if entry_pos is None or entry_pos < 0 or entry_pos >= len(df_plot):
                entry_pos = len(df_plot) - 1

            # Indices de début/fin pour la zone de position
            start_idx = max(0, min(entry_pos, len(df_plot) - 1))
            end_idx   = len(df_plot) - 1  # jamais de bougies futures : on s'arrête à la dernière connue

            # Largeur d’une bougie (en jours) à partir de l’échantillon courant
            x_left, x_right = ax.get_xlim()
            if len(df_plot) > 1:
                step_days = (df_plot.index[1] - df_plot.index[0]).total_seconds() / 86400.0
                if step_days <= 0:
                    step_days = (x_right - x_left) / max(len(df_plot), 1)
            else:
                step_days = (x_right - x_left) / 20.0

            # Coordonnées X de la bougie d'entrée (en unités date-num)
            entry_dt  = df_plot.index[start_idx]
            x0        = mdates.date2num(entry_dt)

            # Nombre de bougies couvertes par la zone (du start_idx jusqu'à end_idx inclus)
            n_bars = max(1, end_idx - start_idx + 1)
            box_width = max(step_days * n_bars, step_days)

            # Limite droite de la zone (toujours <= dernière bougie + 1 step)
            x_end = x0 + box_width

            def _add_box(y1: float, y2: float, color: str, alpha: float) -> None:
                if y1 is None or y2 is None:
                    return
                y_bottom = min(y1, y2)
                height   = abs(y2 - y1)
                if height <= 0:
                    return
                rect = Rectangle(
                    (x0, y_bottom),
                    box_width,
                    height,
                    facecolor=color,
                    edgecolor=color,
                    alpha=alpha,
                    linewidth=1.0,
                    zorder=1
                )
                ax.add_patch(rect)

            # Long ou short
            if tp_price > entry_price:
                # LONG
                _add_box(entry_price, tp_price, '#22c55e', 0.16)   # profit
                _add_box(sl_price, entry_price, '#ef4444', 0.18)   # perte
            else:
                # SHORT
                _add_box(tp_price, entry_price, '#22c55e', 0.16)   # profit
                _add_box(entry_price, sl_price, '#ef4444', 0.18)   # perte

            # Lignes SL / Entry / TP (sur toute la longueur de la zone)
            try:
                ax.hlines(sl_price,    x0, x_end, colors='#ef4444', linewidth=1.0)
                ax.hlines(entry_price, x0, x_end, colors='#60a5fa', linewidth=1.0)
                ax.hlines(tp_price,    x0, x_end, colors='#22c55e', linewidth=1.0)
            except Exception:
                pass

        # 9) Marqueurs verticaux + flèches : bougies de contact / réaction / entrée
        y_min, y_max = ax.get_ylim()
        y_range = max(y_max - y_min, 1e-9)

        contact_pos  = _resolve_bar_position('contact')
        reaction_pos = _resolve_bar_position('reaction')
        entry_pos    = _resolve_bar_position('entry')

        markers = [
            ("Contact",  contact_pos,  '#f97316'),
            ("Réaction", reaction_pos, '#eab308'),
            ("Entrée",   entry_pos,    '#3b82f6'),
        ]

        label_idx = 0
        for name, pos, color in markers:
            if pos is None or pos < 0 or pos >= len(df_plot):
                continue
            x_dt = df_plot.index[pos]

            # Ligne verticale
            ax.axvline(x_dt, color=color, linestyle='--', linewidth=1.1, alpha=0.95)

            # Niveau de la bougie (haut de chandelier pour la flèche)
            try:
                row = df_plot.iloc[pos]
                y_candle = float(row['High'])
            except Exception:
                y_candle = y_max - 0.2 * y_range

            # Position du texte au-dessus + flèche vers la bougie
            y_text = y_max - 0.04 * y_range * (label_idx + 1)
            ax.annotate(
                name,
                xy=(x_dt, y_candle),
                xytext=(x_dt, y_text),
                color=color,
                fontsize=9,
                ha='center',
                va='top',
                arrowprops=dict(
                    arrowstyle='->',
                    color=color,
                    linewidth=1.0,
                    shrinkA=0,
                    shrinkB=2
                ),
                bbox=dict(facecolor='#020617', edgecolor='none', alpha=0.7, pad=1.5)
            )
            label_idx += 1

        # 10) Export PNG en mémoire
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=110)
        buf.seek(0)

        plt.close(fig)
        return buf

    except Exception as e:
        print(f"Erreur de génération de graphique: {e}")
        return None
