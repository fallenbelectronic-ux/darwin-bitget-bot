# Charting.py
import io
import pandas as pd
import mplfinance as mpf
from typing import Dict, Any, Optional

def generate_trade_chart(symbol: str, df: pd.DataFrame, signal: Dict[str, Any]) -> Optional[io.BytesIO]:
    """Génère une image PNG (fond sombre) pour un trade, avec:
       - BB20 turquoise (lignes pleines) + MM20 turquoise pointillé
       - BB80 bleu (plus épais)      + MM80 bleu pointillé
       - Lignes Entrée / SL / TP
       - Bloc vert/rouge Entrée-TP / Entrée-SL (sur toute la fenêtre)
       - Marqueurs verticaux + flèches : Contact / Réaction / Entrée (si dans la fenêtre)
       Retourne un io.BytesIO prêt pour Telegram.
    """
    try:
        import matplotlib.pyplot as plt
        from matplotlib import dates as mdates
        from matplotlib.patches import Rectangle

        # Colonnes nécessaires (inclut bb20_mid pour tracer la MM20)
        required_cols = ['bb20_up', 'bb20_mid', 'bb20_lo',
                         'bb80_up', 'bb80_mid', 'bb80_lo']
        if not all(col in df.columns for col in required_cols):
            print("Erreur graphique: le DataFrame ne contient pas toutes les colonnes de BB nécessaires.")
            return None

        # Fenêtre compacte
        BARS = 20

        # Assurer un DatetimeIndex pour mplfinance
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df = df.copy()
                df.index = pd.to_datetime(df.index, utc=True)
            except Exception:
                return None

        # Sous-DataFrame OHLCV avec noms attendus par mplfinance
        df_plot = df[['open', 'high', 'low', 'close', 'volume']].tail(BARS).copy()
        if df_plot.empty:
            return None
        df_plot.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        # Thème sombre
        bg_fig   = '#121417'   # fond global
        bg_ax    = '#0f1215'   # fond axe
        grid_col = '#1f2937'

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

        # Couleurs BB
        col_turq = '#2dd4bf'   # BB20
        col_blue = '#60a5fa'   # BB80

        # Séries addplot alignées sur la même fenêtre
        seg = slice(-BARS, None)
        plots = [
            # BB20 (pleines) + MM20 (pointillé)
            mpf.make_addplot(df['bb20_up'].iloc[seg],  color=col_turq, linestyle='-',  width=1.2),
            mpf.make_addplot(df['bb20_lo'].iloc[seg],  color=col_turq, linestyle='-',  width=1.2),
            mpf.make_addplot(df['bb20_mid'].iloc[seg], color=col_turq, linestyle='--', width=1.2),

            # BB80 (plus épaisses) + MM80 (pointillé)
            mpf.make_addplot(df['bb80_up'].iloc[seg],  color=col_blue, linestyle='-',  width=1.6),
            mpf.make_addplot(df['bb80_lo'].iloc[seg],  color=col_blue, linestyle='-',  width=1.6),
            mpf.make_addplot(df['bb80_mid'].iloc[seg], color=col_blue, linestyle='--', width=1.2),
        ]

        # --- Lignes horizontales Entrée / SL / TP (comme la version de base) ---
        h_prices = [float(signal['entry']), float(signal['sl']), float(signal['tp'])]
        entry_price, sl_price, tp_price = h_prices
        h_colors = ['#60a5fa', '#ef4444', '#22c55e']  # bleu, rouge, vert
        h_widths = [1.2, 1.2, 1.2]

        fig, axes = mpf.plot(
            df_plot,
            type='candle',
            style=style,
            title=f"Setup de Trade: {symbol} ({signal['regime']})",
            ylabel='Prix (USDT)',
            addplot=plots,
            hlines=dict(hlines=h_prices, colors=h_colors,
                        linewidths=h_widths, alpha=0.95),
            returnfig=True,
            figsize=(12, 7)
        )

        # Axe principal
        try:
            ax = axes[0]
        except TypeError:
            ax = axes

        # -------------------------
        # Bloc position Entrée-TP / Entrée-SL
        # -------------------------
        x_left, x_right = ax.get_xlim()
        width = max(x_right - x_left, 0.0)

        def _add_box(y1: float, y2: float, color: str, alpha: float) -> None:
            if width <= 0:
                return
            y_bottom = min(y1, y2)
            height = abs(y2 - y1)
            if height <= 0:
                return
            rect = Rectangle(
                (x_left, y_bottom),
                width,
                height,
                facecolor=color,
                edgecolor=color,
                alpha=alpha,
                linewidth=1.0,
                zorder=1
            )
            ax.add_patch(rect)

        # LONG ou SHORT
        if tp_price > entry_price:
            # LONG
            _add_box(entry_price, tp_price, '#22c55e', 0.16)   # profit
            _add_box(sl_price, entry_price, '#ef4444', 0.18)   # perte
        else:
            # SHORT
            _add_box(tp_price, entry_price, '#22c55e', 0.16)   # profit
            _add_box(entry_price, sl_price, '#ef4444', 0.18)   # perte

        # -------------------------
        # Marqueurs Contact / Réaction / Entrée
        # -------------------------
        y_min, y_max = ax.get_ylim()
        y_range = max(y_max - y_min, 1e-9)

        def _resolve_bar_position(base_key: str) -> Optional[int]:
            """
            Renvoie l'indice (0..len(df_plot)-1) de la bougie 'base_key'
            (contact / reaction / entry) si elle est dans les 20 dernières
            bougies. Sinon -> None.
            
            VERSION AMÉLIORÉE : Essaie TOUS les champs possibles
            """
            # Liste exhaustive des clés possibles
            all_keys = [
                f"{base_key}_index",
                f"{base_key}_idx",
                f"{base_key}_bar",
                f"{base_key}Index",
                f"{base_key}Idx",
                base_key  # Parfois le signal contient juste "contact": 123
            ]
            
            # 1) Via index sur df complet
            for k in all_keys:
                if k in signal and signal[k] is not None:
                    try:
                        idx_full = int(signal[k])
                    except Exception:
                        continue
                    
                    # Vérifier que l'index est valide
                    if idx_full < 0 or idx_full >= len(df):
                        continue
                    
                    # Récupérer le timestamp de cette bougie
                    ts_full = df.index[idx_full]
                    
                    # Trouver la position dans df_plot (20 dernières bougies)
                    try:
                        # df_plot.index.get_loc() cherche le timestamp exact
                        loc = df_plot.index.get_loc(ts_full)
                        
                        # get_loc() peut renvoyer un int, slice, ou array
                        if isinstance(loc, slice):
                            loc = loc.start
                        if isinstance(loc, (list, tuple)):
                            loc = loc[0]
                        
                        return int(loc)
                    except (KeyError, IndexError):
                        # Ce timestamp n'est pas dans df_plot (trop vieux)
                        continue
            
            # 2) Via timestamp (optionnel mais utile)
            ts_keys = [f"{base_key}_ts", f"{base_key}_time", f"{base_key}_timestamp"]
            for k in ts_keys:
                if k in signal and signal[k] is not None:
                    raw = signal[k]
                    try:
                        # Convertir en timestamp pandas
                        if isinstance(raw, (int, float)):
                            if raw > 1e12:
                                ts = pd.to_datetime(int(raw), unit='ms', utc=True)
                            else:
                                ts = pd.to_datetime(int(raw), unit='s', utc=True)
                        else:
                            ts = pd.to_datetime(raw, utc=True)
                    except Exception:
                        continue
                    
                    # Vérifier que le timestamp est dans la fenêtre
                    if ts < df_plot.index[0] or ts > df_plot.index[-1]:
                        continue
                    
                    # Trouver la position la plus proche
                    try:
                        deltas = (df_plot.index - ts)
                        pos = int(deltas.abs().argmin())
                        return pos
                    except Exception:
                        continue
            
            return None

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

            # ligne verticale
            ax.axvline(x_dt, color=color, linestyle='--', linewidth=1.1, alpha=0.95)

            # haut de la bougie
            try:
                row = df_plot.iloc[pos]
                y_candle = float(row['High'])
            except Exception:
                y_candle = y_max - 0.2 * y_range

            # texte + flèche
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

        # -------------------------
        # Annotations texte à droite (version de base)
        # -------------------------
        last_x = ax.get_xlim()[1]
        labels = ['Entrée', 'SL', 'TP']
        for y, label, color in zip(h_prices, labels, h_colors):
            ax.text(
                last_x, y, f"  {label}",
                va='center', ha='left', color=color, fontsize=9,
                bbox=dict(facecolor='#0b0f14', alpha=0.7, edgecolor='none', pad=1.5)
            )

        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=110)
        buf.seek(0)

        plt.close(fig)
        return buf

    except Exception as e:
        print(f"Erreur de génération de graphique: {e}")
        return None
