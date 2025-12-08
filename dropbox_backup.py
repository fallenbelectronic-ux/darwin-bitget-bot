#dropbox_backup.py
"""
Backup automatique DB Render vers Dropbox
Version CORRIGÉE - Darwin Bot
"""
import os
import glob
import sqlite3
import csv
from datetime import datetime, timedelta
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_PATH_OVERRIDE = "/var/data/darwin_bot.db"
DROPBOX_TOKEN = os.getenv('DROPBOX_TOKEN', '')
DROPBOX_FOLDER = '/TradingBot_Backups'

EXPORT_DB = True
EXPORT_CSV = True
EXPORT_SUMMARY = True

# ============================================================================
# FONCTIONS
# ============================================================================

def init_dropbox():
    """Initialise Dropbox."""
    try:
        import dropbox as dbx_module
        
        if not DROPBOX_TOKEN:
            print("❌ DROPBOX_TOKEN manquant")
            return None
        
        dbx = dbx_module.Dropbox(DROPBOX_TOKEN)
        account = dbx.users_get_current_account()
        print(f"✅ Dropbox connecté : {account.email}")
        return dbx
    
    except ImportError:
        print("❌ Module dropbox manquant")
        print("👉 Ajoutez 'dropbox' dans requirements.txt")
        return None
    except Exception as e:
        print(f"❌ Erreur Dropbox : {e}")
        return None

def create_dropbox_folder(dbx, folder_path):
    """Crée dossier Dropbox."""
    try:
        import dropbox
        dbx.files_get_metadata(folder_path)
    except:
        try:
            import dropbox
            dbx.files_create_folder_v2(folder_path)
            print(f"   ✅ Dossier créé")
        except:
            pass

def upload_to_dropbox(dbx, local_file, dropbox_path):
    """Upload vers Dropbox."""
    try:
        import dropbox
        
        with open(local_file, 'rb') as f:
            dbx.files_upload(
                f.read(),
                dropbox_path,
                mode=dropbox.files.WriteMode.overwrite
            )
        
        print(f"   ☁️ Uploadé : {os.path.basename(dropbox_path)}")
        
        try:
            link = dbx.sharing_create_shared_link_with_settings(dropbox_path)
            dl_link = link.url.replace('?dl=0', '?dl=1')
            print(f"   🔗 Lien : {dl_link}")
            return dl_link
        except:
            try:
                links = dbx.sharing_list_shared_links(path=dropbox_path).links
                if links:
                    dl_link = links[0].url.replace('?dl=0', '?dl=1')
                    print(f"   🔗 Lien : {dl_link}")
                    return dl_link
            except:
                pass
        
        return True
    
    except Exception as e:
        print(f"   ❌ Erreur upload : {e}")
        return False

def export_csv_from_db(db_path, output_file):
    """Export CSV."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Récupérer tous les trades (adaptable)
        cursor.execute("""
            SELECT * FROM trades 
            ORDER BY open_timestamp DESC 
            LIMIT 1000
        """)
        
        trades = cursor.fetchall()
        
        if not trades:
            conn.close()
            print("   ⚠️ Aucun trade")
            return False
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=trades[0].keys())
            writer.writeheader()
            for trade in trades:
                writer.writerow(dict(trade))
        
        conn.close()
        print(f"   ✅ CSV créé ({len(trades)} trades)")
        return True
    
    except Exception as e:
        print(f"   ❌ Erreur CSV : {e}")
        return False

def export_summary_from_db(db_path, output_file):
    """Export résumé (avec détection automatique des colonnes)."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Détecter les colonnes disponibles
        cursor.execute("PRAGMA table_info(trades)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # 2. Identifier la colonne de profit
        profit_col = None
        for col in ['profit', 'pnl', 'realized_pnl', 'net_pnl', 'total_pnl']:
            if col in columns:
                profit_col = col
                break
        
        if not profit_col:
            print(f"   ⚠️ Colonnes disponibles : {', '.join(columns)}")
            print("   ⚠️ Aucune colonne profit trouvée")
            conn.close()
            # Créer un résumé minimal sans stats de profit
            report = f"""
╔══════════════════════════════════════════════════════════════╗
║         RAPPORT DARWIN BOT - {datetime.now().strftime('%Y-%m-%d %H:%M')}          ║
╚══════════════════════════════════════════════════════════════╝

⚠️ Statistiques de profit non disponibles
   Colonnes détectées : {', '.join(columns)}

═══════════════════════════════════════════════════════════════
"""
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"   ✅ Résumé créé (minimal)")
            return True
        
        # 3. Stats globales
        query = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN {profit_col} > 0 THEN 1 ELSE 0 END) as wins,
                SUM({profit_col}) as total_profit,
                AVG({profit_col}) as avg_profit,
                MAX({profit_col}) as best,
                MIN({profit_col}) as worst
            FROM trades
            WHERE status IN ('CLOSED', 'CLOSED_MANUAL', 'CLOSED_BY_EXCHANGE')
        """
        
        cursor.execute(query)
        row = cursor.fetchone()
        total = row[0] or 0
        wins = row[1] or 0
        total_profit = row[2] or 0.0
        avg_profit = row[3] or 0.0
        best = row[4] or 0.0
        worst = row[5] or 0.0
        
        winrate = (wins / total * 100) if total > 0 else 0.0
        
        # 4. Stats 30 jours
        cutoff_30d = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
        
        query_30d = f"""
            SELECT 
                COUNT(*) as total_30d,
                SUM({profit_col}) as profit_30d
            FROM trades
            WHERE status IN ('CLOSED', 'CLOSED_MANUAL', 'CLOSED_BY_EXCHANGE')
            AND open_timestamp > ?
        """
        
        cursor.execute(query_30d, (cutoff_30d,))
        row_30d = cursor.fetchone()
        total_30d = row_30d[0] or 0
        profit_30d = row_30d[1] or 0.0
        
        # 5. Positions ouvertes
        cursor.execute("SELECT COUNT(*) FROM trades WHERE status = 'OPEN'")
        open_pos = cursor.fetchone()[0] or 0
        
        conn.close()
        
        # 6. Générer rapport (CORRIGÉ - pas de f-string imbriqué)
        profit_per_trade_30d = (profit_30d / total_30d) if total_30d > 0 else 0.0
        
        report = """
╔══════════════════════════════════════════════════════════════╗
║         RAPPORT DARWIN BOT - %s          ║
╚══════════════════════════════════════════════════════════════╝

📊 STATISTIQUES GLOBALES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Total Trades         : %d
Positions Ouvertes   : %d

Wins                 : %d
Winrate              : %.2f%%

Profit Total         : %.2f USDT
Profit Moyen         : %.2f USDT
Meilleur Trade       : %.2f USDT
Pire Trade           : %.2f USDT


📈 PERFORMANCE 30 DERNIERS JOURS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Trades               : %d
Profit               : %.2f USDT
Profit par trade     : %.2f USDT


═══════════════════════════════════════════════════════════════
📦 Backup créé automatiquement
🔗 Téléchargez depuis : https://www.dropbox.com/home%s
═══════════════════════════════════════════════════════════════
""" % (
            datetime.now().strftime('%Y-%m-%d %H:%M'),
            total,
            open_pos,
            wins,
            winrate,
            total_profit,
            avg_profit,
            best,
            worst,
            total_30d,
            profit_30d,
            profit_per_trade_30d,
            DROPBOX_FOLDER
        )
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"   ✅ Résumé créé")
        return True
    
    except Exception as e:
        print(f"   ❌ Erreur résumé : {e}")
        import traceback
        traceback.print_exc()
        return False

def run_backup():
    """Backup complet."""
    
    print("=" * 70)
    print("☁️  BACKUP DARWIN BOT → DROPBOX")
    print("=" * 70)
    
    # 1. DB
    db_path = DB_PATH_OVERRIDE
    
    if not os.path.exists(db_path):
        print(f"❌ DB introuvable : {db_path}")
        return False
    
    db_size = os.path.getsize(db_path)
    print(f"📊 DB : {os.path.basename(db_path)}")
    print(f"📏 Taille : {db_size/1024:.1f} KB")
    
    # 2. Dropbox
    print("\n🔐 Connexion Dropbox...")
    dbx = init_dropbox()
    if not dbx:
        return False
    
    create_dropbox_folder(dbx, DROPBOX_FOLDER)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    success = 0
    
    # 3. Exports
    print(f"\n📦 Exports...")
    
    if EXPORT_DB:
        print("• DB complète...")
        if upload_to_dropbox(dbx, db_path, f"{DROPBOX_FOLDER}/darwin_bot_{timestamp}.db"):
            success += 1
    
    if EXPORT_CSV:
        print("• CSV...")
        csv_file = f"trades_{timestamp}.csv"
        if export_csv_from_db(db_path, csv_file):
            if upload_to_dropbox(dbx, csv_file, f"{DROPBOX_FOLDER}/{csv_file}"):
                success += 1
            try: os.remove(csv_file)
            except: pass
    
    if EXPORT_SUMMARY:
        print("• Résumé...")
        summary_file = f"summary_{timestamp}.txt"
        if export_summary_from_db(db_path, summary_file):
            if upload_to_dropbox(dbx, summary_file, f"{DROPBOX_FOLDER}/{summary_file}"):
                success += 1
            try: os.remove(summary_file)
            except: pass
    
    # 4. Résultat
    print("\n" + "=" * 70)
    if success > 0:
        print(f"✅ TERMINÉ : {success} fichiers uploadés")
        print(f"📁 https://www.dropbox.com/home{DROPBOX_FOLDER}")
        print("=" * 70)
        return True
    else:
        print("❌ ÉCHEC : Aucun fichier uploadé")
        print("=" * 70)
        return False

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    try:
        success = run_backup()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⛔ Arrêt")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERREUR FATALE : {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
