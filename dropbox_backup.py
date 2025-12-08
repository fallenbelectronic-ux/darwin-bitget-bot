#dropbox_backup.py
"""
Backup automatique de la DB trading vers Dropbox
Version SIMPLIFIÉE - Prêt à l'emploi
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

# Token Dropbox (configuré dans Render Environment Variables)
DROPBOX_TOKEN = os.getenv('DROPBOX_TOKEN', '')

# Dossier Dropbox où sauvegarder (sera créé automatiquement)
DROPBOX_FOLDER = '/TradingBot_Backups'

# Types d'export
EXPORT_DB = True      # DB complète
EXPORT_CSV = True     # CSV des trades
EXPORT_SUMMARY = True # Résumé texte

# ============================================================================
# FONCTIONS
# ============================================================================

def find_db():
    """Trouve automatiquement le fichier DB."""
    db_files = glob.glob("*.db")
    if not db_files:
        db_files = glob.glob("**/*.db", recursive=True)
    return db_files[0] if db_files else None

def init_dropbox():
    """Initialise la connexion Dropbox."""
    try:
        import dropbox
        
        if not DROPBOX_TOKEN:
            print("❌ DROPBOX_TOKEN non configuré")
            print("👉 Configurez-le dans Render Environment Variables")
            return None
        
        dbx = dropbox.Dropbox(DROPBOX_TOKEN)
        
        # Tester la connexion
        try:
            account = dbx.users_get_current_account()
            print(f"✅ Connecté à Dropbox : {account.email}")
            return dbx
        except Exception as e:
            print(f"❌ Token Dropbox invalide : {e}")
            return None
    
    except ImportError:
        print("❌ Module 'dropbox' non installé")
        print("👉 Ajoutez 'dropbox' dans requirements.txt")
        return None

def create_dropbox_folder(dbx, folder_path):
    """Crée le dossier Dropbox s'il n'existe pas."""
    try:
        dbx.files_get_metadata(folder_path)
        print(f"✅ Dossier existe : {folder_path}")
    except Exception:
        try:
            dbx.files_create_folder_v2(folder_path)
            print(f"✅ Dossier créé : {folder_path}")
        except Exception as e:
            print(f"⚠️ Erreur création dossier : {e}")

def upload_to_dropbox(dbx, local_file: str, dropbox_path: str):
    """Upload un fichier vers Dropbox."""
    try:
        with open(local_file, 'rb') as f:
            file_data = f.read()
        
        # Upload avec mode overwrite
        dbx.files_upload(
            file_data,
            dropbox_path,
            mode=dropbox.files.WriteMode.overwrite
        )
        
        print(f"☁️ Uploadé : {dropbox_path}")
        
        # Générer lien de partage
        try:
            shared_link = dbx.sharing_create_shared_link_with_settings(dropbox_path)
            print(f"🔗 Lien : {shared_link.url}")
            return shared_link.url
        except Exception:
            # Lien existe déjà
            try:
                links = dbx.sharing_list_shared_links(path=dropbox_path).links
                if links:
                    print(f"🔗 Lien : {links[0].url}")
                    return links[0].url
            except:
                pass
        
        return True
    
    except Exception as e:
        print(f"❌ Erreur upload : {e}")
        return False

def export_csv_from_db(db_path: str, output_file: str):
    """Exporte les trades en CSV."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                id, symbol, side, regime, 
                entry_price, sl_price, tp_price, quantity,
                status, profit, 
                open_timestamp, close_timestamp,
                pyramid_count, partial_exits, breakeven_status,
                management_strategy, entry_atr, entry_rsi
            FROM trades
            ORDER BY open_timestamp DESC
            LIMIT 1000
        """)
        
        trades = cursor.fetchall()
        
        if not trades:
            print("⚠️ Aucun trade à exporter")
            conn.close()
            return False
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=trades[0].keys())
            writer.writeheader()
            for trade in trades:
                writer.writerow(dict(trade))
        
        conn.close()
        print(f"✅ CSV créé : {output_file} ({len(trades)} trades)")
        return True
    
    except Exception as e:
        print(f"❌ Erreur export CSV : {e}")
        return False

def export_summary_from_db(db_path: str, output_file: str):
    """Génère un résumé texte des stats."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Stats globales
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN profit > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN profit < 0 THEN 1 ELSE 0 END) as losses,
                AVG(profit) as avg_profit,
                SUM(profit) as total_profit,
                MAX(profit) as best,
                MIN(profit) as worst
            FROM trades
            WHERE status IN ('CLOSED', 'CLOSED_MANUAL', 'CLOSED_BY_EXCHANGE')
        """)
        
        row = cursor.fetchone()
        total, wins, losses, avg_profit, total_profit, best, worst = row or (0,0,0,0,0,0,0)
        
        winrate = (wins / total * 100) if total > 0 else 0
        
        # Stats 30 jours
        cutoff_30d = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_30d,
                SUM(profit) as profit_30d
            FROM trades
            WHERE status IN ('CLOSED', 'CLOSED_MANUAL', 'CLOSED_BY_EXCHANGE')
            AND open_timestamp > ?
        """, (cutoff_30d,))
        
        total_30d, profit_30d = cursor.fetchone() or (0, 0)
        
        # Pyramiding
        cursor.execute("""
            SELECT 
                COUNT(*) as with_pyramid,
                AVG(profit) as avg_profit_pyramid
            FROM trades
            WHERE pyramid_count > 0
            AND status IN ('CLOSED', 'CLOSED_MANUAL', 'CLOSED_BY_EXCHANGE')
        """)
        
        with_pyramid, avg_profit_pyramid = cursor.fetchone() or (0, 0)
        
        # Partial exits
        cursor.execute("""
            SELECT COUNT(*) as with_partial
            FROM trades
            WHERE partial_exits IS NOT NULL
            AND status IN ('CLOSED', 'CLOSED_MANUAL', 'CLOSED_BY_EXCHANGE')
        """)
        
        with_partial = cursor.fetchone()[0] or 0
        
        # Positions ouvertes
        cursor.execute("SELECT COUNT(*) FROM trades WHERE status = 'OPEN'")
        open_pos = cursor.fetchone()[0] or 0
        
        conn.close()
        
        # Générer rapport
        report = f"""
╔══════════════════════════════════════════════════════════════╗
║         RAPPORT DE TRADING - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}       ║
╚══════════════════════════════════════════════════════════════╝

📊 STATISTIQUES GLOBALES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Total Trades         : {total}
Positions Ouvertes   : {open_pos}

Wins                 : {wins}
Losses               : {losses}
Winrate              : {winrate:.2f}%

Profit Total         : {total_profit:.2f} USDT
Profit Moyen         : {avg_profit:.2f} USDT
Meilleur Trade       : {best:.2f} USDT
Pire Trade           : {worst:.2f} USDT


📈 PERFORMANCE 30 DERNIERS JOURS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Trades               : {total_30d}
Profit               : {profit_30d:.2f} USDT
Profit par trade     : {(profit_30d / total_30d if total_30d > 0 else 0):.2f} USDT


🎯 FEATURES AVANCÉES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Pyramiding
  Trades avec pyramid : {with_pyramid}
  Profit moyen        : {(avg_profit_pyramid or 0):.2f} USDT
  
Partial Exits
  Trades avec partial : {with_partial}


═══════════════════════════════════════════════════════════════
📦 Fichiers disponibles sur Dropbox :
   • trading_bot_YYYYMMDD_HHMMSS.db  (DB complète)
   • trades_export_YYYYMMDD_HHMMSS.csv  (données détaillées)
   • stats_summary_YYYYMMDD_HHMMSS.txt  (ce fichier)

🔗 Téléchargez depuis : https://www.dropbox.com/home{DROPBOX_FOLDER}
═══════════════════════════════════════════════════════════════
"""
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"✅ Résumé créé : {output_file}")
        return True
    
    except Exception as e:
        print(f"❌ Erreur export summary : {e}")
        return False

def run_backup():
    """Exécute le backup complet."""
    
    print("=" * 80)
    print("☁️  BACKUP AUTOMATIQUE VERS DROPBOX")
    print("=" * 80)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    # 1. Trouver la DB
    print("🔍 Recherche de la DB...")
    db_path = find_db()
    
    if not db_path:
        print("❌ Aucune DB trouvée")
        return False
    
    print(f"✅ DB trouvée : {db_path}")
    
    # Taille de la DB
    db_size = os.path.getsize(db_path)
    print(f"📏 Taille : {db_size / 1024:.2f} KB ({db_size / (1024*1024):.2f} MB)")
    
    # 2. Connexion Dropbox
    print("\n🔐 Connexion à Dropbox...")
    dbx = init_dropbox()
    
    if not dbx:
        return False
    
    # 3. Créer dossier si nécessaire
    create_dropbox_folder(dbx, DROPBOX_FOLDER)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    success_count = 0
    links = []
    
    # 4. Export DB complète
    if EXPORT_DB:
        print("\n📦 Export DB complète...")
        dropbox_path = f"{DROPBOX_FOLDER}/trading_bot_{timestamp}.db"
        
        result = upload_to_dropbox(dbx, db_path, dropbox_path)
        if result:
            success_count += 1
            if isinstance(result, str):
                links.append(('DB', result))
    
    # 5. Export CSV
    if EXPORT_CSV:
        print("\n📊 Export CSV...")
        csv_file = f"trades_export_{timestamp}.csv"
        
        if export_csv_from_db(db_path, csv_file):
            dropbox_path = f"{DROPBOX_FOLDER}/{csv_file}"
            result = upload_to_dropbox(dbx, csv_file, dropbox_path)
            
            if result:
                success_count += 1
                if isinstance(result, str):
                    links.append(('CSV', result))
                # Nettoyer fichier local
                try:
                    os.remove(csv_file)
                except:
                    pass
    
    # 6. Export Summary
    if EXPORT_SUMMARY:
        print("\n📋 Export résumé...")
        summary_file = f"stats_summary_{timestamp}.txt"
        
        if export_summary_from_db(db_path, summary_file):
            dropbox_path = f"{DROPBOX_FOLDER}/{summary_file}"
            result = upload_to_dropbox(dbx, summary_file, dropbox_path)
            
            if result:
                success_count += 1
                if isinstance(result, str):
                    links.append(('Summary', result))
                # Nettoyer fichier local
                try:
                    os.remove(summary_file)
                except:
                    pass
    
    # 7. Résultat final
    print("\n" + "=" * 80)
    if success_count > 0:
        print(f"✅ BACKUP TERMINÉ : {success_count} fichiers uploadés")
        print(f"📁 Dossier Dropbox : {DROPBOX_FOLDER}")
        
        if links:
            print("\n🔗 LIENS DE TÉLÉCHARGEMENT :")
            for name, link in links:
                # Convertir en lien de téléchargement direct
                download_link = link.replace('?dl=0', '?dl=1')
                print(f"   • {name:10s} : {download_link}")
        
        print("\n💡 Accédez à vos fichiers :")
        print(f"   👉 https://www.dropbox.com/home{DROPBOX_FOLDER}")
        print("=" * 80)
        return True
    else:
        print("❌ BACKUP ÉCHOUÉ : Aucun fichier uploadé")
        print("=" * 80)
        return False

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    try:
        success = run_backup()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⛔ Arrêt demandé")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERREUR FATALE : {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
