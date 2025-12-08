#dropbox_backup.py
"""
Backup automatique DB Render (Disque Persistant /var/data)
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

# Chemin DB (changez selon votre nom exact)
DB_PATH_OVERRIDE = "/var/data/darwin_bot.db"

DROPBOX_TOKEN = os.getenv('DROPBOX_TOKEN', '')
DROPBOX_FOLDER = '/TradingBot_Backups'

EXPORT_DB = True
EXPORT_CSV = True
EXPORT_SUMMARY = True

# ============================================================================
# FONCTIONS
# ============================================================================

def find_db():
    """Trouve DB sur disque persistant /var/data."""
    
    priority_paths = [
        "/var/data",
        "/var/data/database",
        "/var/data/db",
        ".",
    ]
    
    db_patterns = ["*.db", "trading*.db", "bot*.db"]
    
    print("🔍 Recherche DB...")
    
    for base_path in priority_paths:
        if not os.path.exists(base_path):
            continue
        
        print(f"   📂 {base_path}")
        
        for pattern in db_patterns:
            search_pattern = os.path.join(base_path, pattern)
            db_files = glob.glob(search_pattern)
            
            if db_files:
                db_path = db_files[0]
                db_size = os.path.getsize(db_path)
                print(f"   ✅ Trouvée : {os.path.basename(db_path)} ({db_size/1024:.1f} KB)")
                return db_path
    
    # Debug : lister /var/data
    print("\n📋 Contenu /var/data :")
    try:
        if os.path.exists("/var/data"):
            items = os.listdir("/var/data")
            if items:
                for item in items:
                    print(f"   • {item}")
            else:
                print("   (vide)")
    except Exception as e:
        print(f"   ⚠️ {e}")
    
    return None

def init_dropbox():
    """Initialise Dropbox."""
    try:
        import dropbox
        
        if not DROPBOX_TOKEN:
            print("❌ DROPBOX_TOKEN manquant")
            return None
        
        dbx = dropbox.Dropbox(DROPBOX_TOKEN)
        account = dbx.users_get_current_account()
        print(f"✅ Dropbox connecté : {account.email}")
        return dbx
    
    except ImportError:
        print("❌ Module dropbox manquant")
        return None
    except Exception as e:
        print(f"❌ Erreur Dropbox : {e}")
        return None

def create_dropbox_folder(dbx, folder_path):
    """Crée dossier Dropbox."""
    try:
        dbx.files_get_metadata(folder_path)
    except:
        try:
            dbx.files_create_folder_v2(folder_path)
            print(f"✅ Dossier créé : {folder_path}")
        except:
            pass

def upload_to_dropbox(dbx, local_file, dropbox_path):
    """Upload vers Dropbox."""
    try:
        with open(local_file, 'rb') as f:
            dbx.files_upload(
                f.read(),
                dropbox_path,
                mode=dropbox.files.WriteMode.overwrite
            )
        
        print(f"   ☁️ Uploadé : {os.path.basename(dropbox_path)}")
        
        try:
            link = dbx.sharing_create_shared_link_with_settings(dropbox_path)
            return link.url
        except:
            try:
                links = dbx.sharing_list_shared_links(path=dropbox_path).links
                return links[0].url if links else True
            except:
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
        
        cursor.execute("""
            SELECT * FROM trades 
            ORDER BY open_timestamp DESC 
            LIMIT 1000
        """)
        
        trades = cursor.fetchall()
        
        if not trades:
            conn.close()
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
    """Export résumé."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN profit > 0 THEN 1 ELSE 0 END) as wins,
                SUM(profit) as total_profit,
                AVG(profit) as avg_profit
            FROM trades
            WHERE status IN ('CLOSED', 'CLOSED_MANUAL', 'CLOSED_BY_EXCHANGE')
        """)
        
        total, wins, total_profit, avg_profit = cursor.fetchone() or (0,0,0,0)
        winrate = (wins / total * 100) if total > 0 else 0
        
        conn.close()
        
        report = f"""
═══════════════════════════════════════════════════════
   RAPPORT DE TRADING - {datetime.now().strftime('%Y-%m-%d %H:%M')}
═══════════════════════════════════════════════════════

Total Trades  : {total}
Wins          : {wins}
Winrate       : {winrate:.2f}%
Profit Total  : {total_profit:.2f} USDT
Profit Moyen  : {avg_profit:.2f} USDT

═══════════════════════════════════════════════════════
"""
        
        with open(output_file, 'w') as f:
            f.write(report)
        
        print(f"   ✅ Résumé créé")
        return True
    
    except Exception as e:
        print(f"   ❌ Erreur résumé : {e}")
        return False

def run_backup():
    """Backup complet."""
    
    print("=" * 70)
    print("☁️  BACKUP DROPBOX")
    print("=" * 70)
    
    # 1. Trouver DB
    if DB_PATH_OVERRIDE:
        print(f"📍 Chemin forcé : {DB_PATH_OVERRIDE}")
        db_path = DB_PATH_OVERRIDE if os.path.exists(DB_PATH_OVERRIDE) else None
    else:
        db_path = find_db()
    
    if not db_path:
        print("\n❌ ÉCHEC : DB introuvable")
        return False
    
    db_size = os.path.getsize(db_path)
    print(f"📏 Taille : {db_size/1024:.1f} KB")
    
    # 2. Dropbox
    print("\n🔐 Connexion Dropbox...")
    dbx = init_dropbox()
    if not dbx:
        return False
    
    create_dropbox_folder(dbx, DROPBOX_FOLDER)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    success = 0
    
    # 3. Uploads
    print(f"\n📦 Exports...")
    
    if EXPORT_DB:
        print("• DB complète...")
        if upload_to_dropbox(dbx, db_path, f"{DROPBOX_FOLDER}/trading_bot_{timestamp}.db"):
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
    else:
        print("❌ ÉCHEC : Aucun fichier uploadé")
    print("=" * 70)
    
    return success > 0

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
        print(f"\n❌ ERREUR : {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
