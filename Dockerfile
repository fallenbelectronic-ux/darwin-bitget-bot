# ========================================
# BASE IMAGE - Python 3.10 pour compatibilité TA-Lib
# ========================================
FROM python:3.10

# ========================================
# ÉTAPE 1 : Dépendances système pour TA-Lib
# ========================================
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    && rm -rf /var/lib/apt/lists/*

# ========================================
# ÉTAPE 2 : Compiler TA-Lib (bibliothèque C)
# ========================================
WORKDIR /tmp

RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib/ && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    ldconfig && \
    cd .. && \
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# ========================================
# ÉTAPE 3 : Configuration de l'application
# ========================================
WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Copier requirements.txt (pour référence seulement)
COPY requirements.txt .

# Installer TOUTES les dépendances dans le bon ordre avec versions compatibles
RUN pip install --no-cache-dir --root-user-action ignore --upgrade pip && \
    pip install --no-cache-dir --root-user-action ignore "numpy>=1.26.0,<2.0" && \
    pip install --no-cache-dir --root-user-action ignore "pandas>=2.1.0,<2.2.0" && \
    CFLAGS="-I/usr/include/ta-lib" LDFLAGS="-L/usr/lib" pip install --no-cache-dir --root-user-action ignore "TA-Lib==0.4.19" && \
    pip install --no-cache-dir --root-user-action ignore \
        "ccxt==4.2.25" \
        "python-telegram-bot==21.0.1" \
        "requests==2.31.0" \
        "python-dotenv==1.0.1"

# ========================================
# ÉTAPE 4 : Sécurité (utilisateur non-root)
# ========================================
# Copier le code source
COPY . .

# Créer utilisateur non-root et donner permissions
RUN useradd --create-home appuser && \
    chown -R appuser:appuser /app

# Passer à l'utilisateur non-root
USER appuser

# ========================================
# ÉTAPE 5 : Lancement
# ========================================
CMD ["python", "-u", "main.py"]
