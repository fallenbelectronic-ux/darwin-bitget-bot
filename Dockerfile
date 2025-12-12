# ========================================
# BASE IMAGE - Debian Bullseye pour meilleure compatibilité
# ========================================
FROM python:3.10-bullseye

# ========================================
# ÉTAPE 1 : Dépendances système complètes
# ========================================
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    gcc \
    g++ \
    make \
    libssl-dev \
    libffi-dev \
    python3-dev \
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
ENV LD_LIBRARY_PATH=/usr/lib:$LD_LIBRARY_PATH

# Copier requirements.txt
COPY requirements.txt .

# Installer dépendances Python dans le bon ordre
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir "numpy>=1.21.0,<1.24.0" && \
    pip install --no-cache-dir "pandas>=2.0.0,<2.1.0" && \
    pip install --no-cache-dir "TA-Lib==0.4.19" && \
    pip install --no-cache-dir -r requirements.txt

# ========================================
# ÉTAPE 4 : Sécurité (utilisateur non-root)
# ========================================
COPY . .

RUN useradd --create-home appuser && \
    chown -R appuser:appuser /app

USER appuser

# ========================================
# ÉTAPE 5 : Lancement
# ========================================
CMD ["python", "-u", "main.py"]
