# ========================================
# BASE IMAGE
# ========================================
FROM python:3.11-slim

# ========================================
# ÉTAPE 1 : Dépendances système pour TA-Lib
# ========================================
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    libpython3.11-dev \
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
    cd .. && \
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# Configurer les chemins de bibliothèques
ENV LD_LIBRARY_PATH=/usr/lib:$LD_LIBRARY_PATH

# ========================================
# ÉTAPE 3 : Configuration de l'application
# ========================================
WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Copier requirements.txt
COPY requirements.txt .

# Installer dépendances Python (en tant que ROOT)
RUN pip install --no-cache-dir --root-user-action ignore --upgrade pip && \
    pip install --no-cache-dir --root-user-action ignore -r requirements.txt

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
