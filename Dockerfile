# ========================================
# BASE IMAGE
# ========================================
FROM python:3.11

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
    cd .. && \
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# Configurer les chemins pour TA-Lib
ENV LD_LIBRARY_PATH=/usr/lib:$LD_LIBRARY_PATH
ENV TA_LIBRARY_PATH=/usr/lib
ENV TA_INCLUDE_PATH=/usr/include

# ========================================
# ÉTAPE 3 : Configuration de l'application
# ========================================
WORKDIR /app
ENV PYTHONUNBUFFERED=1

# Copier requirements.txt
COPY requirements.txt .

# Installer pip et numpy D'ABORD (requis pour TA-Lib)
RUN pip install --no-cache-dir --root-user-action ignore --upgrade pip && \
    pip install --no-cache-dir --root-user-action ignore numpy==1.26.3

# Installer TA-Lib Python ENSUITE
RUN pip install --no-cache-dir --root-user-action ignore TA-Lib==0.4.28

# Installer le reste des dépendances
RUN pip install --no-cache-dir --root-user-action ignore -r requirements.txt

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
