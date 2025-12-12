FROM python:3.11-slim

# Variables d'environnement
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Répertoire de travail
WORKDIR /app

# Installation dépendances système (si besoin pour matplotlib/scipy)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copie requirements
COPY requirements.txt .

# Installation dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copie code source
COPY . .

# Commande par défaut
CMD ["python", "main.py"]
