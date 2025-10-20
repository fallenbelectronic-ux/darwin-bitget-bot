# --- Image Python ---
FROM python:3.11-slim

# --- Définir l'environnement ---
WORKDIR /app
ENV PYTHONUNBUFFERED=1

# --- Création d'un utilisateur non-root pour la sécurité ---
RUN useradd --create-home --shell /bin/bash appuser

# --- Installation des dépendances ---
COPY requirements.txt .

# On met à jour pip en premier, puis on installe les paquets.
# L'option --no-cache-dir est une bonne pratique.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# --- Copie du code de l'application ---
COPY . .
# Donner la propriété de tous les fichiers à notre nouvel utilisateur
RUN chown -R appuser:appuser /app

# --- Changer d'utilisateur ---
# À partir de ce point, tout est exécuté en tant que 'appuser'
USER appuser

# --- Commande de démarrage du bot ---
CMD ["python", "-u", "main.py"]```

