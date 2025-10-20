# --- Image Python ---
FROM python:3.11-slim

# --- Définir l'environnement ---
WORKDIR /app
ENV PYTHONUNBUFFERED=1

# --- Création d'un utilisateur non-root pour la sécurité ---
RUN useradd --create-home --shell /bin/bash appuser

# --- Installation des dépendances ---
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# --- Copie du code de l'application ---
COPY . .
RUN chown -R appuser:appuser /app

# --- Changer d'utilisateur ---
USER appuser

# --- Commande de démarrage du bot ---
CMD ["python", "-u", "main.py"]
