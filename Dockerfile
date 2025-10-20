# --- Image Python ---
FROM python:3.11-slim

# --- Définir l'environnement ---
WORKDIR /app
ENV PYTHONUNBUFFERED=1

# --- Installation des dépendances ---
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Création d'un utilisateur et copie du code ---
COPY . .
RUN useradd --create-home appuser && chown -R appuser:appuser /app

# --- Changer d'utilisateur ---
USER appuser

# --- Commande de démarrage (sera outrepassée par Render pour plus de sécurité) ---
CMD ["python", "-u", "main.py"]
