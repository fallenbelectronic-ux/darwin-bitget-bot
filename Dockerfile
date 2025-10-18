# --- Image Python ---
FROM python:3.11-slim

# --- Répertoire de travail ---
WORKDIR /app

# --- Installation des dépendances ---
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Copie du code du bot ---
COPY . .

# --- Pour que les logs s'affichent en direct ---
ENV PYTHONUNBUFFERED=1

# --- Commande de démarrage du bot ---
CMD ["python", "-u", "main.py"]
