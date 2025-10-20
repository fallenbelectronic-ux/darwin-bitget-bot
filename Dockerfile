# --- Image Python ---
FROM python:3.11-slim

# --- Définir l'environnement ---
WORKDIR /app
ENV PYTHONUNBUFFERED=1

# --- Installation des dépendances en tant que ROOT ---
COPY requirements.txt .
# On ajoute le drapeau "ignore" aux DEUX commandes pip pour un log propre
RUN pip install --no-cache-dir --root-user-action ignore --upgrade pip && \
    pip install --no-cache-dir --root-user-action ignore -r requirements.txt

# --- Création de l'utilisateur et copie du code ---
COPY . .
RUN useradd --create-home appuser && chown -R appuser:appuser /app

# --- Changer pour l'utilisateur non-root ---
USER appuser

# --- La commande de démarrage est gérée par Render ---
CMD ["python", "-u", "main.py"]
