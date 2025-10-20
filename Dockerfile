# --- Image Python ---
FROM python:3.11-slim

# --- Définir l'environnement ---
WORKDIR /app
ENV PYTHONUNBUFFERED=1

# --- Installation des dépendances en tant que ROOT ---
COPY requirements.txt .
# Met à jour pip et installe les paquets
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# --- Création de l'utilisateur et copie du code ---
COPY . .
RUN useradd --create-home appuser && chown -R appuser:appuser /app

# --- Changer pour l'utilisateur non-root ---
USER appuser

# --- La commande de démarrage est maintenant gérée par Render ---
# On laisse une commande par défaut au cas où
CMD ["python", "-u", "main.py"]
