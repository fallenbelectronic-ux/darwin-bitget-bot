# 🤖 Darwin Trading Bot

Bot de trading crypto automatisé avec stratégie Darwin et gestion de risque multi-niveaux.

---

## 🎯 Fonctionnalités

### **1. Détection de Signaux**
- **Tendance LONG** : Breakout BB80 + confirmation swing high + ADX > 25
- **Tendance SHORT** : Breakout BB80 inverse + confirmation swing low + ADX > 25
- **Contre-Tendance LONG** : Oversold RSI + support BB20 + divergence
- **Contre-Tendance SHORT** : Overbought RSI + résistance BB20 + divergence

### **2. Gestion des Positions**
- **Stop Loss dynamique** : Basé sur swings (tendance) ou ATR (contre-tendance)
- **Take Profit adaptatif** : BB80 (tendance) ou BB20_mid (contre-tendance)
- **Sizing** : 1-2% risque par trade, calculé selon ATR

### **3. Trailing Stop à 3 Niveaux**
- **Breakeven** : SL déplacé au prix d'entrée à +2% profit
- **Paliers** : SL progressif aux 25%, 50%, 75%, 90% du chemin vers TP
- **Final** : Trailing 0.5x ATR sous prix au-delà de 90% TP

### **4. Pyramiding**
- Ajout de positions sur breakouts BB80 ou nouveaux swings
- Max 2 ajouts par position (50% taille initiale chacun)
- Activé uniquement si position en profit > 2%

### **5. Partial Exits (Mode SPLIT)**
- Ferme 40% de la position à 50% du TP
- Ferme 30% additionnel à 75% du TP
- Ferme 30% restant au TP final
- Resserre SL après chaque sortie partielle

### **6. Filtres de Sécurité**
- **Liquidité** : Volume 24h minimum + spread maximum
- **Corrélation** : Limite positions même direction (max 3)
- **Sessions** : Trading pendant heures optimales (évite volatilité excessive)
- **Régime marché** : Adapte SL/TP selon type de marché (Tendance/CT/Range)

### **7. Gestion Avancée**
- Import et gestion automatique des positions manuelles
- Trailing et BE appliqués aux trades manuels
- Cache solde optimisé (évite rate limits exchange)
- Backup automatique vers Dropbox (DB + CSV + Stats)

---

## 📊 Comment le Bot Trade

### **Exemple Trade Tendance LONG**
```
1. Détection Signal
   - Prix casse BB80 à la hausse
   - Nouveau swing high confirmé
   - ADX > 25

2. Ouverture Position
   - Entry : 100 USDT @ 1.00
   - SL : 0.95 (sous dernier swing low)
   - TP : 1.10 (BB80 projeté)

3. Gestion Active
   - À +2% → Breakeven activé (SL à 1.00)
   - À +3% → Breakout BB80 détecté → Pyramiding +50 USDT
   - À 50% TP (1.05) → Ferme 40% position (partial exit)
   - À 75% TP (1.075) → Ferme 30% additionnel
   - À 100% TP (1.10) → Ferme 30% restant

4. Résultat
   - Total investi : 150 USDT (100 + 50 pyramiding)
   - Profit sécurisé via partials : 70% fermé avant TP final
   - Profit total : +15-20 USDT
```

### **Exemple Trade Contre-Tendance SHORT**
```
1. Détection Signal
   - RSI > 70 (overbought)
   - Prix touche BB20 résistance
   - Divergence baissière RSI

2. Ouverture Position
   - Entry : 100 USDT @ 2.00
   - SL : 2.03 (serré, 1.5% ATR)
   - TP : 1.95 (retour BB20_mid)

3. Gestion Active
   - À +1% → Breakeven activé
   - À 50% TP → Ferme 40%
   - À 75% TP → Ferme 30%
   - À 100% TP → Ferme 30% restant

4. Résultat
   - Profit sécurisé rapidement (CT = sorties rapides)
```

---

## 🔄 Cycle de Vie d'un Trade
```
Signal Détecté
    ↓
Vérification Filtres (Liquidité, Corrélation, Session)
    ↓
Calcul Sizing (Risque 1-2%, ATR)
    ↓
Ouverture Position (SL/TP dynamiques)
    ↓
Activation Breakeven (+2% profit)
    ↓
Trailing Progressif (25% → 50% → 75% → 90%)
    ↓
Pyramiding si Breakout (max 2 ajouts)
    ↓
Partial Exits (50% → 75% → 100% TP)
    ↓
Fermeture Finale
```

---

## 🎯 Indicateurs Utilisés

- **Bollinger Bands** : BB20 (moyenne), BB80 (extrêmes)
- **ADX** : Force de tendance (seuil 25)
- **RSI** : Momentum (14 périodes, seuils 30/70)
- **ATR** : Volatilité pour SL/TP/Sizing
- **Swings** : Points pivots hauts/bas pour SL placement
