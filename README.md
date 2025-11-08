# 🚦 Prédiction de trafic – 9 au 11 novembre

Ce dépôt regroupe l’ensemble du code, des données et des modèles utilisés pour prédire le **débit horaire** et le **taux d’occupation** sur trois axes parisiens :

- **Champs-Élysées**
- **Rue de la Convention**
- **Rue des Saints-Pères**

Les prédictions concernent les **9, 10 et 11 novembre** inclus.

---

## 📁 Structure générale du projet

### 📂 `code_final_complet/derniere_maj/`

Ce dossier contient **tout le code final exécuté pour générer les prédictions livrées**.

---

## 📊 Organisation des données

| Dossier | Description |
|---------|-------------|
| `datasets_axes_bruts/` | Données brutes issues de *Paris Data*, un fichier CSV par axe. |
| `datasets_externes_bruts/` | Données externes utilisées pour enrichir les modèles : météo, vacances scolaires, jours fériés, piétonnisations, etc. |
| `traitement_dataset_externes/` | Scripts de nettoyage et d’homogénéisation des données externes afin de les rendre compatibles entre elles. |
| `datasets_externes_clean/` | Données externes **nettoyées et formatées**, prêtes à être fusionnées avec les données trafic. |
| `traitements_remplissage_df/` | Ajout des données externes aux données trafic, analyses statistiques, création des features de lag. |
| `datasets_axes_with_all_features/` | Datasets finaux enrichis (trafic + météo + vacances + lag features). |

---

## 🤖 Modélisation

📂 `modeles/`  
Contient **un script par axe**, chacun réalisant :

- Chargement de son dataset enrichi  
- Entraînement des modèles (débit horaire et taux d’occupation)  
- Génération des prédictions pour les **9, 10 et 11 novembre**
- ✅ **Fichier final livré :** `predictions_finales.csv`

**Colonnes :**

| Colonne | Description |
|---------|-------------|
| `arc` | Nom de l’axe (Champs-Elysées, Convention, Saint-Pères) |
| `datetime` | Date/heure au format `YYYY-MM-DD HH:MM` |
| `debit_horaire` | Débit horaire prédit |
| `taux_occupation` | Taux d’occupation prédit |

---

---

## 📅 Compilation des prédictions finales

📂 `pred j+3/`  
Ce dossier contient :
  
- Les **prévisions météorologiques** sur les 9–11 novembre 
- Le dataframe regroupant les features nécessaires aux prédictions 
- La **fusion des prédictions dans un seul fichier final**  


## 📌 Schéma global du pipeline

```mermaid
flowchart TD
    A[Données brutes Paris Data] --> C
    B[Données externes brutes] --> D

    C[datasets_axes_bruts] --> E
    D[traitement_dataset_externes] --> E

    E[datasets_externes_clean] --> F
    F[traitements_remplissage_df] --> G
    G[datasets_axes_with_all_features] --> H
    H[modeles] --> I[pred j+3]
