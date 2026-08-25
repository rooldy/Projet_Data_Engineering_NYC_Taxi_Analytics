# Récapitulatif de session — 13 juillet 2026
## Projet NYC Taxi Analytics

---

## ✅ Ce qui est fait et validé

### Infrastructure Docker
- `docker-compose.yml` fonctionnel : Postgres, Airflow (webserver + scheduler), Metabase
- Redémarrage propre testé (`docker compose down` / `up`)
- Volumes montés correctement (`dags/`, `dbt_project/`, `scripts/`, `logs/`)
- Accès validés : Airflow sur `localhost:8080` (admin/admin), Metabase sur `localhost:3000`

### Snowflake
- 3 bases créées : `NYC_TAXI_ANALYTICS_DEV`, `NYC_TAXI_ANALYTICS_CI`, `NYC_TAXI_ANALYTICS`
- Schéma `RAW` créé dans les 3 bases, avec les grants (y compris `FUTURE TABLES`)
- Warehouse `TAXI_WH` (XSMALL, auto-suspend) créé
- Rôle `TAXI_DBT_ROLE` créé et attribué à `ROOLDY01`

### Tables RAW (dans `NYC_TAXI_ANALYTICS_DEV`)
- `RAW_WEATHER` : créée, 1 ligne chargée avec succès (test API OpenWeather)
- `RAW_TAXI_TRIPS` : créée, **2 964 624 lignes chargées** (janvier 2024)

### Scripts d'ingestion Python
- `scripts/ingest_weather_data.py` : fonctionnel
- `scripts/ingest_taxi_data.py` : fonctionnel, idempotent (delete+insert), gestion d'erreurs

### Fichiers dbt de base
- `dbt_project.yml`, `profiles.yml` (environnements dev/ci/prod) créés
- Premier modèle staging `stg_taxi_trips.sql` + `_staging.yml` créés
- **Pas encore testés** (`dbt run` jamais lancé)

---

## 🐛 Bugs résolus aujourd'hui (bon contenu pour entretien)

1. **Chemins relatifs Docker Compose** : `context: ..` vs `.` selon l'emplacement du fichier compose
2. **Conflit de build parallèle** : 3 services buildaient la même image simultanément → build unique sur `airflow-init` uniquement
3. **Pliage YAML incorrect** : `command: >` avec indentation inégale a cassé une commande multi-lignes → passage en liste `["-c", "..."]`
4. **Variable d'environnement shell résiduelle** : un `export $(cat .env | xargs)` avait pollué `SNOWFLAKE_PASSWORD` dans la session shell ; `load_dotenv()` ne l'écrasait pas par défaut → `unset` + `override=True`
5. **Casse des identifiants Snowflake** : `write_pandas` quote les colonnes du DataFrame telles quelles (minuscules) alors que la table stocke ses colonnes en majuscules (non quotées à la création) → mise en majuscules des colonnes avant insertion

---

## ⏳ Prochaine session — à faire

1. **Créer les tables staging/marts manquantes côté modèles dbt** (au moins `dim_zones`, `fct_trips`)
2. **Tester `dbt run --target dev`** pour la première fois, depuis le container Airflow ou en local
3. **Corriger les éventuelles erreurs de modèles** (probable au premier essai)
4. **Tester `dbt test`**
5. Si le temps le permet : premier lancement du DAG Airflow complet

---

## 💡 Commandes utiles pour reprendre

```bash
# Redémarrer l'environnement
cd Projet_Data_Engineering_NYC_Taxi_Analytics/nyc_taxi_project
docker compose up -d

# Vérifier que tout tourne
docker compose ps

# Se connecter au container Airflow pour lancer dbt
docker exec -it nyc_taxi_project-airflow-webserver-1 bash
cd /opt/airflow/dbt_project
dbt run --target dev
```

---

## 🔒 Rappel sécurité

Le mot de passe Snowflake et la clé OpenWeather ont été partagés en clair plusieurs fois au fil des sessions de débogage. À changer une fois le projet stabilisé, avant tout partage public du repo (GitHub, portfolio, etc.).
