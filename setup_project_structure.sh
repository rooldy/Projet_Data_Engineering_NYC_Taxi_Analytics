#!/bin/bash

# ============================================================
# Setup automatique de l'arborescence - Projet NYC Taxi Analytics
# Snowflake + dbt + Airflow + Docker
# ============================================================

set -e  # Arrête le script si une commande échoue

PROJECT_NAME="nyc_taxi_project"

echo "Création du projet : $PROJECT_NAME"

# Racine du projet
mkdir -p "$PROJECT_NAME"
cd "$PROJECT_NAME"

# --- Docker ---
mkdir -p docker

# --- Airflow DAGs ---
mkdir -p dags

# --- Scripts d'ingestion Python ---
mkdir -p scripts
mkdir -p tests

# --- Logs Airflow (monté en volume) ---
mkdir -p logs

# --- Projet dbt ---
mkdir -p dbt_project/models/staging
mkdir -p dbt_project/models/intermediate
mkdir -p dbt_project/models/marts
mkdir -p dbt_project/snapshots
mkdir -p dbt_project/tests
mkdir -p dbt_project/macros
mkdir -p dbt_project/seeds

# --- GitHub Actions ---
mkdir -p .github/workflows

# ============================================================
# Création des fichiers vides / squelettes
# ============================================================

# Docker
touch docker/Dockerfile.airflow
touch docker-compose.yml
touch .env
touch .env.example

# Airflow DAGs
touch dags/dbt_pipeline_dag.py
touch dags/ingestion_dag.py

# Scripts Python
touch scripts/__init__.py
touch scripts/ingest_taxi_data.py
touch scripts/ingest_weather_data.py

# Tests Python
touch tests/__init__.py
touch tests/test_ingest_taxi_data.py

# dbt - fichiers de config
touch dbt_project/dbt_project.yml
touch dbt_project/packages.yml

# dbt - staging
touch dbt_project/models/staging/stg_taxi_trips.sql
touch dbt_project/models/staging/stg_weather.sql
touch dbt_project/models/staging/stg_taxi_zones.sql
touch dbt_project/models/staging/_staging.yml

# dbt - intermediate
touch dbt_project/models/intermediate/int_trips_with_zones.sql
touch dbt_project/models/intermediate/int_trips_with_weather.sql
touch dbt_project/models/intermediate/_intermediate.yml

# dbt - marts
touch dbt_project/models/marts/fct_trips.sql
touch dbt_project/models/marts/dim_zones.sql
touch dbt_project/models/marts/dim_date.sql
touch dbt_project/models/marts/mart_daily_revenue.sql
touch dbt_project/models/marts/mart_zone_performance.sql
touch dbt_project/models/marts/mart_weather_impact.sql
touch dbt_project/models/marts/_marts.yml
touch dbt_project/models/marts/_exposures.yml

# dbt - snapshots, tests, macros, seeds
touch dbt_project/snapshots/zones_snapshot.sql
touch dbt_project/tests/assert_positive_fares.sql
touch dbt_project/macros/generate_schema_name.sql
touch dbt_project/seeds/taxi_zone_lookup.csv

# CI/CD
touch .github/workflows/dbt_ci.yml

# Fichiers racine
touch requirements.txt
touch README.md
touch .gitignore

# ============================================================
# Contenu par défaut pour .gitignore
# ============================================================
cat > .gitignore << 'EOF'
# Environnement
.env
*.env

# Python
__pycache__/
*.pyc
.pytest_cache/
venv/
.venv/

# dbt
dbt_project/target/
dbt_project/dbt_packages/
dbt_project/logs/

# Airflow
logs/
*.log

# OS
.DS_Store

# IDE
.vscode/
.idea/
EOF

# ============================================================
# Contenu par défaut pour .env.example
# ============================================================
cat > .env.example << 'EOF'
# Snowflake
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=

# OpenWeather API
OPENWEATHER_API_KEY=

# Airflow (généré automatiquement, à ne pas modifier à la main)
AIRFLOW_UID=50000
EOF

# ============================================================
# Récapitulatif final
# ============================================================
echo ""
echo "Arborescence créée avec succès dans ./$PROJECT_NAME"
echo ""
echo "Structure générée :"
find . -type d | sort

echo ""
echo "Prochaines étapes :"
echo "  1. cd $PROJECT_NAME"
echo "  2. cp .env.example .env  (puis remplir vos credentials)"
echo "  3. git init && git add . && git commit -m 'Initial project structure'"
