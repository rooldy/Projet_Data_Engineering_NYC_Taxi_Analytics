"""
DAG secondaire : ingestion mensuelle des donnees taxi NYC TLC.

Separe du pipeline quotidien (nyc_taxi_pipeline) car le rythme de publication
des fichiers source est mensuel, pas quotidien. Declenche le telechargement
et le chargement du fichier Parquet du mois precedent l'execution.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/scripts')

default_args = {
    'owner': 'rooldy',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
}


def run_taxi_ingestion(**context):
    """
    Ingestion du fichier taxi du mois precedent la date d'execution logique du DAG.
    Ex : execution le 1er mars -> charge le fichier de fevrier.
    """
    import ingest_taxi_data
    logical_date = context['logical_date']
    year = logical_date.year
    month = logical_date.month - 1
    if month == 0:
        month = 12
        year -= 1
    ingest_taxi_data.main(year, month)


with DAG(
    dag_id='nyc_taxi_monthly_ingestion',
    default_args=default_args,
    description='Ingestion mensuelle des fichiers Parquet NYC TLC (mois precedent)',
    schedule_interval='@monthly',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['ingestion', 'taxi', 'monthly'],
) as dag:

    ingest_taxi_trips = PythonOperator(
        task_id='ingest_taxi_trips',
        python_callable=run_taxi_ingestion,
    )