import snowflake.connector
import pandas as pd
import requests
import logging
import sys
from datetime import datetime
import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

_dotenv_loaded = load_dotenv(interpolate=False, override=True)  # True seulement si un vrai fichier .env a ete trouve (contexte local)


def _unescape_docker_dollar(value):
    """
    Le .env local est ecrit au format Docker Compose ($ double). On le de-double ici.
    Meme piege que celui rencontre avec l'ancien mot de passe Snowflake : ne pas
    re-appliquer dans le container (deja de-double par Docker via env_file).
    """
    if value is None:
        return value
    return value.replace('$$', '$')


if _dotenv_loaded and os.environ.get('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'):
    os.environ['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'] = _unescape_docker_dollar(
        os.environ['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']
    )

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IngestionError(Exception):
    """Erreur levée en cas d'échec critique de l'ingestion"""
    pass


# Mapping colonnes source (fichier Parquet NYC TLC) -> colonnes table RAW_TAXI_TRIPS.
# Le schema complet est documente dans Fiches_Modelisation_NYC_Taxi.md
COLUMN_MAPPING = {
    'VendorID': 'vendor_id',
    'tpep_pickup_datetime': 'tpep_pickup_datetime',
    'tpep_dropoff_datetime': 'tpep_dropoff_datetime',
    'passenger_count': 'passenger_count',
    'trip_distance': 'trip_distance',
    'RatecodeID': 'rate_code_id',
    'store_and_fwd_flag': 'store_and_fwd_flag',
    'PULocationID': 'pu_location_id',
    'DOLocationID': 'do_location_id',
    'payment_type': 'payment_type',
    'fare_amount': 'fare_amount',
    'extra': 'extra',
    'mta_tax': 'mta_tax',
    'tip_amount': 'tip_amount',
    'tolls_amount': 'tolls_amount',
    'improvement_surcharge': 'improvement_surcharge',
    'total_amount': 'total_amount',
    'congestion_surcharge': 'congestion_surcharge',
    'airport_fee': 'airport_fee',
}


def load_private_key():
    """
    Charge la cle privee RSA. Chemin par defaut calcule relativement a
    l'emplacement de ce script (scripts/ et keys/ sont des dossiers freres,
    en local comme dans le container) - resout automatiquement le bon
    chemin dans les deux contextes. Override possible via
    SNOWFLAKE_PRIVATE_KEY_PATH.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_key_path = os.path.join(script_dir, '..', 'keys', 'rsa_key.p8')
    key_path = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH', default_key_path)

    passphrase = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

    with open(key_path, 'rb') as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
        )

    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def download_taxi_data(year: int, month: int) -> str:
    """Telecharge un fichier Parquet mensuel NYC Taxi"""
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    local_path = f"/tmp/taxi_{year}_{month:02d}.parquet"

    try:
        logger.info(f"Telechargement depuis {url}")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Fichier telecharge : {local_path}")
        return local_path

    except requests.exceptions.RequestException as e:
        logger.error(f"Echec du telechargement : {e}")
        raise IngestionError(f"Impossible de telecharger {url}") from e


def get_snowflake_connection():
    """Connexion Snowflake par cle privee RSA. Base configurable (defaut : dev)."""
    try:
        database = os.environ.get('SNOWFLAKE_DATABASE', 'NYC_TAXI_ANALYTICS_DEV')
        logger.info(f"Connexion Snowflake (cle RSA) ciblant la base : {database}")

        private_key_bytes = load_private_key()

        return snowflake.connector.connect(
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            user=os.environ['SNOWFLAKE_USER'],
            private_key=private_key_bytes,
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'TAXI_WH'),
            database=database,
            schema='RAW',
            role=os.environ.get('SNOWFLAKE_ROLE', 'TAXI_DBT_ROLE'),
        )
    except KeyError as e:
        raise IngestionError(f"Variable d'environnement manquante : {e}")
    except snowflake.connector.errors.Error as e:
        raise IngestionError(f"Connexion Snowflake echouee : {e}")


def load_to_snowflake(file_path: str, source_file: str):
    """
    Charge le fichier Parquet dans Snowflake RAW, avec le schema complet.
    Idempotent : supprime les donnees du meme fichier source avant reinsertion.
    Note : trip_id est AUTOINCREMENT cote Snowflake, on ne l'envoie jamais depuis pandas.
    """
    conn = None
    try:
        df = pd.read_parquet(file_path)

        if df.empty:
            raise IngestionError(f"Fichier vide : {file_path}")

        missing_source_cols = [c for c in COLUMN_MAPPING if c not in df.columns]
        if missing_source_cols:
            logger.warning(f"Colonnes source absentes du fichier (ignorees) : {missing_source_cols}")

        available_cols = [c for c in COLUMN_MAPPING if c in df.columns]
        df = df[available_cols].rename(columns=COLUMN_MAPPING)

        df['_source_file'] = source_file
        df['_loaded_at'] = datetime.now()

        df.columns = [c.upper() for c in df.columns]

        conn = get_snowflake_connection()
        cursor = conn.cursor()

        logger.info(f"Suppression des donnees existantes pour {source_file}")
        cursor.execute(
            "DELETE FROM RAW_TAXI_TRIPS WHERE _source_file = %s",
            (source_file,)
        )
        deleted_rows = cursor.rowcount
        logger.info(f"{deleted_rows} lignes supprimees (si existantes)")

        from snowflake.connector.pandas_tools import write_pandas
        success, nchunks, nrows, _ = write_pandas(
            conn, df, 'RAW_TAXI_TRIPS'
        )

        if not success:
            raise IngestionError(f"Echec du chargement pour {source_file}")

        conn.commit()
        logger.info(f"{nrows} lignes chargees avec succes depuis {source_file}")

    except IngestionError:
        raise
    except Exception as e:
        logger.error(f"Erreur inattendue lors du chargement : {e}")
        if conn:
            conn.rollback()
        raise IngestionError(f"Echec du chargement de {source_file}") from e
    finally:
        if conn:
            conn.close()


def main(year: int, month: int):
    try:
        file_path = download_taxi_data(year, month)
        load_to_snowflake(file_path, f"yellow_tripdata_{year}-{month:02d}.parquet")
        logger.info("Ingestion terminee avec succes")
    except IngestionError as e:
        logger.error(f"INGESTION ECHOUEE : {e}")
        sys.exit(1)


if __name__ == "__main__":
    year, month = int(sys.argv[1]), int(sys.argv[2])
    main(year, month)