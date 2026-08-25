import requests
import snowflake.connector
import logging
import sys
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

_dotenv_loaded = load_dotenv(interpolate=False, override=True)  # True seulement si un vrai fichier .env a ete trouve (contexte local)


def _unescape_docker_dollar(value):
    """
    Le .env local est ecrit au format Docker Compose ($ double). On le de-double ici.
    IMPORTANT : dans un container, aucun .env n'est monte, load_dotenv() renvoie False,
    et Docker a deja injecte la valeur correctement de-doublee via env_file. Ne pas
    re-appliquer ce de-doublement dans ce cas (meme piege que celui rencontre avec
    l'ancien mot de passe Snowflake - la passphrase de la cle RSA contient les
    memes caracteres speciaux et suit donc exactement la meme regle).
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


def load_private_key():
    """
    Charge la cle privee RSA pour l'authentification Snowflake par cle,
    en remplacement du mot de passe (standard entreprise - voir
    Fiches_Modelisation / roadmap grande entreprise, priorite 1).

    Le chemin par defaut est calcule relativement a l'emplacement de CE
    script (pas au dossier courant d'execution) : scripts/ et keys/ sont
    toujours des dossiers freres, aussi bien en local (nyc_taxi_project/)
    que dans le container Airflow (/opt/airflow/). Ce calcul resout donc
    automatiquement le bon chemin dans les deux contextes, sans variable
    d'environnement differente selon l'endroit d'execution - le meme piege
    que celui rencontre avec le mot de passe est ainsi evite des le depart.
    Un override explicite via SNOWFLAKE_PRIVATE_KEY_PATH reste possible.
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


def fetch_weather_data() -> dict:
    """Recupere la meteo actuelle de New York via l'API OpenWeather"""
    try:
        api_key = os.environ['OPENWEATHER_API_KEY']
    except KeyError as e:
        raise IngestionError(f"Variable d'environnement manquante : {e}")

    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?q=New York&appid={api_key}&units=metric"
    )

    try:
        logger.info("Appel de l'API OpenWeather")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        return {
            # UTC plutot que l'heure locale de la machine, pour la coherence
            # temporelle avec le reste du pipeline (cf. Fiches_Modelisation).
            'weather_timestamp': datetime.now(timezone.utc).replace(tzinfo=None),
            'temperature': data['main']['temp'],
            'precipitation': data.get('rain', {}).get('1h', 0.0),
            'weather_condition': data['weather'][0]['main'],
            'humidity': data['main']['humidity'],
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Echec de l'appel API : {e}")
        raise IngestionError("Impossible de recuperer les donnees meteo") from e
    except (KeyError, IndexError) as e:
        logger.error(f"Reponse API inattendue : {e}")
        raise IngestionError("Format de reponse OpenWeather inattendu") from e


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


def load_weather_to_snowflake(weather_data: dict):
    """Insere une ligne de donnees meteo dans Snowflake RAW"""
    conn = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO RAW_WEATHER
            (weather_timestamp, temperature, precipitation, weather_condition, humidity)
            VALUES (%(weather_timestamp)s, %(temperature)s, %(precipitation)s,
                    %(weather_condition)s, %(humidity)s)
            """,
            weather_data,
        )

        conn.commit()
        logger.info(f"Donnees meteo chargees : {weather_data}")

    except Exception as e:
        logger.error(f"Erreur lors du chargement meteo : {e}")
        if conn:
            conn.rollback()
        raise IngestionError("Echec du chargement des donnees meteo") from e
    finally:
        if conn:
            conn.close()


def main():
    try:
        weather = fetch_weather_data()
        load_weather_to_snowflake(weather)
        logger.info("Ingestion meteo terminee avec succes")
    except IngestionError as e:
        logger.error(f"INGESTION METEO ECHOUEE : {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()