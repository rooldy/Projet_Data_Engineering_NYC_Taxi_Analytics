import pandas as pd
import snowflake.connector
import logging
import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

_dotenv_loaded = load_dotenv(interpolate=False, override=True)  # True seulement si un vrai fichier .env a ete trouve (contexte local)


def _unescape_docker_dollar(value):
    """
    Le .env local est ecrit au format Docker Compose ($ double). On le de-double ici.
    Meme piege que celui rencontre avec l'ancien mot de passe Snowflake.
    """
    if value is None:
        return value
    return value.replace('$$', '$')


if _dotenv_loaded and os.environ.get('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'):
    os.environ['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'] = _unescape_docker_dollar(
        os.environ['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']
    )

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def load_private_key():
    """
    Charge la cle privee RSA. Chemin par defaut calcule relativement a
    l'emplacement de ce script (scripts/ et keys/ sont des dossiers freres,
    en local comme dans le container).
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


def main():
    logger.info("Telechargement du referentiel des zones taxi")
    df = pd.read_csv(ZONE_LOOKUP_URL)

    df = df.rename(columns={
        'LocationID': 'LOCATION_ID',
        'Borough': 'BOROUGH',
        'Zone': 'ZONE',
        'service_zone': 'SERVICE_ZONE',
    })
    df = df[['LOCATION_ID', 'BOROUGH', 'ZONE', 'SERVICE_ZONE']]

    private_key_bytes = load_private_key()

    conn = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        private_key=private_key_bytes,
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'TAXI_WH'),
        database=os.environ.get('SNOWFLAKE_DATABASE', 'NYC_TAXI_ANALYTICS_DEV'),
        schema='RAW',
        role=os.environ.get('SNOWFLAKE_ROLE', 'TAXI_DBT_ROLE'),
    )

    cursor = conn.cursor()
    cursor.execute("DELETE FROM RAW_TAXI_ZONES")

    from snowflake.connector.pandas_tools import write_pandas
    success, _, nrows, _ = write_pandas(conn, df, 'RAW_TAXI_ZONES')

    if not success:
        raise RuntimeError("Echec du chargement des zones")

    conn.commit()
    conn.close()
    logger.info(f"{nrows} zones chargees avec succes")


if __name__ == "__main__":
    main()