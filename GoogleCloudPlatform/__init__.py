import os
from dotenv import load_dotenv

load_dotenv()


if os.environ.get("DEFAULT_GCS_BUCKET") is None:
    raise ValueError("Missing DEFAULT_GCS_BUCKET in .env")


if os.environ.get("DEFAULT_BQ_DATASET") is None:
    raise ValueError("Missing DEFAULT_BQ_DATASET in .env")


if os.environ.get("DATA_PATH") is None:
    raise ValueError("Missing DATA_PATH in .env")

DATA_PATH = os.environ.get("DATA_PATH")
DEFAULT_GCS_BUCKET = os.environ.get("DEFAULT_GCS_BUCKET")
DEFAULT_BQ_DATASET = os.environ.get("DEFAULT_BQ_DATASET")

CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")


GAM_VERSION = "v202305"
PYTZ_TIMEZONE = "GMT"
NETWORK_CODE = '5574'
APP_NAME = 'AdManager'
