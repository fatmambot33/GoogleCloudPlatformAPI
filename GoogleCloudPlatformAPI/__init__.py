from dotenv import load_dotenv

import os


load_dotenv()

GAM_VERSION = "v202305"
PYTZ_TIMEZONE = "GMT"
NETWORK_CODE = '5574'
APP_NAME = 'AdManager'

env_var = ["DEFAULT_GCS_BUCKET", "DATA_PATH",
           "DEFAULT_PROJECT_ID", "DEFAULT_BQ_DATASET"]
for var in env_var:
    if os.environ.get(var) is None:
        raise ValueError(f"Missing {var} in .env")
