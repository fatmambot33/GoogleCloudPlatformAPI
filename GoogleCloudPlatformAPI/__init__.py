from dotenv import load_dotenv

import os


load_dotenv()


env_var = ["DEFAULT_GCS_BUCKET", "DATA_PATH",
           "DEFAULT_PROJECT_ID", "DEFAULT_BQ_DATASET"]
for var in env_var:
    if os.environ.get(var) is None:
        raise ValueError(f"Missing {var} in .env")
