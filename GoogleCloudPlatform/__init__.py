from dotenv import load_dotenv
import os

load_dotenv()

env_var = ["DATA_PATH",
           "GOOGLE_APPLICATION_CREDENTIALS",
           "DEFAULT_BQ_DATASET",
           "DEFAULT_GCS_BUCKET"]
for var in env_var:
    if os.environ.get(var) is None:
        raise ValueError(f"Missing {var} in .env")
