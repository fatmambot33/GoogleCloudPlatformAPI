from typing import List, Optional
from google.oauth2 import service_account, credentials
from googleads import oauth2
import os


class ClientCredentials:
    def __init__(self):
        self.credentials_path = os.environ.get(
            "GOOGLE_APPLICATION_CREDENTIALS")

    @property
    def gcp_credentials(self):
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        if self.credentials_path is not None:
            return service_account.Credentials.from_service_account_file(filename=self.credentials_path, scopes=scopes)
        else:
            return credentials.Credentials(scopes=scopes)  # type: ignore

    @property
    def get_service_account_client(self):
        scope = oauth2.GetAPIScope("ad_manager")
        if self.credentials_path is not None:
            return oauth2.GoogleServiceAccountClient(key_file=credentials, scope=scope)
        else:
            return oauth2.GoogleOAuth2Client()

    def get_cloudplatform(self, credentials_path: Optional[str] = None,
                          scopes: Optional[List[str]] = ["https://www.googleapis.com/auth/cloud-platform"]):
        if credentials_path is not None:
            return service_account.Credentials.from_service_account_file(filename=credentials_path, scopes=scopes)
        else:
            return credentials.Credentials(scopes=scopes)  # type: ignore


class ServiceAccount:
    @staticmethod
    def from_service_account_file(credentials: Optional[str] = None,
                                  scopes: Optional[List[str]] = ["https://www.googleapis.com/auth/cloud-platform"]):
        if credentials is None:
            credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        return service_account.Credentials.from_service_account_file(filename=credentials, scopes=scopes)

    @staticmethod
    def get_service_account_client(credentials: Optional[str] = None,
                                   scope: Optional[str] = "ad_manager"):
        if credentials is None:
            credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        return oauth2.GoogleServiceAccountClient(key_file=credentials,
                                                 scope=oauth2.GetAPIScope(scope))
