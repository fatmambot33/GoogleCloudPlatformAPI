from typing import List, Optional
from google.oauth2 import service_account, credentials
from googleads import oauth2
import os
CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")


class ClientCredentials:
    credentials_path: Optional[str] = None

    @property
    def gcp_credentials(self):
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        if self.credentials_path is not None:
            return service_account.Credentials.from_service_account_file(filename=self.credentials_path, scopes=scopes)
        elif CREDENTIALS is not None:
            return service_account.Credentials.from_service_account_file(filename=CREDENTIALS, scopes=scopes)
        else:
            return credentials.Credentials(scopes=scopes)

    @property
    def get_service_account_client(self):
        scope = oauth2.GetAPIScope("ad_manager")
        if self.credentials_path is not None:
            return oauth2.GoogleServiceAccountClient(key_file=credentials, scope=scope)
        elif CREDENTIALS is not None:
            return oauth2.GoogleServiceAccountClient(key_file=CREDENTIALS, scope=scope)
        else:
            return oauth2.GoogleOAuth2Client()

    def get_cloudplatform(self, credentials_path: Optional[str] = None,
                          scopes: Optional[List[str]] = ["https://www.googleapis.com/auth/cloud-platform"]):
        if credentials_path is not None:
            return service_account.Credentials.from_service_account_file(filename=credentials_path, scopes=scopes)
        elif CREDENTIALS is not None:
            return service_account.Credentials.from_service_account_file(filename=CREDENTIALS, scopes=scopes)
        else:
            return credentials.Credentials(scopes=scopes)


class ServiceAccount:
    @staticmethod
    def from_service_account_file(credentials: Optional[str] = None, scopes: Optional[List[str]] = None):

        if credentials is None:
            credentials = CREDENTIALS

        if scopes is None:
            scopes = ["https://www.googleapis.com/auth/cloud-platform"]

        return service_account.Credentials.from_service_account_file(filename=credentials, scopes=scopes)

    @staticmethod
    def get_service_account_client(credentials: Optional[str] = None,
                                   scope: Optional[str] = "ad_manager"):
        if credentials is None:
            credentials = CREDENTIALS
        return oauth2.GoogleServiceAccountClient(key_file=credentials,
                                                 scope=oauth2.GetAPIScope(scope))
