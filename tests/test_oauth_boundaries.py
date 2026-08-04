"""Deterministic tests for OAuth credential selection boundaries."""

import importlib
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

oauthmod = importlib.import_module("GoogleCloudPlatformAPI.Oauth")


def test_client_credentials_uses_service_account_from_environment(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")
    credential = MagicMock()

    with patch.object(
        oauthmod.service_account.Credentials,
        "from_service_account_file",
        return_value=credential,
    ) as from_file:
        result = oauthmod.ClientCredentials().gcp_credentials

    assert result is credential
    from_file.assert_called_once_with(
        filename="service-account.json",
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


def test_client_credentials_uses_user_credentials_without_environment(monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    credential = MagicMock()

    with patch.object(
        oauthmod.credentials, "Credentials", return_value=credential
    ) as user_credentials:
        result = oauthmod.ClientCredentials().gcp_credentials

    assert result is credential
    user_credentials.assert_called_once_with(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )


def test_client_credentials_builds_ads_service_account_client(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")
    client = MagicMock()

    with ExitStack() as stack:
        get_scope = stack.enter_context(
            patch.object(oauthmod.oauth2, "GetAPIScope", return_value="scope")
        )
        service_client = stack.enter_context(
            patch.object(
                oauthmod.oauth2,
                "GoogleServiceAccountClient",
                return_value=client,
            )
        )
        result = oauthmod.ClientCredentials().get_service_account_client

    assert result is client
    get_scope.assert_called_once_with("ad_manager")
    service_client.assert_called_once_with(
        key_file="service-account.json", scope="scope"
    )


def test_client_credentials_builds_ads_user_client(monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    client = MagicMock()

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(oauthmod.oauth2, "GetAPIScope", return_value="scope")
        )
        user_client = stack.enter_context(
            patch.object(oauthmod.oauth2, "GoogleOAuth2Client", return_value=client)
        )
        result = oauthmod.ClientCredentials().get_service_account_client

    assert result is client
    user_client.assert_called_once_with()


def test_get_cloudplatform_supports_custom_service_account_scopes():
    credential = MagicMock()
    scopes = ["scope-a", "scope-b"]
    client_credentials = oauthmod.ClientCredentials()

    with patch.object(
        oauthmod.service_account.Credentials,
        "from_service_account_file",
        return_value=credential,
    ) as from_file:
        result = client_credentials.get_cloudplatform("service-account.json", scopes)

    assert result is credential
    from_file.assert_called_once_with(filename="service-account.json", scopes=scopes)


def test_service_account_helpers_use_environment_defaults(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")
    credential = MagicMock()
    ads_client = MagicMock()

    with ExitStack() as stack:
        from_file = stack.enter_context(
            patch.object(
                oauthmod.service_account.Credentials,
                "from_service_account_file",
                return_value=credential,
            )
        )
        get_scope = stack.enter_context(
            patch.object(oauthmod.oauth2, "GetAPIScope", return_value="scope")
        )
        service_client = stack.enter_context(
            patch.object(
                oauthmod.oauth2,
                "GoogleServiceAccountClient",
                return_value=ads_client,
            )
        )
        result = oauthmod.ServiceAccount.from_service_account_file()
        client = oauthmod.ServiceAccount.get_service_account_client()

    assert result is credential
    assert client is ads_client
    from_file.assert_called_once_with(
        filename="service-account.json",
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    get_scope.assert_called_once_with("ad_manager")
    service_client.assert_called_once_with(
        key_file="service-account.json", scope="scope"
    )
