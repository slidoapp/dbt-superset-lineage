import logging

import requests

logger = logging.getLogger(__name__)


class Superset:
    """A class for accessing the Superset API in an easy way."""

    def __init__(self, api_url, access_token=None, refresh_token=None):
        """Instantiates the class.

        If ``access_token`` is None, attempts to obtain it using ``refresh_token``.

        Args:
            api_url: Base API URL of a Superset instance, e.g. https://my-superset/api/v1.
            access_token: Access token to use for accessing protected endpoints of the Superset
                API. Can be automatically obtained if ``refresh_token`` is not None.
            refresh_token: Refresh token to use for obtaining or refreshing the ``access_token``.
                If None, no refresh will be done.
        """

        self.api_url = api_url
        self.access_token = access_token
        self.refresh_token = refresh_token

        if self.access_token is None:
            self._refresh_access_token()

    def _headers(self, **headers):
        if self.access_token is None:
            return headers

        return {
            'Authorization': f'Bearer {self.access_token}',
            **headers,
        }

    def _refresh_access_token(self):
        logger.debug("Refreshing API token")

        if self.refresh_token is None:
            logging.warning("Cannot refresh access_token, refresh_token is None")
            return False

        res = self.request('POST', '/security/refresh',
                           headers={'Authorization': f'Bearer {self.refresh_token}'},
                           refresh_token_if_needed=False)
        self.access_token = res['access_token']

        logger.debug("Token refreshed successfully")
        return True

    def request(self, method, endpoint, refresh_token_if_needed=True, headers=None,
                **request_kwargs):
        """Executes a request against the Superset API.

        Args:
            method: HTTP method to use.
            endpoint: Endpoint to use.
            refresh_token_if_needed: Whether the ``access_token`` should be automatically refreshed
                if needed.
            headers: Additional headers to use.
            **request_kwargs: Any ``requests.request`` arguments to use.

        Returns:
            A dictionary containing response body parsed from JSON.

        Raises:
            HTTPError: There is an HTTP error (detected by ``requests.Response.raise_for_status``)
                even after retrying with a fresh ``access_token``.
        """

        logger.info("About to %s execute request for endpoint %s", method, endpoint)

        if headers is None:
            headers = {}

        url = self.api_url + endpoint
        res = requests.request(method, url, headers=self._headers(**headers), **request_kwargs)

        logger.debug("Request finished with status: %d", res.status_code)

        if refresh_token_if_needed and res.status_code == 401 \
                and res.json().get('msg') == 'Token has expired' and self._refresh_access_token():
            logger.debug("Retrying %s request for endpoint %s with refreshed token")
            res = requests.request(method, url, headers=self._headers(**headers))
            logger.debug("Request finished with status: %d", res.status_code)

        res.raise_for_status()
        return res.json()
