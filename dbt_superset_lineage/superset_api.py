import logging

import requests

logger = logging.getLogger(__name__)


class Superset:
    """A class for accessing the Superset API in an easy way."""

    def __init__(self, api_url, access_token=None, refresh_token=None, user=None, password=None):
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
        self.user = user
        self.password = password

        if self.access_token is None and self.user is not None and self.password is not None:
            self._login()

    def _login(self):
        logger.info("Logging in with username/password")

        body = {
            'provider': 'db',
            'username': self.user,
            'password': self.password,
            'refresh': True 
        }
        url = self.api_url + '/security/login'
        res = requests.request('POST', url, headers={},
                           json=body)

        self.access_token = res.json()['access_token']
        self.refresh_token = res.json()['refresh_token']

        logger.info("Logged in successfully")
        return True

    def request(self, method, endpoint, refresh_token_if_needed=True, **request_kwargs):
        """Executes a request against the Superset API.

        Args:
            method: HTTP method to use.
            endpoint: Endpoint to use.
            refresh_token_if_needed: Whether the ``access_token`` should be automatically refreshed
                if needed.
            **request_kwargs: Any ``requests.request`` arguments to use.

        Returns:
            A dictionary containing response body parsed from JSON.

        Raises:
            HTTPError: There is an HTTP error (detected by ``requests.Response.raise_for_status``)
                even after retrying with a fresh ``access_token``.
        """

        logger.info("About to %s execute request for endpoint %s", method, endpoint)

        session = requests.Session()
        session.headers['Authorization'] = 'Bearer ' + self.access_token
        session.headers['Content-Type'] = 'application/json'

        csrf_url = self.api_url + '/security/csrf_token/'
        csrf_res = session.get(csrf_url)
        csrf_token = csrf_res.json()['result']
        session.headers['Referer']= csrf_url
        session.headers['X-CSRFToken'] = csrf_token

        url = self.api_url + endpoint
        #res = requests.request(method, url, headers=self._headers(**headers), **request_kwargs)
        res = session.request(method, url, **request_kwargs)

        logger.debug("Request finished with status: %d", res.status_code)

        res.raise_for_status()
        return res.json()
