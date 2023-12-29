import logging
import json
import os
import requests

logger = logging.getLogger(__name__)

class RegisterException(Exception):
    """Exception raised when a dataset cannot be registered.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class Superset:
    """A class for accessing the Superset API in an easy way."""

    def __init__(self, api_url, access_token=None, refresh_token=None, user=None, password=None):
        """
        If ``access_token`` is None, attempts to obtain it using ``refresh_token``.

        Args:
            api_url: Base API URL of a Superset instance, e.g. https://my-superset/api/v1.
            access_token: Access token to use for accessing protected endpoints of the Superset                
            refresh_token: Refresh token to use for obtaining or refreshing the ``access_token``
            user: Superset username to use for obtaining or refreshing the ``access_token``
            password: Superset password to use for obtaining or refreshing the ``access_token``
        """

        self.api_url = api_url
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.user = user
        self.password = password

        if self.user is not None and self.password is not None:
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
        res = requests.request('POST', url, headers={}, json=body)

        if res.status_code != 200:
            logger.error("Login to Superset failed")
            exit(0)

        self.access_token = res.json()['access_token']
        self.refresh_token = res.json()['refresh_token']

        logger.info("Logged in successfully")
        return True

    def _refresh_token(self):
        logger.debug("Refreshing superset token")
        session = requests.Session()
        session.headers['Authorization'] = 'Bearer ' + self.refresh_token
        session.headers['Accept'] = 'application/json'
        url = self.api_url + '/security/refresh'

        res = session.request("POST", url)

        if res.status_code == 401:
            logger.info("Refresh token expired")
            exit(0)
        
        if res.status_code != 200:
            logger.info("Refresh token failed")
            exit(0)

        self.access_token= res.json()['access_token']

    def _request(self, method, endpoint, **request_kwargs):
        """Executes a request against the Superset API.

        Args:
            method: HTTP method to use.
            endpoint: Endpoint to use.
            **request_kwargs: Any ``requests.request`` arguments to use.

        Returns:
            A dictionary containing response body parsed from JSON.

        Raises:
            HTTPError: There is an HTTP error (detected by ``requests.Response.raise_for_status``)
                even after retrying with a fresh ``access_token``.

        For inspiration on how to do this more beautifully:
        https://github.com/metriql/metriql-superset/blob/main/metriql2superset/superset.py#L21
        https://github.com/apache/superset/issues/16398#issuecomment-1293583699
        """

        logger.info("About to %s execute request for endpoint %s", method, endpoint)

        session = requests.Session()
        if json in request_kwargs:
            session.headers['Content-Type'] = 'application/json'
        session.headers['Accept'] = 'application/json'
        csrf_url = self.api_url + '/security/csrf_token/'

        for _ in range(2):
            session.headers['Authorization'] = 'Bearer ' + self.access_token
            csrf_res = session.get(csrf_url)
        
            if csrf_res.status_code != 401:
                break
            
            self._refresh_token()
           

        csrf_token = csrf_res.json()['result']
        session.headers['Referer']= csrf_url
        session.headers['X-CSRFToken'] = csrf_token

        url = self.api_url + endpoint
        #res = requests.request(method, url, headers=self._headers(**headers), **request_kwargs)
        res = session.request(method, url, **request_kwargs)

        logger.debug("Request finished with status: %d", res.status_code)

        res.raise_for_status()
        return res.json()
        
    def get_datasets(self, superset_db_id):
        logging.info("Getting all datasets from Superset.")

        page_number = 0
        datasets = {}

        while True:
            logging.info("Getting page %d.", page_number + 1)
            res = self._request('GET', f'/dataset/?q={{"page":{page_number},"page_size":100}}')
            result = res["result"]
            if not result:
                break;
            
            for r in result:
                if r["database"]["id"] == superset_db_id:
                    dataset_key = f'{r["schema"]}.{r["table_name"]}'         
                    datasets[dataset_key] = {"kind" : r["kind"],
                                            "dataset_id":r['id'],
                                            "schema": r["schema"],
                                            "table_name": r["table_name"]}
            page_number += 1
        
        return datasets

    def get_columns(self, dataset_id):
        logging.info("Pulling dataset columns info from Superset.")
        res = self._request('GET', f"/dataset/{dataset_id}")
        dataset={'name':res['result']['name'], 'id':res['id'], 'columns': res['result']['columns'], 'meta': res['result']}
        del dataset['meta']['columns']
        return dataset

    def create_physical_dataset(self, superset_db_id, table):
        logging.info("Registering database table in Superset: %s", table)

        schema_name, table_name = table.split('.')

        body = {
            "database": superset_db_id,
            "schema": schema_name,
            "table_name": table_name
        }

        self._request('POST', f"/dataset/", json=body)

    def refresh_dataset(self, dataset_id):
        logging.info("Refreshing columns in Superset.")
        self._request('PUT', f'/dataset/{dataset_id}/refresh')

    def put_columns(self, dataset, debug_dir):
        logging.info("Putting new columns info with descriptions back into Superset.")

        id = dataset['id']

        if debug_dir is not None:
            merged_dataset_file_path = os.path.join(debug_dir, f'merged__dataset_{id}.json')
            with open(merged_dataset_file_path, 'w') as fp:
                json.dump(dataset, fp, sort_keys=True, indent=4)


        body = dataset['meta_new']
        body['columns'] = dataset['columns_new']

        if debug_dir is not None:
            update_body_file_path = os.path.join(debug_dir, f'update_body__dataset_{id}.json')
            with open(update_body_file_path, 'w') as fp:
                json.dump(body, fp, sort_keys=True, indent=4)

        self._request('PUT', f"/dataset/{dataset['id']}?override_columns=true", json=body)

    def rename_dataset(self, dataset_id, new_name):
        logging.info("Rename dataset %d to %s.", dataset_id, new_name)
        try:
            self._request('POST', f"/dataset/duplicate", json={"base_model_id": dataset_id, "table_name": new_name})
        except requests.RequestException as e:
            # it means that renamed is already there, we have to do something
            # so we just forget the current one. This is extremely unlikely to cause issues
            logging.warning("Failed to rename the dataset %s.", e.response.json()['message'])
        # finally delete the old one
        self._request('DELETE', f"/dataset/{dataset_id}")

    def update_virtual_dataset(self, dataset_id, dataset):
        logging.info("Updating dataset %s.", str(dataset_id))
        override_columns = dataset and len(dataset.get('columns', []))>0
        print(json.dumps(dataset))
        self._request('PUT', f"/dataset/{dataset_id}?override_columns={override_columns}", json=dataset)
