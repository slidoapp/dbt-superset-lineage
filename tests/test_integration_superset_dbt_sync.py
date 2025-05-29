# tests/test_integration_superset_dbt_sync.py

import os
import json
import pytest
import dbt_superset_lineage.push_descriptions as pd
from dbt_superset_lineage.push_descriptions import main

class StubResponse:
    def __init__(self, data):
        self._data = data
        # supply a dummy cookie header for CSRF logic
        self.headers = {'set-cookie': 'fake-csrf-cookie'}
        # supply a default HTTP status code
        self.status_code = 200

    def json(self):
        return self._data

    def raise_for_status(self):
        # No-op to simulate requests.Response.raise_for_status()
        return None

class FakeSuperset:
    """
    Minimal in-memory recorder of PUT calls.
    """
    def __init__(self):
        self.calls = []

    def record_put(self, endpoint, payload):
        self.calls.append({
            'method': 'PUT',
            'endpoint': endpoint,
            'json': payload
        })

@pytest.fixture(autouse=True)
def stub_out_requests(tmp_path, monkeypatch):
    # 1) Write fixture manifest.json
    manifest = {
        'nodes': {
            'model.project.my_schema.my_table': {
                'name': 'my_table',
                'schema': 'my_schema',
                'database': None,
                'columns': {
                    'col1': {
                        'description': 'dbt **column** description',
                        'meta': {'label': 'dbt column label'}
                    }
                },
                'description': 'dbt **table** description'
            }
        },
        'sources': {}
    }
    target_dir = tmp_path / 'target'
    target_dir.mkdir()
    (target_dir / 'manifest.json').write_text(json.dumps(manifest))

    # 2) Prepare our FakeSuperset
    fake_sup = FakeSuperset()

    # Counter to simulate pagination
    list_dataset_calls = {'count': 0}

    # 3) Define a single stub for requests.request
    def fake_request(method, url, headers=None, params=None, json=None, **kwargs):
        # CSRF token fetch
        if url.endswith("/security/csrf_token/"):
            return StubResponse({'result': {'csrf_token': 'fake-csrf'}})

        # List datasets with pagination simulation
        if method.upper() == 'GET' and url.endswith("/dataset/"):
            if list_dataset_calls['count'] == 0:
                list_dataset_calls['count'] += 1
                return StubResponse({
                    'result': [{
                        'id': 1,
                        'kind': 'physical',
                        'table_name': 'my_table',
                        'schema': 'my_schema',
                        'database': {'id': None}
                    }]
                })
            else:
                # no more pages
                return StubResponse({'result': []})

        # Fetch dataset details
        if method.upper() == 'GET' and url.endswith("/dataset/1"):
            return StubResponse({
                'result': {
                    'columns': [{
                        'column_name': 'col1',
                        'id': 10,
                        'expression': None,
                        'description': 'old_desc',
                        'verbose_name': 'Old Label'
                    }],
                    'description': 'old_dataset_desc',
                    'owners': [{'id': 100}]
                }
            })

        # Record any PUT
        if method.upper() == 'PUT':
            endpoint = url.split("/api/v1", 1)[1]
            fake_sup.record_put(endpoint, json)
            return StubResponse({'result': None})

        return StubResponse({})

    # 4) Patch the real requests in superset_api
    import dbt_superset_lineage.superset_api as api
    RequestsStub = type('R', (), {'request': staticmethod(fake_request)})
    monkeypatch.setattr(api, 'requests', RequestsStub)

    # 5) Patch any direct imports in push_descriptions (if present)
    monkeypatch.setattr(pd, 'requests', RequestsStub, raising=False)

    # 6) Expose fake_sup for the test to inspect
    pd._fake_sup = fake_sup

    return tmp_path

def test_full_integration_manifest_to_superset(tmp_path):
    """
    Run main() end-to-end and confirm the PUT payload.
    """
    main(
        dbt_project_dir=str(tmp_path),
        dbt_db_name=None,
        superset_url='http://fake-superset',
        superset_db_id=None,
        superset_refresh_columns=False,
        superset_pause_after_update=0,
        superset_access_token='fake_token',
        superset_refresh_token=None
    )

    fake = pd._fake_sup
    put_calls = [c for c in fake.calls if c['method'] == 'PUT']
    assert put_calls, "Expected at least one PUT call"

    put = put_calls[-1]
    assert put['endpoint'] == '/dataset/1?override_columns=false'

    payload = put['json']
    # Table description override
    assert payload['description'] == 'dbt table description'

    # Single column updated
    cols = payload['columns']
    assert len(cols) == 1
    col = cols[0]
    assert col['column_name'] == 'col1'
    assert col['id'] == 10
    assert 'dbt column description' in col['description']
    assert 'dbt column label' in col['verbose_name']

    # Owners preserved
    assert payload['owners'] == [100]
