import json
import logging
import re

from bs4 import BeautifulSoup
from markdown import markdown
from requests import HTTPError

from .superset_api import Superset

logging.basicConfig(level=logging.INFO)


def get_datasets_from_superset(superset, superset_db_id):
    logging.info("Getting physical datasets from Superset.")

    page_number = 0
    datasets = []
    datasets_keys = set()
    while True:
        logging.info("Getting page %d.", page_number + 1)
        res = superset.request('GET', f'/dataset/?q={{"page":{page_number},"page_size":100}}')
        result = res['result']
        if result:
            for r in result:
                kind = r['kind']
                database_id = r['database']['id']

                if kind == 'physical' \
                        and (superset_db_id is None or database_id == superset_db_id):

                    dataset_id = r['id']

                    name = r['table_name']
                    schema = r['schema']
                    dataset_key = f'{schema}.{name}'  # used as unique identifier

                    dataset_dict = {
                        'id': dataset_id,
                        'key': dataset_key
                    }

                    # fail if it breaks uniqueness constraint
                    assert dataset_key not in datasets_keys, \
                        f"Dataset {dataset_key} is a duplicate name (schema + table) " \
                        "across databases. " \
                        "This would result in incorrect matching between Superset and dbt. " \
                        "To fix this, remove duplicates or add the ``superset_db_id`` argument."

                    datasets_keys.add(dataset_key)
                    datasets.append(dataset_dict)
            page_number += 1
        else:
            break

    return datasets


def get_tables_from_dbt(dbt_manifest, dbt_db_name):
    tables = {}
    for table_type in ['nodes', 'sources']:
        manifest_subset = dbt_manifest[table_type]

        for table_key_long in manifest_subset:
            table = manifest_subset[table_key_long]
            name = table['name']
            schema = table['schema']
            database = table['database']

            table_key_short = schema + '.' + name
            columns = table['columns']

            if dbt_db_name is None or database == dbt_db_name:
                # fail if it breaks uniqueness constraint
                assert table_key_short not in tables, \
                    f"Table {table_key_short} is a duplicate name (schema + table) " \
                    f"across databases. " \
                    "This would result in incorrect matching between Superset and dbt. " \
                    "To fix this, remove duplicates or add the ``dbt_db_name`` argument."

                tables[table_key_short] = {'columns': columns}

    assert tables, "Manifest is empty!"

    return tables


def refresh_columns_in_superset(superset, dataset_id):
    logging.info("Refreshing columns in Superset.")
    superset.request('PUT', f'/dataset/{dataset_id}/refresh')


def add_superset_columns(superset, dataset):
    logging.info("Pulling fresh columns info from Superset.")
    res = superset.request('GET', f"/dataset/{dataset['id']}")
    columns = res['result']['columns']
    dataset['columns'] = columns
    return dataset


def convert_markdown_to_plain_text(md_string):
    """Converts a markdown string to plaintext.

    The following solution is used:
    https://gist.github.com/lorey/eb15a7f3338f959a78cc3661fbc255fe
    """

    # md -> html -> text since BeautifulSoup can extract text cleanly
    html = markdown(md_string)

    # remove code snippets
    html = re.sub(r'<pre>(.*?)</pre>', ' ', html)
    html = re.sub(r'<code>(.*?)</code >', ' ', html)

    # extract text
    soup = BeautifulSoup(html, 'html.parser')
    text = ''.join(soup.findAll(text=True))

    # make one line
    single_line = re.sub(r'\s+', ' ', text)

    # make fixes
    single_line = re.sub('→', '->', single_line)
    single_line = re.sub('<null>', '"null"', single_line)

    return single_line


def merge_columns_info(dataset, tables):
    logging.info("Merging columns info from Superset and manifest.json file.")

    key = dataset['key']
    sst_columns = dataset['columns']
    dbt_columns = tables.get(key, {}).get('columns', {})

    columns_new = []
    for sst_column in sst_columns:

        # add the mandatory field
        column_new = {'column_name': sst_column['column_name']}

        # add optional fields only if not already None, otherwise it would error out
        for field in ['expression', 'filterable', 'groupby', 'python_date_format',
                      'verbose_name', 'type', 'is_dttm', 'is_active']:
            if sst_column[field] is not None:
                column_new[field] = sst_column[field]

        # add description
        if sst_column['column_name'] in dbt_columns \
                and 'description' in dbt_columns[sst_column['column_name']] \
                and sst_column['expression'] == '':  # database columns
            description = dbt_columns[sst_column['column_name']]['description']
            description = convert_markdown_to_plain_text(description)
        else:
            description = sst_column['description']
        column_new['description'] = description

        columns_new.append(column_new)

    dataset['columns_new'] = columns_new

    return dataset


def put_columns_to_superset(superset, dataset):
    logging.info("Putting new columns info with descriptions back into Superset.")

    body = {'columns': dataset['columns_new']}
    superset.request('PUT', f"/dataset/{dataset['id']}?override_columns=true", json=body)


def main(dbt_project_dir, dbt_db_name,
         superset_url, superset_db_id, superset_refresh_columns,
         superset_access_token, superset_refresh_token):

    # require at least one token for Superset
    assert superset_access_token is not None or superset_refresh_token is not None, \
           "Add ``SUPERSET_ACCESS_TOKEN`` or ``SUPERSET_REFRESH_TOKEN`` " \
           "to your environment variables or provide in CLI " \
           "via ``superset-access-token`` or ``superset-refresh-token``."

    superset = Superset(superset_url + '/api/v1',
                        access_token=superset_access_token, refresh_token=superset_refresh_token)

    logging.info("Starting the script!")

    sst_datasets = get_datasets_from_superset(superset, superset_db_id)
    logging.info("There are %d physical datasets in Superset.", len(sst_datasets))

    with open(f'{dbt_project_dir}/target/manifest.json') as f:
        dbt_manifest = json.load(f)

    dbt_tables = get_tables_from_dbt(dbt_manifest, dbt_db_name)

    for i, sst_dataset in enumerate(sst_datasets):
        logging.info("Processing dataset %d/%d.", i + 1, len(sst_datasets))
        sst_dataset_id = sst_dataset['id']
        try:
            if superset_refresh_columns:
                refresh_columns_in_superset(superset, sst_dataset_id)
            sst_dataset_w_cols = add_superset_columns(superset, sst_dataset)
            sst_dataset_w_cols_new = merge_columns_info(sst_dataset_w_cols, dbt_tables)
            put_columns_to_superset(superset, sst_dataset_w_cols_new)
        except HTTPError as e:
            logging.error("The dataset with ID=%d wasn't updated. Check the error below.",
                          sst_dataset_id, exc_info=e)

    logging.info("All done!")
