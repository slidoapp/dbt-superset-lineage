import json
import logging
import re
import time

from bs4 import BeautifulSoup
from markdown import markdown
from requests import HTTPError

from .superset_api import Superset

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


def get_datasets_from_superset(superset, superset_db_id):
    logging.info("Getting physical datasets from Superset.")

    page_number = 0
    datasets = []
    datasets_keys = set()
    while True:
        logging.info("Getting page %d.", page_number + 1)

        payload = {
            'q': json.dumps({
                'page': page_number,
                'page_size': 100
            })
        }
        res = superset.request('GET', '/dataset/', params=payload)

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

    assert datasets, "There are no datasets in Superset!"

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
            description = table['description']

            if dbt_db_name is None or database == dbt_db_name:
                # fail if it breaks uniqueness constraint
                assert table_key_short not in tables, \
                    f"Table {table_key_short} is a duplicate name (schema + table) " \
                    f"across databases. " \
                    "This would result in incorrect matching between Superset and dbt. " \
                    "To fix this, remove duplicates or add the ``dbt_db_name`` argument."

                tables[table_key_short] = {'columns': columns, 'description': description}

    assert tables, "Manifest is empty!"

    return tables


def refresh_columns_in_superset(superset, dataset_id):
    logging.info("Refreshing columns in Superset.")
    superset.request('PUT', f'/dataset/{dataset_id}/refresh')


def add_superset_columns(superset, dataset):
    logging.info("Pulling fresh columns info from Superset.")

    res = superset.request('GET', f"/dataset/{dataset['id']}")
    result = res['result']

    dataset['columns'] = result['columns']
    dataset['description'] = result['description']
    dataset['owners'] = result['owners']

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
    text = soup.get_text(separator=' ')

    # make one line
    single_line = re.sub(r'\s+', ' ', text)

    # make fixes
    single_line = re.sub('→', '->', single_line)
    single_line = re.sub('<null>', '"null"', single_line)

    return single_line


def merge_columns_info(dataset, tables):
    logging.info("Merging columns info for dataset %s.", dataset['key'])
    key = dataset['key']
    sst_columns = dataset['columns']
    dbt_columns = tables.get(key, {}).get('columns', {})
    sst_description = dataset.get('description')
    dbt_description = tables.get(key, {}).get('description')
    sst_owners = dataset.get('owners', [])

    columns_new = []
    for sst_col in sst_columns:
        col_name = sst_col['column_name']
        col_id = sst_col['id']
        expr = sst_col.get('expression') or None

        # default to Superset metadata
        desc = sst_col.get('description')
        label = sst_col.get('verbose_name')

        if col_name in dbt_columns:
            # dbt description override for plain columns only
            dbt_col = dbt_columns[col_name]
            if 'description' in dbt_col and expr is None:
                desc = convert_markdown_to_plain_text(dbt_col['description'])

            # dbt label override for plain columns only
            meta = dbt_col.get('meta', {})
            if 'label' in meta and expr is None:
                label = convert_markdown_to_plain_text(meta['label'])

        columns_new.append({
            'column_name': col_name,
            'id': col_id,
            'description': desc,
            'verbose_name': label,
        })

    dataset['columns_new'] = columns_new

    # table‐level description
    if dbt_description is not None:
        dataset['description_new'] = convert_markdown_to_plain_text(dbt_description)
    else:
        dataset['description_new'] = sst_description

    # preserve owners
    dataset['owners_new'] = [o['id'] for o in sst_owners]
    return dataset



def check_columns_equal(lst1, lst2):
    return sorted(lst1, key=lambda c: c["id"]) == sorted(lst2, key=lambda c: c["id"])


def pause_after_update(superset_pause_after_update):
    if superset_pause_after_update:
        logging.info("Pausing the script for %d seconds to allow for databases to catch up with the update.",
                     superset_pause_after_update)
        time.sleep(superset_pause_after_update)
        logging.info("Resuming the script again.")


def put_descriptions_to_superset(superset, dataset, superset_pause_after_update):
    logging.info("Putting model and column descriptions into Superset.")

    description_new = dataset['description_new']
    columns_new = dataset['columns_new']
    owners_new = dataset['owners_new']

    description_old = dataset['description']
    columns_old = [{
        'column_name': col['column_name'],
        'id': col['id'],
        'description': col['description']
    } for col in dataset['columns']]

    if description_new != description_old or \
       not check_columns_equal(columns_new, columns_old):
        payload = {'description': description_new, 'columns': columns_new, 'owners': owners_new}
        superset.request('PUT', f"/dataset/{dataset['id']}?override_columns=false", json=payload)
        pause_after_update(superset_pause_after_update)
    else:
        logging.info("Skipping PUT execute request as nothing would be updated.")


def main(dbt_project_dir, dbt_db_name,
         superset_url, superset_db_id, superset_refresh_columns, superset_pause_after_update,
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
    logging.info("There are %d physical datasets in Superset overall.", len(sst_datasets))

    with open(f'{dbt_project_dir}/target/manifest.json') as f:
        dbt_manifest = json.load(f)

    dbt_tables = get_tables_from_dbt(dbt_manifest, dbt_db_name)

    sst_datasets_dbt_filtered = [d for d in sst_datasets if d["key"] in dbt_tables]
    logging.info("There are %d physical datasets in Superset with a match in dbt.", len(sst_datasets_dbt_filtered))

    for i, sst_dataset in enumerate(sst_datasets_dbt_filtered):
        logging.info("Processing dataset %d/%d.", i + 1, len(sst_datasets_dbt_filtered))
        sst_dataset_id = sst_dataset['id']
        try:
            if superset_refresh_columns:
                refresh_columns_in_superset(superset, sst_dataset_id)
                pause_after_update(superset_pause_after_update)
            sst_dataset_w_cols = add_superset_columns(superset, sst_dataset)
            sst_dataset_w_cols_new = merge_columns_info(sst_dataset_w_cols, dbt_tables)
            put_descriptions_to_superset(superset, sst_dataset_w_cols_new, superset_pause_after_update)
        except HTTPError as e:
            logging.error("The dataset with ID=%d wasn't updated. Check the error below.",
                          sst_dataset_id, exc_info=e)

    logging.info("All done!")
