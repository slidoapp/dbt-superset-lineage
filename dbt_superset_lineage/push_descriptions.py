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
    
    # Not asserting that there are any datasets, as for the first run it may very well be that there is none yet!
    #assert datasets, "There are no datasets in Superset!"

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
            meta = table['meta']
            description = table['description']

            table_key_short = schema + '.' + name
            columns = table['columns']

            if dbt_db_name is None or database == dbt_db_name:
                # fail if it breaks uniqueness constraint
                assert table_key_short not in tables, \
                    f"Table {table_key_short} is a duplicate name (schema + table) " \
                    f"across databases. " \
                    "This would result in incorrect matching between Superset and dbt. " \
                    "To fix this, remove duplicates or add the ``dbt_db_name`` argument."

                tables[table_key_short] = {'columns': columns, 'meta': meta, 'description': description}

    assert tables, "Manifest is empty!"

    # DEBUG
    with open('/Users/philippleufke/tmp/dbt_superset_debug/dbt_tables.json', 'w') as fp:
        json.dump(tables, fp, sort_keys=True, indent=4)

    return tables


def get_auto_register_tables(sst_datasets, dbt_tables):    
    dbt_auto_register_tables = []
    for table, props in dbt_tables.items():
        if props.get('meta').get('bi_integration', {}).get('auto_register', False):
            dbt_auto_register_tables.append(table)
    existing_datasets = [sst_dataset['key'] for sst_dataset in sst_datasets]
    auto_register_tables = [table for table in dbt_auto_register_tables if table not in existing_datasets]

    return auto_register_tables



def refresh_columns_in_superset(superset, dataset_id):
    logging.info("Refreshing columns in Superset.")
    superset.request('PUT', f'/dataset/{dataset_id}/refresh')


def add_superset_columns(superset, dataset):
    logging.info("Pulling dataset columns info from Superset.")
    res = superset.request('GET', f"/dataset/{dataset['id']}")
    columns = res['result']['columns']
    meta = res['result']
    del meta['columns']
    dataset['columns'] = columns
    dataset['meta'] = meta
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

    meta_sst = dataset['meta']
    
    # add the whole dbt meta object of the table/model to the dict for debugging purpose
    meta_dbt = tables.get(key, {}).get('meta', {})
    dataset['meta_dbt'] = meta_dbt

    # Add meta information of the dataset.
    meta_new = {}
    # FIXME: The name of this and related functions is not correctly scoped any longer,
    # as we don't only process columns but also the metadata...

    # Prepopulate the dataset's meta data in case that the dataset is NOT set
    # to be managed externally (i.e., by dbt).
    # The dbt meta data field `prohibit_manual_editing` decides whether the dataset
    # is externally managed, NOT Superset's metadata!
    meta_new['is_managed_externally'] = meta_dbt.get('bi_integration', {}).get('prohibit_manual_editing', False)
    if not meta_new['is_managed_externally']:
        for field in ['cache_timeout', 'description', 'fetch_values_predicate', 'filter_select_enabled', 'is_managed_externally', 'main_dttm_col']:
            if meta_sst[field] is not None:
                meta_new[field] = meta_sst[field]

    dbt_description = tables.get(key, {}).get('description')
    if dbt_description is not None:
        meta_new['description'] = convert_markdown_to_plain_text(dbt_description)

    # Not sure if we need to suppress None values here?!
    meta_new['cache_timeout'] = meta_dbt.get('bi_integration', {}).get('results_cache_timeout_seconds')
    meta_new['fetch_values_predicate'] = meta_dbt.get('bi_integration', {}).get('filter_value_extraction', {}).get('where')
    meta_new['filter_select_enabled'] = meta_dbt.get('bi_integration', {}).get('filter_value_extraction', {}).get('enable')
    meta_new['main_dttm_col'] = meta_dbt.get('bi_integration', {}).get('main_timestamp_column')
        
    dataset['meta_new'] = meta_new

    # Columns:
    sst_columns = dataset['columns']
    dbt_columns = tables.get(key, {}).get('columns', {})

    # DEBUG
    with open('/Users/philippleufke/tmp/dbt_superset_debug/sst_columns.json', 'w') as fp:
        json.dump(sst_columns, fp, sort_keys=True, indent=4)
    with open('/Users/philippleufke/tmp/dbt_superset_debug/dbt_columns.json', 'w') as fp:
        json.dump(dbt_columns, fp, sort_keys=True, indent=4)


    columns_new = []
    for sst_column in sst_columns:

        # add the mandatory field
        column_name = sst_column['column_name']
        column_new = {'column_name': column_name}

        if not meta_new['is_managed_externally']:
            # Pre-populate the columns information with the one that already exists in Superset,
            # but only if the dataset is not managed _exclusively_ via dbt.
            # In the latter case we overwrite the columns information of the columns that 
            # are not calculated columns.


            # add optional fields only if not already None, otherwise it would error out
            # Note: `type_generic` is not yet exposed in PUT but returned in GET dataset
            for field in ['advanced_data_type', 'description', 'expression', 'extra', 'filterable', 'groupby', 'python_date_format',
                        'verbose_name', 'type', 'is_dttm', 'is_active']:
                if sst_column[field] is not None:
                    column_new[field] = sst_column[field]

        # Overwrite if the column is not a calculated column:
        # Note: after initial registration the "expression" field is null and not an empty string!
        if sst_column['expression'] == '' or sst_column['expression'] is None:
            # add description
            if column_name in dbt_columns \
                    and 'description' in dbt_columns[column_name]:
                description = dbt_columns[column_name]['description']
                description = convert_markdown_to_plain_text(description)
            else:
                description = sst_column['description']
            column_new['description'] = description

            
            # Meta fields:
            # The column meta fields are called differently in Superset and thus need to be renamed.
            # For this reason this code is not DRY for now...

            # add verbose_name which is in the `meta` dict in dbt
            if column_name in dbt_columns \
                    and 'verbose_name' in dbt_columns[column_name]['meta']:
                verbose_name = dbt_columns[column_name]['meta']['verbose_name']
                column_new['verbose_name'] = verbose_name
            elif column_name in dbt_columns:
                # Fall back to the column name in title case
                column_new['verbose_name'] = column_name.replace('_', ' ').title()

            # add is_filterable which is in the `meta` dict in the 'bi_integration' section
            if column_name in dbt_columns \
                    and 'is_filterable' in dbt_columns[column_name]['meta'].get('bi_integration', {}):
                is_filterable = dbt_columns[column_name]['meta']['bi_integration']['is_filterable']
                column_new['filterable'] = is_filterable
                # DEBUG
                # logging.info("Column %s is filterable?: %s", column_name, is_filterable)

            # add is_groupable which is in the `meta` dict in the 'bi_integration' section
            if column_name in dbt_columns \
                    and 'is_groupable' in dbt_columns[column_name]['meta'].get('bi_integration', {}):
                is_groupable = dbt_columns[column_name]['meta']['bi_integration']['is_groupable']
                column_new['groupby'] = is_groupable
                # DEBUG
                # logging.info("Column %s is groupable?: %s", column_name, is_groupable)



        columns_new.append(column_new)

    dataset['columns_new'] = columns_new

    return dataset


def register_dataset_in_superset(superset, superset_db_id, table):
    logging.info("Registering database table in Superset: %s", table)

    schema_name, table_name = table.split('.')

    body = {
        "database": superset_db_id,
        "schema": schema_name,
        "table_name": table_name
    }

    # DEBUG
    with open('/Users/philippleufke/tmp/dbt_superset_debug/register_dataset_body.json', 'w') as fp:
        json.dump(body, fp, sort_keys=True, indent=4)

    superset.request('POST', f"/dataset/", json=body)




def put_columns_to_superset(superset, dataset):
    logging.info("Putting new columns info with descriptions back into Superset.")

    # DEBUG
    with open('/Users/philippleufke/tmp/dbt_superset_debug/dataset.json', 'w') as fp:
        json.dump(dataset, fp, sort_keys=True, indent=4)


    body = dataset['meta_new']
    body['columns'] = dataset['columns_new']

    # DEBUG
    with open('/Users/philippleufke/tmp/dbt_superset_debug/dataset_update_body.json', 'w') as fp:
        json.dump(body, fp, sort_keys=True, indent=4)

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

    # Auto-registration of dbt models in Superset:
    # Which tables are set to be auto-registered in dbt and are not yet present in Superset?:
    auto_register_tables = get_auto_register_tables(sst_datasets, dbt_tables)

    # DEBUG
    # with open('/Users/philippleufke/tmp/dbt_superset_debug/auto_register_tables.json', 'w') as fp:
    #     json.dump(auto_register_tables, fp, sort_keys=True, indent=4)

    # Register them
    for table in auto_register_tables:
        try:
            register_dataset_in_superset(superset, superset_db_id,table)
        except HTTPError as e:
            logging.error("The database table %s could not be registeres. Check the error below.",
                          table, exc_info=e)

    # Re-fetch Superset datasets
    sst_datasets = get_datasets_from_superset(superset, superset_db_id)
    logging.info("There are %d physical datasets in Superset.", len(sst_datasets))

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
