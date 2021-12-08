import json
import logging
import re

from pathlib import Path
import ruamel.yaml
import sqlfluff

from .superset_api import Superset

logging.basicConfig(level=logging.INFO)
logging.getLogger('sqlfluff').setLevel(level=logging.WARNING)


def get_tables_from_sql_simple(sql):
    sql = re.sub(r'(--.*)|(#.*)', '', sql)  # remove line comments
    sql = re.sub(r'\s+', ' ', sql).lower()  # make it one line
    sql = re.sub(r'(/\*(.|\n)*\*/)', '', sql)  # remove block comments

    regex = re.compile(r'\b(from|join)\b\s+(\"?(\w+)\"?(\.))?\"?(\w+)\"?\b')  # regex for tables
    tables_match = regex.findall(sql)
    tables = [table[2] + '.' + table[4] if table[2] != '' else table[4]  # full name if with schema
              for table in tables_match
              if table[4] != 'unnest']  # remove false positive

    tables = list(set(tables))  # remove duplicates

    return tables


def get_tables_from_sql(sql, dialect):
    try:
        sql_parsed = sqlfluff.parse(sql, dialect=dialect)
        tables_raw = [table.raw for table in sql_parsed.tree.recursive_crawl('table_reference')]
        tables_cleaned = ['.'.join(table.replace('"', '').lower().split('.')[-2:]) for table in
                          tables_raw]  # full name if with schema
    except (sqlfluff.core.errors.SQLParseError,
            sqlfluff.core.errors.SQLLexError,
            sqlfluff.api.simple.APIParsingError) as e:
        logging.warning("Parsing SQL through sqlfluff failed. "
                        "Let me attempt this via regular expressions at least and "
                        "check the error and problematic query:\n%s",
                        sql, exc_info=e)
        tables_cleaned = get_tables_from_sql_simple(sql)

    tables = list(set(tables_cleaned))

    return tables


def get_tables_from_dbt(dbt_catalog, dbt_db_name):
    tables = {}
    for table_type in ['nodes', 'sources']:
        catalog_subset = dbt_catalog[table_type]

        for table in catalog_subset:
            name = catalog_subset[table]['metadata']['name']
            schema = catalog_subset[table]['metadata']['schema']
            database = catalog_subset[table]['metadata']['database']
            source = catalog_subset[table]['unique_id'].split('.')[-2]
            table_key = schema + '.' + name

            if dbt_db_name is None or database == dbt_db_name:
                # fail if it breaks uniqueness constraint
                assert table_key not in tables, \
                    f"Table {table_key} is a duplicate name (schema + table) across databases. " \
                    "This would result in incorrect matching between Superset and dbt. " \
                    "To fix this, remove duplicates or add ``dbt_db_name``."
                tables[table_key] = {
                    'name': name,
                    'schema': schema,
                    'database': database,
                    'type': table_type[:-1],
                    'ref':
                        f"ref('{name}')" if table_type == 'nodes'
                        else f"source('{source}', '{name}')"
                }

    assert tables, "Catalog is empty!"

    return tables


def get_dashboards_from_superset(superset, superset_url, superset_db_id):
    logging.info("Getting published dashboards from Superset.")
    page_number = 0
    dashboards_id = []
    while True:
        logging.info("Getting page %d.", page_number + 1)
        res = superset.request('GET', f'/dashboard/?q={{"page":{page_number},"page_size":100}}')
        result = res['result']
        if result:
            for r in result:
                if r['published']:
                    dashboards_id.append(r['id'])
            page_number += 1
        else:
            break
    logging.info("There are %d published dashboards in Superset.", len(dashboards_id))

    dashboards = []
    dashboards_datasets_w_db = set()
    for i, d in enumerate(dashboards_id):
        logging.info("Getting info for dashboard %d/%d.", i + 1, len(dashboards_id))
        res = superset.request('GET', f'/dashboard/{d}')
        result = res['result']

        dashboard_id = result['id']
        title = result['dashboard_title']
        url = superset_url + '/superset/dashboard/' + str(dashboard_id)
        owner_name = result['owners'][0]['first_name'] + ' ' + result['owners'][0]['last_name']

        # take unique dataset names, formatted as "[database].[schema].[table]" by Superset
        datasets_raw = list(set(result['table_names'].split(', ')))
        # parse dataset names into parts
        datasets_parsed = [dataset[1:-1].split('].[', maxsplit=2) for dataset in datasets_raw]
        datasets_parsed = [[dataset[0], 'None', dataset[1]]  # add None in the middle
                           if len(dataset) == 2 else dataset  # if missing the schema
                           for dataset in datasets_parsed]

        # put them all back together to get "database.schema.table"
        datasets_w_db = ['.'.join(dataset) for dataset in datasets_parsed]
        dashboards_datasets_w_db.update(datasets_w_db)

        # skip database, i.e. first item, to get only "schema.table"
        datasets_wo_db = ['.'.join(dataset[1:]) for dataset in datasets_parsed]

        dashboard = {
            'id': dashboard_id,
            'title': title,
            'url': url,
            'owner_name': owner_name,
            'owner_email': '',  # required for dbt to accept owner_name but not in response
            'datasets': datasets_wo_db  # add in "schema.table" format
        }
        dashboards.append(dashboard)

    # test if unique when database disregarded
    # loop to get the name of duplicated dataset and work with unique set of datasets w db
    dashboards_datasets = set()
    for dataset_w_db in dashboards_datasets_w_db:
        dataset = '.'.join(dataset_w_db.split('.')[1:])  # similar logic as just a bit above

        # fail if it breaks uniqueness constraint and not limited to one database
        assert dataset not in dashboards_datasets or superset_db_id is not None, \
            f"Dataset {dataset} is a duplicate name (schema + table) across databases. " \
            "This would result in incorrect matching between Superset and dbt. " \
            "To fix this, remove duplicates or add ``superset_db_id``."

        dashboards_datasets.add(dataset)

    return dashboards, dashboards_datasets


def get_datasets_from_superset(superset, dashboards_datasets, dbt_tables,
                               sql_dialect, superset_db_id):
    logging.info("Getting datasets info from Superset.")
    page_number = 0
    datasets = {}
    while True:
        logging.info("Getting page %d.", page_number + 1)
        res = superset.request('GET', f'/dataset/?q={{"page":{page_number},"page_size":100}}')
        result = res['result']
        if result:
            for r in result:
                name = r['table_name']
                schema = r['schema']
                database_name = r['database']['database_name']
                database_id = r['database']['id']

                dataset_key = f'{schema}.{name}'  # same format as in dashboards

                # only add datasets that are in dashboards, optionally limit to one database
                if dataset_key in dashboards_datasets \
                        and (superset_db_id is None or database_id == superset_db_id):
                    kind = r['kind']
                    if kind == 'virtual':  # built on custom sql
                        sql = r['sql']
                        tables = get_tables_from_sql(sql, sql_dialect)
                        tables = [table if '.' in table else f'{schema}.{table}'
                                  for table in tables]
                    else:  # built on tables
                        tables = [dataset_key]
                    dbt_refs = [dbt_tables[table]['ref'] for table in tables
                                if table in dbt_tables]

                    datasets[dataset_key] = {
                        'name': name,
                        'schema': schema,
                        'database': database_name,
                        'kind': kind,
                        'tables': tables,
                        'dbt_refs': dbt_refs
                    }
            page_number += 1
        else:
            break

    return datasets


def merge_dashboards_with_datasets(dashboards, datasets):
    for dashboard in dashboards:
        refs = set()
        for dataset in dashboard['datasets']:
            if dataset in datasets:
                refs.update(datasets[dataset]['dbt_refs'])
        refs = list(sorted(refs))

        dashboard['refs'] = refs

    return dashboards


def get_exposures_dict(dashboards, exposures):
    dashboards.sort(key=lambda dashboard: dashboard['id'])
    titles = [dashboard['title'] for dashboard in dashboards]
    # fail if it breaks uniqueness constraint for exposure names
    assert len(set(titles)) == len(titles), "There are duplicate dashboard names!"

    exposures_orig = {exposure['url']: exposure for exposure in exposures}
    exposures_dict = [{
        'name': dashboard['title'],
        'type': 'dashboard',
        'url': dashboard['url'],
        # get descriptions from original file through url (unique as it's based on dashboard id)
        'description': exposures_orig.get(dashboard['url'], {}).get('description', ''),
        'depends_on': dashboard['refs'],
        'owner': {
            'name': dashboard['owner_name'],
            'email': dashboard['owner_email']
        }
    } for dashboard in dashboards]

    return exposures_dict


class YamlFormatted(ruamel.yaml.YAML):
    def __init__(self):
        super(YamlFormatted, self).__init__()
        self.default_flow_style = False
        self.allow_unicode = True
        self.encoding = 'utf-8'
        self.block_seq_indent = 2
        self.indent = 4


def main(dbt_project_dir, exposures_path, dbt_db_name,
         superset_url, superset_db_id, sql_dialect,
         superset_access_token, superset_refresh_token):

    # require at least one token for Superset
    assert superset_access_token is not None or superset_refresh_token is not None, \
           "Add ``SUPERSET_ACCESS_TOKEN`` or ``SUPERSET_REFRESH_TOKEN`` " \
           "to your environment variables or provide in CLI " \
           "via ``superset-access-token`` or ``superset-refresh-token``."

    superset = Superset(superset_url + '/api/v1',
                        access_token=superset_access_token, refresh_token=superset_refresh_token)

    logging.info("Starting the script!")

    with open(f'{dbt_project_dir}/target/catalog.json') as f:
        dbt_catalog = json.load(f)

    exposures_yaml_path = dbt_project_dir + exposures_path

    try:
        with open(exposures_yaml_path) as f:
            yaml = ruamel.yaml.YAML(typ='safe')
            exposures = yaml.load(f)['exposures']
    except (FileNotFoundError, TypeError):
        Path(exposures_yaml_path).parent.mkdir(parents=True, exist_ok=True)
        Path(exposures_yaml_path).touch(exist_ok=True)
        exposures = {}

    dbt_tables = get_tables_from_dbt(dbt_catalog, dbt_db_name)
    dashboards, dashboards_datasets = get_dashboards_from_superset(superset,
                                                                   superset_url,
                                                                   superset_db_id)
    datasets = get_datasets_from_superset(superset,
                                          dashboards_datasets,
                                          dbt_tables,
                                          sql_dialect,
                                          superset_db_id)
    dashboards = merge_dashboards_with_datasets(dashboards, datasets)
    exposures_dict = get_exposures_dict(dashboards, exposures)

    # insert empty line before each exposure, except the first
    exposures_yaml = ruamel.yaml.comments.CommentedSeq(exposures_dict)
    for e in range(len(exposures_yaml)):
        if e != 0:
            exposures_yaml.yaml_set_comment_before_after_key(e, before='\n')

    exposures_yaml_schema = {
        'version': 2,
        'exposures': exposures_yaml
    }

    exposures_yaml_file = YamlFormatted()
    with open(exposures_yaml_path, 'w+', encoding='utf-8') as f:
        exposures_yaml_file.dump(exposures_yaml_schema, f)

    logging.info("Transferred into a YAML file at %s.", exposures_yaml_path)
    logging.info("All done!")
