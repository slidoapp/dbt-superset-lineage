import json
import logging
import re

from pathlib import Path
from requests import HTTPError
import ruamel.yaml
import sqlfluff

from .superset_api import Superset

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger('sqlfluff').setLevel(level=logging.WARNING)


def crawl_recursive(seq, key):
    if isinstance(seq, dict):
        for k, v in seq.items():
            if k == key:
                yield v
            else:
                yield from crawl_recursive(v, key)
    elif isinstance(seq, list):
        for i in seq:
            yield from crawl_recursive(i, key)


def get_tables_from_sql_fluff(sql, dialect):
    sql_parsed = sqlfluff.parse(sql=sql, dialect=dialect)
    tables_references = crawl_recursive(sql_parsed, 'table_reference')

    tables = set()  # to avoid duplicates
    for identifier in ['naked_identifier', 'quoted_identifier']:
        tables_parsed = [list(crawl_recursive(table_ref, identifier)) for table_ref in tables_references]
        tables_cleaned = ['.'.join(table).replace('"', '').lower()
                          for table in tables_parsed
                          if len(table) >= 2]  # full name if with schema
        tables.update(tables_cleaned)

    return tables


def get_tables_from_sql_simple(sql):
    sql = re.sub(r'(--.*)|(#.*)', '', sql)  # remove line comments
    sql = re.sub(r'\s+', ' ', sql).lower()  # make it one line
    sql = re.sub(r'(/\*(.|\n)*\*/)', '', sql)  # remove block comments

    regex = re.compile(r'\b(from|join)\b\s+(\"?(\w+)\"?(\.))?\"?(\w+)\"?\b')  # regex for tables
    tables_match = regex.findall(sql)
    tables = [table[2] + '.' + table[4] if table[2] != '' else table[4]  # full name if with schema
              for table in tables_match
              if table[4] != 'unnest']  # remove false positive
    tables = set(tables)  # remove duplicates

    return tables


def get_tables_from_sql(sql, dialect):
    try:
        tables = get_tables_from_sql_fluff(sql=sql, dialect=dialect)
    except (sqlfluff.core.errors.SQLParseError,
            sqlfluff.core.errors.SQLLexError,
            sqlfluff.api.simple.APIParsingError) as e:
        logging.warning("Parsing SQL through sqlfluff failed. "
                        "Let me attempt this via regular expressions at least and "
                        "check the problematic query and error below.\n%s",
                        sql, exc_info=e)
        tables = get_tables_from_sql_simple(sql)

    tables = list(tables)  # turn set back into list

    return tables


def get_tables_from_dbt(dbt_manifest, dbt_db_name):
    tables = {}
    for table_type in ['nodes', 'sources']:
        manifest_subset = dbt_manifest[table_type]

        for table_key_long in manifest_subset:
            table = manifest_subset[table_key_long]
            name = table['name']
            schema = table['schema']
            database = table['database']
            source = table['unique_id'].split('.')[-2]
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

    assert tables, "Manifest is empty!"

    return tables


def get_dashboards_from_superset(superset, superset_url, superset_db_id):
    logging.info("Getting published dashboards from Superset.")
    page_number = 0
    dashboards_id = []
    while True:
        logging.info("Getting page %d.", page_number + 1)

        payload = {
            'q': json.dumps({
                'page': page_number,
                'page_size': 100
            })
        }
        res = superset.request('GET', '/dashboard/', params=payload)

        result = res['result']
        if result:
            for r in result:
                if r['published']:
                    dashboards_id.append(r['id'])
            page_number += 1
        else:
            break

    assert dashboards_id, "There are no published dashboards in Superset!"

    logging.info("There are %d published dashboards in Superset.", len(dashboards_id))

    dashboards = []
    dashboards_datasets_w_db = set()
    for i, d in enumerate(dashboards_id):
        try:
            logging.info("Getting info for dashboard %d/%d.", i + 1, len(dashboards_id))
            res_dashboard = superset.request('GET', f'/dashboard/{d}')
            result_dashboard = res_dashboard['result']

            dashboard_id = result_dashboard['id']
            title = result_dashboard['dashboard_title']
            url = superset_url + '/superset/dashboard/' + str(dashboard_id)
            owner_name = result_dashboard['owners'][0]['first_name'] + ' ' + result_dashboard['owners'][0]['last_name']

            logging.info("Getting info about dashboard's datasets.")
            res_datasets = superset.request('GET', f'/dashboard/{d}/datasets')
            result_datasets = res_datasets['result']

            # parse dataset names split into parts
            datasets_parsed = [[dataset['database']['name'], dataset['schema'], dataset['table_name']]
                               for dataset in result_datasets]
            datasets_parsed = [['None' if x is None else x for x in dataset]
                               for dataset in datasets_parsed]  # replace None with string "None" if something missing

            # put them all together to get "database.schema.table"
            datasets_w_db = ['.'.join(dataset) for dataset in datasets_parsed]
            dashboards_datasets_w_db.update(datasets_w_db)

            # skip database, i.e. first item, to get only "schema.table"
            datasets_wo_db = ['.'.join(dataset[1:]) for dataset in datasets_parsed]

            dashboard = {
                'id': dashboard_id,
                'title': title,
                'url': url,
                'owner_name': owner_name,
                'datasets': datasets_wo_db  # add in "schema.table" format
            }
            dashboards.append(dashboard)
        except HTTPError as e:
            logging.error("Info about the dashboard with ID=%d wasn't (fully) obtained. "
                          "Check the error below.", d, exc_info=e)

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
        # remove non-word characters (unless it's space), replace spaces with underscores, make lowercase
        # required since dbt v1.3
        'name': re.sub(r'[^\w ]+', '', dashboard['title']).replace(' ', '_').lower(),
        'label': dashboard['title'],
        'type': 'dashboard',
        'url': dashboard['url'],
        # get descriptions from original file through url (unique as it's based on dashboard id)
        'description': exposures_orig.get(dashboard['url'], {}).get('description', ''),
        'depends_on': dashboard['refs'],
        'owner': {
            'name': dashboard['owner_name'],
            'email': ''  # required for dbt to accept owner.name but not in response
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

    with open(f'{dbt_project_dir}/target/manifest.json') as f:
        dbt_manifest = json.load(f)

    exposures_yaml_path = dbt_project_dir + exposures_path

    try:
        with open(exposures_yaml_path) as f:
            yaml = ruamel.yaml.YAML(typ='safe')
            exposures = yaml.load(f)['exposures']
    except (FileNotFoundError, TypeError):
        Path(exposures_yaml_path).parent.mkdir(parents=True, exist_ok=True)
        Path(exposures_yaml_path).touch(exist_ok=True)
        exposures = {}

    dbt_tables = get_tables_from_dbt(dbt_manifest, dbt_db_name)
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
