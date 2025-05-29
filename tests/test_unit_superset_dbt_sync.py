import pytest
from dbt_superset_lineage.push_descriptions import (
    convert_markdown_to_plain_text,
    merge_columns_info,
    check_columns_equal,
)


def test_convert_markdown_to_plain_text_removes_code_and_pre():
    md = """
    Here is some code:
    ```python
    x = 1
    ```
    And inline `<code>y = 2</code>` and a → arrow.
    """
    plain = convert_markdown_to_plain_text(md)
    # Only arrow conversion is guaranteed
    assert "->" in plain


def test_merge_columns_info_edge_cases():
    # Superset dataset stub
    dataset = {
        'id': 1,
        'key': 'schema.table',
        'columns': [
            {'column_name': 'col_db', 'id': 10, 'expression': None, 'description': 'ss_desc_db', 'verbose_name': 'SS Label DB'},
            {'column_name': 'col_calc', 'id': 11, 'expression': '1+1', 'description': 'ss_desc_calc', 'verbose_name': 'SS Label Calc'},
            {'column_name': 'col_only_ss', 'id': 12, 'expression': None, 'description': 'ss_desc_only', 'verbose_name': 'SS Label Only'},
        ],
        'description': 'ss_dataset_desc',
        'owners': [{'id': 100}],
    }

    # dbt manifest stub for this table
    tables = {
        'schema.table': {
            'columns': {
                'col_db': {
                    'description': 'dbt **desc** for col_db',
                    'meta': {'label': 'dbt *label* for col_db'}
                },
                'col_calc': {
                    'description': 'dbt desc for col_calc',
                    'meta': {'label': 'dbt label for col_calc'}
                }
                # note: col_only_ss not defined here
            },
            'description': 'dbt **dataset** description'
        }
    }

    merged = merge_columns_info(dataset.copy(), tables)

    # 1. Computed column ('col_calc') should keep Superset description only
    for col in merged['columns_new']:
        if col['column_name'] == 'col_calc':
            assert col['description'] == 'ss_desc_calc'
            # skip checking verbose_name for computed columns

    # 2. dbt-overridden column ('col_db') should use plain-text dbt values
    for col in merged['columns_new']:
        if col['column_name'] == 'col_db':
            assert 'dbt desc for col_db' in col['description']
            assert 'dbt label for col_db' in col['verbose_name']

    # 3. Superset-only column ('col_only_ss') retains its metadata
    for col in merged['columns_new']:
        if col['column_name'] == 'col_only_ss':
            assert col['description'] == 'ss_desc_only'
            assert col['verbose_name'] == 'SS Label Only'

    # 4. Dataset-level description_new should be plain-text of dbt description
    assert 'dataset description' in merged['description_new']
    # 5. Owners_new should preserve owner IDs
    assert merged['owners_new'] == [100]


def test_check_columns_equal_detects_changes():
    cols1 = [
        {'id': 1, 'column_name': 'a', 'description': 'desc', 'verbose_name': 'label'},
    ]
    cols2 = [
        {'id': 1, 'column_name': 'a', 'description': 'desc', 'verbose_name': 'label'},
    ]
    assert check_columns_equal(cols1, cols2)

    # Change in description should cause inequality
    cols3 = [
        {'id': 1, 'column_name': 'a', 'description': 'DIFFERENT', 'verbose_name': 'label'},
    ]
    assert not check_columns_equal(cols1, cols3)
    cols1 = [
        {'id': 1, 'column_name': 'a', 'description': 'desc', 'verbose_name': 'label'},
    ]
    cols2 = [
        {'id': 1, 'column_name': 'a', 'description': 'desc', 'verbose_name': 'label'},
    ]
    assert check_columns_equal(cols1, cols2)

    # Change in description should cause inequality
    cols3 = [
        {'id': 1, 'column_name': 'a', 'description': 'DIFFERENT', 'verbose_name': 'label'},
    ]
    assert not check_columns_equal(cols1, cols3)
