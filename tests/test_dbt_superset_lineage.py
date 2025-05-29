from dbt_superset_lineage import __version__


def test_version():
    assert __version__ == '0.3.5'
