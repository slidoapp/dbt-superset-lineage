import typer
from . import from_superset

app = typer.Typer()


@app.command()
def docs_from_superset(dbt_project_dir: str = typer.Option('.', help=""),
                       exposures_path: str = typer.Option('/models/exposures/superset_dashboards.yml',
                                                          # '/models/exposures/superset_dashboards.yml'
                                                          help="If you change this, the path needs to be added"
                                                               "to source-paths in dbt_project.yml."),
                       dbt_db_name: str = typer.Option(None, help=""),
                       superset_url: str = typer.Argument(..., help=""),  # superset.sli.do
                       superset_db_id: int = typer.Option(None, help=""),  # 2
                       sql_dialect: str = typer.Option('ansi', help=""),
                       superset_access_token: str = typer.Option(None, envvar="SUPERSET_ACCESS_TOKEN"),
                       superset_refresh_token: str = typer.Option(None, envvar="SUPERSET_REFRESH_TOKEN")):

    from_superset.main(dbt_project_dir, exposures_path, dbt_db_name,
                       superset_url, superset_db_id, sql_dialect,
                       superset_access_token, superset_refresh_token)


if __name__ == '__main__':
    app()
