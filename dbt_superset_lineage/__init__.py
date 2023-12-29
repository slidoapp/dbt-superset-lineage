import typer
from .push_descriptions import main as push_descriptions_main
__version__=4
app = typer.Typer()

@app.command()
def push_descriptions(dbt_project_dir: str = typer.Option('.', help="Directory path to dbt project."),
                      dbt_db_name: str = typer.Option(None, help="Name of your database within dbt to which the script "
                                                                 "should be reduced to run."),
                      superset_url: str = typer.Argument(..., help="URL of your Superset, e.g. "
                                                                   "https://mysuperset.mycompany.com"),
                      superset_db_id: int = typer.Option(None, help="ID of your database within Superset towards which "
                                                                    "the push should be reduced to run."),
                      superset_debug_dir: str = typer.Option(None, envvar="SUPERSET_DEBUG_DIR",
                                                             help="A path to a directory where debugging files  "
                                                                  "will be placed if this option is specified."),
                      superset_refresh_columns: bool = typer.Option(False, help="Whether columns in Superset should be "
                                                                                "refreshed from database before "
                                                                                "the push."),
                      superset_access_token: str = typer.Option(None, envvar="SUPERSET_ACCESS_TOKEN",
                                                                help="Access token to Superset API."
                                                                     "Can be automatically generated if "
                                                                     "SUPERSET_REFRESH_TOKEN is provided."),
                      superset_refresh_token: str = typer.Option(None, envvar="SUPERSET_REFRESH_TOKEN",
                                                                 help="Refresh token to Superset API."),
                      superset_user: str = typer.Option(None, envvar="SUPERSET_USER",
                                                                help="Superset Username"),
                      superset_password: str = typer.Option(None, envvar="SUPERSET_PASSWORD",
                                                                 help="Password of the Superset user.")):

    push_descriptions_main(dbt_project_dir, dbt_db_name,
                           superset_url, superset_db_id, superset_debug_dir, superset_refresh_columns,
                           superset_access_token, superset_refresh_token,
                           superset_user, superset_password)


if __name__ == '__main__':
    app()
