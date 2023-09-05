from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from datetime import datetime

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table


@aql.transform
def top_movie(input_table: Table):
    return """
        SELECT "Title", "Rating"
        FROM {{ input_table }}
        ORDER BY "Rating" DESC
        LIMIT 10
    """


@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["movie"],
)
def movie():
    load_movie_to_postgres = aql.load_file(
        task_id="load_movie_to_postgres",
        input_file=File(
            path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb.csv",
        ),
        output_table=Table(conn_id="postgres_external", name="movie"),
    )

    @task.external_python(python="/usr/local/airflow/soda_venv/bin/python")
    def check_movie(scan_name="check_movie", checks_subpath="tables/movie.yaml", data_source="postgres_db_external"):
        from include.soda.check_function import check

        return check(scan_name=scan_name, checks_subpath=checks_subpath, data_source=data_source)

    top_table = top_movie(
        input_table=Table(conn_id="postgres_external", name="movie"),
        output_table=Table(conn_id="postgres_external", name="top_movie"),
    )

    @task.external_python(python="/usr/local/airflow/soda_venv/bin/python")
    def check_top_movie(
        scan_name="check_top_movie", checks_subpath="tables/top_movie.yaml", data_source="postgres_db_external"
    ):
        from include.soda.check_function import check

        return check(scan_name=scan_name, checks_subpath=checks_subpath, data_source=data_source)

    chain(load_movie_to_postgres, check_movie(), top_table, check_top_movie())


movie()
