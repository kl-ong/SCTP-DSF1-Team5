
from dagster import (
    Definitions,
    load_assets_from_modules,
)
from dagster_gcp import BigQueryResource

from . import assets


all_assets = load_assets_from_modules([assets])

bigquery_resource = BigQueryResource(
    project="sctp-olist"  # Replace with your GCP project ID
)

defs = Definitions(
    assets=all_assets,
    resources={
        "bigquery": bigquery_resource,
    },
    # jobs=[pandas_job],
    # schedules=[pandas_schedule],
    # resources={
    #     "io_manager": database_io_manager,
    # },
)

# define the job that will materialize the assets
# pandas_job = define_asset_job(name="pandas_job", selection=AssetSelection.all())

# a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
# pandas_schedule = ScheduleDefinition(
#     name="pandas_schedule",
#     job=pandas_job, 
#     cron_schedule="0 0 * * *"  # every day at midnight
# )

# database_io_manager = DuckDBPandasIOManager(database="analytics.pandas_releases")