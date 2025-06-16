import os
from dagster import (
    Definitions,
    load_assets_from_modules,
    fs_io_manager
)

from . import assets

current_script_dir = os.path.dirname(os.path.abspath(__file__))
data_directory = os.path.join(current_script_dir, "data")
os.makedirs(data_directory, exist_ok=True)

all_assets = load_assets_from_modules([assets])

# define the job that will materialize the assets
# pandas_job = define_asset_job(name="pandas_job", selection=AssetSelection.all())

# a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
# pandas_schedule = ScheduleDefinition(
#     name="pandas_schedule",
#     job=pandas_job, 
#     cron_schedule="0 0 * * *"  # every day at midnight
# )

# database_io_manager = DuckDBPandasIOManager(database="analytics.pandas_releases")

defs = Definitions(
    assets=all_assets,
    resources={
        "io_manager": fs_io_manager.configured({"base_dir": data_directory}),
    }
    # jobs=[pandas_job],
    # schedules=[pandas_schedule],
    # resources={
    #     "io_manager": database_io_manager,
    # },
)