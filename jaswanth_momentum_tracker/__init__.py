# jaswanth_momentum_tracker/__init__.py
from dagster import Definitions, define_asset_job, ScheduleDefinition
from .assets import API_CONNECTION,instrument_list,option_spreads,check_signal

# Create job from assets
asset_job = define_asset_job("asset_materialization_job")

# Schedule it to run every 30 mins
asset_schedule = ScheduleDefinition(
    job=asset_job,
    cron_schedule = "* * * * *",  # every 1 minute

    name="run_asset_every_1_mins"
)

defs = Definitions(
    assets=[API_CONNECTION,instrument_list,option_spreads,check_signal],
    jobs=[asset_job],
    schedules=[asset_schedule],
)
