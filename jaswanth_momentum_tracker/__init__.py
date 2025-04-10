# jaswanth_momentum_tracker/__init__.py
from dagster import Definitions, define_asset_job, ScheduleDefinition
from .assets import API_CONNECTION,instrument_list,option_spreads,check_signal

# Create job from assets
asset_job = define_asset_job("asset_materialization_job")

# Schedule it to run every 30 mins
# 2. Create the trading hours schedule (9:15 AM to 3:30 PM, every 30 minutes)
trading_schedule = ScheduleDefinition(
    job=asset_job,
    cron_schedule="15,45 9-14 * * 1-5",  # 9:15-14:45 (2:45 PM)
    execution_timezone="Asia/Kolkata",    # For NSE (adjust as needed)
    name="nse_trading_hours_schedule"
)

# 3. Special 3:30 PM run
closing_schedule = ScheduleDefinition(
    job=asset_job,
    cron_schedule="30 15 * * 1-5",       # Exactly at 3:30 PM
    execution_timezone="Asia/Kolkata",
    name="nse_closing_signal"
)

defs = Definitions(
    assets=[API_CONNECTION,instrument_list,option_spreads,check_signal],
    jobs=[asset_job],
    schedules=[trading_schedule,closing_schedule],
)
