from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from . import assets as a

daily_job = define_asset_job(
    "daily_refresh",
    selection=AssetSelection.assets(a.raw_prices, a.clean_prices, a.daily_signals),
    partitions_def=a.DAILY,
)
extracts_job = define_asset_job("refresh_tableau_extracts", selection=AssetSelection.assets(a.tableau_extracts))

defs = Definitions(
    assets=[a.sp500_universe, a.trading_calendar, a.raw_prices, a.raw_sec_filings,
            a.clean_prices, a.fundamentals_ttm, a.daily_signals, a.tableau_extracts],
    asset_checks=[a.clean_prices_unique],
    jobs=[daily_job, extracts_job],
    schedules=[
        # Fires just after midnight ET and loads the day that just ended.
        build_schedule_from_partitioned_job(daily_job, minute_of_hour=15),
        ScheduleDefinition(job=extracts_job, cron_schedule="0 3 * * *", execution_timezone="America/New_York"),
    ],
)
