import requests
from dagster_duckdb import DuckDBResource
from dagster import asset, AssetExecutionContext, MetadataValue
from . import constants
from ..partitions import monthly_partition
import pandas as pd


@asset(partitions_def=monthly_partition, group_name="raw_file")
def taxi_trips_file(context: AssetExecutionContext):
    partition_date_str = context.asset_partition_key_for_output()
    month_to_fetch = partition_date_str[:-3]
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb"
    ) as output_file:
        output_file.write(raw_trips.content)

    rows = len(pd.read_parquet(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)))
    context.add_output_metadata({"rows": MetadataValue.int(rows)})


@asset(
    deps=["taxi_trips_file"], partitions_def=monthly_partition, group_name="ingested"
)
def taxi_trips(context: AssetExecutionContext, db: DuckDBResource):
    partition_date_str = context.asset_partition_key_for_output()
    month_to_fetch = partition_date_str[:-3]

    with db.get_connection() as conn:
        sql = f"""\
create table if not exists trips (
    partition_key varchar,
    vendor_id integer,
    pickup_zone_id integer,
    dropoff_zone_id integer,
    rate_code_id integer,
    payment_type integer,
    dropoff_datetime timestamp,
    pickup_datetime timestamp,
    trip_distance double,
    passenger_count integer,
    total_amount double
);

delete from trips where partition_key = '{month_to_fetch}';

insert into trips
select
    {month_to_fetch},
    VendorID,
    PULocationID,
    DOLocationID,
    RatecodeID,
    payment_type,
    tpep_dropoff_datetime,
    tpep_pickup_datetime,
    trip_distance,
    passenger_count,
    total_amount
from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';"""
        conn.execute(sql)


@asset(group_name="raw_file")
def taxi_zones_file(context):
    zones = requests.get(
        "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD"
    )
    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as file:
        file.write(zones.content)

    rows = len(pd.read_csv(constants.TAXI_ZONES_FILE_PATH))
    context.add_output_metadata({"rows": MetadataValue.int(rows)})

    


@asset(deps=["taxi_zones_file"], group_name="ingested")
def taxi_zones(db: DuckDBResource):
    sql = f"""
create or replace table zones as (
    select
        LocationID as zone_id,
        zone,
        borough,
        the_geom as geometry
    from '{constants.TAXI_ZONES_FILE_PATH}'
);"""

    with db.get_connection() as conn:
        conn.execute(sql)
