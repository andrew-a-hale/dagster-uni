from dagster import asset, AssetExecutionContext

import plotly.express as px
import plotly.io as pio
import geopandas as gpd
import pandas as pd

from dagster_duckdb import DuckDBResource

from ..partitions import weekly_partition
from . import constants


@asset(deps=["taxi_zones", "taxi_trips"], group_name="metrics")
def manhattan_stats(db: DuckDBResource):
    sql = """\
select 
    zones.zone,
    zones.borough,
    zones.geometry,
    count(1) as num_trips
from trips
left join zones on zones.zone_id = trips.pickup_zone_id
where borough = 'Manhattan' and geometry is not null
group by zone, borough, geometry"""

    with db.get_connection() as conn:
        trips_by_zone = conn.cursor().execute(sql).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, "w") as f:
        f.write(trips_by_zone.to_json())


@asset(deps=["manhattan_stats"], group_name="metrics")
def manhattan_map():
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig = px.choropleth_mapbox(
        trips_by_zone,
        geojson=trips_by_zone.geometry.__geo_interface__,
        locations=trips_by_zone.index,
        color="num_trips",
        color_continuous_scale="Plasma",
        mapbox_style="carto-positron",
        center={"lat": 40.758, "lon": -73.985},
        zoom=11,
        opacity=0.7,
        labels={"num_trips": "Number of Trips"},
    )

    pio.write_image(fig, constants.MANHATTAN_MAP_FILE_PATH)


@asset(deps=["taxi_trips", "taxi_zones"], partitions_def=weekly_partition, group_name="metrics")
def trips_by_week(context: AssetExecutionContext, db: DuckDBResource):
    week = context.asset_partition_key_for_output()

    sql = f"""\
select
    '{week}' as period,
    count(1) as num_trips,
    sum(passenger_count) as passenger_count,
    sum(total_amount) as total_amount,
    sum(trip_distance) as trip_distance
from trips
where pickup_datetime >= '{week}' and pickup_datetime < '{week}'::date + interval '1 week'"""

    with db.get_connection() as conn:
        trips_by_week = conn.cursor().execute(sql).fetch_df()

    with open(constants.TRIPS_BY_WEEK_TEMPLATE_FILE_PATH.format(week), "w") as f:
        f.write(trips_by_week.to_csv(index=False))
