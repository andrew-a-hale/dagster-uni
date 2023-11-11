from dagster import Config, asset, MetadataValue
from dagster_duckdb import DuckDBResource

import plotly.express as px
import plotly.io as pio
import base64 as b64

from . import constants


class AdhocRequestConfig(Config):
    filename: str
    borough: str
    start_date: str
    end_date: str


@asset(deps=["taxi_trips", "taxi_zones"], group_name="requests")
def adhoc_request(context, config: AdhocRequestConfig, db: DuckDBResource):
    file_path = constants.REQUEST_DESTINATION_TEMPLATE_FILE_PATH.format(
        config.filename.split(".")[0]
    )
    stmt = f"""\
select
    date_part('hour', pickup_datetime) as hour,
    date_part('day', pickup_datetime) as day_of_week_num,
    dayname(pickup_datetime) as day_of_week,
    count(1) as num_trips
from trips
inner join zones on zones.zone_id = trips.pickup_zone_id and borough = '{config.borough}'
where pickup_datetime >= '{config.start_date}' and pickup_datetime < '{config.end_date}'
group by pickup_datetime
order by pickup_datetime"""

    with db.get_connection() as conn:
        results = conn.execute(stmt).fetch_df()

    fig = px.bar(
        results,
        x="hour",
        y="num_trips",
        color="day_of_week",
        barmode="stack",
        title=f"Number of trips by hour of day in {config.borough}, from {config.start_date} to {config.end_date}",
        labels={
            "hour_of_day": "Hour of Day",
            "day_of_week": "Day of Week",
            "num_trips": "Number of Trips",
        },
    )

    pio.write_image(fig, file_path)

    with open(file_path, "rb") as file:
        image_data = file.read()

    b64_data = b64.b64encode(image_data).decode("utf-8")
    md = f"![Image](data:image/jpeg;base64,{b64_data})"

    context.add_output_metadata({"preview": MetadataValue.md(md)})