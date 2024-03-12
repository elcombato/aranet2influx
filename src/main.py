import os
from datetime import datetime, timedelta
import logging
import pytz
import aranet4
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


def write_influxdb(record_list: list):
    """Write to InfluxDB

    Args:
        record_list (list): List of InfluxDB points
    """

    influx_url = os.getenv("INFLUX_URL")
    influx_token = os.getenv("INFLUX_TOKEN")
    influx_org = os.getenv("INFLUX_ORG")
    influx_bucket = os.getenv("INFLUX_BUCKET")

    logging.info(
        "Writing to InfluxDB bucket %s in %s (%s):",
        influx_bucket,
        influx_org,
        influx_url,
    )
    logging.info(" -> %s records", len(record_list))

    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=influx_bucket, record=record_list)
    client.close()


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %Z",
    )

    device_mac = os.getenv("ARANET_ADDRESS")

    delta = int(os.getenv("READ_INTERVAL"))
    start_date = datetime.now(tz=pytz.timezone("Europe/Berlin")) - timedelta(minutes=delta)
    history = aranet4.client.get_all_records(
        device_mac, entry_filter={"start": start_date}, remove_empty=True
    )

    influx_records = []
    for item in history.value:
        # TODO check tz
        influx_records.append(
            Point("aranet4")
            .field("temperature", item.temperature)
            .tag("room", "office")
            .time(item.date)
        )
        influx_records.append(
            Point("aranet4")
            .field("co2", item.co2)
            .tag("room", "office")
            .time(item.date)
        )
        influx_records.append(
            Point("aranet4")
            .field("pressure", item.pressure)
            .tag("room", "office")
            .time(item.date)
        )
        influx_records.append(
            Point("aranet4")
            .field("humidity", item.humidity)
            .tag("room", "office")
            .time(item.date)
        )

    write_influxdb(influx_records)