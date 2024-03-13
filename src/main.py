import os
import sys
from time import sleep
import logging
import asyncio
import aranet4
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


def connect_to_influxdb():
    influx_url = os.getenv("INFLUX_URL")
    influx_token = os.getenv("INFLUX_TOKEN")
    influx_org = os.getenv("INFLUX_ORG")

    client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)

    logging.debug("Connected to InfluxDB %s (%s):", influx_url, influx_org)
    return client


def get_latest_timestamp():
    influx_bucket = os.getenv("INFLUX_BUCKET")

    client = connect_to_influxdb()
    query_api = client.query_api()

    tables = query_api.query(
        f'from(bucket: "{influx_bucket}")'
        "|> range(start: -1d)"
        '|> filter(fn: (r) => r["_measurement"] == "aranet4")'
        '|> filter(fn: (r) => r["_field"] == "co2")'
        "|> last()"
    )
    client.close()

    return tables[0].records[0].values["_time"]


def read_aranet_data():
    device_mac = os.getenv("ARANET_ADDRESS")
    start_date = get_latest_timestamp()
    logging.info("Read Aranet starting with %s", start_date.astimezone().isoformat())

    try:
        history = aranet4.client.get_all_records(
            device_mac, entry_filter={"start": start_date}, remove_empty=True
        )
    except asyncio.exceptions.TimeoutError:
        logging.exception("Reading Aranet4 timed out:")
        sys.exit(1)


    record_list = []
    for item in history.value:
        record_list.append(
            Point("aranet4")
            .field("temperature", item.temperature)
            .tag("room", "office")
            .time(item.date)
        )
        record_list.append(
            Point("aranet4")
            .field("co2", item.co2)
            .tag("room", "office")
            .time(item.date)
        )
        record_list.append(
            Point("aranet4")
            .field("pressure", item.pressure)
            .tag("room", "office")
            .time(item.date)
        )
        record_list.append(
            Point("aranet4")
            .field("humidity", item.humidity)
            .tag("room", "office")
            .time(item.date)
        )
    return record_list


def write_influxdb(record_list: list):
    """Write to InfluxDB

    Args:
        record_list (list): List of InfluxDB points
    """
    influx_bucket = os.getenv("INFLUX_BUCKET")

    client = connect_to_influxdb()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=influx_bucket, record=record_list)
    client.close()

    logging.info("Writing %s records to InfluxDB", len(record_list))


def main():
    influx_records = read_aranet_data()
    write_influxdb(influx_records)


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %Z",
        filename=os.getenv("LOGGING_FILE")
    )

    if "READ_INTERVAL" in os.environ:
        READ_INTERVAL = int(os.getenv("READ_INTERVAL"))
        logging.info("Run every %s minutes", READ_INTERVAL)
        try:
            while True:
                main()
                sleep(READ_INTERVAL * 60)
        except KeyboardInterrupt:
            logging.warning("Interrupted")
    else:
        logging.info("Run once")
        main()
