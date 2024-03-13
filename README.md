# Aranet4 to InfluxDB

## Prerequisites
`.env` file:
```
ARANET_ADDRESS=...
INFLUX_URL=...
INFLUX_ORG=...
INFLUX_BUCKET=...
INFLUX_TOKEN=...
LOGGING_FILE=...
READ_INTERVAL=...
```

## Run
```bash
pipenv install
pipenv run python src/main.py
```

## Bluetooth Connection
```bash
bluetoothctl
scan on
pair <aranet-mac-address>
scan off
```

```bash
source ./venv/bin/activate
```

```bash
aranetctl --scan
aranetctl <aranet-mac-address>
```