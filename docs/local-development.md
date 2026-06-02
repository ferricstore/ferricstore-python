# Local Development

This guide gets a local SDK checkout connected to a local FerricStore server.

## SDK setup

```bash
git clone https://github.com/ferricstore/ferricstore-python.git
cd ferricstore-python

python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

## Start FerricStore with Docker Compose

```bash
cp .env.example .env
docker compose up -d ferricstore
```

The compose file uses:

```text
FERRICSTORE_IMAGE=ferricstore/ferricstore:latest
FERRICSTORE_PORT=6379
FERRICSTORE_SHARD_COUNT=4
```

If you built the server image locally, set:

```bash
FERRICSTORE_IMAGE=your-local-image:tag docker compose up -d ferricstore
```

## Run examples

```bash
python examples/order_workflow.py
python examples/queue_worker.py
python examples/native_commands.py
```

## Run tests

```bash
pytest
```

Unit tests use fake Redis executors and do not require the server. Integration
or benchmark runs need a local FerricStore server.

## Stop local services

```bash
docker compose down
```

Delete local data:

```bash
docker compose down -v
```

