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

## Start FerricStore with Docker

Using Docker Compose from this repo:

```bash
docker compose up -d ferricstore
python scripts/wait_for_ferricstore.py
```

Or with a direct Docker command:

```bash
docker run --name ferricstore-dev \
  -p 6388:6388 \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -v ferricstore-dev-data:/data \
  ghcr.io/ferricstore/ferricstore:0.5.2
```

This starts one local FerricStore server on:

```text
ferric://127.0.0.1:6388
```

If you built the server image locally, replace the image name:

```bash
docker run --name ferricstore-dev \
  -p 6388:6388 \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -v ferricstore-dev-data:/data \
  your-local-image:tag
```

## Run examples

```bash
python examples/order_workflow.py
python examples/queue_worker.py
python examples/protocol_commands.py
```

## Run tests

```bash
pytest
```

Unit tests use fake command executors and do not require the server. Integration
or benchmark runs need a local FerricStore server.

Run the integration test against the Compose server:

```bash
FERRICSTORE_INTEGRATION=1 pytest tests/integration
FERRICSTORE_INTEGRATION=1 FERRICSTORE_URL=ferric://127.0.0.1:6388 pytest tests/integration
```

## Stop local services

For the Compose server:

```bash
docker compose down -v
```

For the direct `docker run` server:

```bash
docker stop ferricstore-dev
docker rm ferricstore-dev
```

Delete local data:

```bash
docker volume rm ferricstore-dev-data
```
