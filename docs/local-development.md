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
  quay.io/ferricstore/ferricstore:0.11.14@sha256:f7d29befefa15bce4b3755bf786cf7620c814f13bbd336c0d9955581b323b60e
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

Run the native integration suite with its isolated Docker network:

```bash
scripts/run_native_integration.sh
```

The runner starts the pinned FerricStore image, executes the Python SDK tests in
the same Compose network, and removes the stack afterward. This also avoids
Docker Desktop host-port forwarding issues with native protocol frames. Pass a
test path or normal pytest selectors to narrow a run:

```bash
scripts/run_native_integration.sh tests/integration/test_ferricstore_integration.py -k topology
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
