#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"
PROJECT_NAME="${COMPOSE_PROJECT_NAME:-ferricstore-python-native-integration}"
PYTHON_TEST_IMAGE="${FERRICSTORE_PYTHON_TEST_IMAGE:-python:3.13-alpine@sha256:540c7d91f98ff6880174c40e99067bf5941eb54d818a7a5e094d188b196a934d}"

# The SDK test process runs beside FerricStore in the Compose network. This
# avoids Docker Desktop corrupting or stalling native protocol traffic through
# a published host port while preserving the same topology behavior as Linux CI.
export FERRICSTORE_PORT="${FERRICSTORE_PORT:-0}"
export FERRICSTORE_NATIVE_ADVERTISE_HOST="${FERRICSTORE_NATIVE_ADVERTISE_HOST:-ferricstore}"
export FERRICSTORE_NATIVE_ADVERTISE_PORT="${FERRICSTORE_NATIVE_ADVERTISE_PORT:-6388}"

compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

cleanup() {
  local exit_status=$?

  if [[ "$exit_status" -ne 0 ]]; then
    compose logs --no-color --tail=240 ferricstore >&2 || true
  fi
  if [[ "${KEEP_COMPOSE:-0}" != "1" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

compose down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d ferricstore

container_id="$(compose ps -q ferricstore)"
if [[ -z "$container_id" ]]; then
  echo "FerricStore Compose container was not created" >&2
  exit 1
fi

network_name="$(
  docker inspect \
    --format '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' \
    "$container_id" | sed -n '1p'
)"
if [[ -z "$network_name" ]]; then
  echo "FerricStore Compose network could not be resolved" >&2
  exit 1
fi

if [[ "$#" -eq 0 ]]; then
  set -- tests/integration
fi

docker run --rm \
  --network "$network_name" \
  --volume "$ROOT_DIR:/sdk:ro" \
  --workdir /sdk \
  --env PYTHONPATH=/sdk/src \
  --env PYTHONDONTWRITEBYTECODE=1 \
  --env PIP_DISABLE_PIP_VERSION_CHECK=1 \
  --env PIP_ROOT_USER_ACTION=ignore \
  --env FERRICSTORE_INTEGRATION=1 \
  --env FERRICSTORE_URL=ferric://ferricstore:6388 \
  --env FERRICSTORE_HOST=ferricstore \
  --env FERRICSTORE_PORT=6388 \
  --env FERRICSTORE_WAIT_SECONDS="${FERRICSTORE_WAIT_SECONDS:-180}" \
  --env FERRICSTORE_WAIT_STABLE_SECONDS="${FERRICSTORE_WAIT_STABLE_SECONDS:-5}" \
  "$PYTHON_TEST_IMAGE" \
  sh -c '
    python scripts/wait_for_ferricstore.py
    python -m pip install -q "pytest==9.0.3" ".[langchain]"
    python -m pytest -p no:cacheprovider "$@"
  ' native-integration "$@"
