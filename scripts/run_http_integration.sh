#!/usr/bin/env bash
set -euo pipefail

image="${FERRICSTORE_IMAGE:-quay.io/ferricstore/ferricstore:0.11.11@sha256:d9f488539f0d6c1a513d2315e7a9c2947cc795b393f3774c9de8ba5e5b5c21b5}"
container="ferricstore-python-http-integration-$$"
tls_dir="$(mktemp -d /tmp/ferricstore-python-http-integration.XXXXXX)"
username="sdk-http"
password="sdk-http-secret"
denied_username="sdk-http-denied"
denied_password="sdk-http-denied-secret"

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    docker logs "$container" 2>/dev/null || true
  fi
  docker stop "$container" >/dev/null 2>&1 || true
  case "$tls_dir" in
    /tmp/ferricstore-python-http-integration.*) rm -rf "$tls_dir" ;;
  esac
}
trap cleanup EXIT

for command in docker openssl curl grep python; do
  command -v "$command" >/dev/null || {
    echo "$command is required" >&2
    exit 1
  }
done

openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=FerricStore SDK Test CA" -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -addext "subjectKeyIdentifier=hash" -keyout "$tls_dir/ca.key" -out "$tls_dir/ca.pem" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout "$tls_dir/server.key" -out "$tls_dir/server.csr" >/dev/null 2>&1
printf '%s\n' "basicConstraints=critical,CA:FALSE" "keyUsage=critical,digitalSignature,keyEncipherment" "subjectAltName=DNS:localhost,IP:127.0.0.1" "extendedKeyUsage=serverAuth" >"$tls_dir/extensions.cnf"
openssl x509 -req -in "$tls_dir/server.csr" -CA "$tls_dir/ca.pem" -CAkey "$tls_dir/ca.key" -CAcreateserial -days 1 -out "$tls_dir/server.pem" -extfile "$tls_dir/extensions.cnf" >/dev/null 2>&1
chmod 700 "$tls_dir"
chmod 600 "$tls_dir/ca.key"
chmod 644 "$tls_dir/ca.pem" "$tls_dir/server.pem" "$tls_dir/server.key"
rm -f "$tls_dir/ca.key" "$tls_dir/ca.srl" "$tls_dir/server.csr" "$tls_dir/extensions.cnf"

docker run --detach --rm --name "$container" -p 127.0.0.1::8080 --mount "type=bind,source=$tls_dir/server.pem,target=/tls/server.pem,readonly" --mount "type=bind,source=$tls_dir/server.key,target=/tls/server.key,readonly" -e FERRICSTORE_PROTECTED_MODE=false -e FERRICSTORE_FLOW_SCHEDULER_ENABLED=false -e FERRICSTORE_HTTP_ENABLED=true -e FERRICSTORE_HTTP_BIND=0.0.0.0 -e FERRICSTORE_HTTP_PORT=8080 -e FERRICSTORE_HTTP2_ENABLED=true -e FERRICSTORE_HTTP_TLS_ENABLED=true -e FERRICSTORE_HTTP_TLS_CERT_FILE=/tls/server.pem -e FERRICSTORE_HTTP_TLS_KEY_FILE=/tls/server.key "$image" >/dev/null

port="$(docker port "$container" 8080/tcp | sed 's/.*://')"
ready=false
for _ in {1..120}; do
  if curl --silent --show-error --cacert "$tls_dir/ca.pem" "https://127.0.0.1:$port/health" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 0.5
done
[ "$ready" = true ] || {
  echo "FerricStore HTTPS listener did not become ready" >&2
  exit 1
}

docker exec "$container" bin/ferricstore rpc 'case FerricstoreServer.Acl.set_user("sdk-http", ["on", "resetpass", ">sdk-http-secret", "resetkeys", "+@all", "~*", "&*"]) do :ok -> :ok; other -> raise "ACL bootstrap failed: #{inspect(other)}" end' >/dev/null
docker exec "$container" bin/ferricstore rpc 'case FerricstoreServer.Acl.set_user("sdk-http-denied", ["on", "resetpass", ">sdk-http-denied-secret", "resetkeys", "-@all", "+ping", "~*", "&*"]) do :ok -> :ok; other -> raise "restricted ACL bootstrap failed: #{inspect(other)}" end' >/dev/null

body='{"commands":[["PING"]]}'
unauthenticated="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' --cacert "$tls_dir/ca.pem" -H 'content-type: application/json' --data "$body" "https://127.0.0.1:$port/v1/commands")"
[ "$unauthenticated" = 401 ] || {
  echo "unauthenticated HTTP request returned $unauthenticated, expected 401" >&2
  exit 1
}
denied_response="$(curl --silent --show-error --cacert "$tls_dir/ca.pem" --user "$denied_username:$denied_password" -H 'content-type: application/json' --data '{"commands":[["SET","sdk:http:acl","blocked"]]}' "https://127.0.0.1:$port/v1/commands")"
printf '%s' "$denied_response" | grep -Eq '"status"[[:space:]]*:[[:space:]]*"error"' || {
  echo "ACL authorization probe unexpectedly allowed SET" >&2
  exit 1
}
authenticated="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' --cacert "$tls_dir/ca.pem" --user "$username:$password" -H 'content-type: application/json' --data "$body" "https://127.0.0.1:$port/v1/commands")"
[ "$authenticated" = 200 ] || {
  echo "authenticated HTTP request returned $authenticated, expected 200" >&2
  exit 1
}

env FERRICSTORE_INTEGRATION=1 FERRICSTORE_URL="https://127.0.0.1:$port" FERRICSTORE_USERNAME="$username" FERRICSTORE_PASSWORD="$password" FERRICSTORE_CA_FILE="$tls_dir/ca.pem" FERRICSTORE_HTTP_COMMAND_COVERAGE=1 python -m pytest -q tests/integration/test_ferricstore_integration.py
