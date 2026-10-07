#!/usr/bin/env bash
# Runs the integration tests (integration_test.go) against real elasticsearch clusters in containers.
#
#   scripts/integration-test.sh <source version> <target version> [secure]
#
#   scripts/integration-test.sh 7.17.28 8.19.23
#   scripts/integration-test.sh 8.19.23 9.5.5 secure   # target with security, https and a self-signed certificate
#
# Uses podman, or docker when podman is not installed (set CONTAINER_CLI to choose).
# Each node runs with a 384MB heap, both fit into the default 2GB podman machine.
set -euo pipefail

src_version=${1:?source version, ie: 7.17.28}
dst_version=${2:?target version, ie: 8.19.23}
secure=${3:-}
cli=${CONTAINER_CLI:-$(command -v podman >/dev/null && echo podman || echo docker)}
image=docker.io/library/elasticsearch
password=esm-test-password
src_port=${SOURCE_PORT:-19200}
dst_port=${TARGET_PORT:-19201}
src_name=esm-it-source-$$
dst_name=esm-it-target-$$

cleanup() {
	"$cli" rm -f "$src_name" "$dst_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# start_es <name> <version> <port> <secure>
start_es() {
	local args=(-d --name "$1" -p "127.0.0.1:$3:9200"
		-e discovery.type=single-node -e "ES_JAVA_OPTS=-Xms384m -Xmx384m"
		-e cluster.routing.allocation.disk.threshold_enabled=false)
	if [ -n "$4" ]; then
		# security is configured automatically on the first start: https with a self-signed certificate
		args+=(-e "ELASTIC_PASSWORD=$password")
	elif [ "${2%%.*}" -ge 8 ]; then
		args+=(-e xpack.security.enabled=false)
	fi
	"$cli" run "${args[@]}" "$image:$2" >/dev/null
}

# wait_es <name> <url> [curl options]
wait_es() {
	local name=$1 url=$2
	shift 2
	for _ in $(seq 1 90); do
		if curl -fsS "$@" "$url/_cluster/health?wait_for_status=yellow&timeout=1s" >/dev/null 2>&1; then
			return 0
		fi
		sleep 2
	done
	echo "elasticsearch $name did not start:" >&2
	"$cli" logs --tail 50 "$name" >&2
	return 1
}

echo "starting elasticsearch $src_version and $dst_version${secure:+ (secured target)} with $cli"
start_es "$src_name" "$src_version" "$src_port" ""
start_es "$dst_name" "$dst_version" "$dst_port" "$secure"

export ESM_IT_SOURCE=http://127.0.0.1:$src_port
wait_es "$src_name" "$ESM_IT_SOURCE"
if [ -n "$secure" ]; then
	export ESM_IT_TARGET=https://127.0.0.1:$dst_port ESM_IT_TARGET_AUTH=elastic:$password
	wait_es "$dst_name" "$ESM_IT_TARGET" -k -u "$ESM_IT_TARGET_AUTH"
else
	export ESM_IT_TARGET=http://127.0.0.1:$dst_port
	wait_es "$dst_name" "$ESM_IT_TARGET"
fi

cd "$(dirname "$0")/.."
go test -tags integration -run Integration -count=1 -v .
