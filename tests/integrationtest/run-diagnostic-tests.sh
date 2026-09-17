#!/usr/bin/env bash
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

self_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if [[ "${1:-}" == "--tiup-node" ]]; then
    shift
    # Keep the complete node logs outside TiUP's data directory so failed
    # assertions remain diagnosable after `tiup clean`.
    node_log="${DIAGNOSTIC_TEST_LOG_DIR}/$(basename "$PWD").log"
    set -- "$@" --log-file="$node_log" -L info
    if [[ "$(basename "$PWD")" == tidb-1 ]]; then
        for ((i=0; i<120; i++)); do
            if curl -fsS --max-time 2 http://127.0.0.1:10080/status >/dev/null 2>&1; then
                exec "$DIAGNOSTIC_TIDB_BIN" "$@" --diagnostic-mode
            fi
            sleep 1
        done
        echo 'Normal TiDB bootstrap timed out' >&2
        exit 1
    fi
    exec "$DIAGNOSTIC_TIDB_BIN" "$@"
fi

cd "${self_dir}"
if [[ "${1:-}" == --help ]]; then
    echo 'Usage: TIDB_SERVER_BIN=/absolute/path/to/tidb-server ./run-diagnostic-tests.sh'
    echo 'TiUP updates PD/TiKV/TiDB to the latest stable packages. TIDB_SERVER_BIN overrides TiDB.'
    echo 'Set DIAGNOSTIC_LEGACY_TEST=1 to run the original UniStore test with run-tests.sh options.'
    exit 0
fi
if [[ "${DIAGNOSTIC_LEGACY_TEST:-0}" == "1" ]]; then
    # Keep the existing UniStore read-only coverage.
    TIDB_TEST_STORE_NAME=unistore TIDB_TEST_DIAGNOSTIC_MODE=1 \
        ./run-tests.sh -t ddl/diagnostic_mode "$@"

    exit $?
fi

[[ $# -eq 0 ]] || { echo 'Unexpected arguments; use --help' >&2; exit 1; }
command -v tiup >/dev/null
command -v lsof >/dev/null
tiup update playground pd tikv tidb
tidb_server="${TIDB_SERVER_BIN:-$(tiup --binary tidb)}"
tidb_server="$(cd "$(dirname "$tidb_server")" && pwd)/$(basename "$tidb_server")"
export DIAGNOSTIC_TIDB_BIN="$tidb_server"
if [[ ! -x "${tidb_server}" ]]; then
    echo "tidb-server binary not found: ${tidb_server}" >&2
    exit 1
fi
server_help=$("$tidb_server" --help 2>&1)
if [[ "$server_help" != *-diagnostic-mode* ]]; then
    echo 'Downloaded TiDB does not support --diagnostic-mode; set TIDB_SERVER_BIN to a build of this branch.' >&2
    exit 1
fi

test_dir=$(mktemp -d "${TMPDIR:-/tmp}/tidb-diagnostic-online-ddl.XXXXXX")
export DIAGNOSTIC_TEST_LOG_DIR="$test_dir"
import_file="${test_dir}/import.csv"
printf '1,100\n2,200\n3,300\n' > "${import_file}"

normal_port=4000
diagnostic_port=4001
playground_pid=""
tag="diagnostic-$(basename "$test_dir")"
cleanup_tidb() {
    local result=$?
    trap - EXIT INT TERM
    if [[ -n "$playground_pid" ]]; then
        tiup clean "$tag" || result=1
        wait "$playground_pid" 2>/dev/null || true
    fi
    if [[ "$result" == 0 ]]; then
        rm -r -- "$test_dir"
    else
        echo "Test logs retained at $test_dir" >&2
    fi
    exit "$result"
}
trap cleanup_tidb EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

check_ddl_start_logs() {
    local normal_log="$test_dir/tidb-0.log"
    local diagnostic_log="$test_dir/tidb-1.log"
    local status
    # Missing/empty logs must fail, not masquerade as absence of DDL startup.
    if [[ ! -r "$normal_log" || ! -s "$normal_log" || ! -r "$diagnostic_log" || ! -s "$diagnostic_log" ]]; then
        echo 'Missing or empty TiDB logs; cannot verify DDL startup' >&2
        return 1
    fi
    if ! grep -F '"start DDL"' "$normal_log" >/dev/null; then
        echo "Normal TiDB is missing the expected start DDL log: $normal_log" >&2
        return 1
    fi
    if grep -nF '"start DDL"' "$diagnostic_log"; then
        echo "Diagnostic TiDB unexpectedly started DDL: $diagnostic_log" >&2
        return 1
    else
        status=$?
        if [[ "$status" != 1 ]]; then
            echo "Failed to read Diagnostic TiDB log: $diagnostic_log" >&2
            return "$status"
        fi
    fi
    echo 'DDL startup logs verified: normal present, Diagnostic absent'
}

for port in 2379 2380 4000 4001 10080 10081 20160 20180; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "Required port $port is already in use" >&2
        exit 1
    fi
done
printf '#!/usr/bin/env bash\nexec %q --tiup-node "$@"\n' "$self_dir/run-diagnostic-tests.sh" > "$test_dir/tidb-wrapper"
chmod +x "$test_dir/tidb-wrapper"
tiup playground --tag "$tag" --pd 1 --kv 1 --db 2 --tiflash 0 --without-monitor --db.timeout 180 \
    --db.binpath "$test_dir/tidb-wrapper" > "$test_dir/playground.log" 2>&1 &
playground_pid=$!
for ((i=0; i<180; i++)); do
    if ! kill -0 "$playground_pid" 2>/dev/null; then
        cat "$test_dir/playground.log" >&2
        exit 1
    fi
    if curl -fsS --max-time 2 http://127.0.0.1:10081/status >/dev/null 2>&1; then
        check_ddl_start_logs
        go run ./diagnostictest -normal-port "$normal_port" -diagnostic-port "$diagnostic_port" \
            -etcd-endpoint 127.0.0.1:2379 -keyspace "" -import-file "$import_file"
        check_ddl_start_logs
        echo "Diagnostic online DDL tests passed"
        exit 0
    fi
    sleep 1
done
cat "$test_dir/playground.log" >&2
echo "TiUP cluster readiness timed out" >&2
exit 1
