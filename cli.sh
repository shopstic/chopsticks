#!/usr/bin/env bash
set -euo pipefail

export FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE:-"$(dirname "$(realpath "$0")")/.fdb/cluster.file"}"

start_ephemeral_fdb_server() {
  local background=${1:-"background"}
  local data_dir pid

  # shellcheck disable=SC2317
  cleanup() {
    local exit_code=$?
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "Stopping fdbserver (PID: $pid)" >&2
      kill -15 "$pid" || true
    fi
    if [[ -n "${data_dir:-}" ]] && [[ -d "$data_dir" ]]; then
      rm -rf "$data_dir"
    fi
    exit "$exit_code"
  }
  trap cleanup EXIT

  if [[ -z "${FDB_CLUSTER_FILE:-}" ]]; then
    echo "Error: FDB_CLUSTER_FILE environment variable not set" >&2
    exit 1
  fi

  data_dir="$(mktemp -d)" || {
    echo "Error: Failed to create temporary directory" >&2
    exit 1
  }
  chmod 700 "$data_dir"

  mkdir -p "$data_dir/data" "$data_dir/trace" || {
    echo "Error: Failed to create data directories" >&2
    exit 1
  }
  chmod 700 "$data_dir/data" "$data_dir/trace"

  local fdb_port=4500
  while ! { echo >/dev/tcp/127.0.0.1/"$fdb_port"; } 2>/dev/null; do
    echo "Port $fdb_port is in use, trying another port..." >&2
    fdb_port=$((fdb_port + 1))
  done

  local cluster_dir
  cluster_dir="$(dirname "$FDB_CLUSTER_FILE")"
  mkdir -p "$cluster_dir" || {
    echo "Error: Failed to create cluster file directory" >&2
    exit 1
  }

  local connection_string="t17x3130g3ju1xwxnnwaal6e029grtel:o7q2o6qe@127.0.0.1:$fdb_port"
  echo "Generated connection string: $connection_string" >&2
  echo "$connection_string" >"$FDB_CLUSTER_FILE"
  chmod 600 "$FDB_CLUSTER_FILE"

  local fdbserver_path
  fdbserver_path="$(command -v fdbserver)"
  local -a cmd=(
    "$fdbserver_path"
    --cluster_file "$FDB_CLUSTER_FILE"
    --datadir "$data_dir/data"
    --logdir "$data_dir/trace"
    --listen_address "127.0.0.1:$fdb_port"
    --public_address "127.0.0.1:$fdb_port"
  )

  if [[ "$(uname -s)" =~ [dD]arwin ]]; then
    echo "macOS detected, requesting sudo access..." >&2
    if ! sudo -v; then
      echo "Error: Failed to validate sudo access" >&2
      exit 1
    fi
    sudo "${cmd[@]}" >&2 &
  else
    "${cmd[@]}" >&2 &
  fi

  pid=$!
  echo "FDB server process pid=$pid" >&2

  if ! timeout 15 fdbcli --exec "status" >&2; then
    echo "Failed checking for FDB status" >&2
    exit 1
  fi

  echo "Configuring the cluster.." >&2
  if ! fdbcli --exec "configure new single ssd-2" >&2; then
    echo "Error: Failed to configure cluster" >&2
    exit 1
  fi

  echo "FDB server started successfully (PID: $pid)" >&2

  if [[ "$background" == "background" ]]; then
    echo "$pid"
  else
    wait "$pid"
  fi
}

"$@"
