#!/bin/bash
set -euo pipefail

if [[ -z "${REPO_ROOT:-}" ]]; then
    REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi

STATE_HELPER="$REPO_ROOT/scripts/cluster_state.py"

if [[ -z "${CLUSTER_ID:-}" || -z "${STATE_ROOT:-}" || -z "${SIF_PATH:-}" || -z "${DATA_STORAGE_PATH:-}" || -z "${RAY_PORT:-}" ]]; then
    echo "missing required environment variables" >&2
    exit 1
fi

HOSTNAME_VALUE="$(hostname)"
IP_VALUE="$(hostname -I | awk '{print $1}')"

python3 "$STATE_HELPER" register-node \
    --state-root "$STATE_ROOT" \
    --cluster-id "$CLUSTER_ID" \
    --job-id "$SLURM_JOB_ID" \
    --hostname "$HOSTNAME_VALUE" \
    --ip "$IP_VALUE"

ROLE="worker"
if python3 "$STATE_HELPER" try-become-head \
    --state-root "$STATE_ROOT" \
    --cluster-id "$CLUSTER_ID" \
    --job-id "$SLURM_JOB_ID"; then
    ROLE="head"
fi

module load apptainer
apptainer instance start --nv --bind /tmp:/tmp --bind "$DATA_STORAGE_PATH:$DATA_STORAGE_PATH" "$SIF_PATH" run

if [[ "$ROLE" == "head" ]]; then
    apptainer exec --nv instance://run \
        ray start --head \
        --node-ip-address="$IP_VALUE" \
        --port="$RAY_PORT" \
        --temp-dir=/tmp
    python3 "$STATE_HELPER" publish-head-ip \
        --state-root "$STATE_ROOT" \
        --cluster-id "$CLUSTER_ID" \
        --ip "$IP_VALUE"
else
    HEAD_IP="$(python3 "$STATE_HELPER" wait-head-ip \
        --state-root "$STATE_ROOT" \
        --cluster-id "$CLUSTER_ID")"
    apptainer exec --nv instance://run \
        ray start \
        --address="${HEAD_IP}:${RAY_PORT}"
fi

python3 "$STATE_HELPER" mark-worker-ready \
    --state-root "$STATE_ROOT" \
    --cluster-id "$CLUSTER_ID" \
    --job-id "$SLURM_JOB_ID"
python3 "$STATE_HELPER" maybe-mark-ready \
    --state-root "$STATE_ROOT" \
    --cluster-id "$CLUSTER_ID"

sleep infinity
