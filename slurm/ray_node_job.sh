#!/bin/bash
set -euo pipefail

if [[ -z "${REPO_ROOT:-}" ]]; then
    REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi

STATE_HELPER="$REPO_ROOT/scripts/cluster_state.py"
APPTAINER_BIN="${APPTAINER_BIN:-apptainer}"
SQUEUE_BIN="${SQUEUE_BIN:-squeue}"
HEAD_CHECK_INTERVAL_SECONDS="${HEAD_CHECK_INTERVAL_SECONDS:-30}"
MAX_MONITOR_ITERATIONS="${MAX_MONITOR_ITERATIONS:-0}"

if [[ -z "${CLUSTER_ID:-}" || -z "${STATE_ROOT:-}" || -z "${SIF_PATH:-}" || -z "${DATA_STORAGE_PATH:-}" || -z "${RAY_PORT:-}" || -z "${PARTITION_NAME:-}" ]]; then
    echo "missing required environment variables" >&2
    exit 1
fi

HOSTNAME_VALUE="$(hostname)"
IP_VALUE="$(hostname -I | awk '{print $1}')"

run_state_helper() {
    python3 "$STATE_HELPER" "$@"
}

get_current_epoch() {
    run_state_helper get-epoch \
        --state-root "$STATE_ROOT" \
        --cluster-id "$CLUSTER_ID"
}

get_current_head_job_id() {
    run_state_helper get-head-job-id \
        --state-root "$STATE_ROOT" \
        --cluster-id "$CLUSTER_ID"
}

ray_stop() {
    "$APPTAINER_BIN" exec --nv instance://run ray stop >/dev/null 2>&1 || true
}

start_as_head() {
    ray_stop
    "$APPTAINER_BIN" exec --nv instance://run \
        ray start --head \
        --node-ip-address="$IP_VALUE" \
        --port="$RAY_PORT" \
        --temp-dir=/tmp
    run_state_helper set-head \
        --state-root "$STATE_ROOT" \
        --cluster-id "$CLUSTER_ID" \
        --job-id "$SLURM_JOB_ID" \
        --ip "$IP_VALUE"
    ROLE="head"
}

start_as_worker_for_epoch() {
    local min_epoch=$1
    local head_ip
    head_ip="$(run_state_helper wait-head-update \
        --state-root "$STATE_ROOT" \
        --cluster-id "$CLUSTER_ID" \
        --min-epoch "$min_epoch")"
    ray_stop
    "$APPTAINER_BIN" exec --nv instance://run \
        ray start \
        --address="${head_ip}:${RAY_PORT}"
    ROLE="worker"
}

job_state() {
    local job_id=$1
    "$SQUEUE_BIN" -j "$job_id" -h -O State 2>/dev/null | awk 'NF {print $1; exit}'
}

job_time_left() {
    local job_id=$1
    "$SQUEUE_BIN" -j "$job_id" -h -O TimeLeft 2>/dev/null | awk 'NF {print $1; exit}'
}

parse_time_left_seconds() {
    local raw=$1
    local days=0
    local rest=$raw
    if [[ "$rest" == *-* ]]; then
        days="${rest%%-*}"
        rest="${rest#*-}"
    fi

    IFS=':' read -r a b c <<<"$rest"
    local hours=0
    local minutes=0
    local seconds=0
    if [[ -n "${c:-}" ]]; then
        hours=$a
        minutes=$b
        seconds=$c
    else
        minutes=$a
        seconds=$b
    fi
    echo $((10#$seconds + 60 * (10#$minutes + 60 * (10#$hours + 24 * 10#$days))))
}

select_failover_candidate() {
    local best_job=""
    local best_seconds=-1
    local job_id=""
    while read -r job_id; do
        [[ -n "$job_id" ]] || continue
        local state
        state="$(job_state "$job_id")"
        [[ "$state" == "RUNNING" || "$state" == "COMPLETING" ]] || continue
        local time_left
        time_left="$(job_time_left "$job_id")"
        [[ -n "$time_left" ]] || continue
        local seconds
        seconds="$(parse_time_left_seconds "$time_left")"
        if (( seconds > best_seconds )); then
            best_seconds=$seconds
            best_job=$job_id
        elif (( seconds == best_seconds )) && [[ -n "$best_job" ]] && (( 10#$job_id < 10#$best_job )); then
            best_job=$job_id
        fi
    done < "$STATE_ROOT/$CLUSTER_ID/jobs.txt"
    printf '%s\n' "$best_job"
}

monitor_head_failover() {
    local iteration=0
    while true; do
        if (( MAX_MONITOR_ITERATIONS > 0 && iteration >= MAX_MONITOR_ITERATIONS )); then
            break
        fi
        iteration=$((iteration + 1))

        local current_epoch
        current_epoch="$(get_current_epoch)"
        local head_job_id
        head_job_id="$(get_current_head_job_id)"
        if [[ -z "$head_job_id" ]]; then
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi
        if [[ "$head_job_id" == "$SLURM_JOB_ID" ]]; then
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi
        local state
        state="$(job_state "$head_job_id")"
        if [[ "$state" == "RUNNING" || "$state" == "COMPLETING" ]]; then
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi

        local candidate_job_id
        candidate_job_id="$(select_failover_candidate)"
        if [[ -z "$candidate_job_id" ]]; then
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi

        if [[ "$candidate_job_id" == "$SLURM_JOB_ID" ]]; then
            if run_state_helper try-acquire-failover-lock \
                --state-root "$STATE_ROOT" \
                --cluster-id "$CLUSTER_ID" \
                --job-id "$SLURM_JOB_ID"; then
                start_as_head
                run_state_helper release-failover-lock \
                    --state-root "$STATE_ROOT" \
                    --cluster-id "$CLUSTER_ID"
            fi
        else
            start_as_worker_for_epoch "$current_epoch"
        fi

        sleep "$HEAD_CHECK_INTERVAL_SECONDS"
    done
}

python3 "$STATE_HELPER" register-node \
    --state-root "$STATE_ROOT" \
    --cluster-id "$CLUSTER_ID" \
    --job-id "$SLURM_JOB_ID" \
    --hostname "$HOSTNAME_VALUE" \
    --ip "$IP_VALUE"

if [[ -n "${NOTIFY_EMAIL:-}" ]]; then
    if ! python3 "$STATE_HELPER" notify-node-registered \
        --email "$NOTIFY_EMAIL" \
        --cluster-id "$CLUSTER_ID" \
        --job-id "$SLURM_JOB_ID" \
        --hostname "$HOSTNAME_VALUE" \
        --ip "$IP_VALUE" \
        --partition "$PARTITION_NAME"; then
        echo "warning: node registration email failed for cluster=$CLUSTER_ID job=$SLURM_JOB_ID" >&2
    fi
fi

ROLE="worker"
if run_state_helper try-become-head \
    --state-root "$STATE_ROOT" \
    --cluster-id "$CLUSTER_ID" \
    --job-id "$SLURM_JOB_ID"; then
    ROLE="head"
fi

module load apptainer
"$APPTAINER_BIN" instance start --nv --bind /tmp:/tmp --bind "$DATA_STORAGE_PATH:$DATA_STORAGE_PATH" "$SIF_PATH" run

if [[ "$ROLE" == "head" ]]; then
    start_as_head
else
    start_as_worker_for_epoch 0
fi

run_state_helper mark-worker-ready \
    --state-root "$STATE_ROOT" \
    --cluster-id "$CLUSTER_ID" \
    --job-id "$SLURM_JOB_ID"
run_state_helper maybe-mark-ready \
    --state-root "$STATE_ROOT" \
    --cluster-id "$CLUSTER_ID"

monitor_head_failover
sleep infinity
