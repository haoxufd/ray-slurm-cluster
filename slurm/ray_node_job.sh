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
FAILURE_LOG_TAIL_LINES="${FAILURE_LOG_TAIL_LINES:-80}"
STATE_HELPER_MAX_ATTEMPTS="${STATE_HELPER_MAX_ATTEMPTS:-3}"
STATE_HELPER_RETRY_DELAY_SECONDS="${STATE_HELPER_RETRY_DELAY_SECONDS:-2}"
SQUEUE_QUERY_MAX_ATTEMPTS="${SQUEUE_QUERY_MAX_ATTEMPTS:-3}"
SQUEUE_RETRY_DELAY_SECONDS="${SQUEUE_RETRY_DELAY_SECONDS:-2}"
DEFAULT_TMP_PREFIX="/mnt/vast-nhr/projects/nii00233/xuhao/ray-tmp"

if [[ -z "${CLUSTER_ID:-}" || -z "${STATE_ROOT:-}" || -z "${SIF_PATH:-}" || -z "${DATA_STORAGE_PATH:-}" || -z "${RAY_PORT:-}" || -z "${PARTITION_NAME:-}" ]]; then
    echo "missing required environment variables" >&2
    exit 1
fi

HOSTNAME_VALUE="$(hostname)"
TMP_PREFIX_VALUE="${TMP_PREFIX:-$DEFAULT_TMP_PREFIX}"
TMP_PREFIX_VALUE="${TMP_PREFIX_VALUE%/}"
if [[ -z "$TMP_PREFIX_VALUE" ]]; then
    TMP_PREFIX_VALUE="/tmp"
fi
RAY_TMP_DIR="${TMP_PREFIX_VALUE}/tmp-${HOSTNAME_VALUE}"
RAY_CONTAINER_TMP_DIR="/tmp"
mkdir -p "$RAY_TMP_DIR"

log_bootstrap_diagnostics() {
    echo "bootstrap diagnostics: hostname=$HOSTNAME_VALUE" >&2
    echo "bootstrap diagnostics: ray_tmp_dir=$RAY_TMP_DIR" >&2
    echo "bootstrap diagnostics: ray_container_tmp_dir=$RAY_CONTAINER_TMP_DIR" >&2

    if command -v lscpu >/dev/null 2>&1; then
        echo "bootstrap diagnostics: lscpu summary begin" >&2
        lscpu | awk '
            /^Architecture:/ || /^Model name:/ || /^CPU\(s\):/ || /^Flags:/ { print }
        ' >&2 || true
        echo "bootstrap diagnostics: lscpu summary end" >&2
    else
        echo "bootstrap diagnostics: lscpu unavailable" >&2
    fi

    if command -v "$APPTAINER_BIN" >/dev/null 2>&1; then
        echo "bootstrap diagnostics: apptainer which=$(command -v "$APPTAINER_BIN")" >&2
        if command -v readlink >/dev/null 2>&1; then
            echo "bootstrap diagnostics: apptainer realpath=$(readlink -f "$(command -v "$APPTAINER_BIN")")" >&2
        fi
        echo "bootstrap diagnostics: apptainer version begin" >&2
        "$APPTAINER_BIN" --version >&2 || true
        echo "bootstrap diagnostics: apptainer version end" >&2
    else
        echo "bootstrap diagnostics: apptainer binary not found: $APPTAINER_BIN" >&2
    fi
}

run_apptainer() {
    echo "bootstrap diagnostics: running $APPTAINER_BIN $*" >&2
    "$APPTAINER_BIN" "$@"
}

run_state_helper() {
    python3 "$STATE_HELPER" "$@"
}

monitor_log() {
    local level=$1
    shift
    local timestamp
    timestamp="$(date '+%Y-%m-%dT%H:%M:%S%z')"
    echo "$timestamp monitor[$level] cluster=$CLUSTER_ID job=$SLURM_JOB_ID role=${ROLE:-unknown} $*" >&2
}

compact_message() {
    local message=$1
    message="${message//$'\n'/; }"
    message="${message//$'\t'/ }"
    printf '%s\n' "$message"
}

run_state_helper_with_retry() {
    local description=$1
    shift

    local attempt=1
    local output=""
    while (( attempt <= STATE_HELPER_MAX_ATTEMPTS )); do
        if output="$(run_state_helper "$@" 2>&1)"; then
            printf '%s\n' "$output"
            return 0
        fi

        monitor_log "warn" "$description failed (attempt $attempt/$STATE_HELPER_MAX_ATTEMPTS): $(compact_message "$output")"
        if (( attempt == STATE_HELPER_MAX_ATTEMPTS )); then
            return 1
        fi

        attempt=$((attempt + 1))
        sleep "$STATE_HELPER_RETRY_DELAY_SECONDS"
    done
}

query_squeue_field() {
    local job_id=$1
    local field=$2

    local attempt=1
    local output=""
    while (( attempt <= SQUEUE_QUERY_MAX_ATTEMPTS )); do
        if output="$("$SQUEUE_BIN" -j "$job_id" -h -O "$field" 2>&1)"; then
            local value
            value="$(printf '%s\n' "$output" | awk 'NF {print $1; exit}')"
            if [[ -z "$value" ]]; then
                monitor_log "warn" "squeue returned empty $field for job=$job_id"
                printf 'UNKNOWN\n'
            else
                printf '%s\n' "$value"
            fi
            return 0
        fi

        if [[ "$output" == *"Invalid job id specified"* ]]; then
            monitor_log "info" "squeue reports missing job_id=$job_id field=$field"
            printf 'MISSING\n'
            return 0
        fi

        monitor_log "warn" "squeue query failed for job=$job_id field=$field (attempt $attempt/$SQUEUE_QUERY_MAX_ATTEMPTS): $(compact_message "$output")"
        if (( attempt == SQUEUE_QUERY_MAX_ATTEMPTS )); then
            printf 'UNKNOWN\n'
            return 0
        fi

        attempt=$((attempt + 1))
        sleep "$SQUEUE_RETRY_DELAY_SECONDS"
    done
}

is_definitively_unavailable_head_state() {
    local state=$1
    case "$state" in
        MISSING|CANCELLED|FAILED|TIMEOUT|NODE_FAIL|OUT_OF_MEMORY|PREEMPTED|BOOT_FAIL|DEADLINE|STOPPED|SUSPENDED|REVOKED)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

resolve_log_path() {
    local template="${JOB_LOG_PATH_TEMPLATE:-}"
    if [[ -z "$template" ]]; then
        return 0
    fi
    printf '%s\n' "${template//%j/$SLURM_JOB_ID}"
}

notify_failure_exit() {
    local exit_code=$1
    if (( exit_code == 0 )) || [[ -z "${NOTIFY_EMAIL:-}" ]]; then
        return 0
    fi

    local log_path=""
    log_path="$(resolve_log_path)"

    local log_tail=""
    if [[ -n "$log_path" && -f "$log_path" ]]; then
        log_tail="$(tail -n "$FAILURE_LOG_TAIL_LINES" "$log_path" 2>/dev/null || true)"
    fi

    if ! run_state_helper notify-node-failed \
        --email "$NOTIFY_EMAIL" \
        --cluster-id "$CLUSTER_ID" \
        --job-id "$SLURM_JOB_ID" \
        --hostname "$HOSTNAME_VALUE" \
        --partition "$PARTITION_NAME" \
        --exit-code "$exit_code" \
        --log-path "$log_path" \
        --log-tail "$log_tail"; then
        echo "warning: node failure email failed for cluster=$CLUSTER_ID job=$SLURM_JOB_ID" >&2
    fi
}

trap 'exit_code=$?; trap - EXIT; notify_failure_exit "$exit_code"; exit "$exit_code"' EXIT

get_current_epoch() {
    run_state_helper_with_retry "get-epoch" get-epoch \
        --state-root "$STATE_ROOT" \
        --cluster-id "$CLUSTER_ID"
}

get_current_head_job_id() {
    run_state_helper_with_retry "get-head-job-id" get-head-job-id \
        --state-root "$STATE_ROOT" \
        --cluster-id "$CLUSTER_ID"
}

ray_stop() {
    run_apptainer exec --nv instance://run ray stop >/dev/null 2>&1 || true
}

start_as_head() {
    ray_stop
    if ! run_apptainer exec --nv instance://run \
        ray start --head \
        --port="$RAY_PORT" \
        --temp-dir="$RAY_CONTAINER_TMP_DIR"; then
        return 1
    fi
    if ! run_state_helper set-head \
        --state-root "$STATE_ROOT" \
        --cluster-id "$CLUSTER_ID" \
        --job-id "$SLURM_JOB_ID" \
        --hostname "$HOSTNAME_VALUE"; then
        return 1
    fi
    ROLE="head"
}

start_as_worker_for_epoch() {
    local min_epoch=$1
    local head_hostname
    if ! head_hostname="$(run_state_helper wait-head-update \
        --state-root "$STATE_ROOT" \
        --cluster-id "$CLUSTER_ID" \
        --min-epoch "$min_epoch")"; then
        return 1
    fi
    ray_stop
    if ! run_apptainer exec --nv instance://run \
        ray start \
        --address="${head_hostname}:${RAY_PORT}"; then
        return 1
    fi
    ROLE="worker"
}

recover_from_stale_head_on_bootstrap() {
    monitor_log "warn" "initial worker bootstrap failed; checking whether recorded head is stale"

    local current_epoch
    if ! current_epoch="$(get_current_epoch)"; then
        monitor_log "warn" "failed to read current epoch during bootstrap recovery"
        return 1
    fi
    current_epoch="$(printf '%s\n' "$current_epoch" | awk 'NF {print $1; exit}')"
    if [[ -z "$current_epoch" || ! "$current_epoch" =~ ^[0-9]+$ ]]; then
        monitor_log "warn" "invalid epoch value='$current_epoch' during bootstrap recovery"
        return 1
    fi

    local head_job_id
    if ! head_job_id="$(get_current_head_job_id)"; then
        monitor_log "warn" "failed to read head job id during bootstrap recovery"
        return 1
    fi
    head_job_id="$(printf '%s\n' "$head_job_id" | awk 'NF {print $1; exit}')"

    local head_state="MISSING"
    if [[ -n "$head_job_id" ]]; then
        head_state="$(job_state "$head_job_id")"
        if [[ "$head_state" == "UNKNOWN" ]]; then
            monitor_log "warn" "head job state unknown for job_id=$head_job_id during bootstrap recovery"
            return 1
        fi
    fi
    monitor_log "warn" "bootstrap recovery observed head_job_id=${head_job_id:-none} head_state=$head_state"

    if [[ "$head_state" == "RUNNING" || "$head_state" == "COMPLETING" ]]; then
        monitor_log "warn" "recorded head still appears alive; not forcing failover during bootstrap"
        return 1
    fi

    if ! is_definitively_unavailable_head_state "$head_state"; then
        monitor_log "warn" "head state '$head_state' is not a definitive failure; skip bootstrap failover"
        return 1
    fi

    local candidate_job_id
    candidate_job_id="$(select_failover_candidate)"
    if [[ -z "$candidate_job_id" ]]; then
        monitor_log "warn" "no running failover candidate available during bootstrap recovery"
        return 1
    fi

    if [[ "$candidate_job_id" == "$SLURM_JOB_ID" ]]; then
        monitor_log "info" "this job selected as bootstrap failover candidate"
        if run_state_helper try-acquire-failover-lock \
            --state-root "$STATE_ROOT" \
            --cluster-id "$CLUSTER_ID" \
            --job-id "$SLURM_JOB_ID"; then
            if start_as_head; then
                monitor_log "info" "bootstrap failover promoted this job to head successfully"
                run_state_helper release-failover-lock \
                    --state-root "$STATE_ROOT" \
                    --cluster-id "$CLUSTER_ID" >/dev/null 2>&1 || true
                return 0
            fi
            monitor_log "error" "bootstrap failover failed to promote this job to head"
            run_state_helper release-failover-lock \
                --state-root "$STATE_ROOT" \
                --cluster-id "$CLUSTER_ID" >/dev/null 2>&1 || true
            return 1
        fi
    fi

    monitor_log "info" "bootstrap failover chose candidate job_id=$candidate_job_id; waiting for refreshed head"
    start_as_worker_for_epoch "$current_epoch"
}

job_state() {
    local job_id=$1
    query_squeue_field "$job_id" "State"
}

job_time_left() {
    local job_id=$1
    query_squeue_field "$job_id" "TimeLeft"
}

parse_time_left_seconds() {
    local raw=$1
    if [[ ! "$raw" =~ ^([0-9]+-)?[0-9]{1,2}:[0-9]{2}(:[0-9]{2})?$ ]]; then
        return 1
    fi

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
        if [[ "$state" == "UNKNOWN" ]]; then
            monitor_log "warn" "skip candidate job_id=$job_id due to unknown state"
            continue
        fi
        [[ "$state" == "RUNNING" || "$state" == "COMPLETING" ]] || continue
        local time_left
        time_left="$(job_time_left "$job_id")"
        if [[ -z "$time_left" || "$time_left" == "UNKNOWN" || "$time_left" == "MISSING" ]]; then
            monitor_log "warn" "skip candidate job_id=$job_id due to unavailable time_left=$time_left"
            continue
        fi
        local seconds
        if ! seconds="$(parse_time_left_seconds "$time_left")"; then
            monitor_log "warn" "skip candidate job_id=$job_id due to unparsable time_left=$time_left"
            continue
        fi
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
    monitor_log "info" "head failover monitor started interval=${HEAD_CHECK_INTERVAL_SECONDS}s max_iterations=$MAX_MONITOR_ITERATIONS"
    while true; do
        if (( MAX_MONITOR_ITERATIONS > 0 && iteration >= MAX_MONITOR_ITERATIONS )); then
            break
        fi
        iteration=$((iteration + 1))

        local current_epoch
        if ! current_epoch="$(get_current_epoch)"; then
            monitor_log "warn" "failed to read current epoch; retrying next loop"
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi
        current_epoch="$(printf '%s\n' "$current_epoch" | awk 'NF {print $1; exit}')"
        if [[ -z "$current_epoch" || ! "$current_epoch" =~ ^[0-9]+$ ]]; then
            monitor_log "warn" "invalid epoch value='$current_epoch'; retrying next loop"
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi

        local head_job_id
        if ! head_job_id="$(get_current_head_job_id)"; then
            monitor_log "warn" "failed to read head job id; retrying next loop"
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi
        head_job_id="$(printf '%s\n' "$head_job_id" | awk 'NF {print $1; exit}')"
        if [[ -z "$head_job_id" ]]; then
            monitor_log "info" "head job id is empty; waiting for head election"
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi
        if [[ "$head_job_id" == "$SLURM_JOB_ID" ]]; then
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi

        local state
        state="$(job_state "$head_job_id")"
        if [[ "$state" == "UNKNOWN" ]]; then
            monitor_log "warn" "head job state unknown for job_id=$head_job_id; retrying"
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi
        monitor_log "info" "iteration=$iteration epoch=$current_epoch head_job_id=$head_job_id head_state=$state"
        if [[ "$state" == "RUNNING" || "$state" == "COMPLETING" ]]; then
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi

        if ! is_definitively_unavailable_head_state "$state"; then
            monitor_log "warn" "head state '$state' is not a definitive failure; skip failover this round"
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi

        monitor_log "warn" "head job appears unavailable (state=$state); evaluating failover candidates"
        local candidate_job_id
        candidate_job_id="$(select_failover_candidate)"
        if [[ -z "$candidate_job_id" ]]; then
            monitor_log "warn" "no running failover candidate found; will retry"
            sleep "$HEAD_CHECK_INTERVAL_SECONDS"
            continue
        fi

        if [[ "$candidate_job_id" == "$SLURM_JOB_ID" ]]; then
            monitor_log "info" "this job selected as failover candidate"
            if run_state_helper try-acquire-failover-lock \
                --state-root "$STATE_ROOT" \
                --cluster-id "$CLUSTER_ID" \
                --job-id "$SLURM_JOB_ID"; then
                if start_as_head; then
                    monitor_log "info" "promoted to head successfully"
                else
                    monitor_log "error" "failed to promote to head; keeping current role"
                fi
                run_state_helper release-failover-lock \
                    --state-root "$STATE_ROOT" \
                    --cluster-id "$CLUSTER_ID" >/dev/null 2>&1 || true
            fi
        else
            monitor_log "info" "candidate job_id=$candidate_job_id selected as head; reconnecting as worker"
            if ! start_as_worker_for_epoch "$current_epoch"; then
                monitor_log "warn" "failed to reconnect worker to refreshed head"
            fi
        fi

        sleep "$HEAD_CHECK_INTERVAL_SECONDS"
    done
}

python3 "$STATE_HELPER" register-node \
    --state-root "$STATE_ROOT" \
    --cluster-id "$CLUSTER_ID" \
    --job-id "$SLURM_JOB_ID" \
    --hostname "$HOSTNAME_VALUE"

if [[ -n "${NOTIFY_EMAIL:-}" ]]; then
    if ! python3 "$STATE_HELPER" notify-node-registered \
        --email "$NOTIFY_EMAIL" \
        --cluster-id "$CLUSTER_ID" \
        --job-id "$SLURM_JOB_ID" \
        --hostname "$HOSTNAME_VALUE" \
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
log_bootstrap_diagnostics
run_apptainer instance start --nv --bind "$RAY_TMP_DIR:$RAY_CONTAINER_TMP_DIR" --bind "$DATA_STORAGE_PATH:$DATA_STORAGE_PATH" "$SIF_PATH" run

if [[ "$ROLE" == "head" ]]; then
    start_as_head
else
    if ! start_as_worker_for_epoch 0; then
        recover_from_stale_head_on_bootstrap
    fi
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
