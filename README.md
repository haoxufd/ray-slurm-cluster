# ray-slurm-cluster

Small Slurm + Ray cluster management repo for submitting `N` independent single-node jobs and letting them self-organize into one Ray cluster through a shared filesystem.

## Layout

- `bin/cluster`: CLI for `up`, `status`, `down`
- `config/cluster.env`: runtime and Slurm defaults
- `slurm/ray_node_job.sh`: single-node job bootstrap
- `scripts/cluster_state.py`: cluster state coordination

## Configuration

Edit `config/cluster.env`:

- `SIF_PATH`: Apptainer image path
- `STATE_ROOT`: shared filesystem path for cluster state
- `DATA_STORAGE_PATH`: bind-mounted shared data path
- `LOG_ROOT`: Slurm output directory
- `NOTIFY_EMAIL`: default registration notification email, optional
- `RAY_PORT`: Ray head port
- `TMP_PREFIX`: tmp dir prefix. Each node creates `<TMP_PREFIX>/tmp-<hostname>` and bind-mounts it to container `/tmp`; Ray head starts with `--temp-dir=/tmp` and workers follow the head config.
- `EXCLUDE_NODES`: default Slurm exclude node list (comma-separated), optional
- `SLURM_TIME_LIMIT`: job walltime
- `A100_GPUS_PER_NODE` / `H100_GPUS_PER_NODE`: default `--gpus-per-node` spec per partition family

## Usage

```bash
bin/cluster up 8 ai4science_h100
bin/cluster up 8 ai4science_h100 --email you@example.com
bin/cluster up 8 ai4science_h100 --gpus-per-node H100:2
bin/cluster up 8 ai4science_h100 --ray-tmp-prefix /path/to/ray-tmp
bin/cluster up 8 ai4science_h100 --exclude-nodes ggpu101,ggpu102
bin/cluster up 2 grete:shared --gpus-per-node A100:1 --no-constraint
bin/cluster scale <cluster_id> 4
bin/cluster scale <cluster_id> 4 --gpus-per-node A100:2
bin/cluster scale <cluster_id> 4 --ray-tmp-prefix /path/to/ray-tmp
bin/cluster scale <cluster_id> 4 --exclude-nodes ggpu101,ggpu102
bin/cluster scale <cluster_id> 4 --no-constraint
bin/cluster list
bin/cluster status [cluster_id]
bin/cluster scale [cluster_id] 4
bin/cluster down [cluster_id]
```

`up` returns immediately after submission. Use `squeue --me` to watch scheduling progress.
`list` prints cluster ids under `STATE_ROOT` after pruning stale clusters whose Slurm jobs are no longer active.
Commands that accept `cluster_id` (`status`, `scale`, `down`) can omit it when exactly one cluster exists under `STATE_ROOT`.
`scale` reuses the existing cluster's partition and notification settings, increases `desired_nodes`, and submits additional one-node jobs that join the current Ray cluster as workers.
Both `up` and `scale` accept `--gpus-per-node <spec>` to override the per-node GPU request. If omitted, the command uses the partition family's default from `config/cluster.env`. A `scale` override is saved into the cluster state and becomes the new default for later scaling operations on that cluster.
Both `up` and `scale` accept `--ray-tmp-prefix <path>` to override the tmp prefix. Each node creates `<path>/tmp-<hostname>` and bind-mounts it to container `/tmp`, so Ray uses `/tmp` inside the container while data lands in node-specific subdirectories under the shared prefix. If omitted, the command uses `TMP_PREFIX` from `config/cluster.env` (default `/mnt/vast-nhr/projects/nii00233/xuhao/ray-tmp`), and a `scale` override is saved into cluster state for later scaling operations.
Both `up` and `scale` accept `--exclude-nodes <node1,node2>` to pass Slurm `--exclude` during node allocation, so those nodes are excluded from scheduling. A `scale` override is saved into cluster state and becomes the default exclusion list for later scaling operations on that cluster.
Both `up` and `scale` also accept `--no-constraint` to suppress the partition family's default Slurm `--constraint` flag such as `80gb_vram`. Once used, the cluster saves that preference and later `scale` commands reuse it unless explicitly changed.
If `NOTIFY_EMAIL` is set in config, or `--email` is provided, each node sends one notification email after successful registration by calling the local `mail` command. The job script also sends a failure email if `slurm/ray_node_job.sh` exits non-zero, including the resolved Slurm log path and a tail of the log when available.
`status` only reports currently active nodes and prints node hostnames (no IP fields). It also auto-cleans stale clusters when Slurm is reachable and none of the recorded jobs are active, returning `cluster_removed=1` with `reason=no_active_jobs`.
Stale cleanup is skipped when Slurm is unavailable (for example, transient `squeue` errors), to avoid accidental deletion.

## Head Failover

If the current head job disappears while other cluster jobs are still running, the remaining nodes try to rebuild the Ray control plane automatically.

- Nodes detect head loss by checking whether the recorded head job is still running in Slurm.
- The replacement head is chosen from the remaining running jobs with the longest `TimeLeft`.
- If there is a tie, the smaller Slurm job id wins.
- The new head starts a fresh Ray head process and increments the cluster epoch.
- Other nodes reconnect to the new head after the epoch changes.

This recovery is not transparent to running Ray workloads. Expect a brief control-plane interruption while the new head is elected and the remaining nodes reconnect.

## Shared State

Each cluster creates:

```text
<STATE_ROOT>/<cluster_id>/
```

with:

- `desired_nodes`
- `jobs.txt`
- `nodes/`
- `head.lock`
- `head_hostname`
- `workers/`
- `ready`

## Verification

```bash
python3 -m unittest tests/test_cluster_state.py tests/test_cluster_cli.py
bash -n bin/cluster slurm/ray_node_job.sh
python3 -m py_compile scripts/cluster_state.py
```

## Install CLI

Install executable scripts under `bin/` into `~/bin`:

```bash
bash scripts/install_bin.sh
```

Use a custom destination directory:

```bash
INSTALL_BIN_DIR=/some/path/bin bash scripts/install_bin.sh
```
