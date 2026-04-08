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
- `SLURM_TIME_LIMIT`: job walltime

## Usage

```bash
bin/cluster up 8 ai4science_h100
bin/cluster up 8 ai4science_h100 --email you@example.com
bin/cluster scale <cluster_id> 4
bin/cluster status <cluster_id>
bin/cluster down <cluster_id>
```

`up` returns immediately after submission. Use `squeue --me` to watch scheduling progress.
`scale` reuses the existing cluster's partition and notification settings, increases `desired_nodes`, and submits additional one-node jobs that join the current Ray cluster as workers.
If `NOTIFY_EMAIL` is set in config, or `--email` is provided, each node sends one notification email after successful registration by calling the local `mail` command.

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
- `head_ip`
- `workers/`
- `ready`

## Verification

```bash
python3 -m unittest tests/test_cluster_state.py tests/test_cluster_cli.py
bash -n bin/cluster slurm/ray_node_job.sh
python3 -m py_compile scripts/cluster_state.py
```
