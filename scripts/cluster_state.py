#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

RUNNING_JOB_STATES = {"RUNNING", "COMPLETING", "CONFIGURING"}


def cluster_dir(state_root: str, cluster_id: str) -> Path:
    return Path(state_root) / cluster_id


def require_cluster(state_root: str, cluster_id: str) -> Path:
    path = cluster_dir(state_root, cluster_id)
    if not path.is_dir():
        raise SystemExit(f"cluster not found: {cluster_id}")
    return path


def count_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for item in path.iterdir() if item.is_file())


def query_slurm_job_states(job_ids: list[str]) -> tuple[dict[str, str], bool]:
    if not job_ids:
        return {}, False

    squeue_bin = os.environ.get("SQUEUE_BIN", "squeue")
    try:
        proc = subprocess.run(
            [squeue_bin, "-h", "-j", ",".join(job_ids), "-O", "JobID,State"],
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError:
        return {}, False

    stderr = (proc.stderr or "").strip()
    if proc.returncode != 0:
        if "Invalid job id specified" in stderr:
            # Query succeeded, but none of the queried jobs are active anymore.
            return {}, True
        if stderr:
            print(f"warning: squeue query failed: {stderr}", file=sys.stderr)
        return {}, False

    states: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        job_id = parts[0].split(".", 1)[0]
        if not job_id:
            continue
        states[job_id] = parts[1]
    return states, True


def resolve_active_jobs(root: Path) -> tuple[list[str], list[str], bool]:
    jobs = (root / "jobs.txt").read_text(encoding="utf-8").splitlines()
    slurm_states, slurm_available = query_slurm_job_states(jobs)
    if slurm_available:
        active_job_ids = [job_id for job_id in jobs if slurm_states.get(job_id, "") in RUNNING_JOB_STATES]
    else:
        active_job_ids = jobs
    return jobs, active_job_ids, slurm_available


def should_cleanup_stale_cluster(jobs: list[str], active_job_ids: list[str], slurm_available: bool) -> bool:
    return slurm_available and bool(jobs) and not active_job_ids


def cmd_init(args: argparse.Namespace) -> int:
    root = cluster_dir(args.state_root, args.cluster_id)
    root.mkdir(parents=True, exist_ok=False)
    (root / "nodes").mkdir()
    (root / "workers").mkdir()
    (root / "desired_nodes").write_text(f"{args.num_nodes}\n")
    (root / "jobs.txt").write_text("")
    (root / "epoch").write_text("0\n", encoding="utf-8")
    notify_email = args.notify_email or ""
    cluster_env_lines = [
        f"CLUSTER_ID={args.cluster_id}",
        f"PARTITION={args.partition}",
        f"NOTIFY_EMAIL={notify_email}",
    ]
    (root / "cluster.env").write_text("\n".join(cluster_env_lines) + "\n", encoding="utf-8")
    return 0


def cmd_add_job(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    with (root / "jobs.txt").open("a", encoding="utf-8") as handle:
        handle.write(f"{args.job_id}\n")
    return 0


def cmd_register_node(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    record = {
        "job_id": args.job_id,
        "hostname": args.hostname,
    }
    if args.ip:
        record["ip"] = args.ip
    (root / "nodes" / f"{args.job_id}.json").write_text(
        json.dumps(record, indent=2) + "\n",
        encoding="utf-8",
    )
    return 0


def cmd_try_become_head(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    lock_path = root / "head.lock"
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return 1
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(f"{args.job_id}\n")
    return 0


def resolve_head_endpoint(args: argparse.Namespace) -> str:
    hostname = (getattr(args, "hostname", "") or "").strip()
    if hostname:
        return hostname
    ip = (getattr(args, "ip", "") or "").strip()
    if ip:
        return ip
    raise SystemExit("head endpoint missing: provide --hostname (preferred) or --ip")


def read_head_endpoint(root: Path) -> str:
    head_hostname_path = root / "head_hostname"
    if head_hostname_path.exists():
        return head_hostname_path.read_text(encoding="utf-8").strip()
    head_ip_path = root / "head_ip"
    if head_ip_path.exists():
        return head_ip_path.read_text(encoding="utf-8").strip()
    return ""


def cmd_publish_head_ip(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    endpoint = resolve_head_endpoint(args)
    (root / "head_hostname").write_text(f"{endpoint}\n", encoding="utf-8")
    return 0


def cmd_set_head(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    current_epoch = int((root / "epoch").read_text(encoding="utf-8").strip())
    endpoint = resolve_head_endpoint(args)
    (root / "head.lock").write_text(f"{args.job_id}\n", encoding="utf-8")
    (root / "head_hostname").write_text(f"{endpoint}\n", encoding="utf-8")
    (root / "epoch").write_text(f"{current_epoch + 1}\n", encoding="utf-8")
    return 0


def cmd_wait_head_ip(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    deadline = time.time() + args.timeout_seconds
    while time.time() <= deadline:
        endpoint = read_head_endpoint(root)
        if endpoint:
            print(endpoint)
            return 0
        time.sleep(args.poll_interval)
    print("timed out waiting for head hostname", file=sys.stderr)
    return 1


def cmd_wait_head_update(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    epoch_path = root / "epoch"
    deadline = time.time() + args.timeout_seconds
    while time.time() <= deadline:
        current_epoch = int(epoch_path.read_text(encoding="utf-8").strip())
        endpoint = read_head_endpoint(root)
        if current_epoch > args.min_epoch and endpoint:
            print(endpoint)
            return 0
        time.sleep(args.poll_interval)
    print("timed out waiting for head update", file=sys.stderr)
    return 1


def cmd_get_head_job_id(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    head_lock = root / "head.lock"
    if head_lock.exists():
        print(head_lock.read_text(encoding="utf-8").strip())
    return 0


def cmd_get_epoch(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    print((root / "epoch").read_text(encoding="utf-8").strip())
    return 0


def cmd_try_acquire_failover_lock(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    lock_path = root / "failover.lock"
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return 1
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(f"{args.job_id}\n")
    return 0


def cmd_release_failover_lock(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    lock_path = root / "failover.lock"
    if lock_path.exists():
        lock_path.unlink()
    return 0


def cmd_mark_worker_ready(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    (root / "workers" / args.job_id).write_text("", encoding="utf-8")
    return 0


def cmd_maybe_mark_ready(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    desired = int((root / "desired_nodes").read_text(encoding="utf-8").strip())
    ready_nodes = count_files(root / "workers")
    if ready_nodes >= desired:
        (root / "ready").write_text("ready\n", encoding="utf-8")
    return 0


def cmd_update_desired_nodes(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    desired_path = root / "desired_nodes"
    current = int(desired_path.read_text(encoding="utf-8").strip())
    desired_path.write_text(f"{current + args.add_nodes}\n", encoding="utf-8")
    if (root / "ready").exists():
        (root / "ready").unlink()
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    jobs, active_job_ids, slurm_available = resolve_active_jobs(root)
    if args.auto_cleanup_stale and should_cleanup_stale_cluster(jobs, active_job_ids, slurm_available):
        shutil.rmtree(root)
        print(
            "\n".join(
                [
                    f"cluster_id={args.cluster_id}",
                    "cluster_removed=1",
                    "reason=no_active_jobs",
                ]
            )
        )
        return 0

    if slurm_available:
        active_job_id_set = set(active_job_ids)
        inactive_job_ids = {job_id for job_id in jobs if job_id not in active_job_id_set}
        for job_id in inactive_job_ids:
            node_path = root / "nodes" / f"{job_id}.json"
            worker_path = root / "workers" / job_id
            if node_path.exists():
                node_path.unlink()
            if worker_path.exists():
                worker_path.unlink()

    head_hostname = ""
    head_job_id = ""
    if (root / "head.lock").exists():
        head_job_id = (root / "head.lock").read_text(encoding="utf-8").strip()
        if head_job_id:
            node_path = root / "nodes" / f"{head_job_id}.json"
            if node_path.exists():
                record = json.loads(node_path.read_text(encoding="utf-8"))
                head_hostname = record.get("hostname", "")

    lines = [
        f"cluster_id={args.cluster_id}",
        f"active_nodes={len(active_job_ids)}",
        f"head_node={head_hostname or 'unknown'}",
        f"node_source={'slurm' if slurm_available else 'state'}",
    ]
    if args.auto_cleanup_stale:
        lines.append("cluster_removed=0")
    for job_id in active_job_ids:
        node_path = root / "nodes" / f"{job_id}.json"
        hostname = "unknown"
        if node_path.exists():
            record = json.loads(node_path.read_text(encoding="utf-8"))
            hostname = record.get("hostname", "unknown")
        lines.append(f"node={hostname}")
    print("\n".join(lines))
    return 0


def cmd_cleanup(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    shutil.rmtree(root)
    return 0


def cmd_prune_stale(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    jobs, active_job_ids, slurm_available = resolve_active_jobs(root)
    if should_cleanup_stale_cluster(jobs, active_job_ids, slurm_available):
        shutil.rmtree(root)
        print("stale=1")
        return 0
    print("stale=0")
    return 0


def send_mail(email: str, subject: str, body_lines: list[str]) -> int:
    if not email:
        return 0

    body = "\n".join(body_lines) + "\n"
    try:
        subprocess.run(
            ["mail", "-s", subject, email],
            input=body,
            text=True,
            check=True,
        )
    except FileNotFoundError as exc:
        print(f"mail command not found: {exc}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        print(f"mail command failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode
    return 0


def cmd_notify_node_registered(args: argparse.Namespace) -> int:
    subject = f"[ray-slurm-cluster] node registered for {args.cluster_id}"
    body_lines = [
        f"cluster_id={args.cluster_id}",
        f"job_id={args.job_id}",
        f"hostname={args.hostname}",
        f"partition={args.partition}",
        f"timestamp={time.strftime('%Y-%m-%d %H:%M:%S %Z')}",
    ]
    if args.ip:
        body_lines.insert(3, f"ip={args.ip}")
    return send_mail(args.email, subject, body_lines)


def cmd_notify_node_failed(args: argparse.Namespace) -> int:
    subject = f"[ray-slurm-cluster] node failed for {args.cluster_id}"
    body_lines = [
        f"cluster_id={args.cluster_id}",
        f"job_id={args.job_id}",
        f"hostname={args.hostname}",
        f"partition={args.partition}",
        f"exit_code={args.exit_code}",
        f"log_path={args.log_path}",
        f"timestamp={time.strftime('%Y-%m-%d %H:%M:%S %Z')}",
    ]
    if args.ip:
        body_lines.insert(3, f"ip={args.ip}")
    if args.log_tail:
        body_lines.extend(["", "log_tail:", args.log_tail])
    return send_mail(args.email, subject, body_lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init")
    init_parser.add_argument("--state-root", required=True)
    init_parser.add_argument("--cluster-id", required=True)
    init_parser.add_argument("--num-nodes", type=int, required=True)
    init_parser.add_argument("--partition", required=True)
    init_parser.add_argument("--notify-email", default="")
    init_parser.set_defaults(func=cmd_init)

    add_job_parser = subparsers.add_parser("add-job")
    add_job_parser.add_argument("--state-root", required=True)
    add_job_parser.add_argument("--cluster-id", required=True)
    add_job_parser.add_argument("--job-id", required=True)
    add_job_parser.set_defaults(func=cmd_add_job)

    register_parser = subparsers.add_parser("register-node")
    register_parser.add_argument("--state-root", required=True)
    register_parser.add_argument("--cluster-id", required=True)
    register_parser.add_argument("--job-id", required=True)
    register_parser.add_argument("--hostname", required=True)
    register_parser.add_argument("--ip", default="")
    register_parser.set_defaults(func=cmd_register_node)

    head_parser = subparsers.add_parser("try-become-head")
    head_parser.add_argument("--state-root", required=True)
    head_parser.add_argument("--cluster-id", required=True)
    head_parser.add_argument("--job-id", required=True)
    head_parser.set_defaults(func=cmd_try_become_head)

    publish_parser = subparsers.add_parser("publish-head-ip")
    publish_parser.add_argument("--state-root", required=True)
    publish_parser.add_argument("--cluster-id", required=True)
    publish_parser.add_argument("--hostname", default="")
    publish_parser.add_argument("--ip", default="")
    publish_parser.set_defaults(func=cmd_publish_head_ip)

    set_head_parser = subparsers.add_parser("set-head")
    set_head_parser.add_argument("--state-root", required=True)
    set_head_parser.add_argument("--cluster-id", required=True)
    set_head_parser.add_argument("--job-id", required=True)
    set_head_parser.add_argument("--hostname", default="")
    set_head_parser.add_argument("--ip", default="")
    set_head_parser.set_defaults(func=cmd_set_head)

    wait_parser = subparsers.add_parser("wait-head-ip")
    wait_parser.add_argument("--state-root", required=True)
    wait_parser.add_argument("--cluster-id", required=True)
    wait_parser.add_argument("--timeout-seconds", type=float, default=600)
    wait_parser.add_argument("--poll-interval", type=float, default=1.0)
    wait_parser.set_defaults(func=cmd_wait_head_ip)

    wait_update_parser = subparsers.add_parser("wait-head-update")
    wait_update_parser.add_argument("--state-root", required=True)
    wait_update_parser.add_argument("--cluster-id", required=True)
    wait_update_parser.add_argument("--min-epoch", type=int, required=True)
    wait_update_parser.add_argument("--timeout-seconds", type=float, default=600)
    wait_update_parser.add_argument("--poll-interval", type=float, default=1.0)
    wait_update_parser.set_defaults(func=cmd_wait_head_update)

    get_head_parser = subparsers.add_parser("get-head-job-id")
    get_head_parser.add_argument("--state-root", required=True)
    get_head_parser.add_argument("--cluster-id", required=True)
    get_head_parser.set_defaults(func=cmd_get_head_job_id)

    get_epoch_parser = subparsers.add_parser("get-epoch")
    get_epoch_parser.add_argument("--state-root", required=True)
    get_epoch_parser.add_argument("--cluster-id", required=True)
    get_epoch_parser.set_defaults(func=cmd_get_epoch)

    failover_parser = subparsers.add_parser("try-acquire-failover-lock")
    failover_parser.add_argument("--state-root", required=True)
    failover_parser.add_argument("--cluster-id", required=True)
    failover_parser.add_argument("--job-id", required=True)
    failover_parser.set_defaults(func=cmd_try_acquire_failover_lock)

    release_failover_parser = subparsers.add_parser("release-failover-lock")
    release_failover_parser.add_argument("--state-root", required=True)
    release_failover_parser.add_argument("--cluster-id", required=True)
    release_failover_parser.set_defaults(func=cmd_release_failover_lock)

    ready_parser = subparsers.add_parser("mark-worker-ready")
    ready_parser.add_argument("--state-root", required=True)
    ready_parser.add_argument("--cluster-id", required=True)
    ready_parser.add_argument("--job-id", required=True)
    ready_parser.set_defaults(func=cmd_mark_worker_ready)

    maybe_ready_parser = subparsers.add_parser("maybe-mark-ready")
    maybe_ready_parser.add_argument("--state-root", required=True)
    maybe_ready_parser.add_argument("--cluster-id", required=True)
    maybe_ready_parser.set_defaults(func=cmd_maybe_mark_ready)

    update_desired_parser = subparsers.add_parser("update-desired-nodes")
    update_desired_parser.add_argument("--state-root", required=True)
    update_desired_parser.add_argument("--cluster-id", required=True)
    update_desired_parser.add_argument("--add-nodes", type=int, required=True)
    update_desired_parser.set_defaults(func=cmd_update_desired_nodes)

    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--state-root", required=True)
    status_parser.add_argument("--cluster-id", required=True)
    status_parser.add_argument("--auto-cleanup-stale", action="store_true")
    status_parser.set_defaults(func=cmd_status)

    cleanup_parser = subparsers.add_parser("cleanup")
    cleanup_parser.add_argument("--state-root", required=True)
    cleanup_parser.add_argument("--cluster-id", required=True)
    cleanup_parser.set_defaults(func=cmd_cleanup)

    prune_parser = subparsers.add_parser("prune-stale")
    prune_parser.add_argument("--state-root", required=True)
    prune_parser.add_argument("--cluster-id", required=True)
    prune_parser.set_defaults(func=cmd_prune_stale)

    notify_parser = subparsers.add_parser("notify-node-registered")
    notify_parser.add_argument("--email", default="")
    notify_parser.add_argument("--cluster-id", required=True)
    notify_parser.add_argument("--job-id", required=True)
    notify_parser.add_argument("--hostname", required=True)
    notify_parser.add_argument("--ip", default="")
    notify_parser.add_argument("--partition", required=True)
    notify_parser.set_defaults(func=cmd_notify_node_registered)

    failed_notify_parser = subparsers.add_parser("notify-node-failed")
    failed_notify_parser.add_argument("--email", default="")
    failed_notify_parser.add_argument("--cluster-id", required=True)
    failed_notify_parser.add_argument("--job-id", required=True)
    failed_notify_parser.add_argument("--hostname", required=True)
    failed_notify_parser.add_argument("--ip", default="")
    failed_notify_parser.add_argument("--partition", required=True)
    failed_notify_parser.add_argument("--exit-code", required=True)
    failed_notify_parser.add_argument("--log-path", default="")
    failed_notify_parser.add_argument("--log-tail", default="")
    failed_notify_parser.set_defaults(func=cmd_notify_node_failed)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
