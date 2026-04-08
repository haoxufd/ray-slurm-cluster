#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


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


def cmd_init(args: argparse.Namespace) -> int:
    root = cluster_dir(args.state_root, args.cluster_id)
    root.mkdir(parents=True, exist_ok=False)
    (root / "nodes").mkdir()
    (root / "workers").mkdir()
    (root / "desired_nodes").write_text(f"{args.num_nodes}\n")
    (root / "jobs.txt").write_text("")
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
        "ip": args.ip,
    }
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


def cmd_publish_head_ip(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    (root / "head_ip").write_text(f"{args.ip}\n", encoding="utf-8")
    return 0


def cmd_wait_head_ip(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    head_ip_path = root / "head_ip"
    deadline = time.time() + args.timeout_seconds
    while time.time() <= deadline:
        if head_ip_path.exists():
            print(head_ip_path.read_text(encoding="utf-8").strip())
            return 0
        time.sleep(args.poll_interval)
    print("timed out waiting for head_ip", file=sys.stderr)
    return 1


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


def cmd_status(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    jobs = (root / "jobs.txt").read_text(encoding="utf-8").splitlines()
    desired = (root / "desired_nodes").read_text(encoding="utf-8").strip()
    registered = count_files(root / "nodes")
    ready_nodes = count_files(root / "workers")
    head_ip = "unknown"
    head_job_id = ""
    if (root / "head.lock").exists():
        head_job_id = (root / "head.lock").read_text(encoding="utf-8").strip()
    if (root / "head_ip").exists():
        head_ip = (root / "head_ip").read_text(encoding="utf-8").strip()
    is_ready = "yes" if (root / "ready").exists() else "no"

    lines = [
        f"cluster_id={args.cluster_id}",
        f"desired_nodes={desired}",
        f"submitted_jobs={len(jobs)}",
        f"registered_nodes={registered}",
        f"ready_nodes={ready_nodes}",
        f"head_ip={head_ip}",
        f"cluster_ready={is_ready}",
    ]
    ready_job_ids = {path.name for path in (root / "workers").iterdir() if path.is_file()}
    for job_id in jobs:
        node_path = root / "nodes" / f"{job_id}.json"
        hostname = "unknown"
        ip = "unknown"
        if node_path.exists():
            record = json.loads(node_path.read_text(encoding="utf-8"))
            hostname = record.get("hostname", "unknown")
            ip = record.get("ip", "unknown")
        role = "head" if job_id == head_job_id else "worker"
        ready = "yes" if job_id in ready_job_ids else "no"
        lines.append(f"job={job_id} hostname={hostname} ip={ip} role={role} ready={ready}")
    print("\n".join(lines))
    return 0


def cmd_cleanup(args: argparse.Namespace) -> int:
    root = require_cluster(args.state_root, args.cluster_id)
    shutil.rmtree(root)
    return 0


def cmd_notify_node_registered(args: argparse.Namespace) -> int:
    if not args.email:
        return 0

    subject = f"[ray-slurm-cluster] node registered for {args.cluster_id}"
    body_lines = [
        f"cluster_id={args.cluster_id}",
        f"job_id={args.job_id}",
        f"hostname={args.hostname}",
        f"ip={args.ip}",
        f"partition={args.partition}",
        f"timestamp={time.strftime('%Y-%m-%d %H:%M:%S %Z')}",
    ]
    body = "\n".join(body_lines) + "\n"

    try:
        subprocess.run(
            ["mail", "-s", subject, args.email],
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
    register_parser.add_argument("--ip", required=True)
    register_parser.set_defaults(func=cmd_register_node)

    head_parser = subparsers.add_parser("try-become-head")
    head_parser.add_argument("--state-root", required=True)
    head_parser.add_argument("--cluster-id", required=True)
    head_parser.add_argument("--job-id", required=True)
    head_parser.set_defaults(func=cmd_try_become_head)

    publish_parser = subparsers.add_parser("publish-head-ip")
    publish_parser.add_argument("--state-root", required=True)
    publish_parser.add_argument("--cluster-id", required=True)
    publish_parser.add_argument("--ip", required=True)
    publish_parser.set_defaults(func=cmd_publish_head_ip)

    wait_parser = subparsers.add_parser("wait-head-ip")
    wait_parser.add_argument("--state-root", required=True)
    wait_parser.add_argument("--cluster-id", required=True)
    wait_parser.add_argument("--timeout-seconds", type=float, default=600)
    wait_parser.add_argument("--poll-interval", type=float, default=1.0)
    wait_parser.set_defaults(func=cmd_wait_head_ip)

    ready_parser = subparsers.add_parser("mark-worker-ready")
    ready_parser.add_argument("--state-root", required=True)
    ready_parser.add_argument("--cluster-id", required=True)
    ready_parser.add_argument("--job-id", required=True)
    ready_parser.set_defaults(func=cmd_mark_worker_ready)

    maybe_ready_parser = subparsers.add_parser("maybe-mark-ready")
    maybe_ready_parser.add_argument("--state-root", required=True)
    maybe_ready_parser.add_argument("--cluster-id", required=True)
    maybe_ready_parser.set_defaults(func=cmd_maybe_mark_ready)

    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--state-root", required=True)
    status_parser.add_argument("--cluster-id", required=True)
    status_parser.set_defaults(func=cmd_status)

    cleanup_parser = subparsers.add_parser("cleanup")
    cleanup_parser.add_argument("--state-root", required=True)
    cleanup_parser.add_argument("--cluster-id", required=True)
    cleanup_parser.set_defaults(func=cmd_cleanup)

    notify_parser = subparsers.add_parser("notify-node-registered")
    notify_parser.add_argument("--email", default="")
    notify_parser.add_argument("--cluster-id", required=True)
    notify_parser.add_argument("--job-id", required=True)
    notify_parser.add_argument("--hostname", required=True)
    notify_parser.add_argument("--ip", required=True)
    notify_parser.add_argument("--partition", required=True)
    notify_parser.set_defaults(func=cmd_notify_node_registered)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
