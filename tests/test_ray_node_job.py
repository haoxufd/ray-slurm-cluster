import os
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
JOB_SCRIPT = REPO_ROOT / "slurm" / "ray_node_job.sh"
DEFAULT_TMP_PREFIX = "/mnt/vast-nhr/projects/nii00233/xuhao/ray-tmp"


class RayNodeJobTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.bin_dir = self.root / "bin"
        self.repo_dir = self.root / "repo"
        self.state_root = self.root / "state"
        self.data_dir = self.root / "data"
        self.bin_dir.mkdir()
        self.repo_dir.mkdir()
        self.state_root.mkdir()
        self.data_dir.mkdir()
        (self.root / "image.sif").write_text("", encoding="utf-8")
        (self.root / "job_1234.out").write_text(
            "line-1\nline-2\nline-3\n",
            encoding="utf-8",
        )

        self.make_fake_command(
            "hostname",
            """#!/bin/bash
set -euo pipefail
echo "node-a"
""",
        )
        self.make_fake_command(
            "module",
            """#!/bin/bash
set -euo pipefail
exit 0
""",
        )
        self.make_fake_command(
            "readlink",
            """#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "-f" ]]; then
  echo "$2"
else
  echo "$1"
fi
""",
        )
        self.make_fake_command(
            "lscpu",
            """#!/bin/bash
set -euo pipefail
cat <<'EOF'
Architecture:                         x86_64
Model name:                           AMD EPYC 7A53 64-Core Processor
CPU(s):                               128
Flags:                                fpu vme de pse tsc
EOF
""",
        )
        self.make_fake_command(
            "apptainer",
            """#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> "${TEST_ROOT}/apptainer.log"
if [[ "${1:-}" == "--version" ]]; then
  echo "apptainer version 1.3.4"
  exit 0
fi
if [[ -n "${APPTAINER_FAIL_PATTERN:-}" && "$*" == *"${APPTAINER_FAIL_PATTERN}"* ]]; then
  exit 1
fi
if [[ "${APPTAINER_FAIL:-0}" == "1" && "$*" == *"ray start"* ]]; then
  exit 1
fi
exit 0
""",
        )
        self.make_fake_command(
            "sleep",
            """#!/bin/bash
set -euo pipefail
exit 0
""",
        )
        self.make_fake_command(
            "squeue",
            """#!/bin/bash
set -euo pipefail
job_id=""
field=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -j) job_id="$2"; shift 2 ;;
    -O) field="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [[ "${SQUEUE_FAIL_FIRST_STATE:-0}" == "1" && "$field" == "State" && "$job_id" == "${SQUEUE_FAIL_JOB_ID:-}" ]]; then
  marker="${TEST_ROOT}/squeue-failed-once"
  if [[ ! -f "$marker" ]]; then
    echo "yes" >"$marker"
    echo "slurm_load_jobs error: Socket timed out on send/recv operation" >&2
    exit 1
  fi
fi
case "${job_id}:${field}" in
  9999:State) echo "COMPLETED" ;;
  1234:State) echo "RUNNING" ;;
  5678:State) echo "RUNNING" ;;
  1234:TimeLeft) echo "02:00:00" ;;
  5678:TimeLeft) echo "01:00:00" ;;
esac
""",
        )

        helper = self.repo_dir / "scripts"
        helper.mkdir()
        cluster_dir = self.state_root / "demo"
        cluster_dir.mkdir()
        (cluster_dir / "jobs.txt").write_text("1234\n5678\n", encoding="utf-8")
        (helper / "cluster_state.py").write_text(
            textwrap.dedent(
                """\
                #!/usr/bin/env python3
                import sys
                from pathlib import Path

                log = Path(__import__("os").environ["TEST_ROOT"]) / "helper.log"
                log.write_text(log.read_text() + " ".join(sys.argv[1:]) + "\\n" if log.exists() else " ".join(sys.argv[1:]) + "\\n")
                cmd = sys.argv[1]
                if cmd == "try-become-head":
                    sys.exit(1)
                if cmd == "notify-node-registered":
                    print("mail failed", file=sys.stderr)
                    sys.exit(1)
                if cmd == "notify-node-failed":
                    sys.exit(0)
                if cmd == "wait-head-update":
                    min_epoch = sys.argv[sys.argv.index("--min-epoch") + 1]
                    print("node-head-b" if min_epoch == "0" else "node-head-a")
                    sys.exit(0)
                if cmd == "get-head-job-id":
                    print(__import__("os").environ.get("TEST_HEAD_JOB_ID", "9999"))
                    sys.exit(0)
                if cmd == "get-epoch":
                    if (Path(__import__("os").environ["TEST_ROOT"]) / "set-head.done").exists():
                        print("2")
                    else:
                        print("1")
                    sys.exit(0)
                if cmd == "try-acquire-failover-lock":
                    sys.exit(0)
                if cmd == "set-head":
                    (Path(__import__("os").environ["TEST_ROOT"]) / "set-head.done").write_text("yes\\n")
                    sys.exit(0)
                sys.exit(0)
                """
            ),
            encoding="utf-8",
        )
        os.chmod(helper / "cluster_state.py", 0o755)

    def tearDown(self):
        self.tmpdir.cleanup()

    def make_fake_command(self, name: str, content: str):
        path = self.bin_dir / name
        path.write_text(textwrap.dedent(content), encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)

    def test_notification_failure_does_not_stop_ray_bootstrap(self):
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.bin_dir}:{env['PATH']}",
                "TEST_ROOT": str(self.root),
                "REPO_ROOT": str(self.repo_dir),
                "CLUSTER_ID": "demo",
                "STATE_ROOT": str(self.state_root),
                "SIF_PATH": str(self.root / "image.sif"),
                "DATA_STORAGE_PATH": str(self.data_dir),
                "RAY_PORT": "6379",
                "PARTITION_NAME": "a100q",
                "NOTIFY_EMAIL": "user@example.com",
                "SLURM_JOB_ID": "1234",
                "APPTAINER_BIN": str(self.bin_dir / "apptainer"),
                "SQUEUE_BIN": str(self.bin_dir / "squeue"),
                "HEAD_CHECK_INTERVAL_SECONDS": "0",
                "MAX_MONITOR_ITERATIONS": "1",
            }
        )

        proc = subprocess.run(
            [str(JOB_SCRIPT)],
            text=True,
            capture_output=True,
            env=env,
        )

        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertIn("warning: node registration email failed", proc.stderr)
        apptainer_log = (self.root / "apptainer.log").read_text(encoding="utf-8")
        self.assertIn(
            "exec --nv instance://run ray start --address=node-head-b:6379",
            apptainer_log,
        )
        self.assertNotIn("--address=node-head-b:6379 --temp-dir=", apptainer_log)
        self.assertIn(
            "exec --nv instance://run ray start --head --port=6379 --temp-dir=/tmp",
            apptainer_log,
        )
        self.assertIn(
            f"instance start --nv --bind {DEFAULT_TMP_PREFIX}/tmp-node-a:/tmp",
            apptainer_log,
        )
        helper_log = (self.root / "helper.log").read_text(encoding="utf-8")
        self.assertIn("set-head --state-root", helper_log)
        self.assertIn("--hostname node-a", helper_log)

    def test_ray_tmp_dir_prefix_override_is_applied_to_head_and_worker(self):
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.bin_dir}:{env['PATH']}",
                "TEST_ROOT": str(self.root),
                "REPO_ROOT": str(self.repo_dir),
                "CLUSTER_ID": "demo",
                "STATE_ROOT": str(self.state_root),
                "SIF_PATH": str(self.root / "image.sif"),
                "DATA_STORAGE_PATH": str(self.data_dir),
                "RAY_PORT": "6379",
                "PARTITION_NAME": "a100q",
                "SLURM_JOB_ID": "1234",
                "APPTAINER_BIN": str(self.bin_dir / "apptainer"),
                "SQUEUE_BIN": str(self.bin_dir / "squeue"),
                "HEAD_CHECK_INTERVAL_SECONDS": "0",
                "MAX_MONITOR_ITERATIONS": "1",
                "TMP_PREFIX": "/tmp/ray-custom-prefix",
            }
        )

        proc = subprocess.run(
            [str(JOB_SCRIPT)],
            text=True,
            capture_output=True,
            env=env,
        )

        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        apptainer_log = (self.root / "apptainer.log").read_text(encoding="utf-8")
        self.assertIn(
            "exec --nv instance://run ray start --address=node-head-b:6379",
            apptainer_log,
        )
        self.assertNotIn("--address=node-head-b:6379 --temp-dir=", apptainer_log)
        self.assertIn(
            "exec --nv instance://run ray start --head --port=6379 --temp-dir=/tmp",
            apptainer_log,
        )
        self.assertIn(
            "instance start --nv --bind /tmp/ray-custom-prefix/tmp-node-a:/tmp",
            apptainer_log,
        )

    def test_failure_exit_sends_failure_notification_with_log_excerpt(self):
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.bin_dir}:{env['PATH']}",
                "TEST_ROOT": str(self.root),
                "REPO_ROOT": str(self.repo_dir),
                "CLUSTER_ID": "demo",
                "STATE_ROOT": str(self.state_root),
                "SIF_PATH": str(self.root / "image.sif"),
                "DATA_STORAGE_PATH": str(self.data_dir),
                "RAY_PORT": "6379",
                "PARTITION_NAME": "a100q",
                "NOTIFY_EMAIL": "user@example.com",
                "SLURM_JOB_ID": "1234",
                "APPTAINER_BIN": str(self.bin_dir / "apptainer"),
                "APPTAINER_FAIL": "1",
                "SQUEUE_BIN": str(self.bin_dir / "squeue"),
                "JOB_LOG_PATH_TEMPLATE": str(self.root / "job_%j.out"),
                "MAX_MONITOR_ITERATIONS": "1",
            }
        )

        proc = subprocess.run(
            [str(JOB_SCRIPT)],
            text=True,
            capture_output=True,
            env=env,
        )

        self.assertNotEqual(proc.returncode, 0)
        helper_log = (self.root / "helper.log").read_text(encoding="utf-8")
        self.assertIn("notify-node-failed --email user@example.com", helper_log)
        self.assertIn("--exit-code 1", helper_log)
        self.assertIn(f"--log-path {self.root / 'job_1234.out'}", helper_log)
        self.assertIn("--log-tail line-1\nline-2\nline-3", helper_log)

    def test_bootstrap_logs_runtime_diagnostics_before_apptainer_commands(self):
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.bin_dir}:{env['PATH']}",
                "TEST_ROOT": str(self.root),
                "REPO_ROOT": str(self.repo_dir),
                "CLUSTER_ID": "demo",
                "STATE_ROOT": str(self.state_root),
                "SIF_PATH": str(self.root / "image.sif"),
                "DATA_STORAGE_PATH": str(self.data_dir),
                "RAY_PORT": "6379",
                "PARTITION_NAME": "a100q",
                "SLURM_JOB_ID": "5678",
                "APPTAINER_BIN": str(self.bin_dir / "apptainer"),
                "SQUEUE_BIN": str(self.bin_dir / "squeue"),
                "HEAD_CHECK_INTERVAL_SECONDS": "0",
                "MAX_MONITOR_ITERATIONS": "1",
            }
        )

        proc = subprocess.run(
            [str(JOB_SCRIPT)],
            text=True,
            capture_output=True,
            env=env,
        )

        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertIn("bootstrap diagnostics: hostname=node-a", proc.stderr)
        self.assertIn("bootstrap diagnostics: lscpu summary begin", proc.stderr)
        self.assertIn("Model name:                           AMD EPYC 7A53 64-Core Processor", proc.stderr)
        self.assertIn("bootstrap diagnostics: apptainer which=", proc.stderr)
        self.assertIn("bootstrap diagnostics: apptainer version begin", proc.stderr)
        self.assertIn("apptainer version 1.3.4", proc.stderr)
        self.assertIn(
            f"instance start --nv --bind {DEFAULT_TMP_PREFIX}/tmp-node-a:/tmp",
            proc.stderr,
        )
        self.assertIn("exec --nv instance://run ray start --address=node-head-b:6379", proc.stderr)

    def test_monitor_retries_transient_squeue_state_failures(self):
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.bin_dir}:{env['PATH']}",
                "TEST_ROOT": str(self.root),
                "REPO_ROOT": str(self.repo_dir),
                "CLUSTER_ID": "demo",
                "STATE_ROOT": str(self.state_root),
                "SIF_PATH": str(self.root / "image.sif"),
                "DATA_STORAGE_PATH": str(self.data_dir),
                "RAY_PORT": "6379",
                "PARTITION_NAME": "a100q",
                "SLURM_JOB_ID": "5678",
                "APPTAINER_BIN": str(self.bin_dir / "apptainer"),
                "SQUEUE_BIN": str(self.bin_dir / "squeue"),
                "HEAD_CHECK_INTERVAL_SECONDS": "0",
                "MAX_MONITOR_ITERATIONS": "1",
                "TEST_HEAD_JOB_ID": "1234",
                "SQUEUE_FAIL_FIRST_STATE": "1",
                "SQUEUE_FAIL_JOB_ID": "1234",
                "SQUEUE_QUERY_MAX_ATTEMPTS": "2",
                "SQUEUE_RETRY_DELAY_SECONDS": "0",
            }
        )

        proc = subprocess.run(
            [str(JOB_SCRIPT)],
            text=True,
            capture_output=True,
            env=env,
        )

        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertIn("squeue query failed for job=1234 field=State", proc.stderr)
        self.assertIn("iteration=1 epoch=1 head_job_id=1234 head_state=RUNNING", proc.stderr)

    def test_worker_bootstrap_self_promotes_when_recorded_head_is_stale(self):
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.bin_dir}:{env['PATH']}",
                "TEST_ROOT": str(self.root),
                "REPO_ROOT": str(self.repo_dir),
                "CLUSTER_ID": "demo",
                "STATE_ROOT": str(self.state_root),
                "SIF_PATH": str(self.root / "image.sif"),
                "DATA_STORAGE_PATH": str(self.data_dir),
                "RAY_PORT": "6379",
                "PARTITION_NAME": "a100q",
                "SLURM_JOB_ID": "1234",
                "APPTAINER_BIN": str(self.bin_dir / "apptainer"),
                "APPTAINER_FAIL_PATTERN": "--address=node-head-b:6379",
                "SQUEUE_BIN": str(self.bin_dir / "squeue"),
                "HEAD_CHECK_INTERVAL_SECONDS": "0",
                "MAX_MONITOR_ITERATIONS": "1",
                "TEST_HEAD_JOB_ID": "9999",
            }
        )

        proc = subprocess.run(
            [str(JOB_SCRIPT)],
            text=True,
            capture_output=True,
            env=env,
        )

        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertIn(
            "exec --nv instance://run ray start --head --port=6379 --temp-dir=/tmp",
            proc.stderr,
        )
        helper_log = (self.root / "helper.log").read_text(encoding="utf-8")
        self.assertIn("set-head --state-root", helper_log)

if __name__ == "__main__":
    unittest.main()
