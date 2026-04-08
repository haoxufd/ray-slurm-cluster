import os
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
JOB_SCRIPT = REPO_ROOT / "slurm" / "ray_node_job.sh"


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

        self.make_fake_command(
            "hostname",
            """#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "-I" ]]; then
  echo "10.0.0.1"
else
  echo "node-a"
fi
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
            "apptainer",
            """#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> "${TEST_ROOT}/apptainer.log"
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
                if cmd == "wait-head-update":
                    min_epoch = sys.argv[sys.argv.index("--min-epoch") + 1]
                    print("10.0.0.9" if min_epoch == "0" else "10.0.0.1")
                    sys.exit(0)
                if cmd == "get-head-job-id":
                    print("9999")
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
        self.assertIn("exec --nv instance://run ray start --address=10.0.0.9:6379", apptainer_log)
        self.assertIn("exec --nv instance://run ray start --head --node-ip-address=10.0.0.1 --port=6379 --temp-dir=/tmp", apptainer_log)
        helper_log = (self.root / "helper.log").read_text(encoding="utf-8")
        self.assertIn("set-head --state-root", helper_log)


if __name__ == "__main__":
    unittest.main()
