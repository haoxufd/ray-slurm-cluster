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

        helper = self.repo_dir / "scripts"
        helper.mkdir()
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
                    sys.exit(0)
                if cmd == "notify-node-registered":
                    print("mail failed", file=sys.stderr)
                    sys.exit(1)
                if cmd == "wait-head-ip":
                    print("10.0.0.1")
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
        self.assertIn("exec --nv instance://run ray start --head", (self.root / "apptainer.log").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
