import os
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CLUSTER_BIN = REPO_ROOT / "bin" / "cluster"


class ClusterCliTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.state_root = self.root / "state"
        self.logs_dir = self.root / "logs"
        self.bin_dir = self.root / "fake-bin"
        self.state_root.mkdir()
        self.logs_dir.mkdir()
        self.bin_dir.mkdir()
        self.make_fake_command(
            "sbatch",
            """#!/bin/bash
set -euo pipefail
counter_file="${FAKE_SLURM_ROOT}/sbatch-counter"
if [[ ! -f "$counter_file" ]]; then
  echo 1000 >"$counter_file"
fi
job_id="$(cat "$counter_file")"
echo $((job_id + 1)) >"$counter_file"
printf '%s\n' "$job_id"
""",
        )
        self.make_fake_command(
            "scancel",
            """#!/bin/bash
set -euo pipefail
printf '%s\n' "$1" >> "${FAKE_SLURM_ROOT}/scancel.log"
""",
        )

    def tearDown(self):
        self.tmpdir.cleanup()

    def make_fake_command(self, name: str, content: str):
        path = self.bin_dir / name
        path.write_text(textwrap.dedent(content), encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)

    def env(self):
        env = os.environ.copy()
        env["PATH"] = f"{self.bin_dir}:{env['PATH']}"
        env["FAKE_SLURM_ROOT"] = str(self.root)
        env["CLUSTER_CONFIG"] = str(self.root / "cluster.env")
        return env

    def write_config(self):
        (self.root / "cluster.env").write_text(
            textwrap.dedent(
                f"""\
                SIF_PATH={self.root}/image.sif
                STATE_ROOT={self.state_root}
                DATA_STORAGE_PATH={self.root}/data
                LOG_ROOT={self.logs_dir}
                NOTIFY_EMAIL=
                RAY_PORT=6379
                SLURM_TIME_LIMIT=2-00:00:00
                A100_GPUS_PER_NODE=A100:4
                A100_CONSTRAINT=80gb_vram
                H100_GPUS_PER_NODE=h100:4
                H100_CONSTRAINT=
                """
            ),
            encoding="utf-8",
        )
        (self.root / "image.sif").write_text("", encoding="utf-8")
        (self.root / "data").mkdir()

    def run_cmd(self, *args, check=True):
        proc = subprocess.run(
            [str(CLUSTER_BIN), *args],
            text=True,
            capture_output=True,
            env=self.env(),
        )
        if check and proc.returncode != 0:
            self.fail(
                f"command failed: {' '.join(args)}\nstdout={proc.stdout}\nstderr={proc.stderr}"
            )
        return proc

    def test_up_creates_cluster_state_and_records_jobs(self):
        self.write_config()

        result = self.run_cmd("up", "2", "a100q")

        self.assertIn("cluster_id=", result.stdout)
        cluster_id = result.stdout.strip().split("cluster_id=")[1].splitlines()[0]
        cluster_dir = self.state_root / cluster_id
        jobs = (cluster_dir / "jobs.txt").read_text(encoding="utf-8").splitlines()
        self.assertEqual(jobs, ["1000", "1001"])
        self.assertEqual((cluster_dir / "desired_nodes").read_text().strip(), "2")
        cluster_env = (cluster_dir / "cluster.env").read_text(encoding="utf-8")
        self.assertIn("NOTIFY_EMAIL=", cluster_env)

    def test_status_and_down_use_saved_cluster_state(self):
        self.write_config()
        up = self.run_cmd("up", "2", "a100q")
        cluster_id = up.stdout.strip().split("cluster_id=")[1].splitlines()[0]

        status = self.run_cmd("status", cluster_id)
        self.assertIn(f"cluster_id={cluster_id}", status.stdout)
        self.assertIn("submitted_jobs=2", status.stdout)

        self.run_cmd("down", cluster_id)
        self.assertFalse((self.state_root / cluster_id).exists())
        scancel_log = (self.root / "scancel.log").read_text(encoding="utf-8").splitlines()
        self.assertEqual(scancel_log, ["1000", "1001"])

    def test_up_allows_email_override(self):
        self.write_config()

        result = self.run_cmd("up", "1", "a100q", "--email", "user@example.com")

        cluster_id = result.stdout.strip().split("cluster_id=")[1].splitlines()[0]
        cluster_env = (self.state_root / cluster_id / "cluster.env").read_text(encoding="utf-8")
        self.assertIn("NOTIFY_EMAIL=user@example.com", cluster_env)


if __name__ == "__main__":
    unittest.main()
