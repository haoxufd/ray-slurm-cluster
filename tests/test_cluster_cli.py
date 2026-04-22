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
printf '%s\n' "$*" >> "${FAKE_SLURM_ROOT}/sbatch.log"
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
        self.make_fake_command(
            "squeue",
            """#!/bin/bash
set -euo pipefail
if [[ "${TEST_SQUEUE_FAIL:-0}" == "1" ]]; then
  echo "slurm_load_jobs error: controller unavailable" >&2
  exit 1
fi
job_ids=""
field=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -j) job_ids="$2"; shift 2 ;;
    -O) field="$2"; shift 2 ;;
    -h) shift ;;
    *) shift ;;
  esac
done
if [[ "$field" != "JobID,State" ]]; then
  exit 0
fi
mappings="${TEST_SQUEUE_STATES:-}"
IFS=',' read -r -a mapping_arr <<< "$mappings"
IFS=',' read -r -a job_arr <<< "$job_ids"
for job_id in "${job_arr[@]}"; do
  state=""
  for mapping in "${mapping_arr[@]}"; do
    key="${mapping%%=*}"
    value="${mapping#*=}"
    if [[ "$key" == "$job_id" ]]; then
      state="$value"
      break
    fi
  done
  if [[ -n "$state" ]]; then
    echo "$job_id $state"
  fi
done
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
                TMP_PREFIX=/mnt/vast-nhr/projects/nii00233/xuhao/ray-tmp
                EXCLUDE_NODES=
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

    def run_cmd(self, *args, check=True, extra_env=None):
        env = self.env()
        if extra_env:
            env.update(extra_env)
        proc = subprocess.run(
            [str(CLUSTER_BIN), *args],
            text=True,
            capture_output=True,
            env=env,
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
        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8")
        self.assertIn("JOB_LOG_PATH_TEMPLATE=", sbatch_log)
        self.assertIn(f"JOB_LOG_PATH_TEMPLATE={self.logs_dir}/job_%j.out", sbatch_log)
        self.assertNotIn("--export=ALL,", sbatch_log)
        self.assertRegex(sbatch_log, r"--job-name=[a-z0-9]{4}\s")

    def test_status_and_down_use_saved_cluster_state(self):
        self.write_config()
        up = self.run_cmd("up", "2", "a100q")
        cluster_id = up.stdout.strip().split("cluster_id=")[1].splitlines()[0]
        (self.logs_dir / "job_1000.out").write_text("job1000\n", encoding="utf-8")
        (self.logs_dir / "job_1001.out").write_text("job1001\n", encoding="utf-8")
        (self.logs_dir / "job_9999.out").write_text("keep\n", encoding="utf-8")

        status = self.run_cmd("status", cluster_id, extra_env={"TEST_SQUEUE_FAIL": "1"})
        self.assertIn(f"cluster_id={cluster_id}", status.stdout)
        self.assertIn("active_nodes=2", status.stdout)
        self.assertIn("cluster_removed=0", status.stdout)

        self.run_cmd("down", cluster_id)
        self.assertFalse((self.state_root / cluster_id).exists())
        self.assertFalse((self.logs_dir / "job_1000.out").exists())
        self.assertFalse((self.logs_dir / "job_1001.out").exists())
        self.assertTrue((self.logs_dir / "job_9999.out").exists())
        scancel_log = (self.root / "scancel.log").read_text(encoding="utf-8").splitlines()
        self.assertEqual(scancel_log, ["1000", "1001"])

    def test_list_prints_existing_cluster_ids(self):
        self.write_config()
        first = self.run_cmd("up", "1", "a100q").stdout.strip().split("cluster_id=")[1].splitlines()[0]
        second = self.run_cmd("up", "1", "a100q").stdout.strip().split("cluster_id=")[1].splitlines()[0]

        listed = self.run_cmd("list", extra_env={"TEST_SQUEUE_STATES": "1000=RUNNING,1001=RUNNING"})
        listed_ids = listed.stdout.splitlines()
        self.assertIn(first, listed_ids)
        self.assertIn(second, listed_ids)

    def test_status_auto_cleans_up_stale_cluster(self):
        self.write_config()
        up = self.run_cmd("up", "1", "a100q")
        cluster_id = up.stdout.strip().split("cluster_id=")[1].splitlines()[0]

        status = self.run_cmd("status", cluster_id, extra_env={"TEST_SQUEUE_STATES": "1000=FAILED"})
        self.assertIn(f"cluster_id={cluster_id}", status.stdout)
        self.assertIn("cluster_removed=1", status.stdout)
        self.assertIn("reason=no_active_jobs", status.stdout)
        self.assertFalse((self.state_root / cluster_id).exists())

    def test_list_prunes_stale_clusters_before_printing(self):
        self.write_config()
        stale = self.run_cmd("up", "1", "a100q").stdout.strip().split("cluster_id=")[1].splitlines()[0]
        active = self.run_cmd("up", "1", "a100q").stdout.strip().split("cluster_id=")[1].splitlines()[0]

        listed = self.run_cmd("list", extra_env={"TEST_SQUEUE_STATES": f"1001=RUNNING"})
        listed_ids = listed.stdout.splitlines()
        self.assertIn(active, listed_ids)
        self.assertNotIn(stale, listed_ids)
        self.assertFalse((self.state_root / stale).exists())
        self.assertTrue((self.state_root / active).exists())

    def test_status_without_cluster_id_uses_only_cluster(self):
        self.write_config()
        cluster_id = self.run_cmd("up", "1", "a100q").stdout.strip().split("cluster_id=")[1].splitlines()[0]

        status = self.run_cmd("status")
        self.assertIn(f"cluster_id={cluster_id}", status.stdout)

    def test_status_without_cluster_id_fails_when_multiple_clusters(self):
        self.write_config()
        self.run_cmd("up", "1", "a100q")
        self.run_cmd("up", "1", "a100q")

        status = self.run_cmd("status", check=False)
        self.assertNotEqual(status.returncode, 0)
        self.assertIn("multiple clusters found, please specify cluster_id", status.stderr)

    def test_scale_without_cluster_id_uses_only_cluster(self):
        self.write_config()
        cluster_id = self.run_cmd("up", "1", "a100q").stdout.strip().split("cluster_id=")[1].splitlines()[0]

        result = self.run_cmd("scale", "2")
        self.assertIn(f"cluster_id={cluster_id}", result.stdout)
        jobs = (self.state_root / cluster_id / "jobs.txt").read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(jobs), 3)

    def test_up_allows_email_override(self):
        self.write_config()

        result = self.run_cmd("up", "1", "a100q", "--email", "user@example.com")

        cluster_id = result.stdout.strip().split("cluster_id=")[1].splitlines()[0]
        cluster_env = (self.state_root / cluster_id / "cluster.env").read_text(encoding="utf-8")
        self.assertIn("NOTIFY_EMAIL=user@example.com", cluster_env)

    def test_up_allows_gpu_spec_override(self):
        self.write_config()

        result = self.run_cmd("up", "1", "a100q", "--gpus-per-node", "A100:2")

        cluster_id = result.stdout.strip().split("cluster_id=")[1].splitlines()[0]
        cluster_env = (self.state_root / cluster_id / "cluster.env").read_text(encoding="utf-8")
        self.assertIn("GPUS_PER_NODE=A100:2", cluster_env)
        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8")
        self.assertIn("--gpus-per-node=A100:2", sbatch_log)

    def test_up_uses_default_constraint_without_override(self):
        self.write_config()

        self.run_cmd("up", "1", "a100q")

        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8")
        self.assertIn("--constraint=80gb_vram", sbatch_log)

    def test_up_allows_disabling_constraint(self):
        self.write_config()

        result = self.run_cmd("up", "1", "a100q", "--no-constraint")

        cluster_id = result.stdout.strip().split("cluster_id=")[1].splitlines()[0]
        cluster_env = (self.state_root / cluster_id / "cluster.env").read_text(encoding="utf-8")
        self.assertIn("USE_CONSTRAINT=0", cluster_env)
        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8")
        self.assertNotIn("--constraint=80gb_vram", sbatch_log)

    def test_up_exports_default_ray_tmp_prefix_from_config(self):
        self.write_config()

        self.run_cmd("up", "1", "a100q")

        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8")
        self.assertIn("TMP_PREFIX=/mnt/vast-nhr/projects/nii00233/xuhao/ray-tmp", sbatch_log)

    def test_up_allows_ray_tmp_prefix_override(self):
        self.write_config()

        result = self.run_cmd("up", "1", "a100q", "--ray-tmp-prefix", "/tmp/ray-prefix")

        cluster_id = result.stdout.strip().split("cluster_id=")[1].splitlines()[0]
        cluster_env = (self.state_root / cluster_id / "cluster.env").read_text(encoding="utf-8")
        self.assertIn("TMP_PREFIX=/tmp/ray-prefix", cluster_env)
        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8")
        self.assertIn("TMP_PREFIX=/tmp/ray-prefix", sbatch_log)

    def test_up_allows_exclude_nodes_override(self):
        self.write_config()

        result = self.run_cmd("up", "1", "a100q", "--exclude-nodes", "ggpu101,ggpu102")

        cluster_id = result.stdout.strip().split("cluster_id=")[1].splitlines()[0]
        cluster_env = (self.state_root / cluster_id / "cluster.env").read_text(encoding="utf-8")
        self.assertIn("EXCLUDE_NODES=ggpu101,ggpu102", cluster_env)
        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8")
        self.assertIn("--exclude=ggpu101,ggpu102", sbatch_log)

    def test_up_uses_default_exclude_nodes_from_config(self):
        self.write_config()
        cfg = self.root / "cluster.env"
        cfg.write_text(
            cfg.read_text(encoding="utf-8").replace(
                "EXCLUDE_NODES=\n",
                "EXCLUDE_NODES=ggpu201,ggpu202\n",
            ),
            encoding="utf-8",
        )

        result = self.run_cmd("up", "1", "a100q")

        cluster_id = result.stdout.strip().split("cluster_id=")[1].splitlines()[0]
        cluster_env = (self.state_root / cluster_id / "cluster.env").read_text(encoding="utf-8")
        self.assertIn("EXCLUDE_NODES=ggpu201,ggpu202", cluster_env)
        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8")
        self.assertIn("--exclude=ggpu201,ggpu202", sbatch_log)

    def test_scale_updates_desired_nodes_and_submits_more_jobs(self):
        self.write_config()

        up = self.run_cmd("up", "2", "a100q", "--email", "user@example.com")
        cluster_id = up.stdout.strip().split("cluster_id=")[1].splitlines()[0]

        result = self.run_cmd("scale", cluster_id, "3")

        self.assertIn(f"cluster_id={cluster_id}", result.stdout)
        self.assertIn("added_nodes=3", result.stdout)
        cluster_dir = self.state_root / cluster_id
        jobs = (cluster_dir / "jobs.txt").read_text(encoding="utf-8").splitlines()
        self.assertEqual(jobs, ["1000", "1001", "1002", "1003", "1004"])
        self.assertEqual((cluster_dir / "desired_nodes").read_text(encoding="utf-8").strip(), "5")
        cluster_env = (cluster_dir / "cluster.env").read_text(encoding="utf-8")
        self.assertIn("PARTITION=a100q", cluster_env)
        self.assertIn("NOTIFY_EMAIL=user@example.com", cluster_env)

    def test_scale_reuses_saved_gpu_spec_from_cluster_state(self):
        self.write_config()

        up = self.run_cmd("up", "1", "a100q", "--gpus-per-node", "A100:2")
        cluster_id = up.stdout.strip().split("cluster_id=")[1].splitlines()[0]

        self.run_cmd("scale", cluster_id, "1")

        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8").splitlines()
        self.assertIn("--gpus-per-node=A100:2", sbatch_log[-1])

    def test_scale_allows_gpu_spec_override(self):
        self.write_config()

        up = self.run_cmd("up", "1", "a100q")
        cluster_id = up.stdout.strip().split("cluster_id=")[1].splitlines()[0]

        self.run_cmd("scale", cluster_id, "1", "--gpus-per-node", "A100:2")

        cluster_env = (self.state_root / cluster_id / "cluster.env").read_text(encoding="utf-8")
        self.assertIn("GPUS_PER_NODE=A100:2", cluster_env)
        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8").splitlines()
        self.assertIn("--gpus-per-node=A100:2", sbatch_log[-1])

    def test_scale_reuses_saved_no_constraint_setting(self):
        self.write_config()

        up = self.run_cmd("up", "1", "a100q", "--no-constraint")
        cluster_id = up.stdout.strip().split("cluster_id=")[1].splitlines()[0]

        self.run_cmd("scale", cluster_id, "1")

        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8").splitlines()
        self.assertNotIn("--constraint=80gb_vram", sbatch_log[-1])

    def test_scale_allows_disabling_constraint(self):
        self.write_config()

        up = self.run_cmd("up", "1", "a100q")
        cluster_id = up.stdout.strip().split("cluster_id=")[1].splitlines()[0]

        self.run_cmd("scale", cluster_id, "1", "--no-constraint")

        cluster_env = (self.state_root / cluster_id / "cluster.env").read_text(encoding="utf-8")
        self.assertIn("USE_CONSTRAINT=0", cluster_env)
        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8").splitlines()
        self.assertNotIn("--constraint=80gb_vram", sbatch_log[-1])

    def test_scale_reuses_saved_exclude_nodes_setting(self):
        self.write_config()

        up = self.run_cmd("up", "1", "a100q", "--exclude-nodes", "ggpu101,ggpu102")
        cluster_id = up.stdout.strip().split("cluster_id=")[1].splitlines()[0]

        self.run_cmd("scale", cluster_id, "1")

        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8").splitlines()
        self.assertIn("--exclude=ggpu101,ggpu102", sbatch_log[-1])

    def test_scale_allows_exclude_nodes_override(self):
        self.write_config()

        up = self.run_cmd("up", "1", "a100q")
        cluster_id = up.stdout.strip().split("cluster_id=")[1].splitlines()[0]

        self.run_cmd("scale", cluster_id, "1", "--exclude-nodes", "ggpu109")

        cluster_env = (self.state_root / cluster_id / "cluster.env").read_text(encoding="utf-8")
        self.assertIn("EXCLUDE_NODES=ggpu109", cluster_env)
        sbatch_log = (self.root / "sbatch.log").read_text(encoding="utf-8").splitlines()
        self.assertIn("--exclude=ggpu109", sbatch_log[-1])


if __name__ == "__main__":
    unittest.main()
