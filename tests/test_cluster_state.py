import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "cluster_state.py"


class ClusterStateCliTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.state_root = Path(self.tmpdir.name)
        self.cluster_id = "demo"
        self.bin_dir = self.state_root / "bin"
        self.bin_dir.mkdir()
        squeue_script = self.bin_dir / "squeue"
        squeue_script.write_text(
            "#!/bin/bash\n"
            "set -euo pipefail\n"
            "if [[ \"${TEST_SQUEUE_FAIL:-0}\" == \"1\" ]]; then\n"
            "  echo \"slurm_load_jobs error: controller unavailable\" >&2\n"
            "  exit 1\n"
            "fi\n"
            "job_ids=\"\"\n"
            "field=\"\"\n"
            "while [[ $# -gt 0 ]]; do\n"
            "  case \"$1\" in\n"
            "    -j) job_ids=\"$2\"; shift 2 ;;\n"
            "    -O) field=\"$2\"; shift 2 ;;\n"
            "    -h) shift ;;\n"
            "    *) shift ;;\n"
            "  esac\n"
            "done\n"
            "if [[ \"$field\" != \"JobID,State\" ]]; then\n"
            "  exit 0\n"
            "fi\n"
            "mappings=\"${TEST_SQUEUE_STATES:-101=RUNNING,102=RUNNING}\"\n"
            "IFS=',' read -r -a mapping_arr <<< \"$mappings\"\n"
            "IFS=',' read -r -a job_arr <<< \"$job_ids\"\n"
            "for job_id in \"${job_arr[@]}\"; do\n"
            "  state=\"\"\n"
            "  for mapping in \"${mapping_arr[@]}\"; do\n"
            "    key=\"${mapping%%=*}\"\n"
            "    value=\"${mapping#*=}\"\n"
            "    if [[ \"$key\" == \"$job_id\" ]]; then\n"
            "      state=\"$value\"\n"
            "      break\n"
            "    fi\n"
            "  done\n"
            "  if [[ -n \"$state\" ]]; then\n"
            "    echo \"$job_id $state\"\n"
            "  fi\n"
            "done\n",
            encoding="utf-8",
        )
        squeue_script.chmod(0o755)

    def tearDown(self):
        self.tmpdir.cleanup()

    def run_cmd(self, *args, check=True, extra_env=None):
        env = os.environ.copy()
        env["PATH"] = f"{self.bin_dir}:{env['PATH']}"
        if extra_env:
            env.update(extra_env)
        proc = subprocess.run(
            ["python3", str(SCRIPT), *args],
            text=True,
            capture_output=True,
            env=env,
        )
        if check and proc.returncode != 0:
            self.fail(
                f"command failed: {' '.join(args)}\nstdout={proc.stdout}\nstderr={proc.stderr}"
            )
        return proc

    def test_init_creates_expected_state_layout(self):
        self.run_cmd(
            "init",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--num-nodes",
            "2",
            "--partition",
            "a100q",
        )

        cluster_dir = self.state_root / self.cluster_id
        self.assertTrue((cluster_dir / "nodes").is_dir())
        self.assertTrue((cluster_dir / "workers").is_dir())
        self.assertEqual((cluster_dir / "desired_nodes").read_text().strip(), "2")
        self.assertEqual((cluster_dir / "jobs.txt").read_text(), "")
        self.assertEqual((cluster_dir / "epoch").read_text().strip(), "0")

    def test_add_job_and_register_node_persist_metadata(self):
        self.run_cmd(
            "init",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--num-nodes",
            "2",
            "--partition",
            "a100q",
        )
        self.run_cmd(
            "add-job",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
        )
        self.run_cmd(
            "register-node",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
            "--hostname",
            "n1",
            "--ip",
            "10.0.0.1",
        )

        cluster_dir = self.state_root / self.cluster_id
        self.assertEqual((cluster_dir / "jobs.txt").read_text().splitlines(), ["101"])
        node_record = json.loads((cluster_dir / "nodes" / "101.json").read_text())
        self.assertEqual(node_record["hostname"], "n1")
        self.assertEqual(node_record["ip"], "10.0.0.1")

    def test_try_become_head_is_atomic_and_publish_head_ip(self):
        self.run_cmd(
            "init",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--num-nodes",
            "2",
            "--partition",
            "a100q",
        )

        first = self.run_cmd(
            "try-become-head",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
        )
        second = self.run_cmd(
            "try-become-head",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "102",
            check=False,
        )
        self.run_cmd(
            "set-head",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
            "--ip",
            "10.0.0.1",
        )
        waited = self.run_cmd(
            "wait-head-ip",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--timeout-seconds",
            "0.1",
        )

        self.assertEqual(first.returncode, 0)
        self.assertNotEqual(second.returncode, 0)
        self.assertEqual(waited.stdout.strip(), "10.0.0.1")

    def test_set_head_tracks_head_job_and_epoch(self):
        self.run_cmd(
            "init",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--num-nodes",
            "2",
            "--partition",
            "a100q",
        )
        self.run_cmd(
            "set-head",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
            "--ip",
            "10.0.0.1",
        )
        head_job = self.run_cmd(
            "get-head-job-id",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
        )
        epoch = self.run_cmd(
            "get-epoch",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
        )

        self.assertEqual(head_job.stdout.strip(), "101")
        self.assertEqual(epoch.stdout.strip(), "1")

    def test_failover_lock_and_wait_head_update(self):
        self.run_cmd(
            "init",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--num-nodes",
            "2",
            "--partition",
            "a100q",
        )
        self.run_cmd(
            "set-head",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
            "--ip",
            "10.0.0.1",
        )
        first_lock = self.run_cmd(
            "try-acquire-failover-lock",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "102",
        )
        second_lock = self.run_cmd(
            "try-acquire-failover-lock",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "103",
            check=False,
        )
        self.assertEqual(first_lock.returncode, 0)
        self.assertNotEqual(second_lock.returncode, 0)
        self.run_cmd(
            "release-failover-lock",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
        )
        self.run_cmd(
            "set-head",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "102",
            "--ip",
            "10.0.0.2",
        )
        waited = self.run_cmd(
            "wait-head-update",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--min-epoch",
            "1",
            "--timeout-seconds",
            "0.1",
        )
        self.assertEqual(waited.stdout.strip(), "10.0.0.2")

    def test_status_and_cleanup_reflect_cluster_progress(self):
        self.run_cmd(
            "init",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--num-nodes",
            "2",
            "--partition",
            "a100q",
        )
        self.run_cmd(
            "add-job",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
        )
        self.run_cmd(
            "add-job",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "102",
        )
        self.run_cmd(
            "register-node",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
            "--hostname",
            "n1",
            "--ip",
            "10.0.0.1",
        )
        self.run_cmd(
            "try-become-head",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
        )
        self.run_cmd(
            "set-head",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
            "--ip",
            "10.0.0.1",
        )
        self.run_cmd(
            "register-node",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "102",
            "--hostname",
            "n2",
            "--ip",
            "10.0.0.2",
        )
        self.run_cmd(
            "mark-worker-ready",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
        )
        self.run_cmd(
            "maybe-mark-ready",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
        )

        status = self.run_cmd(
            "status",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
        )

        self.assertIn("cluster_id=demo", status.stdout)
        self.assertIn("active_nodes=2", status.stdout)
        self.assertIn("head_node=n1", status.stdout)
        self.assertIn("node=n1", status.stdout)
        self.assertIn("node=n2", status.stdout)

        self.run_cmd(
            "mark-worker-ready",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "102",
        )
        self.run_cmd(
            "maybe-mark-ready",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
        )
        status_ready = self.run_cmd(
            "status",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
        )
        self.assertIn("active_nodes=2", status_ready.stdout)

        self.run_cmd(
            "cleanup",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
        )
        self.assertFalse((self.state_root / self.cluster_id).exists())

    def test_status_excludes_non_running_jobs_from_ready_nodes(self):
        self.run_cmd(
            "init",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--num-nodes",
            "2",
            "--partition",
            "a100q",
        )
        for job_id, hostname, ip in [("101", "n1", "10.0.0.1"), ("102", "n2", "10.0.0.2")]:
            self.run_cmd(
                "add-job",
                "--state-root",
                str(self.state_root),
                "--cluster-id",
                self.cluster_id,
                "--job-id",
                job_id,
            )
            self.run_cmd(
                "register-node",
                "--state-root",
                str(self.state_root),
                "--cluster-id",
                self.cluster_id,
                "--job-id",
                job_id,
                "--hostname",
                hostname,
                "--ip",
                ip,
            )
            self.run_cmd(
                "mark-worker-ready",
                "--state-root",
                str(self.state_root),
                "--cluster-id",
                self.cluster_id,
                "--job-id",
                job_id,
            )
        self.run_cmd(
            "set-head",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
            "--ip",
            "10.0.0.1",
        )

        status = self.run_cmd(
            "status",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            extra_env={"TEST_SQUEUE_STATES": "101=RUNNING,102=FAILED"},
        )

        self.assertIn("active_nodes=1", status.stdout)
        self.assertIn("node=n1", status.stdout)
        self.assertNotIn("node=n2", status.stdout)
        self.assertFalse((self.state_root / self.cluster_id / "nodes" / "102.json").exists())
        self.assertFalse((self.state_root / self.cluster_id / "workers" / "102").exists())

    def test_status_auto_cleanup_removes_stale_cluster(self):
        self.run_cmd(
            "init",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--num-nodes",
            "1",
            "--partition",
            "a100q",
        )
        self.run_cmd(
            "add-job",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
        )
        self.run_cmd(
            "register-node",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
            "--hostname",
            "n1",
            "--ip",
            "10.0.0.1",
        )

        status = self.run_cmd(
            "status",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--auto-cleanup-stale",
            extra_env={"TEST_SQUEUE_STATES": "101=FAILED"},
        )

        self.assertIn("cluster_id=demo", status.stdout)
        self.assertIn("cluster_removed=1", status.stdout)
        self.assertIn("reason=no_active_jobs", status.stdout)
        self.assertFalse((self.state_root / self.cluster_id).exists())

    def test_status_auto_cleanup_keeps_cluster_when_slurm_unavailable(self):
        self.run_cmd(
            "init",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--num-nodes",
            "1",
            "--partition",
            "a100q",
        )
        self.run_cmd(
            "add-job",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
        )

        status = self.run_cmd(
            "status",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--auto-cleanup-stale",
            extra_env={"TEST_SQUEUE_FAIL": "1"},
        )

        self.assertIn("cluster_id=demo", status.stdout)
        self.assertIn("cluster_removed=0", status.stdout)
        self.assertTrue((self.state_root / self.cluster_id).exists())

    def test_notify_node_registered_uses_mail_command(self):
        mail_log = self.state_root / "mail.log"
        mail_script = self.bin_dir / "mail"
        mail_script.write_text(
            "#!/bin/bash\n"
            "set -euo pipefail\n"
            "body=\"$(cat)\"\n"
            "printf 'args:%s\\n' \"$*\" >> \"" + str(mail_log) + "\"\n"
            "printf 'body:%s\\n' \"$body\" >> \"" + str(mail_log) + "\"\n",
            encoding="utf-8",
        )
        mail_script.chmod(0o755)

        self.run_cmd(
            "notify-node-registered",
            "--email",
            "user@example.com",
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
            "--hostname",
            "n1",
            "--ip",
            "10.0.0.1",
            "--partition",
            "a100q",
        )

        log_text = mail_log.read_text(encoding="utf-8")
        self.assertIn("args:-s [ray-slurm-cluster] node registered for demo user@example.com", log_text)
        self.assertIn("body:cluster_id=demo", log_text)
        self.assertIn("job_id=101", log_text)
        self.assertIn("hostname=n1", log_text)
        self.assertIn("ip=10.0.0.1", log_text)
        self.assertIn("partition=a100q", log_text)

    def test_notify_node_failed_uses_mail_command(self):
        mail_log = self.state_root / "mail.log"
        mail_script = self.bin_dir / "mail"
        mail_script.write_text(
            "#!/bin/bash\n"
            "set -euo pipefail\n"
            "body=\"$(cat)\"\n"
            "printf 'args:%s\\n' \"$*\" >> \"" + str(mail_log) + "\"\n"
            "printf 'body:%s\\n' \"$body\" >> \"" + str(mail_log) + "\"\n",
            encoding="utf-8",
        )
        mail_script.chmod(0o755)

        self.run_cmd(
            "notify-node-failed",
            "--email",
            "user@example.com",
            "--cluster-id",
            self.cluster_id,
            "--job-id",
            "101",
            "--hostname",
            "n1",
            "--ip",
            "10.0.0.1",
            "--partition",
            "a100q",
            "--exit-code",
            "17",
            "--log-path",
            "/tmp/job_101.out",
            "--log-tail",
            "line-1\nline-2",
        )

        log_text = mail_log.read_text(encoding="utf-8")
        self.assertIn("args:-s [ray-slurm-cluster] node failed for demo user@example.com", log_text)
        self.assertIn("body:cluster_id=demo", log_text)
        self.assertIn("job_id=101", log_text)
        self.assertIn("exit_code=17", log_text)
        self.assertIn("log_path=/tmp/job_101.out", log_text)
        self.assertIn("log_tail:\nline-1\nline-2", log_text)

    def test_update_desired_nodes_increases_target_size(self):
        self.run_cmd(
            "init",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--num-nodes",
            "2",
            "--partition",
            "a100q",
            "--notify-email",
            "user@example.com",
        )

        self.run_cmd(
            "update-desired-nodes",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--add-nodes",
            "3",
        )

        cluster_dir = self.state_root / self.cluster_id
        self.assertEqual((cluster_dir / "desired_nodes").read_text(encoding="utf-8").strip(), "5")


if __name__ == "__main__":
    unittest.main()
