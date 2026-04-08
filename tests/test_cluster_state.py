import json
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

    def tearDown(self):
        self.tmpdir.cleanup()

    def run_cmd(self, *args, check=True):
        proc = subprocess.run(
            ["python3", str(SCRIPT), *args],
            text=True,
            capture_output=True,
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
        )

        cluster_dir = self.state_root / self.cluster_id
        self.assertTrue((cluster_dir / "nodes").is_dir())
        self.assertTrue((cluster_dir / "workers").is_dir())
        self.assertEqual((cluster_dir / "desired_nodes").read_text().strip(), "2")
        self.assertEqual((cluster_dir / "jobs.txt").read_text(), "")

    def test_add_job_and_register_node_persist_metadata(self):
        self.run_cmd(
            "init",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--num-nodes",
            "2",
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
            "publish-head-ip",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
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

    def test_status_and_cleanup_reflect_cluster_progress(self):
        self.run_cmd(
            "init",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
            "--num-nodes",
            "2",
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
            "publish-head-ip",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
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
        self.assertIn("desired_nodes=2", status.stdout)
        self.assertIn("submitted_jobs=2", status.stdout)
        self.assertIn("registered_nodes=2", status.stdout)
        self.assertIn("ready_nodes=1", status.stdout)
        self.assertIn("head_ip=10.0.0.1", status.stdout)
        self.assertIn("cluster_ready=no", status.stdout)
        self.assertIn("job=101 hostname=n1 ip=10.0.0.1 role=head ready=yes", status.stdout)
        self.assertIn("job=102 hostname=n2 ip=10.0.0.2 role=worker ready=no", status.stdout)

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
        self.assertIn("cluster_ready=yes", status_ready.stdout)

        self.run_cmd(
            "cleanup",
            "--state-root",
            str(self.state_root),
            "--cluster-id",
            self.cluster_id,
        )
        self.assertFalse((self.state_root / self.cluster_id).exists())


if __name__ == "__main__":
    unittest.main()
