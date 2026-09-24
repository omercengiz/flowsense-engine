from datetime import UTC, datetime

import pytest

from examples.python.showcase import DAGS, verify_seed_runs


class SeedClient:
    def __init__(self, *, missing_run=False, failed_task=False):
        self.missing_run = missing_run
        self.failed_task = failed_task

    def get_dag_runs(self, dag_id):
        return {
            "dag_runs": [
                {"dag_run_id": f"flowsense_showcase_seed_{i:02d}", "state": "success"}
                for i in range(19 if self.missing_run else 20)
            ]
        }

    def get_task_instances(self, dag_id, run_id):
        timestamp = datetime.now(UTC).isoformat()
        return {
            "task_instances": [
                {
                    "dag_id": dag_id,
                    "dag_run_id": run_id,
                    "task_id": f"task_{i}",
                    "state": "failed" if self.failed_task and i == 0 else "success",
                    "start_date": timestamp,
                    "end_date": timestamp,
                }
                for i in range(4)
            ]
        }


def test_showcase_proof_counts_successful_executions(tmp_path):
    proof = verify_seed_runs(SeedClient(), tmp_path)
    assert set(proof["dags"]) == set(DAGS)
    assert sum(d["successful_seed_runs"] for d in proof["dags"].values()) == 120
    assert sum(d["successful_task_executions"] for d in proof["dags"].values()) == 480
    assert (tmp_path / "seed-execution-proof.json").exists()


@pytest.mark.parametrize("options", [{"missing_run": True}, {"failed_task": True}])
def test_showcase_proof_rejects_incomplete_evidence(tmp_path, options):
    with pytest.raises(ValueError, match="expected"):
        verify_seed_runs(SeedClient(**options), tmp_path)
    assert not (tmp_path / "seed-execution-proof.json").exists()
