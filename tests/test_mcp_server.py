import os

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from flowsense.engine.drift import DriftResult
from flowsense.engine.impact import TaskImpact
from flowsense.engine.root_cause import RootCauseResult
from flowsense.mcp.server import serialize_analysis
from flowsense.models import DAGAnalysis


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_mcp_server_exposes_analyze_tool() -> None:
    env = os.environ.copy()

    existing_pythonpath = env.get("PYTHONPATH")

    if existing_pythonpath:
        env["PYTHONPATH"] = f"src:{existing_pythonpath}"
    else:
        env["PYTHONPATH"] = "src"

    server_params = StdioServerParameters(
        command="uv",
        args=[
            "run",
            "python",
            "-m",
            "flowsense.mcp.server",
        ],
        env=env,
    )

    async with (
        stdio_client(server_params) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()

        response = await session.list_tools()

        tool_names = [tool.name for tool in response.tools]

        assert "analyze_airflow_dag" in tool_names


def test_serialize_analysis() -> None:
    analysis = DAGAnalysis(
        dag_id="demo",
        runs_analyzed=5,
        overall_severity="CRITICAL",
        primary_origin=RootCauseResult(
            task_id="transform",
            classification="COMBINED",
            severity="CRITICAL",
            propagation_score=0.8,
        ),
        drift_results={
            "transform": DriftResult(
                task_id="transform",
                baseline=3.0,
                current=9.5,
                mad=0.1,
                robust_z_score=43.84,
                deviation_percent=216.67,
                severity="CRITICAL",
            )
        },
        handoff_drift_results={
            ("extract", "transform"): DriftResult(
                task_id="extract->transform",
                baseline=2.0,
                current=8.0,
                mad=0.2,
                robust_z_score=20.24,
                deviation_percent=300.0,
                severity="CRITICAL",
            )
        },
        task_impacts={
            "transform": TaskImpact(
                task_id="transform",
                classification="COMBINED",
                task_severity="CRITICAL",
                upstream_handoff_severity="CRITICAL",
            )
        },
        propagation_results=[],
        dependencies={
            "transform": [],
        },
    )

    result = serialize_analysis(analysis)

    assert result["dag_id"] == "demo"
    assert result["runs_analyzed"] == 5
    assert result["overall_severity"] == "CRITICAL"

    primary_origin = result["primary_origin"]

    assert primary_origin["task_id"] == "transform"
    assert primary_origin["classification"] == "COMBINED"
    assert primary_origin["severity"] == "CRITICAL"
    assert primary_origin["propagation_score"] == 0.8
    transform = result["drift_results"]["transform"]

    assert transform["baseline"] == 3.0
    assert transform["current"] == 9.5
    assert transform["mad"] == 0.1
    assert transform["robust_z_score"] == 43.84
    assert transform["deviation_percent"] == 216.67
    assert transform["severity"] == "CRITICAL"

    handoff = result["handoff_drift_results"]["extract->transform"]

    assert handoff["baseline"] == 2.0
    assert handoff["current"] == 8.0
    assert handoff["severity"] == "CRITICAL"

    impact = result["task_impacts"]["transform"]

    assert impact["classification"] == "COMBINED"
    assert impact["task_severity"] == "CRITICAL"
    assert impact["upstream_handoff_severity"] == "CRITICAL"

    assert result["propagation_results"] == []
    assert result["dependencies"] == {
        "transform": [],
    }
