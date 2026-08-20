import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from flowsense.engine.drift import DriftResult
from flowsense.mcp.server import serialize_analysis
from flowsense.models import DAGAnalysis


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_mcp_server_exposes_analyze_tool() -> None:
    server_params = StdioServerParameters(
        command="uv",
        args=[
            "run",
            "python",
            "-m",
            "flowsense.mcp.server",
        ],
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
        primary_origin="transform",
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
        propagation_results=[],
        dependencies={
            "transform": [],
        },
    )

    result = serialize_analysis(analysis)

    assert result["dag_id"] == "demo"
    assert result["runs_analyzed"] == 5
    assert result["overall_severity"] == "CRITICAL"
    assert result["primary_origin"] == "transform"

    transform = result["drift_results"]["transform"]

    assert transform["baseline"] == 3.0
    assert transform["current"] == 9.5
    assert transform["mad"] == 0.1
    assert transform["robust_z_score"] == 43.84
    assert transform["deviation_percent"] == 216.67
    assert transform["severity"] == "CRITICAL"

    assert result["propagation_results"] == []
    assert result["dependencies"] == {
        "transform": [],
    }
