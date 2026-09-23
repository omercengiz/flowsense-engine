import os
import sys

import pytest

mcp = pytest.importorskip("mcp")
mcp_stdio = pytest.importorskip("mcp.client.stdio")
ClientSession = mcp.ClientSession
StdioServerParameters = mcp.StdioServerParameters
stdio_client = mcp_stdio.stdio_client


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.integration
@pytest.mark.anyio
async def test_mcp_analyze_airflow_dag_end_to_end() -> None:
    dag_id = os.getenv(
        "FLOWSENSE_TEST_DAG_ID",
        "flowsense_demo",
    )

    env = os.environ.copy()

    existing_pythonpath = env.get("PYTHONPATH")

    if existing_pythonpath:
        env["PYTHONPATH"] = f"src:{existing_pythonpath}"
    else:
        env["PYTHONPATH"] = "src"

    server_params = StdioServerParameters(
        command=sys.executable,
        args=[
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

        result = await session.call_tool(
            "analyze_airflow_dag",
            {
                "dag_id": dag_id,
            },
        )

        assert result.is_error is False
        assert result.content
