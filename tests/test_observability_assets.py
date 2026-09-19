import json
import re
from pathlib import Path

from flowsense.observability.prometheus import PrometheusExporter

ROOT = Path(__file__).resolve().parents[1]
OBSERVABILITY = ROOT / "deploy" / "observability"
DASHBOARD = OBSERVABILITY / "grafana" / "dashboards" / "flowsense-overview.json"


def test_grafana_dashboard_queries_exported_metrics() -> None:
    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    panel_ids = [panel["id"] for panel in dashboard["panels"]]
    expressions = [
        target["expr"]
        for panel in dashboard["panels"]
        for target in panel.get("targets", [])
    ]
    referenced_metrics = {
        metric
        for expression in expressions
        for metric in re.findall(r"flowsense_[a-z_]+", expression)
    }
    exporter_contract = PrometheusExporter().render()

    assert dashboard["uid"] == "flowsense-overview"
    assert dashboard["title"] == "FlowSense Overview"
    assert len(panel_ids) == len(set(panel_ids))
    assert referenced_metrics
    assert all(
        f"# HELP {metric} " in exporter_contract for metric in referenced_metrics
    )


def test_observability_provisioning_is_wired_consistently() -> None:
    compose = (OBSERVABILITY / "docker-compose.yml").read_text(encoding="utf-8")
    prometheus = (OBSERVABILITY / "prometheus.yml").read_text(encoding="utf-8")
    datasource = (
        OBSERVABILITY / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
    ).read_text(encoding="utf-8")
    provider = (
        OBSERVABILITY / "grafana" / "provisioning" / "dashboards" / "flowsense.yml"
    ).read_text(encoding="utf-8")

    assert "prom/prometheus:v3.14.0" in compose
    assert "grafana/grafana:13.2.2" in compose
    assert "host.docker.internal:9108" in prometheus
    assert "uid: flowsense-prometheus" in datasource
    assert "url: http://prometheus:9090" in datasource
    assert "path: /var/lib/grafana/dashboards" in provider
