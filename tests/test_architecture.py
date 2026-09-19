from __future__ import annotations

import ast
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src" / "flowsense"

FORBIDDEN_IMPORTS = {
    "domain": (
        "flowsense.application",
        "flowsense.cli",
        "flowsense.collector",
        "flowsense.infrastructure",
        "flowsense.mcp",
        "flowsense.models",
    ),
    "engine": (
        "flowsense.application",
        "flowsense.cli",
        "flowsense.collector",
        "flowsense.infrastructure",
        "flowsense.mcp",
        "flowsense.models",
    ),
    "application": (
        "flowsense.cli",
        "flowsense.collector",
        "flowsense.infrastructure",
        "flowsense.mcp",
        "flowsense.models",
    ),
    "infrastructure": (
        "flowsense.cli",
        "flowsense.collector",
        "flowsense.mcp",
        "flowsense.models",
    ),
}

# This wrapper preserves the pre-0.2 API and is the only accepted outward
# dependency from the engine layer. It is deprecated and must not grow.
ALLOWED_EXCEPTIONS = {
    Path("engine/analyzer.py"): {
        "flowsense.application",
        "flowsense.infrastructure.airflow",
    }
}


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)

    return modules


def test_layer_dependencies_point_inward() -> None:
    violations: list[str] = []

    for layer, forbidden_prefixes in FORBIDDEN_IMPORTS.items():
        for path in sorted((SOURCE_ROOT / layer).rglob("*.py")):
            relative_path = path.relative_to(SOURCE_ROOT)
            allowed = ALLOWED_EXCEPTIONS.get(relative_path, set())

            for module in _imported_modules(path):
                if module in allowed:
                    continue
                if module.startswith(forbidden_prefixes):
                    violations.append(f"{relative_path}: {module}")

    assert violations == [], "Forbidden layer imports:\n" + "\n".join(violations)
