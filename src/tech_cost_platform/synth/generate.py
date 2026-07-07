"""Deterministic generator for synthetic source exports."""

from __future__ import annotations

import csv
import hashlib
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable, Sequence

import yaml

from .schema import (
    APPLICATION_COLUMNS,
    BUSINESS_UNIT_COLUMNS,
    COST_CENTER_COLUMNS,
    GL_COST_COLUMNS,
    RESOURCE_TOWER_COLUMNS,
    USAGE_METRIC_COLUMNS,
    Application,
    BusinessUnit,
    CostCenter,
    GLCost,
    ResourceTower,
    SynthConfig,
    UsageMetric,
)

DEFAULT_SYNTH_CONFIG = SynthConfig(seed=20260107, period="2026-01", output_dir="data/source")
DEFAULT_GL_TOTAL_EUR = Decimal("61813.95")
TABLE_FILENAMES = {
    "gl_costs": "gl_costs.csv",
    "cost_centers": "cost_centers.csv",
    "resource_towers": "resource_towers.csv",
    "applications": "applications.csv",
    "business_units": "business_units.csv",
    "usage_metrics": "usage_metrics.csv",
}
ACCOUNT_DESCRIPTIONS = {
    "6000": "Salaries",
    "7000": "Cloud",
    "7100": "Software",
    "7200": "Network",
    "7300": "Support",
    "7400": "Consulting",
}
GL_TEMPLATES: tuple[tuple[str, str, int, int], ...] = (
    ("7000", "cloud", 1450, 1980),
    ("7000", "cloud", 1550, 2050),
    ("7100", "software", 780, 1160),
    ("7100", "software", 820, 1220),
    ("7200", "network", 520, 860),
    ("7300", "support", 410, 770),
    ("7400", "consulting", 640, 1040),
    ("6000", "labor", 1650, 2420),
)
GL_COST_CENTER_FACTORS = {
    "CC-CLOUD-COMPUTE": Decimal("1.18"),
    "CC-CLOUD-STORAGE": Decimal("1.08"),
    "CC-NETWORK-EDGE": Decimal("0.92"),
    "CC-PLATFORM-OPS": Decimal("1.27"),
    "CC-BIZ-APPS": Decimal("1.12"),
    "CC-LEGACY": Decimal("0.84"),
}


def repo_root() -> Path:
    """Return the repository root from the synth package."""
    return Path(__file__).resolve().parents[3]


def load_synth_config(config_path: Path | None = None) -> SynthConfig:
    """Load the synth block from config.yaml."""
    resolved_path = config_path or repo_root() / "config.yaml"
    with resolved_path.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle) or {}
    return SynthConfig.model_validate(raw_config.get("synth", DEFAULT_SYNTH_CONFIG.model_dump()))


def stable_int(seed: int, *parts: str, minimum: int, maximum: int) -> int:
    """Return a deterministic pseudo-random integer in the inclusive range."""
    digest_input = "|".join([str(seed), *parts]).encode("utf-8")
    digest = hashlib.sha256(digest_input).hexdigest()
    span = maximum - minimum + 1
    return minimum + (int(digest[:16], 16) % span)


def quantize_2(value: Decimal) -> Decimal:
    """Normalize a decimal to two fractional digits."""
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def format_decimal(value: Decimal) -> str:
    """Render deterministic fixed-scale decimal text."""
    return f"{quantize_2(value):.2f}"


def model_rows(items: Iterable[dict[str, object]], model_type):
    """Validate rows against the output-shape model."""
    return [model_type.model_validate(item) for item in items]


def build_resource_towers() -> list[ResourceTower]:
    return model_rows(
        [
            {"tower_id": "TWR-COMPUTE", "tower_name": "Compute", "tower_type": "infrastructure"},
            {"tower_id": "TWR-LABOR", "tower_name": "Labor", "tower_type": "service"},
            {"tower_id": "TWR-NETWORK", "tower_name": "Network", "tower_type": "infrastructure"},
            {"tower_id": "TWR-STORAGE", "tower_name": "Storage", "tower_type": "infrastructure"},
        ],
        ResourceTower,
    )


def build_cost_centers() -> list[CostCenter]:
    return model_rows(
        [
            {
                "cost_center_id": "CC-BIZ-APPS",
                "cost_center_name": "Business Applications",
                "tower_id": "TWR-LABOR",
            },
            {
                "cost_center_id": "CC-CLOUD-COMPUTE",
                "cost_center_name": "Cloud Compute",
                "tower_id": "TWR-COMPUTE",
            },
            {
                "cost_center_id": "CC-CLOUD-STORAGE",
                "cost_center_name": "Cloud Storage",
                "tower_id": "TWR-STORAGE",
            },
            {
                "cost_center_id": "CC-LEGACY",
                "cost_center_name": "Legacy Shared Services",
                "tower_id": None,
            },
            {
                "cost_center_id": "CC-NETWORK-EDGE",
                "cost_center_name": "Network Edge",
                "tower_id": "TWR-NETWORK",
            },
            {
                "cost_center_id": "CC-PLATFORM-OPS",
                "cost_center_name": "Platform Operations",
                "tower_id": "TWR-LABOR",
            },
        ],
        CostCenter,
    )


def build_applications() -> list[Application]:
    return model_rows(
        [
            {"app_id": "APP-ANALYTICS", "app_name": "Analytics", "business_criticality": "high"},
            {"app_id": "APP-BILLING", "app_name": "Billing", "business_criticality": "high"},
            {"app_id": "APP-CRM", "app_name": "CRM", "business_criticality": "high"},
            {"app_id": "APP-EMAIL", "app_name": "Email", "business_criticality": "med"},
            {"app_id": "APP-ERP", "app_name": "ERP", "business_criticality": "high"},
            {"app_id": "APP-HRIS", "app_name": "HRIS", "business_criticality": "med"},
        ],
        Application,
    )


def build_business_units() -> list[BusinessUnit]:
    return model_rows(
        [
            {"bu_id": "BU-CORP", "bu_name": "Corporate"},
            {"bu_id": "BU-RETAIL", "bu_name": "Retail"},
            {"bu_id": "BU-WHOLESALE", "bu_name": "Wholesale"},
        ],
        BusinessUnit,
    )


def build_usage_metrics(period: str) -> list[UsageMetric]:
    metrics: list[dict[str, object]] = []

    def add(step: str, from_id: str, metric_name: str, values: Sequence[tuple[str, Decimal]]) -> None:
        base_metric_id = f"{step}|{from_id}|{metric_name}"
        for index, (to_id, value) in enumerate(values, start=1):
            metrics.append(
                {
                    "metric_id": f"M-{hashlib.sha256(f'{base_metric_id}|{to_id}'.encode('utf-8')).hexdigest()[:12].upper()}",
                    "period": period,
                    "step": step,
                    "from_id": from_id,
                    "to_id": to_id,
                    "metric_name": metric_name,
                    "value": quantize_2(value),
                }
            )

    add(
        "tower_to_app",
        "TWR-COMPUTE",
        "cpu_hours",
        [
            ("APP-ANALYTICS", Decimal("2400")),
            ("APP-BILLING", Decimal("3200")),
            ("APP-CRM", Decimal("1800")),
            ("APP-ERP", Decimal("2600")),
        ],
    )
    add(
        "tower_to_app",
        "TWR-COMPUTE",
        "named_servers",
        [
            ("APP-ANALYTICS", Decimal("12")),
            ("APP-BILLING", Decimal("18")),
            ("APP-CRM", Decimal("10")),
            ("APP-ERP", Decimal("15")),
        ],
    )
    add(
        "tower_to_app",
        "TWR-LABOR",
        "named_users",
        [
            ("APP-ANALYTICS", Decimal("70")),
            ("APP-BILLING", Decimal("160")),
            ("APP-CRM", Decimal("140")),
            ("APP-EMAIL", Decimal("210")),
            ("APP-ERP", Decimal("120")),
            ("APP-HRIS", Decimal("90")),
        ],
    )
    add(
        "tower_to_app",
        "TWR-LABOR",
        "ticket_count",
        [
            ("APP-ANALYTICS", Decimal("45")),
            ("APP-BILLING", Decimal("120")),
            ("APP-CRM", Decimal("110")),
            ("APP-EMAIL", Decimal("130")),
            ("APP-ERP", Decimal("95")),
            ("APP-HRIS", Decimal("60")),
        ],
    )
    add(
        "tower_to_app",
        "TWR-NETWORK",
        "bandwidth_tb",
        [
            ("APP-BILLING", Decimal("58")),
            ("APP-CRM", Decimal("42")),
            ("APP-EMAIL", Decimal("88")),
        ],
    )
    add(
        "tower_to_app",
        "TWR-NETWORK",
        "active_accounts",
        [
            ("APP-BILLING", Decimal("24")),
            ("APP-CRM", Decimal("18")),
            ("APP-EMAIL", Decimal("52")),
        ],
    )
    add(
        "tower_to_app",
        "TWR-STORAGE",
        "backup_jobs",
        [
            ("APP-ANALYTICS", Decimal("12")),
            ("APP-BILLING", Decimal("22")),
            ("APP-ERP", Decimal("18")),
            ("APP-HRIS", Decimal("8")),
        ],
    )
    add(
        "tower_to_app",
        "TWR-STORAGE",
        "storage_gb",
        [
            ("APP-ANALYTICS", Decimal("1500")),
            ("APP-BILLING", Decimal("2200")),
            ("APP-ERP", Decimal("1850")),
            ("APP-HRIS", Decimal("600")),
        ],
    )

    add(
        "app_to_bu",
        "APP-ANALYTICS",
        "named_users",
        [
            ("BU-CORP", Decimal("40")),
            ("BU-RETAIL", Decimal("20")),
            ("BU-WHOLESALE", Decimal("10")),
        ],
    )
    add(
        "app_to_bu",
        "APP-ANALYTICS",
        "storage_gb",
        [
            ("BU-CORP", Decimal("0")),
            ("BU-RETAIL", Decimal("0")),
            ("BU-WHOLESALE", Decimal("0")),
        ],
    )
    add(
        "app_to_bu",
        "APP-BILLING",
        "named_users",
        [
            ("BU-CORP", Decimal("60")),
            ("BU-RETAIL", Decimal("20")),
            ("BU-WHOLESALE", Decimal("20")),
        ],
    )
    add(
        "app_to_bu",
        "APP-BILLING",
        "transactions",
        [
            ("BU-CORP", Decimal("1000")),
            ("BU-RETAIL", Decimal("8000")),
            ("BU-WHOLESALE", Decimal("1000")),
        ],
    )
    add(
        "app_to_bu",
        "APP-CRM",
        "named_users",
        [
            ("BU-CORP", Decimal("30")),
            ("BU-RETAIL", Decimal("50")),
            ("BU-WHOLESALE", Decimal("20")),
        ],
    )
    add(
        "app_to_bu",
        "APP-CRM",
        "ticket_count",
        [
            ("BU-CORP", Decimal("35")),
            ("BU-RETAIL", Decimal("40")),
            ("BU-WHOLESALE", Decimal("25")),
        ],
    )
    add(
        "app_to_bu",
        "APP-ERP",
        "named_users",
        [
            ("BU-CORP", Decimal("20")),
            ("BU-RETAIL", Decimal("30")),
            ("BU-WHOLESALE", Decimal("50")),
        ],
    )
    add(
        "app_to_bu",
        "APP-ERP",
        "revenue_share",
        [
            ("BU-CORP", Decimal("15")),
            ("BU-RETAIL", Decimal("35")),
            ("BU-WHOLESALE", Decimal("50")),
        ],
    )
    add(
        "app_to_bu",
        "APP-HRIS",
        "headcount",
        [
            ("BU-CORP", Decimal("60")),
            ("BU-RETAIL", Decimal("20")),
            ("BU-WHOLESALE", Decimal("20")),
        ],
    )
    add(
        "app_to_bu",
        "APP-HRIS",
        "named_users",
        [
            ("BU-CORP", Decimal("55")),
            ("BU-RETAIL", Decimal("25")),
            ("BU-WHOLESALE", Decimal("20")),
        ],
    )

    return sorted(model_rows(metrics, UsageMetric), key=lambda row: row.metric_id)


def build_gl_costs(period: str, seed: int, cost_centers: Sequence[CostCenter]) -> list[GLCost]:
    rows: list[dict[str, object]] = []
    line_number = 1

    for cost_center in sorted(cost_centers, key=lambda row: row.cost_center_id):
        factor = GL_COST_CENTER_FACTORS[cost_center.cost_center_id]
        for template_index, (gl_account, category, minimum, maximum) in enumerate(GL_TEMPLATES, start=1):
            sample = stable_int(
                seed,
                cost_center.cost_center_id,
                gl_account,
                str(template_index),
                minimum=minimum,
                maximum=maximum,
            )
            amount = quantize_2((Decimal(sample) * factor) + Decimal("0.37"))
            description = (
                f"{cost_center.cost_center_name} {ACCOUNT_DESCRIPTIONS[gl_account].lower()} "
                f"{category} line {template_index}"
            )
            rows.append(
                {
                    "gl_line_id": f"GL-{line_number:06d}",
                    "period": period,
                    "gl_account": gl_account,
                    "cost_center_id": cost_center.cost_center_id,
                    "amount_eur": amount,
                    "description": description,
                }
            )
            line_number += 1

    modeled_rows = sorted(model_rows(rows, GLCost), key=lambda row: row.gl_line_id)
    total = sum((row.amount_eur for row in modeled_rows), start=Decimal("0.00"))
    if total != DEFAULT_GL_TOTAL_EUR:
        raise ValueError(
            f"Expected deterministic GL total {DEFAULT_GL_TOTAL_EUR}, got {total}. "
            "Update the committed lock only if the fixture design changes intentionally."
        )
    return modeled_rows


def serialize_row(row, columns: Sequence[str]) -> dict[str, str]:
    """Serialize a model row to deterministic CSV text."""
    data = row.model_dump()
    serialized: dict[str, str] = {}
    for column in columns:
        value = data[column]
        if value is None:
            serialized[column] = ""
        elif isinstance(value, Decimal):
            serialized[column] = format_decimal(value)
        else:
            serialized[column] = str(value)
    return serialized


def write_csv(path: Path, rows: Sequence, columns: Sequence[str]) -> None:
    """Write a deterministic LF-terminated CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(serialize_row(row, columns))


def generate_source_exports(config: SynthConfig | None = None, config_path: Path | None = None) -> dict[str, Path]:
    """Generate all source CSV exports and return their file paths."""
    synth_config = config or load_synth_config(config_path)
    output_dir_value = Path(synth_config.output_dir)
    output_dir = output_dir_value if output_dir_value.is_absolute() else repo_root() / output_dir_value

    resource_towers = build_resource_towers()
    cost_centers = build_cost_centers()
    applications = build_applications()
    business_units = build_business_units()
    usage_metrics = build_usage_metrics(synth_config.period)
    gl_costs = build_gl_costs(synth_config.period, synth_config.seed, cost_centers)

    datasets = {
        "gl_costs": (gl_costs, GL_COST_COLUMNS),
        "cost_centers": (cost_centers, COST_CENTER_COLUMNS),
        "resource_towers": (resource_towers, RESOURCE_TOWER_COLUMNS),
        "applications": (applications, APPLICATION_COLUMNS),
        "business_units": (business_units, BUSINESS_UNIT_COLUMNS),
        "usage_metrics": (usage_metrics, USAGE_METRIC_COLUMNS),
    }

    output_paths: dict[str, Path] = {}
    for table_name, (rows, columns) in datasets.items():
        table_path = output_dir / TABLE_FILENAMES[table_name]
        write_csv(table_path, rows, columns)
        output_paths[table_name] = table_path

    return output_paths


def main() -> int:
    """CLI entrypoint for the synthetic source-data generator."""
    config = load_synth_config()
    output_paths = generate_source_exports(config)
    print(f"[tech-cost-platform] synth seed={config.seed} period={config.period}")
    print(f"[tech-cost-platform] output_dir={output_paths['gl_costs'].parent}")
    print(f"[tech-cost-platform] files_written={len(output_paths)} gl_total_eur={DEFAULT_GL_TOTAL_EUR}")
    return 0
