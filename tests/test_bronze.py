"""Offline tests for the bronze ingestion layer."""

from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path

import pytest

from tech_cost_platform.bronze.ingest import (
    TABLE_SPECS,
    BronzeValidationError,
)
from tech_cost_platform.delta_tables import read_delta_table
from tech_cost_platform.runtime import repo_root

EXPECTED_GL_TOTAL_EUR = Decimal("61813.95")


def csv_row_count(path: Path) -> int:
    """Return the number of data rows in a CSV file."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def test_bronze_creates_all_delta_tables_and_reconciles_counts(synth_data: Path, bronze_ingest) -> None:
    """All six source tables should land in bronze Delta with matching row counts."""
    run = bronze_ingest()
    output_paths = run.ingest()

    for spec in TABLE_SPECS:
        delta_table = read_delta_table(output_paths[spec.table_name])
        assert output_paths[spec.table_name].exists()
        assert delta_table.num_rows == csv_row_count(synth_data / spec.filename)


def test_bronze_preserves_gl_total_and_lineage_anchor(bronze_ingest) -> None:
    """Bronze should preserve the GL aggregate, ids, and source columns."""
    run = bronze_ingest()
    output_paths = run.ingest()

    table = read_delta_table(output_paths["gl_costs"])
    rows = table.to_pylist()
    columns = set(table.column_names)
    gl_rows = [row for row in rows if row["gl_line_id"] == "GL-000001"]
    aggregate = sum((row["amount_eur"] for row in rows), start=Decimal("0.00"))

    assert columns.issuperset(
        {"gl_line_id", "period", "gl_account", "cost_center_id", "amount_eur", "description", "_source_file"}
    )
    assert len({row["gl_line_id"] for row in rows}) == len(rows)
    assert aggregate == EXPECTED_GL_TOTAL_EUR
    assert gl_rows[0]["gl_line_id"] == "GL-000001"
    assert gl_rows[0]["cost_center_id"] == "CC-BIZ-APPS"


def test_bronze_preserves_intentional_null_tower_id(bronze_ingest) -> None:
    """The unmapped residual anchor must survive bronze as a null tower_id."""
    run = bronze_ingest()
    output_paths = run.ingest()

    rows = read_delta_table(output_paths["cost_centers"]).to_pylist()
    row = next(row for row in rows if row["cost_center_id"] == "CC-LEGACY")

    assert row["tower_id"] is None
    assert sum(1 for row in rows if row["tower_id"] is None) >= 1


def test_bronze_rejects_malformed_input_without_writing(bronze_ingest) -> None:
    """Malformed source input should fail validation before any Delta tables are written."""
    run = bronze_ingest()
    malformed_path = repo_root() / "tests" / "fixtures" / "gl_costs_malformed.csv"

    with pytest.raises(BronzeValidationError):
        run.ingest(
            source_overrides={"gl_costs": malformed_path},
        )

    for spec in TABLE_SPECS:
        assert not (run.bronze_dir / spec.table_name).exists()
