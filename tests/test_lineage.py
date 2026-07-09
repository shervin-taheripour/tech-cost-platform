"""Fast tests for authoritative lineage tracing and round-trip reconciliation."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import yaml
from deltalake import DeltaTable

from tech_cost_platform.delta_tables import build_arrow_table, read_delta_table, write_delta_table
from tech_cost_platform.lineage import (
    LINEAGE_TABLE,
    LineageValidationError,
    MAX_PROPORTION_DRIFT_EUR,
    build_worked_example_payload,
    trace_backward,
    trace_forward,
    validate_lineage_per_line,
    validate_lineage_round_trip,
    validate_proportion_consistency,
)
from tech_cost_platform.rules import RuleRegistry
from tech_cost_platform.runtime import repo_root
from tech_cost_platform.synth.generate import DEFAULT_GL_TOTAL_EUR


def write_rule_version(test_workspace: Path, version_id: str, payload: dict[str, object]) -> Path:
    """Write a temporary governed rule version for lineage tests."""
    rules_dir = test_workspace / "lineage-rules" / version_id
    rules_dir.mkdir(parents=True, exist_ok=True)
    rule_path = rules_dir / f"{version_id}.yaml"
    rule_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return rules_dir


def build_rule_payload(
    *,
    version_id: str,
    tower_metric: str = "cpu_hours",
    app_metric: str = "transactions",
) -> dict[str, object]:
    """Return a minimal governed-valid rule payload for lineage variants."""
    return {
        "version_id": version_id,
        "description": f"Lineage integration test rule {version_id}.",
        "created": "2026-07-09",
        "gl_to_tower": {"basis": "cost_center_mapping"},
        "tower_to_app": {"strategy": "consumption", "metric_name": tower_metric},
        "app_to_bu": {"strategy": "consumption", "metric_name": app_metric},
    }


def _build_v1_lineage(lineage):
    run = lineage()
    result = run.build(rule_version_id="v1_transactions")
    return run, result


def _build_variant_lineage(lineage, test_workspace: Path, *, version_id: str, app_metric: str):
    run = lineage()
    rules_dir = write_rule_version(
        test_workspace,
        version_id,
        build_rule_payload(version_id=version_id, app_metric=app_metric),
    )
    result = run.build(rule_version_id=version_id, rules_dir=rules_dir)
    return run, result


def test_lineage_materializes_as_valid_delta(lineage) -> None:
    """Lineage stage should write a readable Delta table."""
    run, result = _build_v1_lineage(lineage)

    output_path = result.output_paths[LINEAGE_TABLE]

    assert output_path.exists()
    assert (output_path / "_delta_log").exists()
    assert DeltaTable(str(output_path)).to_pyarrow_table().num_rows > 0
    assert result.reconciliation.total_gl_eur == DEFAULT_GL_TOTAL_EUR
    assert run.gold_dir == output_path.parent


def test_lineage_per_gl_line_completeness_is_exact(lineage) -> None:
    """Every GL line should resolve exactly once across allocated and residual outcomes."""
    run, _ = _build_v1_lineage(lineage)
    lineage_rows = read_delta_table(run.gold_dir / LINEAGE_TABLE).to_pylist()

    validate_lineage_per_line(lineage_rows)

    totals: dict[str, Decimal] = {}
    expected: dict[str, Decimal] = {}
    for row in lineage_rows:
        expected[row["gl_line_id"]] = row["gl_amount_eur"]
        totals[row["gl_line_id"]] = totals.get(row["gl_line_id"], Decimal("0.00")) + row["terminal_amount_eur"]

    assert totals == expected


def test_lineage_proportion_consistency_is_bounded_not_exact(lineage) -> None:
    """Stored proportions should explain the split within the three-hop rounding bound."""
    run, _ = _build_v1_lineage(lineage)
    lineage_rows = read_delta_table(run.gold_dir / LINEAGE_TABLE).to_pylist()

    validate_proportion_consistency(lineage_rows)

    drifts = []
    for row in lineage_rows:
        if row["outcome"] != "allocated":
            continue
        product = (
            row["gl_amount_eur"]
            * row["prop_gl_to_tower"]
            * row["prop_tower_to_app"]
            * row["prop_app_to_bu"]
        )
        drift = abs(product - row["allocated_amount_eur"])
        drifts.append(drift)
        assert drift <= MAX_PROPORTION_DRIFT_EUR

    assert drifts
    assert any(drift > Decimal("0.00") for drift in drifts)


def test_lineage_round_trip_reproduces_total_gl_exactly(lineage) -> None:
    """Summing lineage outcomes should reproduce the governed GL total exactly."""
    run, _ = _build_v1_lineage(lineage)

    round_trip = validate_lineage_round_trip(
        silver_dir=run.silver_dir,
        gold_dir=run.gold_dir,
        lineage_dir=run.gold_dir / LINEAGE_TABLE,
        rule_version_id="v1_transactions",
    )

    assert round_trip.total_gl_eur == DEFAULT_GL_TOTAL_EUR
    assert round_trip.total_lineage_allocated_eur + round_trip.total_lineage_residual_eur == DEFAULT_GL_TOTAL_EUR
    assert round_trip.balanced is True
    assert round_trip.difference_eur == Decimal("0.00")


def test_lineage_round_trip_fails_on_tampered_input(lineage) -> None:
    """Dropping one lineage path should break the exact round-trip check loudly."""
    run, _ = _build_v1_lineage(lineage)
    lineage_path = run.gold_dir / LINEAGE_TABLE
    lineage_table = read_delta_table(lineage_path)
    lineage_rows = lineage_table.to_pylist()

    write_delta_table(
        build_arrow_table(lineage_rows[1:], lineage_table.schema),
        lineage_path,
        sort_columns=[
            "rule_version",
            "outcome",
            "gl_line_id",
            "tower_id",
            "app_id",
            "bu_id",
            "reason_code",
        ],
    )

    try:
        validate_lineage_round_trip(
            silver_dir=run.silver_dir,
            gold_dir=run.gold_dir,
            lineage_dir=lineage_path,
            rule_version_id="v1_transactions",
        )
        raise AssertionError("Expected lineage round-trip validation to fail on tampered input.")
    except LineageValidationError as exc:
        assert exc.result is not None
        assert exc.result.balanced is False
        assert exc.result.difference_eur != Decimal("0.00")


def test_residual_line_is_traceable_with_reason_code(lineage) -> None:
    """The seeded CC-LEGACY residual should appear in lineage as an unmapped GL exit."""
    run, _ = _build_v1_lineage(lineage)
    lineage_rows = read_delta_table(run.gold_dir / LINEAGE_TABLE).to_pylist()

    residual_row = next(
        row
        for row in lineage_rows
        if row["cost_center_id"] == "CC-LEGACY" and row["outcome"] == "residual"
    )

    assert residual_row["reason_code"] == "unmapped"
    assert residual_row["failed_step"] == "gl_to_tower"
    assert residual_row["tower_id"] is None
    assert residual_row["app_id"] is None
    assert residual_row["bu_id"] is None


def test_trace_backward_ties_to_bu_allocation_total(lineage) -> None:
    """Backward tracing for a BU should add back to that BU's allocation total exactly."""
    run, _ = _build_v1_lineage(lineage)
    allocation_rows = read_delta_table(run.gold_dir / "allocation").to_pylist()
    lineage_path = run.gold_dir / LINEAGE_TABLE
    bu_id = sorted({row["bu_id"] for row in allocation_rows})[0]

    traced_rows = trace_backward(
        lineage_dir=lineage_path,
        rule_version_id="v1_transactions",
        bu_id=bu_id,
    )
    traced_total = sum((row["allocated_amount_eur"] for row in traced_rows), start=Decimal("0.00"))
    expected_total = sum(
        (row["allocated_amount_eur"] for row in allocation_rows if row["bu_id"] == bu_id),
        start=Decimal("0.00"),
    )

    assert traced_total == expected_total


def test_trace_forward_for_fanned_out_gl_line_sums_to_gl_amount(lineage) -> None:
    """Forward tracing should return every terminal outcome for a fanned-out GL line."""
    run, _ = _build_v1_lineage(lineage)
    lineage_rows = read_delta_table(run.gold_dir / LINEAGE_TABLE).to_pylist()
    fanned_out_gl_line_id = next(
        gl_line_id
        for gl_line_id in sorted({row["gl_line_id"] for row in lineage_rows})
        if sum(1 for row in lineage_rows if row["gl_line_id"] == gl_line_id) > 1
    )

    traced_rows = trace_forward(
        lineage_dir=run.gold_dir / LINEAGE_TABLE,
        rule_version_id="v1_transactions",
        gl_line_id=fanned_out_gl_line_id,
    )
    terminal_total = sum((row["terminal_amount_eur"] for row in traced_rows), start=Decimal("0.00"))

    assert terminal_total == traced_rows[0]["gl_amount_eur"]


def test_lineage_is_version_aware_and_both_versions_round_trip(
    lineage,
    test_workspace: Path,
) -> None:
    """The seeded v1/v2 divergence should be visible in backward traces while both reconcile."""
    v1_run, _ = _build_v1_lineage(lineage)
    v2_run, _ = _build_variant_lineage(
        lineage,
        test_workspace,
        version_id="v2_named_users",
        app_metric="named_users",
    )
    v1_allocation = read_delta_table(v1_run.gold_dir / "allocation").to_pylist()
    v2_allocation = read_delta_table(v2_run.gold_dir / "allocation").to_pylist()

    v1_totals = {
        bu_id: sum(
            (row["allocated_amount_eur"] for row in v1_allocation if row["bu_id"] == bu_id),
            start=Decimal("0.00"),
        )
        for bu_id in sorted({row["bu_id"] for row in v1_allocation})
    }
    v2_totals = {
        bu_id: sum(
            (row["allocated_amount_eur"] for row in v2_allocation if row["bu_id"] == bu_id),
            start=Decimal("0.00"),
        )
        for bu_id in sorted({row["bu_id"] for row in v2_allocation})
    }
    divergent_bu_id = next(bu_id for bu_id in v1_totals if v1_totals[bu_id] != v2_totals[bu_id])

    v1_trace = trace_backward(
        lineage_dir=v1_run.gold_dir / LINEAGE_TABLE,
        rule_version_id="v1_transactions",
        bu_id=divergent_bu_id,
    )
    v2_trace = trace_backward(
        lineage_dir=v2_run.gold_dir / LINEAGE_TABLE,
        rule_version_id="v2_named_users",
        bu_id=divergent_bu_id,
    )

    assert sum((row["allocated_amount_eur"] for row in v1_trace), start=Decimal("0.00")) == v1_totals[divergent_bu_id]
    assert sum((row["allocated_amount_eur"] for row in v2_trace), start=Decimal("0.00")) == v2_totals[divergent_bu_id]
    assert v1_totals[divergent_bu_id] != v2_totals[divergent_bu_id]
    assert validate_lineage_round_trip(
        silver_dir=v1_run.silver_dir,
        gold_dir=v1_run.gold_dir,
        lineage_dir=v1_run.gold_dir / LINEAGE_TABLE,
        rule_version_id="v1_transactions",
    ).total_gl_eur == DEFAULT_GL_TOTAL_EUR
    assert validate_lineage_round_trip(
        silver_dir=v2_run.silver_dir,
        gold_dir=v2_run.gold_dir,
        lineage_dir=v2_run.gold_dir / LINEAGE_TABLE,
        rule_version_id="v2_named_users",
    ).total_gl_eur == DEFAULT_GL_TOTAL_EUR


def test_committed_worked_example_matches_fresh_run(lineage) -> None:
    """The committed worked example should match a freshly generated lineage run exactly."""
    run, _ = _build_v1_lineage(lineage)
    lineage_rows = [
        row
        for row in read_delta_table(run.gold_dir / LINEAGE_TABLE).to_pylist()
        if row["rule_version"] == "v1_transactions"
    ]
    rule = RuleRegistry().resolve("v1_transactions")
    fresh_payload = build_worked_example_payload(lineage_rows=lineage_rows, rule=rule)
    committed_payload = json.loads(
        (repo_root() / "examples" / "lineage_worked_example.json").read_text(encoding="utf-8")
    )

    assert fresh_payload == committed_payload
