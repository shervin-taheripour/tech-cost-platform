"""Tests for gold report views — P-009 acceptance criteria."""

from __future__ import annotations

from decimal import Decimal

from deltalake import DeltaTable

from tech_cost_platform.delta_tables import read_delta_table
from tech_cost_platform.gold import (
    REPORT_APP_TCO_TABLE,
    REPORT_BU_SHOWBACK_TABLE,
    REPORT_DRIVER_COMPARISON_BY_APP_TABLE,
    REPORT_DRIVER_COMPARISON_TABLE,
    REPORT_LINEAGE_TABLE,
    REPORT_RESIDUAL_TABLE,
)
from tech_cost_platform.synth.generate import DEFAULT_GL_TOTAL_EUR

_ALL_FIVE = [
    REPORT_APP_TCO_TABLE,
    REPORT_BU_SHOWBACK_TABLE,
    REPORT_RESIDUAL_TABLE,
    REPORT_LINEAGE_TABLE,
    REPORT_DRIVER_COMPARISON_TABLE,
]
_V1 = "v1_transactions"


def test_all_five_views_materialize_as_valid_delta(gold_reports) -> None:
    """All five report views must be readable Delta tables with at least one row."""
    run = gold_reports()
    result = run.build()

    for table_name in _ALL_FIVE:
        path = result.output_paths[table_name]
        assert path.exists(), f"{table_name} path missing"
        assert (path / "_delta_log").exists(), f"{table_name} has no _delta_log"
        assert DeltaTable(str(path)).to_pyarrow_table().num_rows > 0, f"{table_name} is empty"

    assert REPORT_DRIVER_COMPARISON_BY_APP_TABLE in result.output_paths
    by_app_path = result.output_paths[REPORT_DRIVER_COMPARISON_BY_APP_TABLE]
    assert (by_app_path / "_delta_log").exists()
    assert DeltaTable(str(by_app_path)).to_pyarrow_table().num_rows > 0


def test_totals_tie_back_to_allocation_exactly(gold_reports) -> None:
    """App TCO sum and BU showback sum must both equal the gold allocation total exactly."""
    run = gold_reports()
    result = run.build()

    allocation_rows = read_delta_table(run.gold_dir / "allocation").to_pylist()
    app_tco_rows = read_delta_table(result.output_paths[REPORT_APP_TCO_TABLE]).to_pylist()
    bu_showback_rows = read_delta_table(result.output_paths[REPORT_BU_SHOWBACK_TABLE]).to_pylist()

    alloc_total = sum(
        (r["allocated_amount_eur"] for r in allocation_rows if r["rule_version"] == _V1),
        start=Decimal("0.00"),
    )
    app_total = sum(
        (r["allocated_amount_eur"] for r in app_tco_rows if r["rule_version"] == _V1),
        start=Decimal("0.00"),
    )
    bu_total = sum(
        (r["allocated_amount_eur"] for r in bu_showback_rows if r["rule_version"] == _V1),
        start=Decimal("0.00"),
    )

    assert app_total == alloc_total, f"App TCO total {app_total} != allocation {alloc_total}"
    assert bu_total == alloc_total, f"BU showback total {bu_total} != allocation {alloc_total}"


def test_full_reconciliation_to_gl_total(gold_reports) -> None:
    """BU showback allocated + residual report must equal 61813.95 exactly."""
    run = gold_reports()
    result = run.build()

    bu_showback_rows = read_delta_table(result.output_paths[REPORT_BU_SHOWBACK_TABLE]).to_pylist()
    residual_rows = read_delta_table(result.output_paths[REPORT_RESIDUAL_TABLE]).to_pylist()

    allocated = sum(
        (r["allocated_amount_eur"] for r in bu_showback_rows if r["rule_version"] == _V1),
        start=Decimal("0.00"),
    )
    residual = sum(
        (r["residual_amount_eur"] for r in residual_rows if r["rule_version"] == _V1),
        start=Decimal("0.00"),
    )

    assert allocated + residual == DEFAULT_GL_TOTAL_EUR, (
        f"allocated={allocated} residual={residual} sum={allocated + residual} "
        f"expected={DEFAULT_GL_TOTAL_EUR}"
    )
    assert allocated < DEFAULT_GL_TOTAL_EUR, "No residual implies 100% allocation — unexpected"


def test_driver_comparison_diverges_with_app_billing_flip(gold_reports) -> None:
    """Driver comparison must show divergent BU splits and a ≥20pp share delta."""
    run = gold_reports()
    result = run.build()

    comparison = read_delta_table(result.output_paths[REPORT_DRIVER_COMPARISON_TABLE]).to_pylist()

    assert any(
        r["delta_eur"] != Decimal("0.00") for r in comparison
    ), "Expected non-zero delta_eur for at least one BU"

    max_abs_share_delta = max(abs(r["share_delta_pp"]) for r in comparison)
    assert max_abs_share_delta >= Decimal("0.200000"), (
        f"Expected ≥20pp share_delta_pp for the APP-BILLING flip; got max={max_abs_share_delta}"
    )


def test_both_rule_versions_reconcile_to_gl_total(gold_reports) -> None:
    """Both v1_transactions and v2_named_users must reconcile to 61813.95 exactly."""
    run = gold_reports()
    result = run.build()

    assert result.gl_total_eur == DEFAULT_GL_TOTAL_EUR
    assert result.v1_allocated_eur + result.v1_residual_eur == DEFAULT_GL_TOTAL_EUR, (
        f"v1: allocated={result.v1_allocated_eur} residual={result.v1_residual_eur}"
    )
    assert result.v2_allocated_eur + result.v2_residual_eur == DEFAULT_GL_TOTAL_EUR, (
        f"v2: allocated={result.v2_allocated_eur} residual={result.v2_residual_eur}"
    )


def test_rule_version_isolation(gold_reports) -> None:
    """Views with rule_version must partition cleanly — no row mixes versions."""
    run = gold_reports()
    result = run.build()

    for table_name in [
        REPORT_APP_TCO_TABLE,
        REPORT_BU_SHOWBACK_TABLE,
        REPORT_RESIDUAL_TABLE,
        REPORT_LINEAGE_TABLE,
    ]:
        rows = read_delta_table(result.output_paths[table_name]).to_pylist()
        assert all(r["rule_version"] is not None for r in rows), f"{table_name} has null rule_version"
        for rv in {r["rule_version"] for r in rows}:
            assert all(
                r["rule_version"] == rv for r in rows if r["rule_version"] == rv
            ), f"{table_name} mixes versions in {rv} partition"
        found_versions = {r["rule_version"] for r in rows}
        assert found_versions == {_V1}, (
            f"{table_name} contains unexpected rule versions: {found_versions}"
        )


def test_residual_and_lineage_views_are_exact_passthroughs(gold_reports) -> None:
    """Report residual and lineage totals must equal their source tables exactly."""
    run = gold_reports()
    result = run.build()

    source_residual = sum(
        (r["residual_amount_eur"] for r in read_delta_table(run.gold_dir / "residual_report").to_pylist()),
        start=Decimal("0.00"),
    )
    report_residual = sum(
        (r["residual_amount_eur"] for r in read_delta_table(result.output_paths[REPORT_RESIDUAL_TABLE]).to_pylist()),
        start=Decimal("0.00"),
    )
    assert source_residual == report_residual, (
        f"Residual passthrough drifted: source={source_residual} report={report_residual}"
    )

    source_lineage_count = read_delta_table(run.gold_dir / "lineage").num_rows
    report_lineage_count = read_delta_table(result.output_paths[REPORT_LINEAGE_TABLE]).num_rows
    assert source_lineage_count == report_lineage_count, (
        f"Lineage passthrough row count drifted: source={source_lineage_count} report={report_lineage_count}"
    )


def test_determinism(gold_reports) -> None:
    """Two consecutive make-reports runs must produce identical view contents."""
    run = gold_reports()
    run.build()

    result1 = sorted(
        read_delta_table(run.gold_dir / REPORT_BU_SHOWBACK_TABLE).to_pylist(),
        key=lambda r: (r["rule_version"], r["bu_id"]),
    )
    run.build_reports()
    result2 = sorted(
        read_delta_table(run.gold_dir / REPORT_BU_SHOWBACK_TABLE).to_pylist(),
        key=lambda r: (r["rule_version"], r["bu_id"]),
    )

    assert result1 == result2, "Second reports build produced different BU showback output"
