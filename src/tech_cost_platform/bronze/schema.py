"""Explicit Arrow schemas for bronze CSV ingestion."""

from __future__ import annotations

import pyarrow as pa

from ..delta_tables import MONEY_TYPE

STRING_TYPE = pa.string()

GL_COST_SCHEMA = pa.schema(
    [
        ("gl_line_id", STRING_TYPE),
        ("period", STRING_TYPE),
        ("gl_account", STRING_TYPE),
        ("cost_center_id", STRING_TYPE),
        ("amount_eur", MONEY_TYPE),
        ("description", STRING_TYPE),
    ]
)

COST_CENTER_SCHEMA = pa.schema(
    [
        ("cost_center_id", STRING_TYPE),
        ("cost_center_name", STRING_TYPE),
        ("tower_id", STRING_TYPE),
    ]
)

RESOURCE_TOWER_SCHEMA = pa.schema(
    [
        ("tower_id", STRING_TYPE),
        ("tower_name", STRING_TYPE),
        ("tower_type", STRING_TYPE),
    ]
)

APPLICATION_SCHEMA = pa.schema(
    [
        ("app_id", STRING_TYPE),
        ("app_name", STRING_TYPE),
        ("business_criticality", STRING_TYPE),
    ]
)

BUSINESS_UNIT_SCHEMA = pa.schema(
    [
        ("bu_id", STRING_TYPE),
        ("bu_name", STRING_TYPE),
    ]
)

USAGE_METRIC_SCHEMA = pa.schema(
    [
        ("metric_id", STRING_TYPE),
        ("period", STRING_TYPE),
        ("step", STRING_TYPE),
        ("from_id", STRING_TYPE),
        ("to_id", STRING_TYPE),
        ("metric_name", STRING_TYPE),
        ("value", MONEY_TYPE),
    ]
)
