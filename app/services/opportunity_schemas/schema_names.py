"""Single source of truth for human-facing opportunity schema names.

The detection engine identifies findings by ``SchemaCode`` (e.g. ``OPS_01``)
internally, but every user-facing surface (API responses, the brain agent,
admin-ui) must present the human-readable name instead of the code. This module
is the canonical mapping; downstream code (and the spec's "respond with names,
not codes" requirement) reads from ``SCHEMA_NAMES`` rather than hardcoding
strings.

Covers all 19 ``SchemaCode`` members (OPS_01..08, MKT_01..07, FIN_01..03,
DQ_01). The "18" used in the spec/initiative branding is an arithmetic
shorthand — the true member count is 19.
"""

from app.models.opportunity import SchemaCode

# Human-readable name for every SchemaCode. Names are concise and analyst-facing.
SCHEMA_NAMES: dict[SchemaCode, str] = {
    # Operational
    SchemaCode.OPS_01: "Volatile Disruption Periods",
    SchemaCode.OPS_02: "Performance Seasonality",
    SchemaCode.OPS_03: "Misaligned Contracting Strategy",
    SchemaCode.OPS_04: "Turbine Degradation",
    SchemaCode.OPS_05: "Grid Curtailment",
    SchemaCode.OPS_06: "Persistent Power-Curve Underperformance",
    SchemaCode.OPS_07: "Fleet-Age / End-of-Life Risk",
    SchemaCode.OPS_08: "Structural Export Constraint",
    # Market
    SchemaCode.MKT_01: "Low Capture Rate — Contracting",
    SchemaCode.MKT_02: "Low Capture Rate — Storage",
    SchemaCode.MKT_03: "High Cannibalisation",
    SchemaCode.MKT_04: "PPA Expiry Risk",
    SchemaCode.MKT_05: "PPA Underpricing",
    SchemaCode.MKT_06: "Negative-Price Hours Exposure",
    SchemaCode.MKT_07: "Forecast Deviation",
    # Financial
    SchemaCode.FIN_01: "P50 Generation Attainment",
    SchemaCode.FIN_02: "Onshore OPEX Overrun",
    SchemaCode.FIN_03: "Offshore OPEX Overrun",
    # Data Quality
    SchemaCode.DQ_01: "Generation Data Gaps",
}
