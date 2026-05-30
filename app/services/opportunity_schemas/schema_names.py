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

# One-line analyst-facing meaning for every SchemaCode. Surfaced to the brain
# agent alongside the human name so it can describe a finding without reading
# the detector source. Kept in lock-step with ``SCHEMA_NAMES`` (same key set).
SCHEMA_ONE_LINERS: dict[SchemaCode, str] = {
    # Operational
    SchemaCode.OPS_01: "Recurring low-availability months (ODI proxy) — concentrated or structural disruption.",
    SchemaCode.OPS_02: "High-wind season underperforms low-wind season (HODI/SSR) — mechanical stress or maintenance timing.",
    SchemaCode.OPS_03: "OEM/AM contract doesn't incentivise uptime. Inherits OPS-01 severity; only fires when OPS-01 exists.",
    SchemaCode.OPS_04: "Statistically significant power-curve degradation slope (capped at INDICATIVE on placeholder baseline).",
    SchemaCode.OPS_05: "Curtailed energy as a share of available output (UK/ELEXON only — curtailment data unavailable elsewhere).",
    SchemaCode.OPS_06: "Consecutive months with wind-normalised index below threshold — persistent power-curve underperformance.",
    SchemaCode.OPS_07: "Turbines in their final operating years or past design life — end-of-life capex/repowering risk.",
    SchemaCode.OPS_08: "Confirmed structural export/grid constraint suppressing output for a sustained window.",
    # Market
    SchemaCode.MKT_01: "Capture-rate gap vs the bidzone average (percentage points) — contracting/hedging exposure.",
    SchemaCode.MKT_02: "Storage (BESS) shifting opportunity downstream of a low capture rate. Only fires if MKT-01 exists.",
    SchemaCode.MKT_03: "High cannibalisation index (1 / capture_rate) — prices depressed when the asset generates.",
    SchemaCode.MKT_04: "PPA approaching expiry (months-to-expiry tiers) — re-contracting / merchant-exposure risk.",
    SchemaCode.MKT_05: "PPA priced below market. INACTIVE — no PPA price data ingested yet (emits no findings).",
    SchemaCode.MKT_06: "Hours of negative wholesale price while generating — direct merchant downside exposure.",
    SchemaCode.MKT_07: "Forecast vs actual deviation. INACTIVE — no forecast data ingested yet (emits no findings).",
    # Financial
    SchemaCode.FIN_01: "Actual generation below the P50 target for consecutive years — yield shortfall vs the bankable case.",
    SchemaCode.FIN_02: "Onshore OPEX per MWh above the onshore zone median — cost overrun vs peers.",
    SchemaCode.FIN_03: "Offshore OPEX per MWh above the offshore zone median — cost overrun vs peers.",
    # Data Quality
    SchemaCode.DQ_01: "Generation-data gap detected in the analysis window. Gate: suppresses generation-dependent schemas.",
}

# Domain grouping for catalogue rendering / agent narration.
SCHEMA_DOMAINS: list[tuple[str, list[SchemaCode]]] = [
    (
        "Operational (OPS)",
        [
            SchemaCode.OPS_01,
            SchemaCode.OPS_02,
            SchemaCode.OPS_03,
            SchemaCode.OPS_04,
            SchemaCode.OPS_05,
            SchemaCode.OPS_06,
            SchemaCode.OPS_07,
            SchemaCode.OPS_08,
        ],
    ),
    (
        "Market (MKT)",
        [
            SchemaCode.MKT_01,
            SchemaCode.MKT_02,
            SchemaCode.MKT_03,
            SchemaCode.MKT_04,
            SchemaCode.MKT_05,
            SchemaCode.MKT_06,
            SchemaCode.MKT_07,
        ],
    ),
    (
        "Financial (FIN)",
        [SchemaCode.FIN_01, SchemaCode.FIN_02, SchemaCode.FIN_03],
    ),
    (
        "Data Quality (DQ)",
        [SchemaCode.DQ_01],
    ),
]


def get_schema_name(schema_code: str) -> str | None:
    """Resolve a raw ``schema_code`` string to its human name.

    Returns ``None`` for an unknown/legacy code so callers can fall back
    gracefully (e.g. surface the raw code) rather than crashing.
    """
    try:
        return SCHEMA_NAMES[SchemaCode(schema_code)]
    except (ValueError, KeyError):
        return None


def format_schema_catalogue() -> str:
    """Render the full schema catalogue as a markdown block from SCHEMA_NAMES.

    Single source of truth for the brain-agent skill/prompt copy: lists every
    schema (currently 19) grouped by domain, as ``- **CODE — Name** — meaning``.
    Generating this from ``SCHEMA_NAMES`` keeps the agent's view in lock-step
    with the registry rather than a hand-maintained divergent copy.
    """
    lines: list[str] = []
    for domain_label, codes in SCHEMA_DOMAINS:
        lines.append(f"### {domain_label}")
        for code in codes:
            name = SCHEMA_NAMES[code]
            meaning = SCHEMA_ONE_LINERS[code]
            lines.append(f"- **{code.value} — {name}** — {meaning}")
        lines.append("")
    return "\n".join(lines).rstrip()
