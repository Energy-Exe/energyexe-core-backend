"""Pure helper behind the watch-list rows of GET /scada/opportunities (D-088): each per-turbine register row gets the
code or signal it is about and, for Raggovidda's alarm codes, the code's text from the asset's legend. No DB — the
SQLite test suite cannot run scada.* SQL; the labels are the condition records reduced to their keys."""

from app.services.scada_opportunity_service import attach_condition_labels


def _reg(trigger, scope, cls, item):
    return {"trigger": trigger, "scope": scope, "cls": cls, "item": item}


def _lab(trigger, scope, cls, item, component, text=None, legend=None):
    return {
        "action_trigger": trigger,
        "action_scope": scope,
        "action_cls": cls,
        "register_item": item,
        "component": component,
        "component_text": text,
        "component_legend": legend,
    }


def test_rows_sharing_a_register_key_are_told_apart_by_their_item():
    a = "diag33_alarm_recurrence:103026 [RECURRING_VS_FLEET]"
    b = "diag33_alarm_recurrence:180019 [RECURRING_VS_FLEET]"
    items = [_reg("DIAG_33", "T01", "ALERT", a), _reg("DIAG_33", "T01", "ALERT", b)]
    labels = [
        _lab("DIAG_33", "T01", "ALERT", b, "180019", None, "alarm_code"),
        _lab(
            "DIAG_33",
            "T01",
            "ALERT",
            a,
            "103026",
            "Pitch A tracking speed/direction error during stop",
            "alarm_code",
        ),
    ]
    out = attach_condition_labels(items, labels)
    assert out[0]["component"] == "103026"
    assert out[0]["component_text"] == "Pitch A tracking speed/direction error during stop"
    assert out[0]["component_legend"] == "alarm_code"
    # legend set, no text: the legend does not describe the code
    assert (out[1]["component"], out[1]["component_text"], out[1]["component_legend"]) == (
        "180019",
        None,
        "alarm_code",
    )


def test_fleet_lines_and_rows_without_a_record_get_nulls():
    items = [
        {"trigger": "MKT_03", "scope": "FLEET", "cls": "CURTAILMENT", "item": "Curtailment"},
        _reg("DET02", "T13", "ALERT", "det02_err_com [HIGH_ACTIVE]"),
        _reg(None, "T02", "CONTEXT", "measured generation deficit"),
    ]
    labels = [_lab("MKT_03", "FLEET", "CURTAILMENT", "Curtailment", "x")]
    out = attach_condition_labels(items, labels)
    for row in out:
        assert (
            row["component"] is None
            and row["component_text"] is None
            and row["component_legend"] is None
        )


def test_a_component_without_a_legend_carries_no_text():
    item = "diag_thermal_by_component:gbx_brg_hs_gen [DRIFTING_UP] -> gearbox"
    out = attach_condition_labels(
        [_reg("DIAG_01", "T03", "CONDITIONAL", item)],
        [_lab("DIAG_01", "T03", "CONDITIONAL", item, "gbx_brg_hs_gen")],
    )
    assert (
        out[0]["component"] == "gbx_brg_hs_gen"
        and out[0]["component_text"] is None
        and out[0]["component_legend"] is None
    )


def test_a_null_trigger_matches_the_empty_action_trigger():
    item = "measured generation deficit -11.33% since 2020-07-01 (step)"
    out = attach_condition_labels(
        [_reg(None, "T03", "CONTEXT", item)], [_lab("", "T03", "CONTEXT", item, None)]
    )
    assert out[0]["component"] is None
