"""Preview and reconcile Vinddatasæt (+ Parkproduktion) readings one source month at a time."""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import and_, or_, select, text

from app.models.generation_data import GenerationDataRaw
from app.models.turbine_model import TurbineModel
from app.models.turbine_unit import TurbineUnit
from app.models.windfarm import Windfarm
from app.services.energistyrelsen_parks import parse_parkproduktion, split_park_values
from app.services.energistyrelsen_workbook import parse_vinddata, turbine_attributes

TOLERANCE_MWH = Decimal("0.0015")
REVISION_EXAMPLES_PER_MONTH = 50


def month_bounds(month):
    year, number = map(int, month.split("-"))
    start = datetime(year, number, 1, tzinfo=timezone.utc)
    end = datetime(year + (number == 12), number % 12 + 1, 1, tzinfo=timezone.utc)
    return start, end


def infer_park_map(attributes, turbines, overrides=None):
    """Park GUID → windfarm id, from turbines that already exist, plus overrides."""
    parks = defaultdict(set)
    for gsrn, turbine in turbines.items():
        park = attributes.get(gsrn, {}).get("park")
        if park:
            parks[park].add(turbine.windfarm_id)
    mapping = {park: next(iter(ids)) for park, ids in parks.items() if len(ids) == 1}
    ambiguous = {park: sorted(ids) for park, ids in parks.items() if len(ids) > 1}
    mapping.update({str(park).strip().upper(): int(windfarm_id)
                    for park, windfarm_id in (overrides or {}).items()})
    return mapping, ambiguous


def plan_turbine_changes(attributes, turbines, park_map, model_ids, windfarms,
                         start_month, update_decommissions=True, adopt_existing=False):
    """Pure planning of turbine_units changes implied by the stamdata.

    New turbines are only created for parks that map to a tracked windfarm.
    ``model_ids`` maps lower-cased model names (DEA spelling or Perform
    spelling) to turbine_models.id; ``windfarms`` maps id → Windfarm.

    A windfarm that already has placeholder units (codes that are not GSRNs
    in the workbook) is never given additional units: with
    ``adopt_existing`` the placeholders receive the GSRNs, earliest
    connection first, otherwise the GSRNs are reported as unresolved.
    """
    create, decommission, unresolved, adopt = [], [], [], []
    placeholders = defaultdict(list)
    mapped_windfarms = set(park_map.values())
    for code, turbine in turbines.items():
        if turbine.windfarm_id in mapped_windfarms and code not in attributes:
            placeholders[turbine.windfarm_id].append(turbine)
    for units in placeholders.values():
        units.sort(key=lambda unit: str(unit.code))
    candidates = []
    for gsrn, attrs in sorted(attributes.items()):
        if gsrn in turbines:
            if update_decommissions and attrs.get("decommissioned"):
                turbine = turbines[gsrn]
                decommission.append({
                    "gsrn": gsrn, "turbine_unit_id": turbine.id, "windfarm_id": turbine.windfarm_id,
                    "end_date": attrs["decommissioned"].isoformat(),
                    "already_applied": turbine.end_date == attrs["decommissioned"]
                    and turbine.status == "decommissioned",
                })
            continue
        park = attrs.get("park")
        if not park or park not in park_map:
            continue
        decommissioned = attrs.get("decommissioned")
        if decommissioned and decommissioned.isoformat()[:7] < start_month:
            continue  # historical turbine, no data in range
        windfarm = windfarms.get(park_map[park])
        model_name = (attrs.get("model") or "").strip().lower()
        model_id = model_ids.get(model_name)
        problems = []
        if windfarm is None:
            problems.append(f"windfarm_missing:{park_map[park]}")
        # Adopted placeholders keep their own model, so only a new unit needs one.
        if not model_id and not (windfarm is not None and placeholders.get(windfarm.id)):
            problems.append(f"model_unknown:{attrs.get('model')}")
        if not attrs.get("connected"):
            problems.append("no_connection_date")
        if problems:
            unresolved.append({"gsrn": gsrn, "park": park, "reasons": problems})
            continue
        candidates.append((attrs["connected"], gsrn, {
            "code": gsrn, "windfarm_id": windfarm.id, "turbine_model_id": model_id,
            "lat": windfarm.lat or 0.0, "lng": windfarm.lng or 0.0, "status": "operational",
            "hub_height_m": attrs.get("hub_height_m"), "start_date": attrs["connected"],
            "end_date": decommissioned,
        }))
    for connected, gsrn, row in sorted(candidates, key=lambda item: (item[0], item[1])):
        units = placeholders.get(row["windfarm_id"])
        if not units:
            if row["turbine_model_id"] is None:
                unresolved.append({"gsrn": gsrn, "park": attributes[gsrn]["park"],
                                   "reasons": [f"model_unknown:{attributes[gsrn].get('model')}"]})
            else:
                create.append(row)
        elif adopt_existing:
            unit = units.pop(0)
            adopt.append({"turbine_unit_id": unit.id, "old_code": str(unit.code), "code": gsrn,
                          "windfarm_id": row["windfarm_id"], "start_date": row["start_date"],
                          "end_date": row["end_date"], "hub_height_m": row["hub_height_m"]})
        else:
            unresolved.append({"gsrn": gsrn, "park": attributes[gsrn]["park"],
                               "reasons": [f"windfarm_has_placeholder_units:{len(units)}"]})
    return {"create": create, "decommission": decommission, "unresolved": unresolved, "adopt": adopt}


async def _load_turbines(db):
    return {str(t.code): t for t in (await db.execute(select(TurbineUnit))).scalars()}


async def reconcile_vinddata(db, content, start_month, end_month, apply=False, progress=None, *,
                             park_content=None, park_map=None, model_map=None,
                             add_turbines=False, update_decommissions=False,
                             adopt_existing=False, tolerance=TOLERANCE_MWH):
    values, metadata, blanks = parse_vinddata(content, start_month, end_month)
    attributes = {gsrn: turbine_attributes(meta) for gsrn, meta in metadata.items()}
    park_values = parse_parkproduktion(park_content, start_month, end_month) if park_content else {}
    turbines = await _load_turbines(db)
    mapping, ambiguous = infer_park_map(attributes, turbines, park_map)

    report = {"months": {}, "parks": {"ambiguous": ambiguous}, "turbines": {}}

    # Turbine catalogue changes first, in their own transaction, so the new
    # GSRNs take part in the month reconciliation below.
    model_ids = {}
    if add_turbines or update_decommissions:
        for model in (await db.execute(select(TurbineModel))).scalars():
            model_ids[model.model.strip().lower()] = model.id
        for name, model_id in (model_map or {}).items():
            model_ids[str(name).strip().lower()] = int(model_id)
        windfarm_ids = set(mapping.values())
        windfarms = {w.id: w for w in (await db.execute(
            select(Windfarm).where(Windfarm.id.in_(windfarm_ids)))).scalars()} if windfarm_ids else {}
        changes = plan_turbine_changes(attributes, turbines, mapping, model_ids, windfarms,
                                       start_month, update_decommissions, adopt_existing)
        if not add_turbines:
            changes["create"], changes["adopt"] = [], []

        def _json(row):
            return dict(row, hub_height_m=str(row["hub_height_m"]) if row["hub_height_m"] is not None else None,
                        start_date=row["start_date"].isoformat(),
                        end_date=row["end_date"].isoformat() if row["end_date"] else None)
        report["turbines"] = {
            "create": [_json(row) for row in changes["create"]],
            "adopt": [_json(row) for row in changes["adopt"]],
            "decommission": changes["decommission"],
            "unresolved": changes["unresolved"],
            "applied": False,
        }
        if apply:
            for row in changes["create"]:
                db.add(TurbineUnit(**row))
            for row in changes["adopt"]:
                unit = next(t for t in turbines.values() if t.id == row["turbine_unit_id"])
                unit.code = row["code"]
                unit.start_date = row["start_date"]
                unit.end_date = row["end_date"]
                if row["hub_height_m"] is not None:
                    unit.hub_height_m = row["hub_height_m"]
            for item in changes["decommission"]:
                if item["already_applied"]:
                    continue
                turbine = turbines[item["gsrn"]]
                turbine.end_date = datetime.fromisoformat(item["end_date"]).date()
                turbine.status = "decommissioned"
            await db.commit()
            report["turbines"]["applied"] = True
            turbines = await _load_turbines(db)
            mapping, ambiguous = infer_park_map(attributes, turbines, park_map)
            report["parks"]["ambiguous"] = ambiguous

    provenance, park_report = split_park_values(park_values, attributes, set(turbines), values)
    park_report["mapped"] = {park: dict(info, windfarm_id=mapping.get(park))
                             for park, info in park_report["mapped"].items()}
    report["parks"].update(park_report)

    by_month = defaultdict(dict)
    unmatched = {}
    unmatched_by_month = defaultdict(int)
    for (gsrn, month), mwh in values.items():
        if gsrn in turbines:
            by_month[month][gsrn] = mwh
        else:
            park = attributes.get(gsrn, {}).get("park")
            unmatched[gsrn] = {"source_metadata": metadata.get(gsrn, {}),
                               "candidate_windfarm_ids": [mapping[park]] if park in mapping else []}
            unmatched_by_month[month] += 1
    report.update({"unmatched_gsrns": unmatched, "unmatched_by_month": dict(unmatched_by_month),
                   "missing_values": blanks})

    from scripts.seeds.aggregate_generation_data.process_generation_data_monthly import MonthlyGenerationProcessor

    processor = MonthlyGenerationProcessor(db)
    if apply:
        await processor.load_turbine_units()
    months = sorted(by_month)
    for index, month in enumerate(months):
        start, end = month_bounds(month)
        if apply:
            await db.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
                             {"key": f"ENERGISTYRELSEN:{month}"})
        recorded_month = GenerationDataRaw.data["month"].astext
        existing = (await db.execute(select(GenerationDataRaw).where(
            GenerationDataRaw.source == "ENERGISTYRELSEN",
            GenerationDataRaw.period_type == "month",
            or_(recorded_month == month,
                and_(recorded_month.is_(None),
                     or_(and_(GenerationDataRaw.period_start >= start,
                              GenerationDataRaw.period_start < end),
                         GenerationDataRaw.period_start == start - timedelta(hours=6)))),
        ))).scalars().all()
        old = defaultdict(list)
        for row in existing:
            # The legacy import used a six-hour offset. Its recorded month,
            # rather than exact timestamp equality, identifies the same reading.
            old[str(row.identifier)].append(row)
            if apply:
                row.period_start = start
                row.period_end = end
        counts = {"added": 0, "revised": 0, "unchanged": 0,
                  "preserved_absent": len(set(old) - set(by_month[month])),
                  "duplicate_legacy_rows": 0, "from_park": 0}
        revisions = []
        try:
            for gsrn, value in by_month[month].items():
                rows = old.get(gsrn, [])
                counts["duplicate_legacy_rows"] += max(len(rows) - 1, 0)
                if (gsrn, month) in provenance:
                    counts["from_park"] += 1
                unchanged = len(rows) == 1 and abs(rows[0].value_extracted - value) <= tolerance
                if not rows:
                    counts["added"] += 1
                elif unchanged:
                    counts["unchanged"] += 1
                else:
                    counts["revised"] += 1
                    if len(revisions) < REVISION_EXAMPLES_PER_MONTH:
                        revisions.append({"gsrn": gsrn, "windfarm_id": turbines[gsrn].windfarm_id,
                                          "old": sorted(str(row.value_extracted) for row in rows),
                                          "new": str(value)})
                if not apply:
                    continue
                if unchanged:
                    continue
                for row in rows:
                    await db.delete(row)
                db.add(GenerationDataRaw(
                    source="ENERGISTYRELSEN", source_type="file",
                    period_start=start, period_end=end, period_type="month",
                    identifier=gsrn, value_extracted=value, unit="MWh",
                    data={"gsrn": gsrn, "month": month,
                          "generation_kwh": str(value * Decimal(1000)),
                          "turbine_unit_id": turbines[gsrn].id,
                          **provenance.get((gsrn, month), {})},
                ))
            counts["revisions"] = revisions
            if apply:
                await db.flush()
                result = await processor.process_source_for_month("ENERGISTYRELSEN", start, end)
                if result.get("error"):
                    raise ValueError(result["error"])
                counts["processed"] = result["saved"]
                await db.commit()
            report["months"][month] = counts
            if progress:
                await progress(month, counts, index, len(months))
        except Exception:
            await db.rollback()
            raise
    return report
