#!/usr/bin/env python3
"""EPR-138 integration check in a newly created, disposable local PostgreSQL DB.

Run with the backend venv and --admin-url postgresql://127.0.0.1:5432/postgres.
Requires the sibling pipeline checkout/venv. Never reads an application's DB URL.
The golden replay is a scratch fixture only; this does not bypass release gates.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import Column, MetaData, Table, create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

BACKEND = Path(__file__).resolve().parents[1]
PIPELINE = BACKEND.parent / "energyexe-scada-pipeline"
sys.path[:0] = [str(BACKEND), str(PIPELINE)]


def verify(admin_url: str) -> dict:
    if not __debug__:
        raise ValueError("Run verification without Python optimization (-O)")
    url = make_url(admin_url)
    if (
        url.drivername not in {"postgresql", "postgresql+psycopg2"}
        or url.host != "127.0.0.1"
        or url.database != "postgres"
        or url.query
    ):
        raise ValueError(
            "Use an explicit 127.0.0.1 PostgreSQL /postgres admin URL without query options"
        )
    admin = create_engine(url.set(drivername="postgresql+psycopg2"), isolation_level="AUTOCOMMIT")
    database = "epr138_verify_" + uuid.uuid4().hex
    scratch = url.set(drivername="postgresql+psycopg2", database=database)
    engine = None
    created = False
    report = {"database": database, "fixture_only": True}
    try:
        with admin.connect() as db:
            identity = db.execute(
                text(
                    "SELECT current_database(), host(inet_server_addr()), inet_server_port(), version()"
                )
            ).one()
            assert identity[0] == "postgres" and identity[1] == "127.0.0.1"
            report["local_identity"] = list(identity)
            db.execute(text(f'CREATE DATABASE "{database}"'))
            created = True
        engine = create_engine(scratch)
        # Both config names are explicit before importing backend models or invoking Alembic.
        os.environ["DATABASE_URL"] = scratch.set(drivername="postgresql+asyncpg").render_as_string(
            hide_password=False
        )
        os.environ["SCADA_DATABASE_URL"] = scratch.set(
            drivername="postgresql+psycopg"
        ).render_as_string(hide_password=False)

        def migrate(direction, target):
            result = subprocess.run(
                [str(PIPELINE / ".venv/bin/python"), "-m", "alembic", direction, target],
                cwd=PIPELINE,
                env=dict(os.environ),
                text=True,
                capture_output=True,
            )
            if result.returncode:
                raise RuntimeError(result.stderr)

        migrate("upgrade", "a9c3e7f1b2d4")
        with engine.begin() as db:
            assert db.execute(
                text("SELECT current_database(), host(inet_server_addr())")
            ).one() == (database, "127.0.0.1")
            db.execute(
                text(
                    "INSERT INTO scada.dim_farm (farm,name,tz,source_format,pipeline_version,windfarm_id) VALUES ('hill_of_towie','Fictional verification farm','Europe/London','fixture','test',42)"
                )
            )
            db.execute(
                text(
                    "INSERT INTO scada.opportunity_register (farm,id,run_id,scope,scope_kind,cls,gbp_year,additive,rank_gbp,pipeline_version) VALUES ('hill_of_towie',9999,'legacy','FLEET','FLEET','CONTEXT',1.25,false,1.25,'test')"
                )
            )
        migrate("upgrade", "f138a6b7c8d9")
        with engine.connect() as db:
            columns = {
                c["name"]: c
                for c in inspect(db).get_columns("opportunity_register", schema="scada")
            }
            assert all(columns[c]["nullable"] for c in ("price_basis", "offtake_regime"))
            assert db.execute(
                text(
                    "SELECT price_basis,offtake_regime,gbp_year FROM scada.opportunity_register WHERE id=9999"
                )
            ).one() == (None, None, Decimal("1.25"))
        migrate("downgrade", "a9c3e7f1b2d4")
        with engine.connect() as db:
            assert not {"price_basis", "offtake_regime"}.intersection(
                c["name"] for c in inspect(db).get_columns("opportunity_register", schema="scada")
            )
            assert db.execute(
                text("SELECT gbp_year FROM scada.opportunity_register WHERE id=9999")
            ).scalar_one() == Decimal("1.25")
        migrate("upgrade", "f138a6b7c8d9")
        report[
            "migration"
        ] = "full chain to predecessor, upgrade/downgrade/upgrade; legacy value preserved, metadata nullable"

        from app.models import (
            FinancialData,
            FinancialEntity,
            ScadaFindingAction,
            ScadaPpa,
            User,
            Windfarm,
            WindfarmFinancialEntity,
        )

        # Source fixture tables retain actual column names/types; unrelated core FKs and
        # required fields are omitted to avoid constructing the entire platform database.
        source_schema = MetaData()
        for model in (
            FinancialData,
            FinancialEntity,
            ScadaFindingAction,
            ScadaPpa,
            User,
            Windfarm,
            WindfarmFinancialEntity,
        ):
            Table(
                model.__tablename__,
                source_schema,
                *(
                    Column(c.name, c.type, primary_key=c.primary_key, nullable=not c.primary_key)
                    for c in model.__table__.columns
                ),
            )
        source_schema.create_all(engine)
        with engine.begin() as db:
            db.execute(
                text(
                    "CREATE TABLE public.price_data (windfarm_id integer,hour timestamptz,day_ahead_price numeric,source text,currency text,updated_at timestamptz)"
                )
            )
            db.execute(
                text("CREATE TABLE public.epr138_probe (id integer PRIMARY KEY,value integer)")
            )
            db.execute(text("INSERT INTO public.epr138_probe VALUES (1,1)"))
            db.execute(
                source_schema.tables["users"].insert(), {"id": 7, "username": "fixture_owner"}
            )
            db.execute(
                source_schema.tables["windfarms"].insert(),
                {"id": 42, "code": "fixture", "name": "Fictional farm"},
            )
            db.execute(
                source_schema.tables["scada_finding_action"].insert(),
                {
                    "id": 1,
                    "farm": "hill_of_towie",
                    "trigger": "OPS_04",
                    "scope": "FLEET",
                    "cls": "RECOVERABLE",
                    "status": "ACKNOWLEDGED",
                    "updated_by": 7,
                },
            )

        from scada_pipeline.gold import schema
        from scada_pipeline.opportunities import golden, persist

        dump = json.loads((golden.STAGED_DIR / "register_rows.json").read_text())
        assert persist.check_recon(dump) == []
        frames, dimensions = persist.to_frames("hill_of_towie", dump)
        with engine.begin() as db:
            for name, frame in {**dimensions, **frames}.items():
                table = getattr(schema, name)
                db.execute(
                    table.insert(), [{**r, "pipeline_version": "test"} for r in frame.to_dicts()]
                )
        report["scratch_fixture_rows"] = {name: frame.height for name, frame in frames.items()}
        report.update(asyncio.run(verify_reads(engine, scratch, source_schema)))
    finally:
        if engine is not None:
            engine.dispose()
        if created:
            with admin.connect() as db:
                db.execute(text(f'DROP DATABASE "{database}"'))
                assert not db.execute(
                    text("SELECT 1 FROM pg_database WHERE datname=:name"), {"name": database}
                ).scalar()
            report["scratch_database_dropped"] = True
        admin.dispose()
    return report


async def verify_reads(sync_engine, scratch, source_schema):
    from app.schemas.scada_opportunity import (
        ScadaOpportunity,
        ScadaOpportunityByYear,
        ScadaOpportunityListResponse,
    )
    from app.services.scada_offtake_export import ExportRequest, ScadaOfftakeExporter
    from app.services.scada_opportunity_service import ScadaOpportunityService
    from scripts import export_scada_offtake as exporter

    report = {}
    async_engine = create_async_engine(scratch.set(drivername="postgresql+asyncpg"))
    try:
        async with AsyncSession(async_engine) as db:
            service = ScadaOpportunityService(db)
            payload = ScadaOpportunityListResponse.model_validate(
                await service.list_opportunities("hill_of_towie")
            ).model_dump(mode="json")
            legacy = ScadaOpportunity.model_validate(
                await service.get("hill_of_towie", 9999)
            ).model_dump(mode="json")
            assert legacy["price_basis"] is None and legacy["offtake_regime"] is None
            assert payload["summary"]["headline_gbp_year"] == 779922
            assert sum(r["gbp_year"] or 0 for r in payload["items"] if r["additive"]) == 779922
            assert {
                k: payload["summary"]["by_class"][k]["gbp_year"]
                for k in ("REALIZED", "RECOVERABLE", "CURTAILMENT")
            } == {"REALIZED": 329280, "RECOVERABLE": 72019, "CURTAILMENT": 378623}
            priced = [r for r in payload["items"] if r["additive"]]
            assert all(
                r["price_basis"] == "SPOT" and "UNKNOWN" in r["offtake_regime"] for r in priced
            )
            ops04 = next(
                r for r in payload["items"] if r["trigger"] == "OPS_04" and r["scope"] == "FLEET"
            )
            assert (
                ops04["cls"] == "CONTEXT"
                and not ops04["additive"]
                and ops04["lifecycle_status"] is None
            )
            assert (
                await db.execute(
                    text("SELECT status,cls FROM public.scada_finding_action WHERE id=1")
                )
            ).one() == ("ACKNOWLEDGED", "RECOVERABLE")
            by_year = ScadaOpportunityByYear.model_validate(
                await service.by_year("hill_of_towie")
            ).model_dump(mode="json")
            assert by_year["headline_gbp_year"] == 779922 and len(by_year["rows"]) == 110
            report["api_serialization"] = {
                "headline_gbp_year": 779922,
                "legacy_metadata": [None, None],
                "by_year_rows": 110,
                "old_action_preserved_reclassified_new": True,
            }

        request = ExportRequest(
            "missing_fixture_farm", 7, date(2025, 1, 1), date(2025, 12, 31), "EUR"
        )
        url, source = exporter.explicit_source(
            scratch.set(drivername="postgresql").render_as_string(hide_password=False)
        )

        class ProbedExporter(ScadaOfftakeExporter):
            async def build_snapshot(self, request, source):
                # Probe the actual exporter transaction, then run the original implementation.
                flags = (
                    await self.db.execute(
                        text(
                            "SELECT current_database(),host(inet_server_addr()),current_setting('search_path'),current_setting('transaction_read_only'),current_setting('transaction_isolation')"
                        )
                    )
                ).one()
                assert flags == (scratch.database, "127.0.0.1", "public", "on", "repeatable read")
                assert (
                    await self.db.execute(text("SELECT value FROM public.epr138_probe WHERE id=1"))
                ).scalar_one() == 1
                with sync_engine.begin() as writer:
                    writer.execute(text("UPDATE public.epr138_probe SET value=2 WHERE id=1"))
                assert (
                    await self.db.execute(text("SELECT value FROM public.epr138_probe WHERE id=1"))
                ).scalar_one() == 1
                try:
                    async with self.db.begin_nested():
                        await self.db.execute(
                            text("UPDATE public.epr138_probe SET value=3 WHERE id=1")
                        )
                except DBAPIError as error:
                    assert error.orig.sqlstate == "25006"
                else:
                    raise AssertionError("Exporter transaction accepted a write")
                report["source_transaction"] = {
                    "search_path": flags[2],
                    "read_only": flags[3],
                    "isolation": flags[4],
                    "write_rejected_sqlstate": "25006",
                    "concurrent_change_hidden": True,
                }
                return await super().build_snapshot(request, source)

        with patch.object(exporter, "ScadaOfftakeExporter", ProbedExporter):
            missing = await exporter.export_snapshot(request, url, source)
        assert missing["readiness"]["blocked_reasons"] == ["farm_mapping_missing"]
        assert missing["source"]["transaction"]["read_only"] == "on"

        # 201 records crosses the real service's 200-row page boundary. EPR-143: the register is
        # shared per farm, so the other creator (id 202) and the NULL creator (id 205) are INCLUDED
        # whoever exports; lowercase status, Draft and the other farm are still excluded.
        now = datetime(2025, 1, 1)
        terms = [
            {
                "id": i,
                "windfarm_id": 42,
                "created_by_id": 7,
                "ppa_code": f"fixture-{i:04d}",
                "ppa_buyer": "Fictional fixture buyer",
                "ppa_status": "Active",
                "currency": "EUR",
                "power_share_pct": Decimal("50.00"),
                "strike_price": Decimal("123.45"),
                "effective_date": date(2025, 1, 1),
                "expiration_date": date(2025, 12, 31),
                "created_at": now,
                "updated_at": now,
            }
            for i in range(1, 202)
        ]
        terms += [
            {**terms[0], "id": 202, "created_by_id": 8},
            {**terms[0], "id": 203, "ppa_status": "active"},
            {**terms[0], "id": 204, "ppa_status": "Draft"},
            {**terms[0], "id": 205, "created_by_id": None},
            {**terms[0], "id": 206, "windfarm_id": 43},
        ]
        with sync_engine.begin() as db:
            db.execute(source_schema.tables["scada_ppa"].insert(), terms)
        counts = []
        for exporting_user, count in ((7, 203), (8, 203), (9, 203)):
            req = ExportRequest(
                "hill_of_towie", exporting_user, request.period_start, request.period_end, "EUR"
            )
            snapshot = await exporter.export_snapshot(req, url, source)
            assert snapshot["readiness"]["status"] == "prepared", snapshot["readiness"]
            assert snapshot["scope"] == "farm" and snapshot["owner_user_id"] == exporting_user
            assert len(snapshot["original_terms"]) == count
            assert {t["created_by_id"] for t in snapshot["original_terms"]} == {7, 8, None}
            assert all(
                t["strike_price"] == "123.45" and t["power_share_pct"] == "50.00"
                for t in snapshot["original_terms"]
            )
            assert snapshot["readiness"]["ppa_monetary_activation"] is False
            assert all(Decimal(row["rate"]) == 1 for row in snapshot["fx_schedule"])
            counts.append(count)
        report["exporter"] = {
            "farm_counts_by_exporting_user": counts,
            "missing_mapping_blocked": True,
            "native_decimals_preserved": True,
            "activation": False,
        }
        with sync_engine.begin() as db:
            db.execute(text("DROP TABLE public.price_data"))
        failed_read = await exporter.export_snapshot(req, url, source)
        failed_connection = await exporter.export_snapshot(
            req, url.set(database=scratch.database + "_absent"), source
        )
        for failed in (failed_read, failed_connection):
            assert failed["readiness"]["status"] == "blocked"
            assert failed["readiness"]["blocked_reasons"] == ["source_access_or_read_failure"]
        report["exporter"]["read_and_connection_failures_blocked"] = True
    finally:
        await async_engine.dispose()
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin-url", required=True)
    print(json.dumps(verify(parser.parse_args().admin_url), indent=2))
