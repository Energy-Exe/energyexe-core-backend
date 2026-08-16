"""Report generation platform (EPR-81/82).

Shared two-pass pipeline all report types run on:

- ``registry``      — declarative report-type specs (sections, builders, prompts)
- ``context``       — per-run inputs handed to every data builder
- ``service``       — CRUD, scoping, retain-on-export versioning
- ``orchestrator``  — async two-pass runner (fire-and-forget task + DB status)
- ``pdf``           — reportlab renderer producing the S3-stored artifact
"""

from app.services.reports.registry import REPORT_TYPE_REGISTRY, get_report_type

__all__ = ["REPORT_TYPE_REGISTRY", "get_report_type"]
