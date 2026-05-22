"""Service layer for legal-document consent tracking."""

from typing import Dict, List, Optional, Tuple

import structlog
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.exceptions import ValidationException
from app.models.user_consent import UserConsent
from app.schemas.consent import (
    ConsentDocumentStatus,
    ConsentStatusResponse,
    DocumentType,
)

logger = structlog.get_logger()


# Single source of truth mapping the document type string to its current
# configured version. Adding a new document type means a new entry here +
# a new field on ``ConsentStatusResponse``. Resolved lazily so tests can
# bump the version mid-run via ``monkeypatch.setenv``.
def _current_versions() -> Dict[str, str]:
    s = get_settings()
    return {
        "terms": s.TERMS_VERSION,
        "privacy": s.PRIVACY_VERSION,
    }


class ConsentService:
    """Read + record user acceptance of versioned legal documents."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def _latest_acceptance(
        self, user_id: int, document_type: str
    ) -> Optional[UserConsent]:
        result = await self.db.execute(
            select(UserConsent)
            .where(
                UserConsent.user_id == user_id,
                UserConsent.document_type == document_type,
            )
            .order_by(desc(UserConsent.accepted_at))
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def get_status(self, user_id: int) -> ConsentStatusResponse:
        versions = _current_versions()
        doc_states: Dict[str, ConsentDocumentStatus] = {}
        changed: List[DocumentType] = []
        any_first_time = False

        for doc_type, current_version in versions.items():
            latest = await self._latest_acceptance(user_id, doc_type)
            doc_states[doc_type] = ConsentDocumentStatus(
                current_version=current_version,
                accepted_version=latest.document_version if latest else None,
                accepted_at=latest.accepted_at if latest else None,
            )
            if latest is None:
                any_first_time = True
            elif latest.document_version != current_version:
                # Only flag as "changed" when the user has accepted before but
                # the version has since moved on. First-time users get a blank
                # ``changed_documents`` list so the client can show the
                # initial-acceptance copy.
                changed.append(doc_type)  # type: ignore[arg-type]

        requires_acceptance = any_first_time or bool(changed)

        return ConsentStatusResponse(
            terms=doc_states["terms"],
            privacy=doc_states["privacy"],
            requires_acceptance=requires_acceptance,
            changed_documents=changed,
        )

    async def record_acceptance(
        self,
        user_id: int,
        terms_version: str,
        privacy_version: str,
        *,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> ConsentStatusResponse:
        versions = _current_versions()
        submitted: Dict[str, str] = {
            "terms": terms_version,
            "privacy": privacy_version,
        }

        mismatches: List[Tuple[str, str, str]] = [
            (doc, submitted[doc], versions[doc])
            for doc in versions
            if submitted[doc] != versions[doc]
        ]
        if mismatches:
            details = "; ".join(
                f"{doc}: submitted={got!r} current={want!r}"
                for doc, got, want in mismatches
            )
            raise ValidationException(
                f"Submitted document version does not match server: {details}"
            )

        for doc_type, version in versions.items():
            self.db.add(
                UserConsent(
                    user_id=user_id,
                    document_type=doc_type,
                    document_version=version,
                    ip_address=ip_address,
                    user_agent=(user_agent[:512] if user_agent else None),
                )
            )

        await self.db.commit()
        logger.info(
            "user_consent_recorded",
            user_id=user_id,
            terms_version=versions["terms"],
            privacy_version=versions["privacy"],
        )

        return await self.get_status(user_id)
