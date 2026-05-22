"""Schemas for the first-login Terms of Use / Privacy Policy acceptance flow."""

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


DocumentType = Literal["terms", "privacy"]


class ConsentDocumentStatus(BaseModel):
    """Acceptance state for a single legal document."""

    current_version: str
    accepted_version: Optional[str] = None
    accepted_at: Optional[datetime] = None


class ConsentStatusResponse(BaseModel):
    """Full consent state for the current user."""

    terms: ConsentDocumentStatus
    privacy: ConsentDocumentStatus
    requires_acceptance: bool
    # Empty list when this is the user's very first acceptance (everything is
    # "new"). Populated when only a subset of documents was bumped, so the
    # client can call out which one changed.
    changed_documents: List[DocumentType] = Field(default_factory=list)


class ConsentAcceptRequest(BaseModel):
    """Versions the client displayed and the user accepted."""

    terms_version: str
    privacy_version: str
