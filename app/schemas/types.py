"""Shared Pydantic field types."""

from typing import Annotated

from pydantic import AfterValidator, EmailStr


def _normalize_email(value: str) -> str:
    """Trim whitespace and lowercase an email so storage and lookups stay
    case-insensitive. Runs after EmailStr has validated the address format."""
    return value.strip().lower()


# Use in place of EmailStr on any inbound schema field that gets stored or
# looked up, so emails are persisted lowercased and never silently miss a
# case-insensitive lookup (paired with UserService.get_by_email).
NormalizedEmailStr = Annotated[EmailStr, AfterValidator(_normalize_email)]
