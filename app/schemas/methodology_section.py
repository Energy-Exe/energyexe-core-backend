from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class MethodologySectionBase(BaseModel):
    section_key: str = Field(..., min_length=1, max_length=100)
    title: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=500)
    content_md: str = Field(..., min_length=1)
    sort_order: int = Field(0, ge=0)

    @field_validator("section_key")
    @classmethod
    def _validate_section_key(cls, v: str) -> str:
        v = v.strip().lower()
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError("section_key must be alphanumeric with hyphens/underscores")
        return v


class MethodologySectionCreate(MethodologySectionBase):
    pass


class MethodologySectionUpdate(BaseModel):
    section_key: Optional[str] = Field(None, min_length=1, max_length=100)
    title: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=500)
    content_md: Optional[str] = Field(None, min_length=1)
    sort_order: Optional[int] = Field(None, ge=0)

    @field_validator("section_key")
    @classmethod
    def _validate_section_key(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip().lower()
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError("section_key must be alphanumeric with hyphens/underscores")
        return v


class MethodologySection(MethodologySectionBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
