from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class PlatformUpdateBase(BaseModel):
    title: str = Field(..., min_length=1, max_length=255)
    body_md: str = Field(..., min_length=1)
    category: Optional[str] = Field(None, max_length=50)
    published_at: date = Field(default_factory=date.today)
    is_active: bool = True

    @field_validator("category")
    @classmethod
    def _normalize_category(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip().lower()
        return v or None


class PlatformUpdateCreate(PlatformUpdateBase):
    pass


class PlatformUpdateUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=255)
    body_md: Optional[str] = Field(None, min_length=1)
    category: Optional[str] = Field(None, max_length=50)
    published_at: Optional[date] = None
    is_active: Optional[bool] = None

    @field_validator("category")
    @classmethod
    def _normalize_category(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip().lower()
        return v or None


class PlatformUpdate(PlatformUpdateBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
