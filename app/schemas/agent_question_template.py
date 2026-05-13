from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class QuestionItem(BaseModel):
    template: str = Field(..., min_length=1, max_length=500)
    fallback: Optional[str] = Field(None, max_length=500)


class AgentQuestionTemplateBase(BaseModel):
    route_path: str = Field(..., min_length=1, max_length=255)
    label: str = Field(..., min_length=1, max_length=255)
    questions: List[QuestionItem] = Field(..., min_length=1)

    @field_validator("route_path")
    @classmethod
    def _validate_route_path(cls, v: str) -> str:
        v = v.strip()
        if not v.startswith("/"):
            raise ValueError("route_path must start with '/'")
        return v


class AgentQuestionTemplateCreate(AgentQuestionTemplateBase):
    pass


class AgentQuestionTemplateUpdate(BaseModel):
    route_path: Optional[str] = Field(None, min_length=1, max_length=255)
    label: Optional[str] = Field(None, min_length=1, max_length=255)
    questions: Optional[List[QuestionItem]] = Field(None, min_length=1)

    @field_validator("route_path")
    @classmethod
    def _validate_route_path(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip()
        if not v.startswith("/"):
            raise ValueError("route_path must start with '/'")
        return v


class AgentQuestionTemplate(AgentQuestionTemplateBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
