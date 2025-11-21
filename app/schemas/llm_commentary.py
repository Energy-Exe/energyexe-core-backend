"""Schemas for LLM commentary generation and retrieval."""

from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class CommentaryGenerationRequest(BaseModel):
    """Request to generate commentary for a specific section."""
    section_type: str = Field(
        ...,
        pattern="^(executive_summary|wind_resource|power_generation|peer_comparison|market_context|ownership_history|technology_assessment|methodology)$",
        description="Type of section to generate commentary for"
    )
    start_date: datetime = Field(..., description="Start date for analysis period")
    end_date: datetime = Field(..., description="End date for analysis period")
    regenerate: bool = Field(
        default=False,
        description="Force regeneration even if cached version exists"
    )
    temperature: Optional[float] = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="LLM temperature parameter (0.0-1.0)"
    )
    max_tokens: Optional[int] = Field(
        default=600,
        ge=100,
        le=2000,
        description="Maximum tokens for generated commentary"
    )


class CommentaryResponse(BaseModel):
    """Response containing generated commentary."""
    id: int
    windfarm_id: int
    section_type: str
    commentary_text: str

    # Metadata
    llm_provider: str
    llm_model: str
    prompt_template_version: str

    # Usage info
    token_count_input: int
    token_count_output: int
    generation_cost_usd: Decimal
    generation_duration_seconds: Decimal

    # Status
    status: str
    version: int
    is_current: bool

    # Timestamps
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class CommentarySummary(BaseModel):
    """Lightweight summary of commentary (for listings)."""
    id: int
    section_type: str
    status: str
    created_at: datetime
    word_count: int
    generation_cost_usd: Decimal

    class Config:
        from_attributes = True


class BulkCommentaryGenerationRequest(BaseModel):
    """Request to generate commentary for multiple sections."""
    section_types: List[str] = Field(
        ...,
        description="List of section types to generate"
    )
    start_date: datetime
    end_date: datetime
    regenerate: bool = Field(default=False)


class BulkCommentaryGenerationResponse(BaseModel):
    """Response for bulk commentary generation."""
    windfarm_id: int
    total_sections: int
    successful: int
    failed: int
    total_cost_usd: Decimal
    total_duration_seconds: Decimal
    commentaries: List[CommentaryResponse]
    errors: Optional[Dict[str, str]] = Field(
        default=None,
        description="Map of section_type to error message for failed generations"
    )


class CommentaryUpdateRequest(BaseModel):
    """Request to update existing commentary."""
    commentary_text: str = Field(..., min_length=10)
    status: Optional[str] = Field(
        default=None,
        pattern="^(draft|approved|published)$"
    )


class LLMUsageStats(BaseModel):
    """Statistics about LLM usage."""
    total_commentaries: int
    total_cost_usd: Decimal
    total_tokens_input: int
    total_tokens_output: int
    avg_cost_per_commentary: Decimal
    cost_by_section_type: Dict[str, Decimal]
    commentaries_by_provider: Dict[str, int]


# Section type constants
SECTION_TYPES = {
    "executive_summary": "Executive Summary",
    "wind_resource": "Wind Resource Analysis",
    "power_generation": "Power Generation Analysis",
    "peer_comparison": "Peer Comparison Analysis",
    "market_context": "Market & Policy Context",
    "ownership_history": "Ownership & Transaction History",
    "technology_assessment": "Technology Assessment",
    "methodology": "Methodology"
}

# Sections that have LLM commentary
COMMENTARY_SECTIONS = [
    "executive_summary",
    "wind_resource",
    "power_generation",
    "peer_comparison",
    "market_context",
    "ownership_history",
    "technology_assessment"
]
