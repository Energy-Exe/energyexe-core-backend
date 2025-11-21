"""Service for generating LLM commentary for report sections."""

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
import time

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    anthropic = None

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    openai = None

from app.models.report_commentary import ReportCommentary
from app.services.prompt_builder_service import PromptBuilderService
from app.services.fact_checker_service import FactCheckerService
from app.core.config import get_settings

logger = structlog.get_logger(__name__)


class LLMCommentaryService:
    """Service for generating report commentary using Claude API."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.prompt_builder = PromptBuilderService()
        self.fact_checker = FactCheckerService()

        # Get settings
        settings = get_settings()

        # Determine provider
        self.provider = getattr(settings, 'LLM_PROVIDER', 'claude').lower()

        # Initialize clients
        self.claude_client = None
        self.openai_client = None

        if self.provider == 'claude':
            if ANTHROPIC_AVAILABLE and hasattr(settings, 'ANTHROPIC_API_KEY') and settings.ANTHROPIC_API_KEY:
                self.claude_client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)
            else:
                logger.warning(
                    "claude_client_not_initialized",
                    reason="Anthropic library not installed or API key not configured"
                )
        elif self.provider == 'openai':
            if OPENAI_AVAILABLE and hasattr(settings, 'OPENAI_API_KEY') and settings.OPENAI_API_KEY:
                self.openai_client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
            else:
                logger.warning(
                    "openai_client_not_initialized",
                    reason="OpenAI library not installed or API key not configured"
                )

        # Default LLM settings
        self.default_model = getattr(settings, 'LLM_MODEL',
            'claude-3-5-sonnet-20241022' if self.provider == 'claude' else 'gpt-4o')
        self.default_temperature = 0.3
        self.default_max_tokens = 600

        # Cache duration (hours)
        self.cache_duration_hours = getattr(settings, 'LLM_CACHE_DURATION_HOURS', 24)

    async def generate_commentary(
        self,
        windfarm_id: int,
        section_type: str,
        data: Dict[str, Any],
        date_range: Tuple[datetime, datetime],
        regenerate: bool = False,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None
    ) -> ReportCommentary:
        """
        Generate or retrieve cached commentary for a section.

        Args:
            windfarm_id: ID of windfarm
            section_type: Type of section (wind_resource, power_generation, etc.)
            data: Data dictionary for prompt template
            date_range: Tuple of (start_date, end_date)
            regenerate: Force regeneration even if cached version exists
            temperature: LLM temperature (0.0-1.0)
            max_tokens: Maximum tokens to generate

        Returns:
            ReportCommentary instance

        Raises:
            ValueError: If LLM client not initialized or data validation fails
            Exception: If API call fails
        """
        start_date, end_date = date_range

        # Check cache first (unless regenerate requested)
        if not regenerate:
            cached = await self._get_cached_commentary(
                windfarm_id, section_type, start_date, end_date
            )
            if cached:
                logger.info(
                    "commentary_cache_hit",
                    windfarm_id=windfarm_id,
                    section_type=section_type
                )
                return cached

        # Validate that we can generate
        if self.provider == 'claude' and not self.claude_client:
            raise ValueError(
                "Claude client not initialized. Check ANTHROPIC_API_KEY in settings."
            )
        elif self.provider == 'openai' and not self.openai_client:
            raise ValueError(
                "OpenAI client not initialized. Check OPENAI_API_KEY in settings."
            )

        # Validate data
        is_valid, missing_fields = self.prompt_builder.validate_data(section_type, data)
        if not is_valid:
            raise ValueError(
                f"Missing required fields for {section_type}: {missing_fields}"
            )

        # Build prompt
        try:
            prompt = self.prompt_builder.build_prompt(section_type, data)
        except Exception as e:
            logger.error("prompt_build_failed", error=str(e), section_type=section_type)
            raise

        # Call LLM (Claude or OpenAI based on provider)
        start_time = time.time()
        response = await self._call_llm(
            prompt,
            temperature=temperature or self.default_temperature,
            max_tokens=max_tokens or self.default_max_tokens
        )
        duration = time.time() - start_time

        # Extract text from response
        commentary_text = response['text']
        token_count_input = response['usage']['input_tokens']
        token_count_output = response['usage']['output_tokens']

        # Validate commentary
        is_valid, validation_report = self.fact_checker.validate(commentary_text, data)

        if not is_valid:
            logger.warning(
                "commentary_validation_failed",
                section_type=section_type,
                errors=validation_report.get('errors', [])
            )
            # Could choose to reject here, but we'll save with warnings for now

        # Calculate cost
        cost = self._calculate_cost(token_count_input, token_count_output)

        # Save to database
        commentary = await self._save_commentary(
            windfarm_id=windfarm_id,
            section_type=section_type,
            commentary_text=commentary_text,
            data_snapshot=data,
            date_range=(start_date, end_date),
            llm_model=self.default_model,
            token_count_input=token_count_input,
            token_count_output=token_count_output,
            generation_cost_usd=cost,
            generation_duration_seconds=duration
        )

        logger.info(
            "commentary_generated",
            windfarm_id=windfarm_id,
            section_type=section_type,
            word_count=validation_report['stats']['word_count'],
            cost_usd=float(cost),
            duration_seconds=duration
        )

        return commentary

    async def generate_all_sections(
        self,
        windfarm_id: int,
        report_data: Dict[str, Any],
        selected_sections: List[str],
        regenerate: bool = False,
        skip_db_cache: bool = False
    ) -> Dict[str, ReportCommentary]:
        """
        Generate commentary for all selected sections in parallel.

        Args:
            windfarm_id: ID of windfarm
            report_data: Complete report data dictionary
            selected_sections: List of section types to generate
            regenerate: Force regeneration
            skip_db_cache: Skip database cache check (use when already in db session)

        Returns:
            Dictionary mapping section_type to ReportCommentary
        """
        # Filter to only sections that support commentary
        from app.schemas.llm_commentary import COMMENTARY_SECTIONS
        sections_to_generate = [
            section for section in selected_sections
            if section in COMMENTARY_SECTIONS
        ]

        if not sections_to_generate:
            return {}

        # If skip_db_cache, generate directly without database queries
        if skip_db_cache:
            results = {}
            for section in sections_to_generate:
                try:
                    section_data = self._extract_section_data(section, report_data)

                    # Generate commentary without database cache check
                    commentary_text = await self._generate_commentary_direct(
                        section_type=section,
                        data=section_data
                    )

                    # Create in-memory commentary object (not saved to DB during report gen)
                    from app.schemas.windfarm_report import CommentarySection
                    results[section] = type('obj', (object,), {
                        'commentary_text': commentary_text,
                        'created_at': datetime.utcnow(),
                        'section_type': section
                    })()

                except Exception as e:
                    logger.error(
                        "section_generation_failed",
                        section_type=section,
                        error=str(e)
                    )
            return results

        # Normal path with database caching
        # Create tasks for parallel generation
        tasks = []
        for section in sections_to_generate:
            # Extract relevant data for this section
            section_data = self._extract_section_data(section, report_data)

            task = self.generate_commentary(
                windfarm_id=windfarm_id,
                section_type=section,
                data=section_data,
                date_range=(
                    report_data['date_range_start'],
                    report_data['date_range_end']
                ),
                regenerate=regenerate
            )
            tasks.append((section, task))

        # Execute in parallel
        results = {}
        for section, task in tasks:
            try:
                commentary = await task
                results[section] = commentary
            except Exception as e:
                logger.error(
                    "section_generation_failed",
                    section_type=section,
                    error=str(e)
                )
                # Continue with other sections even if one fails

        return results

    async def _generate_commentary_direct(
        self,
        section_type: str,
        data: Dict[str, Any]
    ) -> str:
        """
        Generate commentary directly without database operations.
        Used during report generation to avoid async conflicts.

        Args:
            section_type: Type of section
            data: Data for prompt

        Returns:
            Commentary text string
        """
        # Validate client is initialized
        if self.provider == 'claude' and not self.claude_client:
            raise ValueError("Claude client not initialized")
        elif self.provider == 'openai' and not self.openai_client:
            raise ValueError("OpenAI client not initialized")

        # Validate data
        is_valid, missing_fields = self.prompt_builder.validate_data(section_type, data)
        if not is_valid:
            logger.warning(
                "commentary_data_incomplete",
                section_type=section_type,
                missing_fields=missing_fields
            )
            # Continue anyway with available data

        # Build prompt
        prompt = self.prompt_builder.build_prompt(section_type, data)

        # Call LLM
        response = await self._call_llm(prompt)

        return response['text']

    async def _call_llm(
        self,
        prompt: str,
        temperature: float = 0.3,
        max_tokens: int = 600
    ) -> Dict[str, Any]:
        """
        Call LLM API (Claude or OpenAI based on provider).

        Args:
            prompt: Prompt text
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate

        Returns:
            Dictionary with 'text' and 'usage' keys
        """
        try:
            if self.provider == 'claude':
                response = await self.claude_client.messages.create(
                    model=self.default_model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )

                # Extract text from response
                text = response.content[0].text

                return {
                    'text': text,
                    'usage': {
                        'input_tokens': response.usage.input_tokens,
                        'output_tokens': response.usage.output_tokens
                    }
                }

            elif self.provider == 'openai':
                response = await self.openai_client.chat.completions.create(
                    model=self.default_model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )

                # Extract text from response
                text = response.choices[0].message.content

                return {
                    'text': text,
                    'usage': {
                        'input_tokens': response.usage.prompt_tokens,
                        'output_tokens': response.usage.completion_tokens
                    }
                }

            else:
                raise ValueError(f"Unsupported LLM provider: {self.provider}")

        except Exception as e:
            logger.error("llm_api_call_failed", provider=self.provider, error=str(e))
            raise

    def _calculate_cost(self, input_tokens: int, output_tokens: int) -> Decimal:
        """
        Calculate cost in USD based on provider and model pricing.

        Pricing per 1M tokens (as of Jan 2025):

        Claude:
        - Claude 3.5 Sonnet: $3 input / $15 output

        OpenAI:
        - GPT-4o: $2.50 input / $10 output
        - GPT-4-turbo: $10 input / $30 output
        - GPT-5: TBD (likely $10-20 input / $30-60 output)

        Args:
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens

        Returns:
            Cost in USD
        """
        # Pricing per million tokens based on model
        pricing = {
            # Claude models
            'claude-3-5-sonnet-20241022': (Decimal('3.00'), Decimal('15.00')),
            'claude-3-sonnet': (Decimal('3.00'), Decimal('15.00')),

            # OpenAI models
            'gpt-4o': (Decimal('2.50'), Decimal('10.00')),
            'gpt-4o-mini': (Decimal('0.15'), Decimal('0.60')),
            'gpt-4-turbo': (Decimal('10.00'), Decimal('30.00')),
            'gpt-4': (Decimal('30.00'), Decimal('60.00')),

            # GPT-5 (when available - estimated)
            'gpt-5': (Decimal('10.00'), Decimal('30.00')),
            'gpt-5-turbo': (Decimal('5.00'), Decimal('15.00')),
        }

        # Get pricing for current model, default to Claude pricing
        input_cost_per_million, output_cost_per_million = pricing.get(
            self.default_model,
            (Decimal('3.00'), Decimal('15.00'))  # Default fallback
        )

        input_cost = (Decimal(input_tokens) / Decimal('1000000')) * input_cost_per_million
        output_cost = (Decimal(output_tokens) / Decimal('1000000')) * output_cost_per_million

        total_cost = input_cost + output_cost

        return total_cost.quantize(Decimal('0.000001'))  # 6 decimal places

    async def _get_cached_commentary(
        self,
        windfarm_id: int,
        section_type: str,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[ReportCommentary]:
        """
        Retrieve cached commentary if it exists and is recent.

        Args:
            windfarm_id: ID of windfarm
            section_type: Type of section
            start_date: Start date of analysis period
            end_date: End date of analysis period

        Returns:
            ReportCommentary if found, None otherwise
        """
        cache_cutoff = datetime.utcnow() - timedelta(hours=self.cache_duration_hours)

        stmt = select(ReportCommentary).where(
            and_(
                ReportCommentary.windfarm_id == windfarm_id,
                ReportCommentary.section_type == section_type,
                ReportCommentary.date_range_start == start_date,
                ReportCommentary.date_range_end == end_date,
                ReportCommentary.is_current == True,
                ReportCommentary.created_at >= cache_cutoff
            )
        ).order_by(ReportCommentary.created_at.desc())

        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def _save_commentary(
        self,
        windfarm_id: int,
        section_type: str,
        commentary_text: str,
        data_snapshot: Dict[str, Any],
        date_range: Tuple[datetime, datetime],
        llm_model: str,
        token_count_input: int,
        token_count_output: int,
        generation_cost_usd: Decimal,
        generation_duration_seconds: float
    ) -> ReportCommentary:
        """
        Save commentary to database.

        Args:
            windfarm_id: ID of windfarm
            section_type: Type of section
            commentary_text: Generated commentary text
            data_snapshot: Data used for generation
            date_range: Tuple of (start_date, end_date)
            llm_model: LLM model used
            token_count_input: Input token count
            token_count_output: Output token count
            generation_cost_usd: Generation cost
            generation_duration_seconds: Duration in seconds

        Returns:
            Saved ReportCommentary instance
        """
        start_date, end_date = date_range

        # Mark any existing commentary for this section as not current
        await self.db.execute(
            ReportCommentary.__table__.update().where(
                and_(
                    ReportCommentary.windfarm_id == windfarm_id,
                    ReportCommentary.section_type == section_type,
                    ReportCommentary.date_range_start == start_date,
                    ReportCommentary.date_range_end == end_date
                )
            ).values(is_current=False)
        )

        # Create new commentary
        commentary = ReportCommentary(
            windfarm_id=windfarm_id,
            section_type=section_type,
            commentary_text=commentary_text,
            data_snapshot=data_snapshot,
            date_range_start=start_date,
            date_range_end=end_date,
            llm_provider=self.provider,  # Use configured provider (claude, openai, etc.)
            llm_model=llm_model,
            prompt_template_version='v1',
            token_count_input=token_count_input,
            token_count_output=token_count_output,
            generation_cost_usd=generation_cost_usd,
            generation_duration_seconds=Decimal(str(generation_duration_seconds)),
            status='published',
            version=1,
            is_current=True
        )

        self.db.add(commentary)
        await self.db.commit()
        await self.db.refresh(commentary)

        return commentary

    def _extract_section_data(self, section_type: str, report_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant data for a specific section from complete report data.

        Args:
            section_type: Type of section
            report_data: Complete report data

        Returns:
            Data dictionary for this section
        """
        # This is a simplified version - should be customized based on section needs
        return {
            **report_data.get(section_type, {}),
            'windfarm_name': report_data.get('windfarm_name', ''),
            'start_date': report_data.get('date_range_start', ''),
            'end_date': report_data.get('date_range_end', ''),
        }
