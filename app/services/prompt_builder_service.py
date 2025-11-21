"""Service for building LLM prompts from templates and data."""

import json
from pathlib import Path
from typing import Dict, Any
from jinja2 import Template, Environment, FileSystemLoader, select_autoescape
import structlog

logger = structlog.get_logger(__name__)


class PromptBuilderService:
    """Build LLM prompts from Jinja2 templates and data."""

    def __init__(self):
        self.template_dir = Path(__file__).parent.parent / "prompts"

        # Initialize Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=select_autoescape(['html', 'xml']),
            trim_blocks=True,
            lstrip_blocks=True
        )

        # Add custom filters
        self.jinja_env.filters['tojson_pretty'] = lambda x: json.dumps(x, indent=2)

    def build_prompt(self, section_type: str, data: Dict[str, Any]) -> str:
        """
        Load template and render with provided data.

        Args:
            section_type: Type of section (wind_resource, power_generation, etc.)
            data: Dictionary containing all data to inject into template

        Returns:
            Rendered prompt string

        Raises:
            FileNotFoundError: If template file doesn't exist
            Exception: If template rendering fails
        """
        try:
            template_filename = f"{section_type}.txt"
            template_path = self.template_dir / template_filename

            if not template_path.exists():
                logger.error(
                    "template_not_found",
                    section_type=section_type,
                    path=str(template_path)
                )
                raise FileNotFoundError(f"Template not found: {template_filename}")

            # Load and render template
            template = self.jinja_env.get_template(template_filename)
            rendered = template.render(**data)

            logger.info(
                "prompt_built",
                section_type=section_type,
                prompt_length=len(rendered),
                data_keys=list(data.keys())
            )

            return rendered

        except Exception as e:
            logger.error(
                "prompt_build_failed",
                section_type=section_type,
                error=str(e)
            )
            raise

    def estimate_tokens(self, text: str) -> int:
        """
        Rough estimation of token count.
        More accurate estimation would use tiktoken library.

        Args:
            text: Text to estimate tokens for

        Returns:
            Estimated token count
        """
        # Rough approximation: 1 token ~= 4 characters for English text
        return len(text) // 4

    def get_available_templates(self) -> list[str]:
        """
        Get list of available template names.

        Returns:
            List of section_type strings
        """
        templates = []
        if self.template_dir.exists():
            for file in self.template_dir.glob("*.txt"):
                templates.append(file.stem)

        return sorted(templates)

    def validate_data(self, section_type: str, data: Dict[str, Any]) -> tuple[bool, list[str]]:
        """
        Validate that required data fields are present for a template.

        Args:
            section_type: Type of section
            data: Data dictionary to validate

        Returns:
            Tuple of (is_valid, list_of_missing_fields)
        """
        # Define required fields for each section type
        required_fields = {
            "wind_resource": [
                "windfarm_name", "location", "country_name",
                "start_date", "end_date"
            ],
            "power_generation": [
                "windfarm_name", "installed_capacity_mw", "turbine_count",
                "start_date", "end_date", "avg_capacity_factor"
            ],
            "peer_comparison": [
                "windfarm_name", "peer_group_name", "peer_group_type",
                "peer_count", "target_median_cf", "peer_median_cf"
            ],
            "executive_summary": [
                "windfarm_name", "location", "country_name",
                "avg_capacity_factor", "total_generation_gwh"
            ],
            "market_context": [
                "country_name", "windfarm_name", "start_date", "end_date"
            ],
            "technology_assessment": [
                "windfarm_name", "turbine_model", "manufacturer",
                "rated_capacity_mw", "turbine_count"
            ],
            "ownership_history": [
                "windfarm_name", "current_owners"
            ]
        }

        required = required_fields.get(section_type, [])
        missing = [field for field in required if field not in data or data[field] is None]

        is_valid = len(missing) == 0

        if not is_valid:
            logger.warning(
                "data_validation_failed",
                section_type=section_type,
                missing_fields=missing
            )

        return is_valid, missing
