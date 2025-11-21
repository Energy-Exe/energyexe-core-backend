"""Service for validating LLM-generated commentary against source data."""

import re
from typing import Dict, Any, List, Tuple
import structlog

logger = structlog.get_logger(__name__)


class FactCheckerService:
    """Validate LLM-generated commentary for factual accuracy."""

    # Words that often indicate the LLM is hedging or uncertain
    HEDGE_WORDS = [
        'approximately', 'around', 'roughly', 'about', 'nearly',
        'almost', 'close to', 'estimated', 'probably', 'likely',
        'possibly', 'perhaps', 'may', 'might', 'could be'
    ]

    def validate(
        self,
        commentary: str,
        source_data: Dict[str, Any],
        tolerance: float = 0.5
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate commentary against source data.

        Args:
            commentary: LLM-generated text to validate
            source_data: Source data dictionary used to generate commentary
            tolerance: Acceptable percentage difference for numerical claims (default 0.5%)

        Returns:
            Tuple of (is_valid, validation_report)
        """
        validation_report = {
            "is_valid": True,
            "warnings": [],
            "errors": [],
            "stats": {
                "word_count": self._count_words(commentary),
                "number_count": 0,
                "hedge_word_count": 0
            }
        }

        # Extract all numbers from commentary
        numbers = self._extract_numbers(commentary)
        validation_report["stats"]["number_count"] = len(numbers)

        # Check for hedge words
        hedge_count = self._check_hedge_words(commentary)
        validation_report["stats"]["hedge_word_count"] = hedge_count

        if hedge_count > 5:
            validation_report["warnings"].append(
                f"High number of hedge words ({hedge_count}) may indicate uncertainty or hallucination"
            )

        # Verify numbers (simplified check - just logs warnings)
        # More sophisticated implementation would parse source_data structure
        unverified_numbers = []
        for num in numbers:
            if not self._number_appears_in_data(num, source_data, tolerance):
                unverified_numbers.append(num)

        if unverified_numbers:
            validation_report["warnings"].append(
                f"Could not verify these numbers in source data: {unverified_numbers[:5]}"
            )

        # Check for reasonable length
        word_count = validation_report["stats"]["word_count"]
        if word_count < 100:
            validation_report["errors"].append(
                f"Commentary too short ({word_count} words, expected 150+)"
            )
            validation_report["is_valid"] = False
        elif word_count > 600:
            validation_report["warnings"].append(
                f"Commentary quite long ({word_count} words)"
            )

        # Check for placeholder text
        if "TODO" in commentary or "PLACEHOLDER" in commentary or "XXX" in commentary:
            validation_report["errors"].append("Commentary contains placeholder text")
            validation_report["is_valid"] = False

        # Log validation results
        logger.info(
            "commentary_validated",
            is_valid=validation_report["is_valid"],
            word_count=word_count,
            number_count=len(numbers),
            hedge_word_count=hedge_count,
            warnings=len(validation_report["warnings"]),
            errors=len(validation_report["errors"])
        )

        return validation_report["is_valid"], validation_report

    def _extract_numbers(self, text: str) -> List[float]:
        """
        Extract all numbers from text.

        Args:
            text: Text to extract numbers from

        Returns:
            List of numbers found
        """
        # Pattern matches integers and decimals, including negative and percentages
        pattern = r'[-+]?\d*\.?\d+'
        matches = re.findall(pattern, text)

        numbers = []
        for match in matches:
            try:
                num = float(match)
                # Filter out years (likely not data values)
                if not (1900 <= num <= 2100):
                    numbers.append(num)
            except ValueError:
                continue

        return numbers

    def _check_hedge_words(self, text: str) -> int:
        """
        Count hedge words in text.

        Args:
            text: Text to check

        Returns:
            Count of hedge words found
        """
        text_lower = text.lower()
        count = sum(1 for word in self.HEDGE_WORDS if word in text_lower)
        return count

    def _number_appears_in_data(
        self,
        number: float,
        data: Dict[str, Any],
        tolerance: float = 0.5
    ) -> bool:
        """
        Check if a number appears anywhere in the source data (with tolerance).

        Args:
            number: Number to search for
            data: Source data dictionary
            tolerance: Acceptable percentage difference

        Returns:
            True if number found within tolerance
        """
        def _recursive_search(obj: Any) -> bool:
            """Recursively search through nested structures."""
            if isinstance(obj, (int, float)):
                # Check if within tolerance
                if abs(obj - number) <= abs(number * tolerance / 100):
                    return True
            elif isinstance(obj, dict):
                return any(_recursive_search(v) for v in obj.values())
            elif isinstance(obj, (list, tuple)):
                return any(_recursive_search(item) for item in obj)
            elif isinstance(obj, str):
                # Check if number appears in string representation
                try:
                    str_num = float(obj)
                    if abs(str_num - number) <= abs(number * tolerance / 100):
                        return True
                except ValueError:
                    pass
            return False

        return _recursive_search(data)

    def _count_words(self, text: str) -> int:
        """
        Count words in text.

        Args:
            text: Text to count words in

        Returns:
            Word count
        """
        words = text.split()
        return len(words)

    def get_commentary_stats(self, commentary: str) -> Dict[str, Any]:
        """
        Get statistics about commentary without validation.

        Args:
            commentary: Commentary text

        Returns:
            Statistics dictionary
        """
        return {
            "word_count": self._count_words(commentary),
            "character_count": len(commentary),
            "number_count": len(self._extract_numbers(commentary)),
            "hedge_word_count": self._check_hedge_words(commentary),
            "paragraph_count": len([p for p in commentary.split('\n\n') if p.strip()])
        }
