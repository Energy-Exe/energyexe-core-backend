"""Brain chat service — agentic LLM orchestration with tool use and SSE streaming."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional

import anthropic
import structlog
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.services.brain_tools import BrainToolRegistry

logger = structlog.get_logger(__name__)

MAX_TOOL_ROUNDS = 8


@dataclass
class SSEEvent:
    """A single SSE event to stream to the client."""

    event_type: str  # text_delta, tool_call_start, tool_result, done, error
    data: Dict[str, Any]


class BrainChatService:
    """Core agentic chat service with streaming and tool use."""

    def __init__(self, db: AsyncSession):
        self.db = db
        settings = get_settings()
        self.claude_client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)

        self.default_model = getattr(settings, "BRAIN_MODEL", "claude-sonnet-4-20250514")
        self.max_tokens = getattr(settings, "BRAIN_MAX_TOKENS", 4096)
        self.temperature = getattr(settings, "BRAIN_TEMPERATURE", 0.3)
        self.tool_registry = BrainToolRegistry(db)

        # Jinja2 for system prompt
        template_dir = Path(__file__).parent.parent / "prompts"
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=select_autoescape(["html", "xml"]),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    async def stream_response(
        self,
        messages: List[Dict[str, str]],
        context: Optional[Dict] = None,
        user: Any = None,
        model_override: Optional[str] = None,
    ) -> AsyncGenerator[SSEEvent, None]:
        """Async generator yielding SSE events for the chat response.

        Implements an agentic loop: Claude may call tools, and we feed results
        back until Claude produces a final text response (max MAX_TOOL_ROUNDS).
        """
        model = model_override or self.default_model
        system_prompt = self._build_system_prompt(context, user)
        tool_defs = self.tool_registry.get_definitions()
        user_id = user.id if user and hasattr(user, "id") else None

        # Convert messages to Anthropic format
        api_messages = [{"role": m["role"], "content": m["content"]} for m in messages]

        total_input_tokens = 0
        total_output_tokens = 0

        for round_num in range(MAX_TOOL_ROUNDS):
            try:
                response = await self.claude_client.messages.create(
                    model=model,
                    max_tokens=self.max_tokens,
                    system=system_prompt,
                    messages=api_messages,
                    tools=tool_defs,
                    temperature=self.temperature,
                )
            except anthropic.APIError as e:
                yield SSEEvent(event_type="error", data={"message": str(e), "code": "api_error"})
                return

            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens

            # Process response content blocks
            tool_uses = []
            text_parts = []

            for block in response.content:
                if block.type == "text":
                    text_parts.append(block.text)
                    yield SSEEvent(event_type="text_delta", data={"text": block.text})

                elif block.type == "tool_use":
                    tool_uses.append(block)
                    yield SSEEvent(
                        event_type="tool_call_start",
                        data={
                            "tool_name": block.name,
                            "tool_id": block.id,
                            "input": block.input,
                        },
                    )

            # If no tool calls, we're done
            if not tool_uses:
                break

            # Execute tools and feed results back
            # Build the assistant message with all content blocks
            assistant_content = []
            for block in response.content:
                if block.type == "text":
                    assistant_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    assistant_content.append({
                        "type": "tool_use",
                        "id": block.id,
                        "name": block.name,
                        "input": block.input,
                    })

            api_messages.append({"role": "assistant", "content": assistant_content})

            # Execute each tool and build tool results
            tool_results = []
            for tool_block in tool_uses:
                result_str = await self.tool_registry.execute(
                    tool_block.name, tool_block.input, user_id=user_id
                )

                # Send tool result event to frontend
                # Summarize for display
                try:
                    result_data = json.loads(result_str)
                    display_summary = self._summarize_tool_result(tool_block.name, result_data)
                except (json.JSONDecodeError, Exception):
                    display_summary = result_str[:200]

                yield SSEEvent(
                    event_type="tool_result",
                    data={
                        "tool_id": tool_block.id,
                        "tool_name": tool_block.name,
                        "summary": display_summary,
                    },
                )

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_block.id,
                    "content": result_str,
                })

            api_messages.append({"role": "user", "content": tool_results})

            # If stop_reason is end_turn (not tool_use), break
            if response.stop_reason != "tool_use":
                break

        yield SSEEvent(
            event_type="done",
            data={
                "tokens_input": total_input_tokens,
                "tokens_output": total_output_tokens,
                "model": model,
            },
        )

    def _build_system_prompt(self, context: Optional[Dict], user: Any) -> str:
        """Render the system prompt template with context."""
        try:
            template = self.jinja_env.get_template("brain_system.txt")
        except Exception:
            # Fallback if template can't be loaded
            return "You are EnergyExe Brain, an intelligent energy analytics assistant."

        template_vars = {
            "user_name": None,
            "windfarm_id": None,
            "windfarm_name": None,
            "page_route": None,
        }

        if user:
            name_parts = []
            if hasattr(user, "first_name") and user.first_name:
                name_parts.append(user.first_name)
            if hasattr(user, "last_name") and user.last_name:
                name_parts.append(user.last_name)
            template_vars["user_name"] = " ".join(name_parts) if name_parts else None

        if context:
            template_vars["windfarm_id"] = context.get("windfarm_id")
            template_vars["windfarm_name"] = context.get("windfarm_name")
            template_vars["page_route"] = context.get("page_route")

        return template.render(**template_vars)

    @staticmethod
    def _summarize_tool_result(tool_name: str, data: dict) -> str:
        """Create a short display summary of a tool result for the frontend."""
        if "error" in data:
            return f"Error: {data['error']}"

        if tool_name == "list_windfarms":
            count = data.get("count", 0)
            names = [w["name"] for w in data.get("windfarms", [])[:5]]
            return f"Found {count} windfarms: {', '.join(names)}{'...' if count > 5 else ''}"

        if tool_name == "get_windfarm_info":
            name = data.get("name", "Unknown")
            cap = data.get("nameplate_capacity_mw")
            return f"{name} — {cap} MW, {data.get('status', 'N/A')}, {data.get('country', 'N/A')}"

        if tool_name == "get_generation_summary":
            total = data.get("total_generation_mwh", 0)
            cf = data.get("avg_capacity_factor_pct", 0)
            return f"{data.get('windfarm', 'Windfarm')}: {total:,.0f} MWh, {cf}% CF"

        if tool_name == "get_price_analytics":
            avg = data.get("avg_price", 0)
            cr = data.get("capture_rate_pct")
            return f"Avg price: {avg}/MWh, Capture rate: {cr}%" if cr else f"Avg price: {avg}/MWh"

        if tool_name == "compare_windfarms":
            farms = data.get("windfarms", [])
            return f"Compared {len(farms)} windfarms over {data.get('period_days', 0)} days"

        if tool_name == "get_data_availability":
            gen = data.get("generation", {})
            return f"Generation: {gen.get('total_records', 0)} records ({gen.get('first_date', 'N/A')} to {gen.get('last_date', 'N/A')})"

        if tool_name == "search_by_country_or_region":
            return f"Found {data.get('count', 0)} windfarms for '{data.get('query', '')}'"

        # Generic fallback
        return json.dumps(data, default=str)[:200]

    @staticmethod
    def get_available_models() -> List[Dict[str, str]]:
        """Return list of available models based on configured API keys."""
        settings = get_settings()
        models = []

        if settings.ANTHROPIC_API_KEY:
            models.extend([
                {"id": "claude-sonnet-4-20250514", "name": "Claude Sonnet 4", "provider": "anthropic"},
                {"id": "claude-haiku-4-5-20251001", "name": "Claude Haiku 4.5", "provider": "anthropic"},
            ])

        if settings.OPENAI_API_KEY:
            models.extend([
                {"id": "gpt-4o", "name": "GPT-4o", "provider": "openai"},
                {"id": "gpt-4o-mini", "name": "GPT-4o Mini", "provider": "openai"},
            ])

        return models
