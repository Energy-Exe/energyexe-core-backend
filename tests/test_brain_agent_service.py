"""Tests for BrainAgentService SDK-message conversion.

These exercise the pure transcript→message mapping used to persist threads and
build the authoritative `result` payload — no SDK runtime or DB needed.
"""

from types import SimpleNamespace

import pytest

# The service imports claude_agent_sdk at module load; skip cleanly if the
# agent SDK isn't installed in the current environment.
pytest.importorskip("claude_agent_sdk")

from app.services.brain_agent_service import BrainAgentService  # noqa: E402


def _sm(type_: str, uuid: str, content):
    """Build a duck-typed SessionMessage (the converter reads .type/.uuid/.message)."""
    return SimpleNamespace(type=type_, uuid=uuid, message={"role": type_, "content": content})


def test_convert_user_and_assistant_with_tool_use():
    sdk_messages = [
        _sm("user", "u1", [{"type": "text", "text": "List farms"}]),
        _sm(
            "assistant",
            "a1",
            [
                {"type": "text", "text": "Sure"},
                {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}},
            ],
        ),
        _sm(
            "user",
            "u2",
            [{"type": "tool_result", "tool_use_id": "t1", "content": "output", "is_error": False}],
        ),
    ]

    out = BrainAgentService._convert_sdk_messages(sdk_messages)

    # The tool-result-only user message carries no text, so it isn't emitted.
    assert [m["type"] for m in out] == ["user", "assistant"]
    assert out[0]["content"] == "List farms"
    assert out[1]["content"] == "Sure"

    tool_calls = out[1]["toolCalls"]
    assert len(tool_calls) == 1
    assert tool_calls[0]["tool_name"] == "Bash"
    assert tool_calls[0]["input"] == {"command": "ls"}
    assert tool_calls[0]["result"] == "output"
    assert tool_calls[0]["isError"] is False


def test_tool_result_error_flag_and_truncation():
    long_output = "y" * 2500
    sdk_messages = [
        _sm("assistant", "a1", [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {}}]),
        _sm(
            "user",
            "u1",
            [{"type": "tool_result", "tool_use_id": "t1", "content": long_output, "is_error": True}],
        ),
    ]

    out = BrainAgentService._convert_sdk_messages(sdk_messages)

    assert len(out) == 1  # assistant only; the tool-result user message has no text
    tc = out[0]["toolCalls"][0]
    assert tc["isError"] is True
    assert tc["result"].endswith("...")
    assert len(tc["result"]) == 2003  # 2000 chars + "..."


def test_list_tool_result_content_is_flattened():
    sdk_messages = [
        _sm("assistant", "a1", [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {}}]),
        _sm(
            "user",
            "u1",
            [
                {
                    "type": "tool_result",
                    "tool_use_id": "t1",
                    "content": [{"type": "text", "text": "line1"}, {"type": "text", "text": "line2"}],
                    "is_error": False,
                }
            ],
        ),
    ]

    out = BrainAgentService._convert_sdk_messages(sdk_messages)
    assert out[0]["toolCalls"][0]["result"] == "line1 line2"


# ── SCADA gating (schema-presence check + prompt injection) ──


class _FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _FakeDB:
    def __init__(self, value=None, raise_exc=False):
        self._value = value
        self._raise = raise_exc

    async def execute(self, *_args, **_kwargs):
        if self._raise:
            raise RuntimeError("connection refused")
        return _FakeResult(self._value)


async def test_scada_schema_present_true_when_dim_farm_exists():
    svc = BrainAgentService(db=_FakeDB(value=1))
    assert await svc._scada_schema_present() is True


async def test_scada_schema_present_false_when_schema_absent():
    svc = BrainAgentService(db=_FakeDB(value=None))
    assert await svc._scada_schema_present() is False


async def test_scada_schema_present_false_on_db_error():
    # Best-effort: a failing check must degrade to "no scada", never raise.
    svc = BrainAgentService(db=_FakeDB(raise_exc=True))
    assert await svc._scada_schema_present() is False


def test_system_prompt_scada_lines_injected_when_enabled():
    prompt = BrainAgentService._build_system_prompt(scada_enabled=True)
    assert "skill_scada.md" in prompt
    assert "skill_scada_queries.md" in prompt
    assert "{{SCADA_SKILL_LINES}}" not in prompt


def test_system_prompt_scada_lines_absent_when_disabled():
    # Prod before the scada cut: no placeholder residue, no scada mention.
    prompt = BrainAgentService._build_system_prompt(scada_enabled=False)
    assert "skill_scada" not in prompt
    assert "{{SCADA_SKILL_LINES}}" not in prompt


def test_client_prompt_never_mentions_scada():
    # The client surface has no scada grants; its prompt file carries no
    # placeholder, so the flag must be a no-op there either way.
    for enabled in (True, False):
        prompt = BrainAgentService._build_system_prompt(
            prompt_file="brain_agent_system_client.md", scada_enabled=enabled
        )
        assert "skill_scada" not in prompt
