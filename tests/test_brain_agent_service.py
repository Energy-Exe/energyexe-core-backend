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


def test_thinking_only_assistant_entry_is_dropped():
    """Adaptive thinking writes a thinking block as its own transcript entry.

    It has no text and no tool_use, so persisting it would render an empty
    assistant bubble. Verified against a live Sonnet 5 run: the transcript came
    back as user → assistant[thinking] → assistant[tool_use] → user[tool_result]
    → assistant[text].
    """
    sdk_messages = [
        _sm("user", "u1", [{"type": "text", "text": "Which quarter underperformed?"}]),
        _sm("assistant", "a1", [{"type": "thinking", "thinking": "", "signature": "sig"}]),
        _sm("assistant", "a2", [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {}}]),
        _sm(
            "user",
            "u2",
            [{"type": "tool_result", "tool_use_id": "t1", "content": "Q3", "is_error": False}],
        ),
        _sm("assistant", "a3", [{"type": "text", "text": "Q3 underperformed."}]),
    ]

    out = BrainAgentService._convert_sdk_messages(sdk_messages)

    assert [m["id"] for m in out] == ["u1", "a2", "a3"]
    assert all(m["content"] or m.get("toolCalls") for m in out)
    # The tool-only entry still carries its call and result.
    assert out[1]["toolCalls"][0]["result"] == "Q3"


# ── per-profile SDK options ──


def test_only_admin_asks_for_readable_thinking():
    """Reasoning is model-authored prose that can name internal tables.

    Admin gets display="summarized" (readable, streamed to the UI); the client
    profile leaves it off so its thinking blocks stay encrypted (EPR-59).
    """
    from app.services.brain_agent_service import PROFILES

    assert PROFILES["admin"]["stream_thinking"] is True
    assert PROFILES["client"]["stream_thinking"] is False


def test_every_profile_declares_a_task_budget():
    """task_budget lets the model pace itself instead of being cut off by
    max_budget_usd mid-answer — a profile without one silently loses that."""
    from app.services.brain_agent_service import PROFILES

    for name, profile in PROFILES.items():
        budget = profile.get("task_budget_tokens")
        assert isinstance(budget, int) and budget > 0, name
        # Client is the more constrained surface; keep that ordering true.
    assert (
        PROFILES["client"]["task_budget_tokens"] <= PROFILES["admin"]["task_budget_tokens"]
    )


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
    # Farm NAMES must be in the prompt index: a question naming Penmanshiel
    # without the word "SCADA" otherwise routes to public.windfarms and the
    # agent wrongly reports the farm missing (battery test q04).
    for name in ("Hill of Towie", "Kelmarsh", "Penmanshiel"):
        assert name in prompt


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
            prompt_file="brain_agent_system_client.md",
            scada_enabled=enabled,
            silver_enabled=enabled,
        )
        assert "skill_scada" not in prompt
        assert "silver.py" not in prompt


def test_system_prompt_silver_line_gated_on_both_flags():
    # silver routing appears only when scada AND silver are enabled.
    both = BrainAgentService._build_system_prompt(scada_enabled=True, silver_enabled=True)
    assert "skill_scada_silver.md" in both
    assert "silver.py" in both

    no_silver = BrainAgentService._build_system_prompt(scada_enabled=True, silver_enabled=False)
    assert "skill_scada_silver" not in no_silver

    # silver without scada must be a no-op (helper is never seeded then).
    no_scada = BrainAgentService._build_system_prompt(scada_enabled=False, silver_enabled=True)
    assert "silver" not in no_scada.lower() or "skill_scada_silver" not in no_scada


# ---------------------------------------------------------------------------
# EPR-98: duplicate-send guard + transcript de-amplifier
# ---------------------------------------------------------------------------

import time as _time

from app.services.brain_agent_service import STALE_RUN_SECONDS


@pytest.fixture()
def _isolated_sessions():
    """Snapshot/restore the class-level session dict so tests can't bleed."""
    saved = dict(BrainAgentService._sessions)
    BrainAgentService._sessions.clear()
    try:
        yield BrainAgentService._sessions
    finally:
        BrainAgentService._sessions.clear()
        BrainAgentService._sessions.update(saved)


def _busy_session(session_id="s1", **overrides):
    """Duck-typed AgentSession carrying only what check_duplicate_send reads."""
    defaults = dict(
        session_id=session_id,
        is_busy=True,
        run_started_at=_time.time(),
        current_message_id="m1",
        last_message_id="m1",
        current_prompt="show me T21",
        detach_task=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_duplicate_send_same_message_id_is_rejected(_isolated_sessions):
    _isolated_sessions["s1"] = _busy_session()
    assert BrainAgentService.check_duplicate_send("s1", "m1", "show me T21") is True


def test_duplicate_send_same_prompt_no_id_is_rejected(_isolated_sessions):
    # Old deployed clients send no message_id — the raw prompt is the fallback key.
    _isolated_sessions["s1"] = _busy_session(current_message_id=None, last_message_id=None)
    assert BrainAgentService.check_duplicate_send("s1", None, "show me T21") is True


def test_different_prompt_while_busy_is_not_a_duplicate(_isolated_sessions):
    # Supersede path: "cancel, then ask something else" must keep working.
    _isolated_sessions["s1"] = _busy_session()
    assert BrainAgentService.check_duplicate_send("s1", "m2", "different question") is False
    assert BrainAgentService.check_duplicate_send("s1", None, "different question") is False


def test_idle_session_is_never_a_duplicate(_isolated_sessions):
    _isolated_sessions["s1"] = _busy_session(is_busy=False)
    assert BrainAgentService.check_duplicate_send("s1", "m1", "show me T21") is False


def test_unknown_session_is_never_a_duplicate(_isolated_sessions):
    assert BrainAgentService.check_duplicate_send("nope", "m1", "show me T21") is False


def test_stale_busy_session_is_not_a_duplicate(_isolated_sessions):
    # A wedged run older than the TTL must not block the thread forever.
    _isolated_sessions["s1"] = _busy_session(
        run_started_at=_time.time() - STALE_RUN_SECONDS - 1
    )
    assert BrainAgentService.check_duplicate_send("s1", "m1", "show me T21") is False


def test_drop_consecutive_duplicates_collapses_identical_user_pair():
    messages = [
        {"type": "user", "content": "same question"},
        {"type": "user", "content": "same question"},
        {"type": "assistant", "content": "answer"},
    ]
    out = BrainAgentService._drop_consecutive_duplicates(messages)
    assert [m["type"] for m in out] == ["user", "assistant"]


def test_drop_consecutive_duplicates_keeps_alternating_repeats():
    # user → assistant → user with the same text is a legitimate repeat.
    messages = [
        {"type": "user", "content": "yes"},
        {"type": "assistant", "content": "confirm?"},
        {"type": "user", "content": "yes"},
    ]
    out = BrainAgentService._drop_consecutive_duplicates(messages)
    assert len(out) == 3


def test_drop_consecutive_duplicates_never_touches_tool_call_messages():
    messages = [
        {"type": "assistant", "content": "running", "toolCalls": [{"tool_name": "Bash"}]},
        {"type": "assistant", "content": "running", "toolCalls": [{"tool_name": "Bash"}]},
    ]
    out = BrainAgentService._drop_consecutive_duplicates(messages)
    assert len(out) == 2


def test_convert_sdk_messages_dedupes_duplicated_user_turns():
    # The EPR-98 storm wrote the same user turn into a session twice; the
    # persisted transcript must not carry the pair.
    sdk_messages = [
        _sm("user", "u1", [{"type": "text", "text": "show me T21"}]),
        _sm("user", "u2", [{"type": "text", "text": "show me T21"}]),
        _sm("assistant", "a1", [{"type": "text", "text": "T21 report"}]),
    ]
    out = BrainAgentService._convert_sdk_messages(sdk_messages)
    assert [m["type"] for m in out] == ["user", "assistant"]


def test_build_prompt_with_history_skips_adjacent_duplicates():
    history = [
        {"type": "user", "content": "show me T21"},
        {"type": "user", "content": "show me T21"},
        {"type": "assistant", "content": "T21 report"},
    ]
    prompt = BrainAgentService._build_prompt_with_history("next question", history)
    assert prompt.count("Human: show me T21") == 1
    assert "Assistant: T21 report" in prompt
    assert prompt.rstrip().endswith("next question")


# ---------------------------------------------------------------------------
# EPR-98: detached-run consumer (_finish_orphaned_run)
# ---------------------------------------------------------------------------

import asyncio

from claude_agent_sdk import ResultMessage

from app.services.brain_agent_service import AgentSession


def _result_message(session_id="sdk-abc"):
    return ResultMessage(
        subtype="success",
        duration_ms=1200,
        duration_api_ms=1000,
        is_error=False,
        num_turns=1,
        session_id=session_id,
    )


class _FakeClient:
    """Duck-typed SDK client: yields queued messages, records interrupts."""

    def __init__(self, messages):
        self._messages = list(messages)
        self.interrupted = False

    async def receive_messages(self):
        for m in self._messages:
            yield m

    async def interrupt(self):
        self.interrupted = True


def _detached_session(client) -> AgentSession:
    session = AgentSession(
        session_id="s-detach",
        user_id=USER_ID_DETACH,
        client=client,
        created_at=_time.time(),
        last_activity=_time.time(),
    )
    session.is_busy = True
    session.current_message_id = "m1"
    session.current_prompt = "long analysis"
    session.run_started_at = _time.time()
    return session


USER_ID_DETACH = 77


def test_finish_orphaned_run_persists_and_clears_busy(monkeypatch):
    fake_client = _FakeClient([SimpleNamespace(kind="noise"), _result_message()])
    session = _detached_session(fake_client)

    persisted = {}

    async def _fake_persist(self, user_id, session_id, result_message, model=None):
        persisted.update(user_id=user_id, session_id=session_id, result=result_message)
        return [{"type": "user", "content": "long analysis"}]

    monkeypatch.setattr(BrainAgentService, "_persist_completed_run", _fake_persist)
    monkeypatch.setattr(BrainAgentService, "_scan_for_new_files", lambda self, s: [])

    service = BrainAgentService(db=None)
    asyncio.run(service._finish_orphaned_run(session, model="claude-sonnet-5"))

    assert persisted["session_id"] == "s-detach"
    assert persisted["user_id"] == USER_ID_DETACH
    assert isinstance(persisted["result"], ResultMessage)
    # The detach task owns the busy state and must release it when done.
    assert session.is_busy is False
    assert session.current_message_id is None
    assert session.current_prompt is None
    assert session.detach_task is None


def test_finish_orphaned_run_survives_persist_failure(monkeypatch):
    fake_client = _FakeClient([_result_message()])
    session = _detached_session(fake_client)

    async def _boom(self, **_kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(BrainAgentService, "_persist_completed_run", _boom)

    service = BrainAgentService(db=None)
    # Must not raise, and must still clear busy state.
    asyncio.run(service._finish_orphaned_run(session))
    assert session.is_busy is False
