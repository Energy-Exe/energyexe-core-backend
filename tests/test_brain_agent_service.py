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


def _result_message(session_id="sdk-abc", **overrides):
    kwargs = dict(
        subtype="success",
        duration_ms=1200,
        duration_api_ms=1000,
        is_error=False,
        num_turns=1,
        session_id=session_id,
    )
    kwargs.update(overrides)
    return ResultMessage(**kwargs)


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

    async def _fake_persist(
        self, user_id, session_id, result_message, model=None, session=None, detached=False
    ):
        persisted.update(
            user_id=user_id, session_id=session_id, result=result_message, detached=detached
        )
        return PersistOutcome(
            messages=[{"type": "user", "content": "long analysis"}],
            terminal_info=derive_terminal_info(result_message),
        )

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


# ---------------------------------------------------------------------------
# Terminal-info derivation (silent-kill fix): budget/turn/API kills must be
# classified and never masquerade as clean completions.
# ---------------------------------------------------------------------------

from app.services.brain_agent_service import PersistOutcome, derive_terminal_info


def test_terminal_info_budget_kill():
    # Verified prod shape: budget kill carries no `result` text, only errors[].
    rm = _result_message(
        subtype="error_max_budget_usd",
        is_error=True,
        terminal_reason="budget_exhausted",
        errors=["Reached maximum budget ($2)"],
        total_cost_usd=2.04,
    )
    info = derive_terminal_info(rm)
    assert info["kind"] == "budget_exhausted"
    assert info["is_error"] is True
    assert info["reason_message"]
    assert "spending limit" in info["reason_message"]


def test_terminal_info_max_turns():
    rm = _result_message(
        subtype="error_max_turns",
        is_error=True,
        terminal_reason="max_turns",
        errors=["Reached maximum number of turns (25)"],
    )
    info = derive_terminal_info(rm)
    assert info["kind"] == "max_turns"
    assert "follow-up" in info["reason_message"]


def test_terminal_info_api_error_masquerades_as_success():
    # Regression trap: a post-retry 529 arrives as subtype="success" with
    # is_error=True + api_error_status — classification must NOT branch on
    # subtype alone.
    rm = _result_message(subtype="success", is_error=True, api_error_status=529)
    info = derive_terminal_info(rm)
    assert info["kind"] == "api_error"
    assert "overloaded" in info["reason_message"]

    rm500 = _result_message(subtype="success", is_error=True, api_error_status=500)
    assert derive_terminal_info(rm500)["kind"] == "api_error"
    assert "500" in derive_terminal_info(rm500)["reason_message"]


def test_terminal_info_interrupt_is_quiet_stopped():
    rm = _result_message(is_error=False, terminal_reason="aborted_streaming")
    info = derive_terminal_info(rm)
    assert info["kind"] == "stopped"
    assert info["is_error"] is False

    rm_tools = _result_message(is_error=False, terminal_reason="aborted_tools")
    assert derive_terminal_info(rm_tools)["kind"] == "stopped"


def test_terminal_info_unknown_error_carries_first_error():
    rm = _result_message(subtype="error_during_execution", is_error=True, errors=["boom"])
    info = derive_terminal_info(rm)
    assert info["kind"] == "unknown_error"
    assert "boom" in info["reason_message"]


def test_terminal_info_clean_success():
    info = derive_terminal_info(_result_message(terminal_reason="completed"))
    assert info["kind"] == "success"
    assert info["reason_message"] is None
    assert info["is_error"] is False


def test_terminal_info_defensive_on_minimal_object():
    # Lightweight fakes (and older SDKs) may lack the optional fields entirely.
    info = derive_terminal_info(SimpleNamespace(subtype="success"))
    assert info["kind"] == "success"
    assert info["terminal_reason"] is None
    assert info["errors"] is None


def test_chat_result_event_carries_terminal_fields(monkeypatch, _isolated_sessions):
    """A budget-killed run's `result` SSE event must explain itself."""
    kill = _result_message(
        subtype="error_max_budget_usd",
        is_error=True,
        terminal_reason="budget_exhausted",
        errors=["Reached maximum budget ($2)"],
        total_cost_usd=2.04,
    )
    fake_client = _FakeClient([kill])
    session = AgentSession(
        session_id="s-term",
        user_id=1,
        client=fake_client,
        created_at=_time.time(),
        last_activity=_time.time(),
    )

    async def _fake_get_or_create(self, user_id, session_id, *a, **kw):
        BrainAgentService._sessions[session_id] = session
        return session, False

    async def _fake_persist(
        self, user_id, session_id, result_message, model=None, session=None, detached=False
    ):
        return PersistOutcome(
            messages=[{"type": "assistant", "content": "partial"}],
            terminal_info=derive_terminal_info(result_message),
        )

    async def _fake_set_streaming(self, session_id, value):
        return None

    async def _fake_query(prompt):
        return None

    fake_client.query = _fake_query
    monkeypatch.setattr(BrainAgentService, "_get_or_create_session", _fake_get_or_create)
    monkeypatch.setattr(BrainAgentService, "_persist_completed_run", _fake_persist)
    monkeypatch.setattr(BrainAgentService, "_set_thread_streaming", _fake_set_streaming)
    monkeypatch.setattr(BrainAgentService, "_cleanup_stale_sessions", lambda self: None)

    class _NoBudgetDB:
        async def execute(self, *_a, **_kw):
            return SimpleNamespace(scalar_one_or_none=lambda: None)

    async def _run():
        service = BrainAgentService(db=_NoBudgetDB())
        events = []
        async for ev in service.chat(user_id=1, session_id="s-term", prompt="deep dive"):
            events.append(ev)
        return events

    events = asyncio.run(_run())
    result = next(e for e in events if e.event_type == "result")
    # New terminal fields present…
    assert result.data["kind"] == "budget_exhausted"
    assert result.data["is_error"] is True
    assert result.data["reason_message"]
    assert result.data["terminal_reason"] == "budget_exhausted"
    # …and the legacy keys unchanged.
    assert result.data["session_id"] == "s-term"
    assert result.data["cost_usd"] == 2.04
    assert result.data["messages"] == [{"type": "assistant", "content": "partial"}]


# ---------------------------------------------------------------------------
# Honest persistence on error terminals (_persist_completed_run)
# ---------------------------------------------------------------------------

import app.services.brain_agent_service as _svc_mod


def _budget_kill_result():
    return _result_message(
        subtype="error_max_budget_usd",
        is_error=True,
        terminal_reason="budget_exhausted",
        errors=["Reached maximum budget ($2)"],
        total_cost_usd=2.04,
    )


def _persist_harness(monkeypatch, transcript_messages, sleeps):
    """Wire _persist_completed_run's collaborators to fakes; return saved dict."""
    saved = {}

    def _fake_get_session_messages(session_id, directory):
        return list(transcript_messages)

    async def _fake_save(
        self, session_id, user_id, messages, model=None, cost_delta_usd=None, num_turns=0
    ):
        saved.update(session_id=session_id, messages=messages, cost_delta_usd=cost_delta_usd)
        return None

    async def _fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(_svc_mod, "get_session_messages", _fake_get_session_messages)
    monkeypatch.setattr(BrainAgentService, "_save_thread_to_db", _fake_save)
    monkeypatch.setattr(_svc_mod.asyncio, "sleep", _fake_sleep)
    return saved


def test_persist_error_terminal_appends_marker_without_waiting(monkeypatch):
    # Transcript legitimately ends on a user-type entry (tool_result) after a
    # budget kill — the persist must not spin waiting for an assistant tail.
    sleeps = []
    saved = _persist_harness(
        monkeypatch,
        [_sm("user", "u1", "deep question"), _sm("assistant", "a1", "working on it")],
        sleeps,
    )
    # Make the converted list end on a user turn:
    transcript = [_sm("assistant", "a1", "working on it"), _sm("user", "u2", "tool output")]
    monkeypatch.setattr(_svc_mod, "get_session_messages", lambda session_id, directory: transcript)

    service = BrainAgentService(db=None)
    outcome = asyncio.run(
        service._persist_completed_run(
            user_id=1, session_id="s-err", result_message=_budget_kill_result()
        )
    )

    assert sleeps == []  # zero retry sleeps on an error terminal
    assert outcome.terminal_info["kind"] == "budget_exhausted"
    tail = outcome.messages[-1]
    assert tail["type"] == "assistant"
    assert tail["terminal"]["kind"] == "budget_exhausted"
    assert "spending limit" in tail["content"]
    assert saved["messages"][-1] is tail  # marker was persisted


def test_persist_success_with_tail_present_appends_no_marker(monkeypatch):
    sleeps = []
    saved = _persist_harness(
        monkeypatch,
        [_sm("user", "u1", "question"), _sm("assistant", "a1", "the answer")],
        sleeps,
    )
    service = BrainAgentService(db=None)
    outcome = asyncio.run(
        service._persist_completed_run(
            user_id=1, session_id="s-ok", result_message=_result_message()
        )
    )
    assert sleeps == []
    assert outcome.terminal_info["kind"] == "success"
    assert all("terminal" not in m for m in outcome.messages)
    assert saved["messages"] == outcome.messages


def test_persist_success_missing_tail_still_retries(monkeypatch):
    sleeps = []
    _persist_harness(
        monkeypatch,
        [_sm("user", "u1", "question")],  # no assistant tail, ever
        sleeps,
    )
    service = BrainAgentService(db=None)
    outcome = asyncio.run(
        service._persist_completed_run(
            user_id=1, session_id="s-lag", result_message=_result_message()
        )
    )
    assert len(sleeps) == 5  # 6 attempts, sleeps between them
    assert outcome.terminal_info["kind"] == "success"
    # No result text to synthesize from → messages persist as-is, no synthetic.
    assert outcome.messages[-1]["type"] == "user"


def test_persist_success_tool_call_tail_synthesizes_answer(monkeypatch):
    # The prod repro: the CLI flushes the final assistant TEXT after the
    # ResultMessage, so the transcript still ends on the assistant tool-call
    # entry (type "assistant" but empty content). The old type-only tail check
    # accepted it and silently dropped the answer; now the answer is
    # synthesized from ResultMessage.result.
    sleeps = []
    saved = _persist_harness(
        monkeypatch,
        [
            _sm("user", "u1", [{"type": "text", "text": "how many turbines?"}]),
            _sm(
                "assistant",
                "a1",
                [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "q"}}],
            ),
            _sm(
                "user",
                "u2",
                [{"type": "tool_result", "tool_use_id": "t1", "content": "95899", "is_error": False}],
            ),
        ],
        sleeps,
    )
    service = BrainAgentService(db=None)
    outcome = asyncio.run(
        service._persist_completed_run(
            user_id=1,
            session_id="s-toolcall-tail",
            result_message=_result_message(result="There are 95,899 turbine units."),
        )
    )
    assert len(sleeps) == 5  # retried the full window before synthesizing
    tail = outcome.messages[-1]
    assert tail["type"] == "assistant"
    assert tail["content"] == "There are 95,899 turbine units."
    assert "terminal" not in tail  # a synthesized answer is a normal message
    assert "toolCalls" not in tail
    # The tool-call message before it is preserved from the transcript.
    assert outcome.messages[-2].get("toolCalls")
    assert saved["messages"] == outcome.messages


def test_persist_success_answer_tail_breaks_immediately_on_text(monkeypatch):
    # A transcript whose tail already carries the answer text must not spin
    # or synthesize — first read wins.
    sleeps = []
    saved = _persist_harness(
        monkeypatch,
        [
            _sm("user", "u1", "question"),
            _sm(
                "assistant",
                "a1",
                [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {}}],
            ),
            _sm("assistant", "a2", "the flushed answer"),
        ],
        sleeps,
    )
    service = BrainAgentService(db=None)
    outcome = asyncio.run(
        service._persist_completed_run(
            user_id=1,
            session_id="s-flushed",
            result_message=_result_message(result="the flushed answer"),
        )
    )
    assert sleeps == []
    assert outcome.messages[-1]["content"] == "the flushed answer"
    assert len(saved["messages"]) == len(outcome.messages)


def test_persist_success_failed_transcript_synthesizes_onto_stored(monkeypatch):
    # get_session_messages raises on a success terminal → never wipe the
    # thread: the synthesized answer is appended to what's already stored.
    sleeps = []
    saved = _persist_harness(monkeypatch, [], sleeps)

    def _boom(session_id, directory):
        raise RuntimeError("transcript gone")

    monkeypatch.setattr(_svc_mod, "get_session_messages", _boom)

    stored = [{"type": "user", "content": "original question"}]

    async def _fake_load(self, session_id):
        return list(stored)

    monkeypatch.setattr(BrainAgentService, "_load_thread_messages", _fake_load)

    service = BrainAgentService(db=None)
    outcome = asyncio.run(
        service._persist_completed_run(
            user_id=1,
            session_id="s-gone-ok",
            result_message=_result_message(result="the answer"),
        )
    )
    assert outcome.messages[0] == stored[0]
    assert outcome.messages[-1]["content"] == "the answer"
    assert "terminal" not in outcome.messages[-1]
    assert saved["messages"] == outcome.messages


def test_persist_error_terminal_with_failed_transcript_appends_to_stored(monkeypatch):
    # get_session_messages raises → never wipe the thread; append the marker
    # to what's already stored in the DB.
    sleeps = []
    saved = _persist_harness(monkeypatch, [], sleeps)

    def _boom(session_id, directory):
        raise RuntimeError("transcript gone")

    monkeypatch.setattr(_svc_mod, "get_session_messages", _boom)

    stored = [{"type": "user", "content": "original question"}]

    async def _fake_load(self, session_id):
        return list(stored)

    monkeypatch.setattr(BrainAgentService, "_load_thread_messages", _fake_load)

    service = BrainAgentService(db=None)
    outcome = asyncio.run(
        service._persist_completed_run(
            user_id=1, session_id="s-gone", result_message=_budget_kill_result()
        )
    )
    assert outcome.messages[0] == stored[0]  # existing messages preserved
    assert outcome.messages[-1]["terminal"]["kind"] == "budget_exhausted"
    assert saved["messages"] == outcome.messages


def test_persist_stopped_marker_is_not_an_error(monkeypatch):
    sleeps = []
    _persist_harness(
        monkeypatch,
        [_sm("assistant", "a1", "partial"), _sm("user", "u2", "tool output")],
        sleeps,
    )
    service = BrainAgentService(db=None)
    rm = _result_message(is_error=False, terminal_reason="aborted_streaming")
    outcome = asyncio.run(
        service._persist_completed_run(user_id=1, session_id="s-stop", result_message=rm)
    )
    tail = outcome.messages[-1]
    assert tail["terminal"]["kind"] == "stopped"
    assert tail["terminal"]["is_error"] is False
    assert tail["content"] == "You stopped this response."


# ---------------------------------------------------------------------------
# Budget semantics: dynamic SDK cap + per-run delta accounting
# ---------------------------------------------------------------------------

from app.services.brain_agent_service import PROFILES, compute_session_budget


def test_no_profile_declares_a_static_sdk_budget():
    # Regression trap: the SDK's max_budget_usd is process-cumulative, so a
    # static per-profile value silently caps the whole conversation (the $2
    # client bug). The SDK cap must only ever be computed at session creation.
    for name, profile in PROFILES.items():
        assert "max_budget_usd" not in profile, name
        assert profile["max_thread_budget_usd"] > 0, name
        assert profile["min_session_budget_usd"] > 0, name
    assert PROFILES["client"]["max_thread_budget_usd"] <= PROFILES["admin"]["max_thread_budget_usd"]
    assert PROFILES["client"]["min_session_budget_usd"] <= PROFILES["admin"]["min_session_budget_usd"]


def test_compute_session_budget_table():
    client = PROFILES["client"]  # cap 10, floor 1
    assert compute_session_budget(client, None) == 10.0
    assert compute_session_budget(client, 0) == 10.0
    assert compute_session_budget(client, 4.5) == 5.5
    assert compute_session_budget(client, 9.5) == 1.0  # floored
    assert compute_session_budget(client, 12.0) == 1.0  # already over — floor, guard refuses anyway
    from decimal import Decimal

    assert compute_session_budget(client, Decimal("2.5")) == 7.5


def _session_for_delta():
    return AgentSession(
        session_id="s-cost",
        user_id=1,
        client=None,
        created_at=_time.time(),
        last_activity=_time.time(),
    )


def test_take_cost_delta_chain_and_recreation():
    session = _session_for_delta()
    take = BrainAgentService._take_cost_delta
    assert take(session, _result_message(total_cost_usd=1.0)) == 1.0
    assert take(session, _result_message(total_cost_usd=1.6)) == pytest.approx(0.6)
    # Recreation: fresh AgentSession = fresh CLI counter — delta restarts.
    fresh = _session_for_delta()
    assert take(fresh, _result_message(total_cost_usd=0.4)) == pytest.approx(0.4)
    # No cost reported → no delta.
    assert take(fresh, _result_message()) is None
    # Impossible negative (should never happen) clamps to 0.
    assert take(fresh, _result_message(total_cost_usd=0.1)) == 0.0


def test_save_thread_accumulates_cost_delta():
    from decimal import Decimal

    thread = SimpleNamespace(
        messages=[], message_count=0, is_streaming=True, title="t",
        total_cost_usd=Decimal("2.0"), total_turns=3,
    )

    class _ThreadDB:
        def __init__(self):
            self.committed = False

        async def execute(self, *_a, **_kw):
            return SimpleNamespace(scalar_one_or_none=lambda: thread)

        async def commit(self):
            self.committed = True

        async def rollback(self):
            pass

    service = BrainAgentService(db=_ThreadDB())
    total = asyncio.run(
        service._save_thread_to_db(
            session_id="s", user_id=1,
            messages=[{"type": "user", "content": "q"}],
            cost_delta_usd=0.6, num_turns=2,
        )
    )
    assert thread.total_cost_usd == Decimal("2.6")
    assert total == pytest.approx(2.6)
    assert thread.total_turns == 5
    assert thread.is_streaming is False


def test_persist_across_recreation_accumulates_not_overwrites(monkeypatch):
    """The exact prod undercount bug: two session processes, one thread."""
    sleeps = []
    _persist_harness(
        monkeypatch,
        [_sm("user", "u1", "q"), _sm("assistant", "a1", "answer")],
        sleeps,
    )

    totals = {"value": 0.0}

    async def _accumulating_save(
        self, session_id, user_id, messages, model=None, cost_delta_usd=None, num_turns=0
    ):
        if cost_delta_usd is not None:
            totals["value"] += cost_delta_usd
        return totals["value"]

    monkeypatch.setattr(BrainAgentService, "_save_thread_to_db", _accumulating_save)

    service = BrainAgentService(db=None)
    # Process 1: one turn costing 1.0.
    s1 = _session_for_delta()
    out1 = asyncio.run(
        service._persist_completed_run(
            user_id=1, session_id="thread-x",
            result_message=_result_message(total_cost_usd=1.0), session=s1,
        )
    )
    # Process 2 (recreation): its cumulative counter restarts at 0.7.
    s2 = _session_for_delta()
    out2 = asyncio.run(
        service._persist_completed_run(
            user_id=1, session_id="thread-x",
            result_message=_result_message(total_cost_usd=0.7), session=s2,
        )
    )
    assert out1.cost_delta_usd == 1.0
    assert out2.cost_delta_usd == pytest.approx(0.7)
    assert out2.thread_total_cost_usd == pytest.approx(1.7)  # not 0.7


# --- Files/images survive the transcript rebuild (2026-09-05) ---------------


def _img(name, session_id="s-img", user_id=1):
    return {"url": f"/brain-agent/files/{user_id}/{session_id}/{name}", "filename": name}


def _session_with_run_files(*names) -> AgentSession:
    session = AgentSession(
        session_id="s-img",
        user_id=1,
        client=None,
        created_at=_time.time(),
        last_activity=_time.time(),
    )
    session.run_files = [_img(n) for n in names]
    return session


def test_persist_attaches_run_files_and_carries_stored_images(monkeypatch):
    sleeps = []
    saved = _persist_harness(
        monkeypatch,
        [
            _sm("user", "u1", "q0"),
            _sm("assistant", "a1", "a0"),
            _sm("user", "u2", "q1"),
            _sm("assistant", "a2", "a1"),
        ],
        sleeps,
    )

    async def _stored(self, session_id):
        # What the thread looked like before this run: turn 0 had a chart,
        # and the client persisted the new prompt at send time.
        return [
            {"id": "u1", "type": "user", "content": "q0"},
            {"id": "a1", "type": "assistant", "content": "a0", "images": [_img("old.png")]},
            {"id": "u2", "type": "user", "content": "q1"},
        ]

    monkeypatch.setattr(BrainAgentService, "_load_thread_messages", _stored)

    service = BrainAgentService(db=None)
    outcome = asyncio.run(
        service._persist_completed_run(
            user_id=1,
            session_id="s-img",
            result_message=_result_message(),
            session=_session_with_run_files("chart.png"),
        )
    )

    msgs = saved["messages"]
    assert [m["content"] for m in msgs] == ["q0", "a0", "q1", "a1"]
    assert msgs[1]["images"] == [_img("old.png")]
    assert msgs[3]["images"] == [_img("chart.png")]
    assert "images" not in msgs[0] and "images" not in msgs[2]
    assert outcome.messages == msgs


def test_persist_error_terminal_keeps_run_files_off_the_marker(monkeypatch):
    sleeps = []
    saved = _persist_harness(
        monkeypatch,
        [_sm("user", "u1", "q0"), _sm("assistant", "a1", "working on it")],
        sleeps,
    )

    service = BrainAgentService(db=None)
    asyncio.run(
        service._persist_completed_run(
            user_id=1,
            session_id="s-img",
            result_message=_budget_kill_result(),
            session=_session_with_run_files("chart.png"),
        )
    )

    msgs = saved["messages"]
    assert msgs[-1]["terminal"]["kind"] == "budget_exhausted"
    assert "images" not in msgs[-1]
    assert msgs[-2]["content"] == "working on it"
    assert msgs[-2]["images"] == [_img("chart.png")]


def test_persist_synthetic_tail_carries_run_files(monkeypatch):
    sleeps = []
    saved = _persist_harness(
        monkeypatch,
        [
            _sm("user", "u1", "q0"),
            _sm(
                "assistant",
                "a1",
                [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "python chart.py"}}],
            ),
        ],
        sleeps,
    )

    service = BrainAgentService(db=None)
    asyncio.run(
        service._persist_completed_run(
            user_id=1,
            session_id="s-img",
            result_message=_result_message(result="Here is the chart."),
            session=_session_with_run_files("chart.png"),
        )
    )

    tail = saved["messages"][-1]
    assert tail["content"] == "Here is the chart."
    assert tail["images"] == [_img("chart.png")]
    assert "images" not in saved["messages"][-2]  # the tool-call entry


def test_finish_orphaned_run_records_late_files_before_persisting(monkeypatch):
    fake_client = _FakeClient([_result_message()])
    session = _detached_session(fake_client)
    seen = {}

    async def _fake_persist(
        self, user_id, session_id, result_message, model=None, session=None, detached=False
    ):
        seen["run_files"] = list(session.run_files)
        return PersistOutcome(messages=[], terminal_info=derive_terminal_info(result_message))

    async def _no_upload(self, user_id, session_id, filename, file_path):
        seen.setdefault("uploaded", []).append(filename)

    monkeypatch.setattr(BrainAgentService, "_persist_completed_run", _fake_persist)
    monkeypatch.setattr(BrainAgentService, "_scan_for_new_files", lambda self, s: ["late.png"])
    monkeypatch.setattr(BrainAgentService, "_upload_file_to_s3", _no_upload)

    service = BrainAgentService(db=None)
    asyncio.run(service._finish_orphaned_run(session))

    assert seen["uploaded"] == ["late.png"]
    assert seen["run_files"] == [
        {"url": f"/brain-agent/files/{USER_ID_DETACH}/s-detach/late.png", "filename": "late.png"}
    ]
