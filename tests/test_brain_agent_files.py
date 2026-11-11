"""Pure helpers that keep agent-produced files on the right chat bubble."""

from app.services.brain_agent_files import (
    attach_run_files,
    dedupe_images,
    merge_images_by_turn,
    split_turns,
)

NOW = 1_700_000_000_000
HISTORY = "<conversation_history>\nUser: q0\nAssistant: a0\n</conversation_history>\n\n"


def U(text, **extra):
    return {"id": f"u-{text}", "type": "user", "content": text, "timestamp": 1, **extra}


def A(text, images=None, **extra):
    msg = {"id": f"a-{text}", "type": "assistant", "content": text, "timestamp": 2, **extra}
    if images:
        msg["images"] = list(images)
    return msg


def img(name):
    return {"url": f"/brain-agent/files/1/t/{name}", "filename": name}


def images_of(messages):
    return [[i["filename"] for i in m.get("images", [])] for m in messages]


class TestSplitTurns:
    def test_buckets_at_user_messages(self):
        turns = split_turns([U("q0"), A("a0"), A("a0b"), U("q1"), A("a1")])
        assert [len(t) for t in turns] == [3, 2]

    def test_leading_assistant_prefix_is_its_own_bucket(self):
        turns = split_turns([A("orphan"), U("q0"), A("a0")])
        assert turns[0] == [A("orphan")]
        assert turns[1][0]["type"] == "user"


class TestDedupe:
    def test_first_filename_wins_and_junk_is_dropped(self):
        out = dedupe_images([img("a.png"), {"url": "x"}, "junk", img("a.png"), img("b.csv")])
        assert [i["filename"] for i in out] == ["a.png", "b.csv"]


class TestMergeImagesByTurn:
    def test_normal_turn_keeps_earlier_images_and_leaves_the_new_turn_bare(self):
        stored = [U("q0"), A("a0", [img("old.png")])]
        rebuilt = [U("q0"), A("a0"), U("q1"), A("a1")]

        out = merge_images_by_turn(stored, rebuilt, now_ms=NOW)

        assert images_of(out) == [[], ["old.png"], [], []]
        assert stored[1]["images"] == [img("old.png")]  # stored untouched

    def test_stored_may_already_hold_the_current_user_message(self):
        # The client persists at send, so the stored thread ends on the
        # current prompt with no answer yet.
        stored = [U("q0"), A("a0", [img("old.png")]), U("q1")]
        rebuilt = [U("q0"), A("a0"), U("q1"), A("a1")]

        out = merge_images_by_turn(stored, rebuilt, now_ms=NOW)

        assert images_of(out) == [[], ["old.png"], [], []]

    def test_recreated_session_suffix_pairs_from_the_end(self):
        stored = [
            U("q0"),
            A("a0", [img("c0.png")]),
            U("q1"),
            A("a1", [img("c1.png")]),
            U("q2"),
            A("a2", [img("c2.png")]),
            U("q3"),
        ]
        # After a recreation the transcript starts at the folded history + q2.
        rebuilt = [U(HISTORY + "q2"), A("a2"), U("q3"), A("a3")]

        out = merge_images_by_turn(stored, rebuilt, now_ms=NOW)

        assert images_of(out) == [[], ["c2.png"], [], []]

    def test_recreated_first_turn_only_has_nothing_to_pair(self):
        stored = [U("q0"), A("a0", [img("c0.png")]), U("q1")]
        rebuilt = [U(HISTORY + "q1"), A("a1")]

        out = merge_images_by_turn(stored, rebuilt, now_ms=NOW)

        assert images_of(out) == [[], []]

    def test_regenerate_repeats_the_prompt_and_keeps_images_on_the_old_attempt(self):
        stored = [U("q0"), A("a0", [img("c0.png")])]
        rebuilt = [U("q0"), A("a0"), U("q0"), A("a0-again")]

        out = merge_images_by_turn(stored, rebuilt, now_ms=NOW)

        assert images_of(out) == [[], ["c0.png"], [], []]

    def test_repeated_continuation_prompt_prefers_the_longer_chain(self):
        stored = [U("q0"), A("a0", [img("c0.png")]), U("continue"), A("a1", [img("c1.png")])]
        rebuilt = [U("q0"), A("a0"), U("continue"), A("a1"), U("continue"), A("a2")]

        out = merge_images_by_turn(stored, rebuilt, now_ms=NOW)

        assert images_of(out) == [[], ["c0.png"], [], ["c1.png"], [], []]

    def test_attachment_note_prefix_still_pairs(self):
        stored = [U("q0"), A("a0", [img("c0.png")]), U("q1")]
        rebuilt = [U("<attachments>report.csv</attachments>\n\nq0"), A("a0"), U("q1"), A("a1")]

        out = merge_images_by_turn(stored, rebuilt, now_ms=NOW)

        assert images_of(out) == [[], ["c0.png"], [], []]

    def test_turn_without_assistant_gets_an_images_only_carrier(self):
        stored = [U("q0"), A("a0", [img("c0.png")])]
        rebuilt = [U("q0")]

        out = merge_images_by_turn(stored, rebuilt, now_ms=NOW)

        assert len(out) == 2
        carrier = out[1]
        assert carrier["type"] == "assistant" and carrier["content"] == ""
        assert carrier["id"].startswith("files-") and carrier["timestamp"] == NOW
        assert [i["filename"] for i in carrier["images"]] == ["c0.png"]

    def test_images_are_deduped_by_filename(self):
        stored = [U("q0"), A("a0", [img("c0.png")]), A("a0b", [img("c0.png"), img("d.csv")])]
        rebuilt = [U("q0"), A("a0", [img("c0.png")])]

        out = merge_images_by_turn(stored, rebuilt, now_ms=NOW)

        assert images_of(out) == [[], ["c0.png", "d.csv"]]

    def test_images_go_on_the_last_assistant_of_the_turn(self):
        stored = [U("q0"), A("a0", [img("c0.png")])]
        rebuilt = [U("q0"), A("tool-call"), A("answer")]

        out = merge_images_by_turn(stored, rebuilt, now_ms=NOW)

        assert images_of(out) == [[], [], ["c0.png"]]

    def test_unpaired_stored_turns_are_dropped_not_guessed(self):
        stored = [U("other question"), A("x", [img("c0.png")])]
        rebuilt = [U("q0"), A("a0")]

        out = merge_images_by_turn(stored, rebuilt, now_ms=NOW)

        assert images_of(out) == [[], []]

    def test_empty_inputs(self):
        rebuilt = [U("q0"), A("a0")]
        assert merge_images_by_turn([], rebuilt, now_ms=NOW) == rebuilt
        assert merge_images_by_turn([U("q0"), A("a0", [img("c.png")])], [], now_ms=NOW) == []


class TestAttachRunFiles:
    def test_files_land_on_the_last_turns_answer_only(self):
        messages = [U("q0"), A("a0"), U("q1"), A("tool-call"), A("a1")]

        out = attach_run_files(messages, [img("chart.png"), img("data.csv")], now_ms=NOW)

        assert images_of(out) == [[], [], [], [], ["chart.png", "data.csv"]]

    def test_merges_with_existing_images_and_dedupes(self):
        messages = [U("q0"), A("a0", [img("chart.png")])]

        out = attach_run_files(messages, [img("chart.png"), img("more.png")], now_ms=NOW)

        assert images_of(out) == [[], ["chart.png", "more.png"]]

    def test_no_files_or_no_messages_is_a_noop(self):
        messages = [U("q0"), A("a0")]
        assert attach_run_files(messages, [], now_ms=NOW) is messages
        assert attach_run_files([], [img("chart.png")], now_ms=NOW) == []

    def test_last_turn_without_assistant_gets_a_carrier(self):
        out = attach_run_files([U("q0"), A("a0"), U("q1")], [img("chart.png")], now_ms=NOW)

        assert len(out) == 4
        assert out[3]["content"] == "" and images_of(out)[3] == ["chart.png"]


# --- EPR-185: folded history, compaction, session base --------------------

from app.services.brain_agent_files import (  # noqa: E402
    history_base,
    is_compact_summary,
    is_machine_title,
    session_messages,
    strip_folded_history,
    title_from_prompt,
)

NOTE = "<attachments>\nThe user has attached the following file(s):\n- data.csv\n</attachments>"
SUMMARY = (
    "This session is being continued from a previous conversation that ran out of "
    "context. The conversation is summarized below: ..."
)


def test_strip_folded_history_keeps_only_the_prompt():
    assert strip_folded_history(HISTORY + "q1") == "q1"
    assert strip_folded_history("plain prompt") == "plain prompt"
    assert strip_folded_history(None) is None


def test_strip_folded_history_keeps_the_attachment_note_before_the_block():
    assert strip_folded_history(f"{NOTE}\n\n{HISTORY}q1") == f"{NOTE}\n\nq1"


def test_strip_folded_history_handles_nested_blocks():
    nested = f"<conversation_history>\nHuman: {HISTORY}q0\n</conversation_history>\n\nq1"
    assert strip_folded_history(nested) == "q1"


def test_is_compact_summary():
    assert is_compact_summary(U(SUMMARY))
    assert not is_compact_summary(A(SUMMARY))
    assert not is_compact_summary(U("q1"))


def test_title_from_prompt_strips_machine_text():
    assert title_from_prompt(HISTORY + "how many farms?") == "how many farms?"
    assert title_from_prompt(f"{NOTE}\n\nsummarise this file") == "summarise this file"
    assert title_from_prompt("x" * 200) == "x" * 80


def test_is_machine_title():
    assert is_machine_title(None) and is_machine_title("  ")
    assert is_machine_title("<conversation_history> This is a continu")
    assert is_machine_title("<attachments> The user has attached th")
    assert not is_machine_title("My Norway analysis")


def test_history_base_drops_a_trailing_copy_of_the_current_prompt():
    stored = [U("q0"), A("a0"), U("q1")]
    assert history_base(stored, "q1") == [U("q0"), A("a0")]
    # an unanswered DIFFERENT prompt is history, keep it
    assert history_base(stored, "q2") == stored
    # answered turns are never dropped, even when the prompt repeats
    assert history_base([U("continue"), A("a0")], "continue") == [U("continue"), A("a0")]


def test_session_messages_strips_the_folded_prompt():
    rebuilt = [U(HISTORY + "q1"), A("a1")]
    out = session_messages(rebuilt, [], "q1", now_ms=NOW)
    assert [m["content"] for m in out] == ["q1", "a1"]
    assert rebuilt[0]["content"] == HISTORY + "q1"  # input untouched


def test_session_messages_mid_turn_compaction_reinserts_the_prompt():
    previous = [U("q1"), A("a1")]
    rebuilt = [U(SUMMARY), A("a2-after-compaction")]
    out = session_messages(rebuilt, previous, "q2", now_ms=NOW)
    assert [m["type"] for m in out] == ["user", "assistant", "user", "assistant"]
    assert [m["content"] for m in out] == ["q1", "a1", "q2", "a2-after-compaction"]


def test_session_messages_earlier_compaction_adds_only_new_messages():
    # Compaction happened during q2; q3 is this run. Everything up to a3's
    # predecessor is already in `previous` (matched by transcript id).
    previous = [U("q1"), A("a1"), U("q2"), A("a2")]
    rebuilt = [U(SUMMARY), A("a2"), U("q3"), A("a3")]
    out = session_messages(rebuilt, previous, "q3", now_ms=NOW)
    assert [m["content"] for m in out] == ["q1", "a1", "q2", "a2", "q3", "a3"]


def test_session_messages_compaction_at_prompt_time_needs_no_reinsert():
    previous = [U("q1"), A("a1")]
    rebuilt = [U(SUMMARY), U("q2"), A("a2")]
    out = session_messages(rebuilt, previous, "q2", now_ms=NOW)
    assert [m["content"] for m in out] == ["q1", "a1", "q2", "a2"]
