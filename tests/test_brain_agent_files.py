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
