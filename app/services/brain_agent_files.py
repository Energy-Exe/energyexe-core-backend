"""Re-attach agent-produced files (charts, CSVs) to persisted thread messages.

Files the agent produces reach the client only as SSE ``image``/``file``
events during a run. The persisted thread is rebuilt from the SDK transcript
after every run, and the transcript knows nothing about those files, so until
2026-09-05 every earlier bubble's ``images`` vanished on the next turn — in
the UI (which adopts the server's list) and then in the DB (which the UI PUT
back). Two pure helpers fix that at the source:

* ``attach_run_files`` — this run's files onto this run's answer bubble;
* ``merge_images_by_turn`` — the stored thread's images onto the rebuilt list,
  paired turn-by-turn from the END. From the end, because after a session
  recreation the rebuilt transcript is a SUFFIX of the thread whose first
  prompt carries the folded history (``<conversation_history>…``), so index 0
  of the two lists is not the same turn. A stored turn that cannot be paired
  keeps nothing — never guess a position.

Message shape is the persisted JSON: ``type`` (user|assistant), ``content``,
optional ``images`` = [{"url", "filename"}] (``file`` downloads share the same
array on the client). Nothing here touches the DB.
"""

import uuid
from typing import Any, Dict, Iterable, List, Tuple

import structlog

logger = structlog.get_logger(__name__)

HISTORY_CLOSE_TAG = "</conversation_history>"

Message = Dict[str, Any]
Turn = List[Message]
Image = Dict[str, str]


def _prompt_key(content: Any) -> str:
    """The user's own prompt text: strip a folded-history prefix, if any."""
    text = content.strip() if isinstance(content, str) else ""
    if HISTORY_CLOSE_TAG in text:
        text = text.rsplit(HISTORY_CLOSE_TAG, 1)[1].strip()
    return text


def _same_turn(a: Message, b: Message) -> bool:
    """Two user messages are the same turn when one prompt ends with the other.

    ``endswith`` both ways tolerates the server-side ``<attachments>`` note that
    is *prepended* to a prompt on the transcript side but absent on the client
    side, and vice versa.
    """
    if a.get("type") != "user" or b.get("type") != "user":
        return False
    ka, kb = _prompt_key(a.get("content")), _prompt_key(b.get("content"))
    return bool(ka) and bool(kb) and (ka.endswith(kb) or kb.endswith(ka))


def split_turns(messages: Iterable[Message]) -> List[Turn]:
    """Bucket messages at each user message. A leading assistant-only run is
    its own bucket (no user key, so it never pairs)."""
    turns: List[Turn] = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if msg.get("type") == "user" or not turns:
            turns.append([msg])
        else:
            turns[-1].append(msg)
    return turns


def dedupe_images(images: Iterable[Any]) -> List[Image]:
    """First occurrence per filename wins; entries without a filename are dropped."""
    seen = set()
    out: List[Image] = []
    for img in images:
        if not isinstance(img, dict):
            continue
        name = img.get("filename")
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(img)
    return out


def _turn_images(turn: Turn) -> List[Image]:
    collected: List[Any] = []
    for msg in turn:
        if msg.get("type") == "assistant":
            collected.extend(msg.get("images") or [])
    return dedupe_images(collected)


def _carrier(images: List[Image], now_ms: int) -> Message:
    """An images-only assistant message. The UIs keep empty assistant messages
    that carry images (``isEmptyStub``) and merge them into the adjacent bubble."""
    return {
        "id": f"files-{uuid.uuid4()}",
        "type": "assistant",
        "content": "",
        "timestamp": now_ms,
        "images": images,
    }


def attach_images_to_turn(turn: Turn, images: Iterable[Any], *, now_ms: int) -> None:
    """Attach ``images`` to the LAST assistant message of ``turn`` (in place).
    A turn with no assistant message gets an images-only carrier appended."""
    images = dedupe_images(images)
    if not images:
        return
    for msg in reversed(turn):
        if msg.get("type") == "assistant":
            msg["images"] = dedupe_images([*(msg.get("images") or []), *images])
            return
    turn.append(_carrier(images, now_ms))


def attach_run_files(
    messages: List[Message], run_files: Iterable[Any], *, now_ms: int
) -> List[Message]:
    """This run's files onto the answer bubble of the last turn.

    Call BEFORE appending a terminal marker so the marker never carries
    images. An empty ``messages`` is returned unchanged: creating a lone
    carrier would replace the thread's stored history on save.
    """
    run_files = dedupe_images(run_files)
    if not run_files or not messages:
        return messages
    turns = split_turns(messages)
    attach_images_to_turn(turns[-1], run_files, now_ms=now_ms)
    return [msg for turn in turns for msg in turn]


def _chain(
    s_turns: List[Turn], r_turns: List[Turn], skip_s: int, skip_r: int
) -> List[Tuple[int, int]]:
    """Pairs (stored_idx, rebuilt_idx) walking both lists from the end while
    the user prompts agree."""
    pairs: List[Tuple[int, int]] = []
    i, j = len(s_turns) - 1 - skip_s, len(r_turns) - 1 - skip_r
    while i >= 0 and j >= 0 and _same_turn(s_turns[i][0], r_turns[j][0]):
        pairs.append((i, j))
        i -= 1
        j -= 1
    return pairs


def merge_images_by_turn(
    stored: List[Message], rebuilt: List[Message], *, now_ms: int
) -> List[Message]:
    """Return ``rebuilt`` with the images of ``stored`` re-attached per turn.

    Three end-anchored alignments are tried — the lists' tails may differ by
    one turn (the current turn is not stored yet; a regenerate repeats the
    prompt; a continuation prompt repeats verbatim) — and the longest
    prompt-consistent chain wins. Mutates the message dicts of ``rebuilt``,
    never ``stored``.
    """
    s_turns, r_turns = split_turns(stored), split_turns(rebuilt)
    if not s_turns or not r_turns:
        return list(rebuilt)

    candidates = {
        (0, 0): _chain(s_turns, r_turns, 0, 0),
        (1, 0): _chain(s_turns, r_turns, 1, 0),
        (0, 1): _chain(s_turns, r_turns, 0, 1),
    }

    def _prefer(key: Tuple[int, int]) -> bool:
        # Tie-break: skip the tail of the LONGER list (it holds the turn the
        # other lacks); equal lengths -> straight alignment.
        return {
            (0, 0): len(s_turns) == len(r_turns),
            (1, 0): len(s_turns) > len(r_turns),
            (0, 1): len(r_turns) > len(s_turns),
        }[key]

    best = max(candidates, key=lambda key: (len(candidates[key]), _prefer(key)))
    pairs = candidates[best]
    for i, j in pairs:
        attach_images_to_turn(r_turns[j], _turn_images(s_turns[i]), now_ms=now_ms)

    paired = {i for i, _ in pairs}
    dropped = sum(1 for i, turn in enumerate(s_turns) if i not in paired and _turn_images(turn))
    if dropped:
        logger.warning(
            "brain_agent_images_unaligned",
            stored_turns=len(s_turns),
            rebuilt_turns=len(r_turns),
            paired_turns=len(pairs),
            dropped_turns_with_images=dropped,
        )
    return [msg for turn in r_turns for msg in turn]
