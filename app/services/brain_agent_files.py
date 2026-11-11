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

HISTORY_OPEN_TAG = "<conversation_history>"
HISTORY_CLOSE_TAG = "</conversation_history>"
ATTACHMENTS_OPEN_TAG = "<attachments>"
ATTACHMENTS_CLOSE_TAG = "</attachments>"
# Text the bundled CLI opens its auto-compaction summary with. The SDK's
# SessionMessage carries no isCompactSummary flag, so the text is the only
# signal (claude_agent_sdk 0.2.x; get_session_messages keeps the summary and
# drops everything before it).
COMPACT_SUMMARY_PREFIX = "This session is being continued from a previous conversation"
# Prefixes that mark a title derived from a server-rewritten prompt rather
# than from anything the user typed or chose.
MACHINE_TITLE_PREFIXES = (HISTORY_OPEN_TAG, ATTACHMENTS_OPEN_TAG, "<user_input>")

Message = Dict[str, Any]
Turn = List[Message]
Image = Dict[str, str]


def _prompt_key(content: Any) -> str:
    """The user's own prompt text: strip a folded-history prefix and the
    ``<user_input>`` wrapper some profiles add (mirrors the client's promptKey)."""
    text = content.strip() if isinstance(content, str) else ""
    if HISTORY_CLOSE_TAG in text:
        text = text.rsplit(HISTORY_CLOSE_TAG, 1)[1].strip()
    return text.replace("<user_input>", "").replace("</user_input>", "").strip()


def strip_folded_history(content: Any) -> Any:
    """Drop a ``<conversation_history>`` block from a stored user prompt.

    A recreated agent session gets the earlier chat folded into its first
    prompt; the transcript records that whole prompt as the user's message.
    Keeps an ``<attachments>`` note (it sits BEFORE the block, see
    BrainAgentService.chat) and the text after the LAST close tag — damaged
    threads can carry nested blocks. Non-strings and prompts without a block
    are returned unchanged.
    """
    if not isinstance(content, str) or HISTORY_CLOSE_TAG not in content:
        return content
    note = ""
    stripped = content.lstrip()
    if stripped.startswith(ATTACHMENTS_OPEN_TAG) and ATTACHMENTS_CLOSE_TAG in stripped:
        head = stripped.split(ATTACHMENTS_CLOSE_TAG, 1)[0] + ATTACHMENTS_CLOSE_TAG
        if HISTORY_OPEN_TAG not in head:
            note = head
    prompt = content.rsplit(HISTORY_CLOSE_TAG, 1)[1].strip()
    return f"{note}\n\n{prompt}".strip() if note else prompt


def is_compact_summary(msg: Any) -> bool:
    """True for the CLI's auto-compaction summary, stored as a user message."""
    if not isinstance(msg, dict) or msg.get("type") != "user":
        return False
    content = msg.get("content")
    return isinstance(content, str) and content.lstrip().startswith(COMPACT_SUMMARY_PREFIX)


def title_from_prompt(content: Any, limit: int = 80) -> str:
    """A chat title from a stored first prompt: no history block, no
    attachment note, no ``<user_input>`` wrapper."""
    text = _prompt_key(content)
    if text.startswith(ATTACHMENTS_OPEN_TAG) and ATTACHMENTS_CLOSE_TAG in text:
        text = text.split(ATTACHMENTS_CLOSE_TAG, 1)[1].strip()
    text = " ".join(text.split())
    return text[:limit]


def is_machine_title(title: Any) -> bool:
    """Empty, or derived from a server-rewritten prompt — safe to replace.
    Anything else was typed or chosen by the user and must be kept."""
    if not isinstance(title, str) or not title.strip():
        return True
    return title.lstrip().startswith(MACHINE_TITLE_PREFIXES)


def session_messages(
    rebuilt: List[Message],
    previous: List[Message],
    current_prompt: Any,
    *,
    now_ms: int,
) -> List[Message]:
    """This agent session's own messages from its rebuilt transcript.

    * Folded ``<conversation_history>`` prompts are stripped to the user's text.
    * After an auto-compaction the transcript starts at the summary: what came
      before it in this session is ``previous`` (the last list persisted for
      this session); the summary itself is dropped and only the messages after
      the last one ``previous`` already knows (by transcript uuid) are added.
      A compaction that fired mid-turn leaves the current turn without its
      prompt, so it is re-inserted from ``current_prompt``.

    Mutates nothing it is given.
    """
    cleaned: List[Message] = []
    for msg in rebuilt:
        if isinstance(msg, dict) and msg.get("type") == "user":
            content = strip_folded_history(msg.get("content"))
            if content != msg.get("content"):
                msg = {**msg, "content": content}
        cleaned.append(msg)

    summary_idx = max((i for i, m in enumerate(cleaned) if is_compact_summary(m)), default=-1)
    if summary_idx < 0:
        return cleaned

    post = cleaned[summary_idx + 1 :]
    known = {m.get("id") for m in previous if isinstance(m, dict) and m.get("id")}
    last_known = max((i for i, m in enumerate(post) if m.get("id") in known), default=-1)
    new = post[last_known + 1 :]
    if new and new[0].get("type") != "user" and isinstance(current_prompt, str):
        if current_prompt.strip():
            new = [
                {
                    "id": f"prompt-{uuid.uuid4()}",
                    "type": "user",
                    "content": current_prompt,
                    "timestamp": now_ms,
                },
                *new,
            ]
    logger.info(
        "brain_agent_compaction_spliced",
        previous=len(previous),
        post_summary=len(post),
        added=len(new),
    )
    return [*previous, *new]


def history_base(stored: List[Message], current_prompt: Any) -> List[Message]:
    """The stored thread as it stood before an agent session's first run.

    A trailing user message with no answer that repeats ``current_prompt`` is
    dropped: the client's first-send save may already have stored the prompt
    the session is about to answer, and the transcript will carry it again.
    """
    base = [m for m in stored if isinstance(m, dict)]
    if base and base[-1].get("type") == "user":
        key = _prompt_key(current_prompt)
        if key and _prompt_key(base[-1].get("content")) == key:
            base = base[:-1]
    return base


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
