"""Tests for brain agent file attachments.

The sandbox is also the agent's cwd and it runs Bash there, so the filename and
extension rules are load-bearing, not cosmetic.
"""

import json

import pytest

from app.services.brain_agent_service import SANDBOX_SEED_FILES, _is_bookkeeping_file
from app.services.brain_agent_uploads import (
    ALLOWED_UPLOAD_EXTENSIONS,
    UPLOAD_MANIFEST,
    UploadRejected,
    build_attachment_note,
    existing_upload_bytes,
    read_upload_manifest,
    record_upload,
    sanitize_upload_name,
    work_dir_for,
    write_upload_manifest,
)


# ── filename validation ──


@pytest.mark.parametrize(
    "given,expected",
    [
        ("data.csv", "data.csv"),
        ("Monthly Report.xlsx", "Monthly Report.xlsx"),
        # Every directory component is stripped, so traversal collapses to a name.
        ("../../etc/passwd.csv", "passwd.csv"),
        ("/etc/shadow.csv", "shadow.csv"),
        ("a/b/../c.csv", "c.csv"),
        ("report.PDF", "report.PDF"),  # extension check is case-insensitive
    ],
)
def test_accepted_names_are_normalised(given, expected):
    assert sanitize_upload_name(given, SANDBOX_SEED_FILES) == expected


@pytest.mark.parametrize("name", ["evil.py", "run.sh", "query.sql"])
def test_executable_extensions_are_rejected(name):
    """The agent runs Bash in this directory — a script isn't an attachment."""
    with pytest.raises(UploadRejected):
        sanitize_upload_name(name, SANDBOX_SEED_FILES)


@pytest.mark.parametrize(
    "name",
    [
        "",
        ".",
        "..",
        ".uploads.json",  # the manifest itself
        ".hidden.csv",  # dotfiles are bookkeeping, never user content
        "noextension",
        "..\\..\\windows.csv",  # collapses to a leading-dot name on POSIX
    ],
)
def test_invalid_names_are_rejected(name):
    with pytest.raises(UploadRejected):
        sanitize_upload_name(name, SANDBOX_SEED_FILES)


def test_seed_file_collision_is_rejected():
    """Overwriting db.py or a skill file would break the sandbox."""
    with pytest.raises(UploadRejected):
        sanitize_upload_name("skill_schema.md", SANDBOX_SEED_FILES)


def test_no_executable_extension_is_ever_allowed():
    assert not {".py", ".sh", ".bash", ".sql"} & ALLOWED_UPLOAD_EXTENSIONS


# ── manifest ──


def test_manifest_round_trip(tmp_path):
    record_upload(tmp_path, "a.csv")
    record_upload(tmp_path, "b.xlsx")
    record_upload(tmp_path, "a.csv")  # idempotent

    assert read_upload_manifest(tmp_path) == ["a.csv", "b.xlsx"]
    stored = json.loads((tmp_path / UPLOAD_MANIFEST).read_text())
    assert stored == {"files": ["a.csv", "b.xlsx"]}


def test_missing_manifest_reads_as_empty(tmp_path):
    assert read_upload_manifest(tmp_path) == []


def test_corrupt_manifest_does_not_raise(tmp_path):
    """A broken manifest must degrade to "no attachments", not kill the session."""
    (tmp_path / UPLOAD_MANIFEST).write_text("{not json")
    assert read_upload_manifest(tmp_path) == []


def test_manifest_entries_are_resanitised_on_read(tmp_path):
    """The manifest feeds the trusted part of the prompt, so re-check on read.

    A path can only get in here by tampering with the sandbox or S3, but the
    cost of being wrong is a filename landing outside <user_input>.
    """
    write_upload_manifest(tmp_path, ["../../etc/passwd", "ok.csv", ""])
    assert read_upload_manifest(tmp_path) == ["passwd", "ok.csv"]


def test_existing_upload_bytes_counts_only_listed_files(tmp_path):
    (tmp_path / "a.csv").write_bytes(b"x" * 100)
    (tmp_path / "unlisted.csv").write_bytes(b"y" * 500)
    record_upload(tmp_path, "a.csv")

    assert existing_upload_bytes(tmp_path) == 100


def test_existing_upload_bytes_tolerates_a_vanished_file(tmp_path):
    record_upload(tmp_path, "gone.csv")
    assert existing_upload_bytes(tmp_path) == 0


# ── prompt note ──


def test_attachment_note_lists_files():
    note = build_attachment_note(["a.csv", "b.pdf"])
    assert "<attachments>" in note and "</attachments>" in note
    assert "- a.csv" in note and "- b.pdf" in note


def test_attachment_note_tells_the_agent_contents_are_data():
    """Attachment text must not be executable as instructions."""
    note = build_attachment_note(["a.csv"])
    assert "not as instructions" in note


def test_no_attachments_yields_no_note():
    assert build_attachment_note([]) == ""


# ── artifact scanning ──


def test_manifest_is_not_reported_as_agent_output():
    """Otherwise the user gets their own bookkeeping file as a download chip."""
    assert _is_bookkeeping_file(UPLOAD_MANIFEST)
    assert not _is_bookkeeping_file("chart.png")


def test_work_dir_matches_the_service_layout():
    assert str(work_dir_for(7, "abc")) == "/tmp/brain-agent/7/abc"
