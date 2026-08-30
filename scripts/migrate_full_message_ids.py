"""One-time migration: truncated 8-char IDs → full Gmail message IDs in markdown files.

Markdown files were historically named ``{slug}_{message_id[:8]}.md``. Gmail message IDs
are time-ordered, so an 8-char prefix collides for emails that arrive close together —
six prefixes collide in the live corpus, which corrupted the downstream newsletter tree.
``MarkdownWriter`` now writes ``{slug}_{message_id}.md``, and ``MarkdownConverter`` emits
the ID into the YAML front matter.

This script brings existing output in line, with no Gmail API calls and no re-conversion.
Everything it needs is already in SQLite: ``message_id`` is the full-length PRIMARY KEY
and ``markdown_path`` holds the current truncated path.

Four phases::

    0. plan      read-only; validate every row and detect duplicate targets
    1. rename    {slug}_{8char}.md → {slug}_{full_id}.md  (journalled first)
    2. database  UPDATE messages SET markdown_path = ...  (single transaction)
    3. backfill  insert `id: "..."` into each file's front matter

Every phase is idempotent, so the script is safe to re-run after an interruption.

Usage::

    uv run python scripts/migrate_full_message_ids.py              # dry run (default)
    uv run python scripts/migrate_full_message_ids.py --apply
    uv run python scripts/migrate_full_message_ids.py --verify
    uv run python scripts/migrate_full_message_ids.py --rollback data/migration_<ts>.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from gmail_ingestor.config.settings import GmailIngestorSettings

logger = logging.getLogger("migrate")

# Paths in the DB are stored relative to the repo root (e.g. "../output/markdown/x.md"),
# so they only resolve when anchored here rather than to the caller's CWD.
REPO_ROOT = Path(__file__).resolve().parent.parent

SHORT_ID_LEN = 8
FULL_ID_LEN = 16


def _escape_yaml(value: str) -> str:
    """Escape for a YAML double-quoted scalar — backslashes first, then quotes.

    Kept in sync with ``gmail_ingestor.core.converter._escape_yaml``.
    """
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _resolve(path_str: str) -> Path:
    """Anchor a possibly-relative DB path to the repo root."""
    p = Path(path_str)
    return p if p.is_absolute() else (REPO_ROOT / p)


# ---------------------------------------------------------------------------
# Phase 0 — plan
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Rename:
    """A single planned filename change, in both DB and on-disk form."""

    message_id: str
    old_db: str
    new_db: str

    @property
    def old_path(self) -> Path:
        return _resolve(self.old_db)

    @property
    def new_path(self) -> Path:
        return _resolve(self.new_db)


@dataclass
class Plan:
    renames: list[Rename] = field(default_factory=list)
    already_migrated: list[str] = field(default_factory=list)
    missing_files: list[tuple[str, str]] = field(default_factory=list)
    guard_failures: list[tuple[str, str, str]] = field(default_factory=list)
    duplicate_targets: list[str] = field(default_factory=list)

    @property
    def is_safe(self) -> bool:
        return not self.duplicate_targets and not self.guard_failures


def build_plan(conn: sqlite3.Connection) -> Plan:
    """Inspect every tracked markdown file and decide what needs to change.

    Read-only. Rows are classified rather than silently skipped so that anything
    unexpected surfaces in the dry run instead of during the apply.
    """
    plan = Plan()
    rows = conn.execute(
        "SELECT message_id, markdown_path FROM messages "
        "WHERE markdown_path != '' ORDER BY message_id"
    ).fetchall()

    for message_id, markdown_path in rows:
        old_db = str(markdown_path)
        stem = Path(old_db).stem
        # rpartition, not split: slugify preserves "_" (it is in \w), so a subject
        # like "foo_bar" yields "foo_bar_<id>" — only the LAST segment is the ID.
        slug, sep, suffix = stem.rpartition("_")
        if not sep:
            plan.guard_failures.append((message_id, old_db, "no '_' separator in filename"))
            continue

        if suffix == message_id:
            plan.already_migrated.append(message_id)
            continue

        if suffix != message_id[:SHORT_ID_LEN]:
            plan.guard_failures.append(
                (message_id, old_db, f"suffix {suffix!r} != message_id[:8] {message_id[:8]!r}")
            )
            continue

        new_db = str(Path(old_db).with_name(f"{slug}_{message_id}.md"))
        rename = Rename(message_id=message_id, old_db=old_db, new_db=new_db)

        if rename.old_path.exists():
            plan.renames.append(rename)
        elif rename.new_path.exists():
            # File already moved by an earlier interrupted run; the DB still needs
            # updating, so keep it in the rename list — phase 1 will no-op on it.
            plan.renames.append(rename)
        else:
            plan.missing_files.append((message_id, old_db))

    counts = Counter(r.new_db for r in plan.renames)
    plan.duplicate_targets = sorted(target for target, n in counts.items() if n > 1)
    return plan


def report_plan(plan: Plan) -> None:
    logger.info("Planned renames ........ %d", len(plan.renames))
    logger.info("Already migrated ....... %d", len(plan.already_migrated))
    logger.info("Missing on disk ........ %d", len(plan.missing_files))
    logger.info("Guard failures ......... %d", len(plan.guard_failures))
    logger.info("Duplicate targets ...... %d", len(plan.duplicate_targets))

    for message_id, path in plan.missing_files:
        logger.warning("missing: %s (%s) — DB row left untouched", path, message_id)
    for message_id, path, reason in plan.guard_failures:
        logger.error("guard failed: %s (%s): %s", path, message_id, reason)
    for target in plan.duplicate_targets:
        logger.error("duplicate target: %s", target)


# ---------------------------------------------------------------------------
# Phases 1-3 — apply
# ---------------------------------------------------------------------------


def write_journal(plan: Plan, journal_path: Path, inserted: set[str] | None = None) -> None:
    """Persist the rename list so --rollback can reverse an interrupted run."""
    inserted = inserted or set()
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    journal_path.write_text(
        json.dumps(
            {
                "created": datetime.now(UTC).isoformat(),
                "repo_root": str(REPO_ROOT),
                "renames": [
                    {
                        "message_id": r.message_id,
                        "old": r.old_db,
                        "new": r.new_db,
                        "id_inserted": r.message_id in inserted,
                    }
                    for r in plan.renames
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def rename_files(plan: Plan) -> int:
    """Phase 1. Skips entries already at their destination, so re-runs are safe."""
    renamed = 0
    for r in plan.renames:
        if not r.old_path.exists():
            continue  # already moved by an earlier run
        r.old_path.rename(r.new_path)
        renamed += 1
    return renamed


def update_database(conn: sqlite3.Connection, plan: Plan) -> int:
    """Phase 2. One transaction, so the DB is never left half-updated."""
    now = datetime.now(UTC).isoformat()
    with conn:
        conn.executemany(
            "UPDATE messages SET markdown_path = ?, updated_at = ? WHERE message_id = ?",
            [(r.new_db, now, r.message_id) for r in plan.renames],
        )
    return len(plan.renames)


def insert_front_matter_id(path: Path, message_id: str) -> str:
    """Phase 3, single file. Returns 'inserted', 'present', or 'no_front_matter'.

    Writes via a temp file + os.replace so an interruption cannot truncate a file.
    """
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return "no_front_matter"

    end = text.find("\n---", 4)
    if end == -1:
        return "no_front_matter"

    for line in text[4:end].splitlines():
        if line.startswith("id:"):
            return "present"

    # Mirror MarkdownConverter exactly: id leads the block, and the value is quoted
    # so an all-digit Gmail ID survives yaml.safe_load as a str rather than an int.
    updated = text[:4] + f'id: "{message_id}"\n' + text[4:]
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(updated, encoding="utf-8")
    os.replace(tmp, path)
    return "inserted"


def backfill_front_matter(plan: Plan) -> tuple[set[str], Counter]:
    """Phase 3. Returns the IDs actually modified plus a tally of outcomes."""
    inserted: set[str] = set()
    stats: Counter = Counter()
    for r in plan.renames:
        if not r.new_path.exists():
            stats["missing"] += 1
            continue
        outcome = insert_front_matter_id(r.new_path, r.message_id)
        stats[outcome] += 1
        if outcome == "inserted":
            inserted.add(r.message_id)
        elif outcome == "no_front_matter":
            logger.warning("no front matter block: %s", r.new_db)
    return inserted, stats


def backup_database(db_path: Path) -> Path:
    """Snapshot the DB using SQLite's backup API (WAL-safe, unlike a file copy)."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    dest_path = db_path.with_name(f"{db_path.name}.bak-{stamp}")
    with sqlite3.connect(db_path) as src, sqlite3.connect(dest_path) as dest:
        src.backup(dest)
    return dest_path


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------


def rollback(journal_path: Path, db_path: Path) -> None:
    """Undo a run: strip inserted `id:` lines, move files back, restore DB paths."""
    data = json.loads(journal_path.read_text(encoding="utf-8"))
    entries = data["renames"]
    logger.info("Rolling back %d entries from %s", len(entries), journal_path)

    stripped = restored = 0
    for entry in entries:
        new_path = _resolve(entry["new"])
        old_path = _resolve(entry["old"])

        # Strip the front-matter line first, while the file is still at its new path.
        if entry.get("id_inserted") and new_path.exists():
            text = new_path.read_text(encoding="utf-8")
            marker = f'id: "{entry["message_id"]}"\n'
            if text.startswith("---\n" + marker):
                new_path.write_text(text.replace(marker, "", 1), encoding="utf-8")
                stripped += 1

        if new_path.exists() and not old_path.exists():
            new_path.rename(old_path)
            restored += 1

    now = datetime.now(UTC).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            "UPDATE messages SET markdown_path = ?, updated_at = ? WHERE message_id = ?",
            [(e["old"], now, e["message_id"]) for e in entries],
        )

    logger.info("Rollback complete: %d files restored, %d id lines stripped", restored, stripped)


# ---------------------------------------------------------------------------
# Repair — malformed YAML front matter
# ---------------------------------------------------------------------------


def repair_front_matter(conn: sqlite3.Connection, apply: bool) -> int:
    """Rewrite front-matter lines that a real YAML parser rejects.

    Independent of the ID migration. ``MarkdownConverter`` used to escape quotes but
    not backslashes, so any header containing one (RFC 2822 quoted-strings do) yielded
    a double-quoted scalar that terminated early. Files in that state are skipped
    outright by ingestor-tools, which calls ``yaml.safe_load`` and bails on failure.

    Values are re-emitted from the authoritative DB columns rather than un-mangled
    in place, so the repair cannot compound the original escaping mistake.
    """
    import yaml

    rows = conn.execute(
        "SELECT message_id, markdown_path, subject, sender FROM messages "
        "WHERE markdown_path != ''"
    ).fetchall()

    repaired = unfixable = 0
    for message_id, markdown_path, subject, sender in rows:
        path = _resolve(markdown_path)
        if not path.exists():
            continue

        text = path.read_text(encoding="utf-8")
        if not text.startswith("---\n"):
            continue
        end = text.find("\n---", 4)
        if end == -1:
            continue

        block = text[4:end]
        try:
            yaml.safe_load(block)
            continue  # already valid
        except yaml.YAMLError:
            pass

        # Re-emit the two fields the DB is authoritative for.
        new_lines = []
        for line in block.splitlines():
            if line.startswith("subject: "):
                new_lines.append(f'subject: "{_escape_yaml(subject or "")}"')
            elif line.startswith("from: "):
                new_lines.append(f'from: "{_escape_yaml(sender or "")}"')
            else:
                new_lines.append(line)
        new_block = "\n".join(new_lines)

        try:
            yaml.safe_load(new_block)
        except yaml.YAMLError as exc:
            unfixable += 1
            logger.error(
                "still unparseable after repair: %s (%s)",
                markdown_path,
                exc.__class__.__name__,
            )
            continue

        logger.info("repairable: %s", Path(markdown_path).name)
        if apply:
            updated = text[:4] + new_block + text[end:]
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text(updated, encoding="utf-8")
            os.replace(tmp, path)
        repaired += 1

    verb = "Repaired" if apply else "Would repair"
    logger.info("%s %d file(s); %d still unparseable", verb, repaired, unfixable)
    return unfixable


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------


def verify(conn: sqlite3.Connection, settings: GmailIngestorSettings) -> bool:
    """Post-migration assertions. Returns True when everything checks out."""
    import re

    ok = True
    markdown_dir = _resolve(str(settings.output_markdown_dir))
    raw_dir = _resolve(str(settings.output_raw_dir))

    md_files = list(markdown_dir.glob("*.md"))
    short_pattern = re.compile(r"_[0-9a-f]{8}\.md$")
    stragglers = [p.name for p in md_files if short_pattern.search(p.name)]
    logger.info("Markdown files on disk .......... %d", len(md_files))
    logger.info("Still using an 8-char suffix .... %d", len(stragglers))
    if stragglers:
        ok = False
        for name in stragglers[:10]:
            logger.error("  straggler: %s", name)

    rows = conn.execute(
        "SELECT message_id, markdown_path FROM messages WHERE markdown_path != ''"
    ).fetchall()

    missing_md = [p for _, p in rows if not _resolve(p).exists()]
    # A stale suffix only counts as a failure when the file is actually there. Rows
    # whose file was deleted outside the pipeline are reported as drift, not errors —
    # the migration deliberately leaves those DB rows untouched.
    bad_suffix = [
        m
        for m, p in rows
        if Path(p).stem.rpartition("_")[2] != m and _resolve(p).exists()
    ]
    missing_raw = [
        m
        for m, _ in rows
        if not (raw_dir / f"{m}.html").exists() and not (raw_dir / f"{m}.txt").exists()
    ]
    logger.info("DB rows with markdown_path ...... %d", len(rows))
    logger.info("  suffix != message_id .......... %d", len(bad_suffix))
    logger.info("  markdown missing on disk ...... %d  (known drift, not a failure)",
                len(missing_md))
    logger.info("  no raw .html/.txt by exact name %d", len(missing_raw))
    if bad_suffix:
        ok = False
    for path in missing_md[:10]:
        logger.warning("  markdown missing: %s", path)
    for message_id in missing_raw[:10]:
        logger.warning("  raw missing: %s", message_id)

    # Front matter: check every file that exists, and make sure at least one
    # all-numeric ID is among them — that is the case quoting exists to protect.
    try:
        import yaml
    except ImportError:
        logger.warning("PyYAML not installed; skipping front-matter parse check")
        return ok

    checked = numeric_checked = mismatched = no_id = unparseable = 0
    for message_id, path in rows:
        resolved = _resolve(path)
        if not resolved.exists():
            continue
        text = resolved.read_text(encoding="utf-8")
        if not text.startswith("---\n"):
            continue
        end = text.find("\n---", 4)
        if end == -1:
            continue
        try:
            meta = yaml.safe_load(text[4:end]) or {}
        except yaml.YAMLError:
            unparseable += 1
            if unparseable <= 10:
                logger.error("  unparseable front matter: %s (run --repair-front-matter)", path)
            continue
        checked += 1
        if "id" not in meta:
            no_id += 1
        elif meta["id"] != message_id or not isinstance(meta["id"], str):
            mismatched += 1
            if mismatched <= 10:
                logger.error("  front-matter id mismatch in %s: %r", path, meta.get("id"))
        elif message_id.isdigit():
            numeric_checked += 1

    logger.info("Front matter parsed ............. %d", checked)
    logger.info("  missing an id field ........... %d", no_id)
    logger.info("  id mismatched or not a str .... %d", mismatched)
    logger.info("  all-numeric ids verified ...... %d", numeric_checked)
    logger.info("  unparseable front matter ...... %d", unparseable)
    if mismatched or no_id or unparseable:
        ok = False

    return ok


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate markdown filenames from 8-char ID prefixes to full Gmail IDs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Perform the migration")
    mode.add_argument("--verify", action="store_true", help="Check post-migration state")
    mode.add_argument("--rollback", metavar="JOURNAL", help="Undo a run from its journal file")
    mode.add_argument(
        "--repair-front-matter",
        action="store_true",
        help="Rewrite front matter that a YAML parser rejects (independent of the ID migration)",
    )
    parser.add_argument(
        "--limit", type=int, help="Process at most N renames (for rehearsals)"
    )
    parser.add_argument(
        "--apply-repair",
        action="store_true",
        help="With --repair-front-matter, write the fixes instead of previewing them",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-7s %(message)s",
    )

    settings = GmailIngestorSettings(_env_file=str(REPO_ROOT / ".env"))  # type: ignore[call-arg]
    db_path = _resolve(str(settings.database_path))
    if not db_path.exists():
        logger.error("Database not found: %s", db_path)
        sys.exit(1)

    if args.rollback:
        rollback(Path(args.rollback), db_path)
        return

    conn = sqlite3.connect(db_path)
    try:
        if args.repair_front_matter:
            unfixable = repair_front_matter(conn, apply=args.apply_repair)
            sys.exit(1 if unfixable else 0)

        if args.verify:
            sys.exit(0 if verify(conn, settings) else 1)

        logger.info("Database: %s", db_path)
        logger.info("Markdown: %s", _resolve(str(settings.output_markdown_dir)))
        plan = build_plan(conn)
        if args.limit is not None:
            plan.renames = plan.renames[: args.limit]
            logger.info("Limited to first %d renames", len(plan.renames))
        report_plan(plan)

        if not plan.is_safe:
            logger.error("Refusing to proceed — resolve the issues above first.")
            sys.exit(1)

        if not args.apply:
            logger.info("")
            logger.info("Dry run. Re-run with --apply to perform %d renames.", len(plan.renames))
            for r in plan.renames[:5]:
                logger.info("  %s", Path(r.old_db).name)
                logger.info("    -> %s", Path(r.new_db).name)
            return

        if not plan.renames:
            logger.info("Nothing to do.")
            return

        backup = backup_database(db_path)
        logger.info("Database backed up to %s", backup)

        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        journal_path = db_path.parent / f"migration_{stamp}.json"
        write_journal(plan, journal_path)
        logger.info("Journal written to %s", journal_path)

        renamed = rename_files(plan)
        logger.info("Phase 1: renamed %d files", renamed)

        updated = update_database(conn, plan)
        logger.info("Phase 2: updated %d database rows", updated)

        inserted, stats = backfill_front_matter(plan)
        logger.info(
            "Phase 3: front matter — %d inserted, %d already present, %d without a block",
            stats["inserted"],
            stats["present"],
            stats["no_front_matter"],
        )

        write_journal(plan, journal_path, inserted)
        logger.info("Done. Roll back with: --rollback %s", journal_path)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
