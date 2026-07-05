"""Maintenance policy — the compact / vacuum decisions, owned in one place.

Extracted from the post-write upkeep that was inlined in ``engine._maintain`` (and duplicated in the
Tier-0 safe button) so the trigger thresholds live in one testable object instead of scattered magic
numbers. The byte-trigger design (2026-07-05):

  small file  := size < 0.5 × target_file_size
  compact iff := count(small) >= min_small_files AND sum(small) >= byte_floor_multiplier × target
  scope       := only partitions containing offending small files
  vacuum      := only after a compaction actually ran, AND not more often than min_vacuum_interval
  failure     := a maintenance CommitFailedError (a compaction that lost a race AFTER the data commit
                 already succeeded) is swallowed + logged; maintenance NEVER fails the write.

A raw file COUNT is deliberately not the trigger: a healthy big table sits at hundreds of
target-sized files forever and must never be compacted, while a hot small table earns a compaction on
its byte debt, not its file count.
"""
from typing import Callable, Iterable, Set, Tuple

from deltalake.exceptions import CommitFailedError

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("Duckrun")

# The one read-layout target every file write, compaction, and sort-rewrite uses (see engine).
DEFAULT_TARGET_FILE_SIZE = 256 * 1024 * 1024


class MaintenancePolicy:
    """Owns the compact/vacuum decisions. Pure logic — it decides, the caller executes (so it stays
    unit-testable with no Delta I/O)."""

    def __init__(self, target_file_size: int = DEFAULT_TARGET_FILE_SIZE, *,
                 min_small_files: int = 8, byte_floor_multiplier: int = 2,
                 min_vacuum_interval_s: int = 168 * 3600):
        self.target_file_size = target_file_size
        self.min_small_files = min_small_files
        self.byte_floor_multiplier = byte_floor_multiplier
        self.min_vacuum_interval_s = min_vacuum_interval_s

    @property
    def small_file_threshold(self) -> float:
        """A file is a compaction candidate ("small") if it is under HALF the target size."""
        return 0.5 * self.target_file_size

    def should_compact(self, sizes: Iterable[int]) -> bool:
        """Fire iff there are enough small files AND enough small bytes to be worth a commit — never
        on file count alone."""
        small = [s for s in sizes if s < self.small_file_threshold]
        return (len(small) >= self.min_small_files
                and sum(small) >= self.byte_floor_multiplier * self.target_file_size)

    def partitions_to_compact(self, files: Iterable[Tuple[str, int]]) -> Set[str]:
        """The distinct partitions holding at least one small file — the only partitions worth
        rewriting. ``files`` is ``(partition_label, size)`` pairs."""
        return {part for part, size in files if size < self.small_file_threshold}

    def should_vacuum(self, compacted: bool, last_vacuum_age_s: float) -> bool:
        """Vacuum only after a compaction actually ran (fresh tombstones to reclaim) and not more
        often than the retention window."""
        return compacted and last_vacuum_age_s >= self.min_vacuum_interval_s

    def run_maintenance(self, compact_fn: Callable[[], None], vacuum_fn: Callable[[], None],
                        should: bool) -> None:
        """Run ``compact_fn`` (then ``vacuum_fn``) when ``should`` — swallowing a ``CommitFailedError``
        (a compaction that lost a race after the data already committed) and logging it, because the
        durable outcome the caller asked for already succeeded and the byte trigger simply re-fires
        next run. Any OTHER exception propagates: it is a real fault, not a lost maintenance race."""
        if not should:
            return
        try:
            compact_fn()
            vacuum_fn()
        except CommitFailedError as e:
            logger.warning(f"post-write maintenance skipped (data commit already succeeded): {e}")
