"""Layout policy — write-geometry (row-group sizing) + compact/vacuum decisions, in one place.

Pure decisions, no I/O: the fixed write geometry and the ``MaintenancePolicy`` compact/vacuum
thresholds. The engine owns the *mechanism* (building
WriterProperties, running the compaction); this module owns *what shape to aim for*, so the write
path and maintenance size row groups identically from one place.

The maintenance half was extracted from the post-write upkeep that was inlined in ``engine._maintain``
(and duplicated in the Tier-0 safe button) so the trigger thresholds live in one testable object
instead of scattered magic numbers. The byte-trigger design (2026-07-05):

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
# 128 MB — the same bin size Fabric Spark's optimize-write targets, and small enough that a
# scattered-update MERGE never copy-on-writes fat files. 256 MB was a hedge against the v0.4.58
# gate failure (>67 GiB of DataFusion disk spill at the then-8M-row-group/128 MB geometry vs
# <59 GiB at 256 MB); the shipping 6M-row-group geometry is re-validated at 128 MB by the SF=10
# merge-spill gate before any release tags.
DEFAULT_TARGET_FILE_SIZE = 128 * 1024 * 1024

# Delta checkpoint cadence, stamped as `delta.checkpointInterval` on every table duckrun creates.
# delta-rs's post-commit hook honors the property and writes the checkpoint itself; without it the
# delta-rs default is 100 commits, which leaves an incrementally-written table replaying up to 99
# JSON commits on every open. Creation-only: an existing table keeps whatever it has.
CHECKPOINT_INTERVAL = 10

# ------------------------------------------------------------------------- write-layout geometry
# The FIXED row-group ceiling EVERY write uses — 6M rows. A Parquet row group maps 1:1 to a Direct
# Lake column segment; anything in the 1M-16M band is a healthy segment (Power BI's own default
# segment size is 8M), and 6M trades a little per-segment density for one more group to scan in
# parallel per file. A CEILING, not a size: the 128 MB file roll usually closes the group first.
# There is no derived sizing anywhere: the planner-estimate machinery (EXPLAIN cardinality,
# prior-log floors) and, later, the SORTED BY AUTO byte-model geometry (rg_for / tfs_for / one row
# group per file) each cost plan walks, counts and a calibrated bytes/row model for no measured
# read-side advantage over the fixed constants. SORTED BY AUTO now only picks the sort key; the
# write is shaped like any other.
ROW_GROUP_DEFAULT_ROWS = 6_000_000


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
