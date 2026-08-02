"""Layout policy — write-geometry (row-group sizing) + compact/vacuum decisions, in one place.

Pure decisions, no I/O: the row-group ``rg_for`` sizing rule and the ``MaintenancePolicy`` compact/
vacuum thresholds. The engine owns the *mechanism* (estimating rows, building WriterProperties, running
the compaction); this module owns *what shape to aim for*, so both the write path and maintenance size
row groups identically from one place. See ``rg_for`` below for the write-geometry half.

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
import os
from typing import Callable, Iterable, Set, Tuple

from deltalake.exceptions import CommitFailedError

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("Duckrun")

# The one read-layout target every file write, compaction, and sort-rewrite uses (see engine).
DEFAULT_TARGET_FILE_SIZE = 256 * 1024 * 1024

# ------------------------------------------------------------------------- write-layout geometry
# A Parquet row group maps 1:1 to a Direct Lake column segment: any size from 1M to 16M rows is a fine
# segment, 16M is the ceiling (kept under 2^24 so one row group stays one segment), and 16M is also the
# write-memory ceiling (arrow-rs buffers a full uncompressed row group per open writer). This is a
# CEILING, not a size: the 256 MB file roll usually closes the group first, so a write left at RG_MAX
# still lands well inside the band. A SMALL write shrinks the ceiling so the table still yields
# ~RG_LANES groups — each segment transcodes on its own lane, so 2 groups cold-load on 2 lanes.
# Only max_row_group_size moves — never bytes.
ROW_GROUP_MAX_ROWS = 16_000_000
RG_LANES = int(os.environ.get("DUCKRUN_RG_LANES", "8"))  # target row groups for a small overwrite
RG_MIN = 1_000_000
# Floor for a row count that came from the DuckDB PLANNER rather than the Delta log. The planner's
# estimate is not a measurement and can be an order of magnitude low: DuckDB applies a fixed 0.2
# selectivity guess to filters and anti/semi joins, a set-op parent carries no cardinality of its own,
# and a CSV is extrapolated from FILE SIZE (so a gzipped source is off by its compression ratio) —
# none of which duckrun can fix without reimplementing a planner. The failure is asymmetric: an
# over-estimate is harmless (it caps at RG_MAX and the file roll decides), while an under-estimate
# pins a huge table to the bottom of the band permanently — measured on a 370M-row fact, a 9x
# under-estimate produced 380 row groups where ~34 belong (issue #22). A guess therefore never drives
# the ceiling below 6M. This costs the ~RG_LANES target below RG_LANES × RG_MIN_ESTIMATED rows, which
# is the deliberate trade: fewer lanes on a small table, no floor-pinning on a large one.
RG_MIN_ESTIMATED = 6_000_000
RG_MAX = ROW_GROUP_MAX_ROWS  # big/unknown estimates keep this exactly


def rg_for(est, floor=RG_MIN):
    """Row-group CEILING for a write, from the result rows. Big/unknown -> the 16M constant (the
    pre-adaptive layout, unchanged); a small result shrinks toward ~RG_LANES groups, floored at
    ``floor``.

    ``floor`` is how much the caller's row count can be trusted, and it is the only knob:
    ``RG_MIN`` for an EXACT count read from the Delta log (compaction, ``optimize``), which can be
    taken at face value, and ``RG_MIN_ESTIMATED`` for a DuckDB PLANNER estimate (the overwrite
    path), which cannot — see RG_MIN_ESTIMATED for why."""
    if est is None or est >= RG_LANES * RG_MAX:
        return RG_MAX
    return max(floor, min(RG_MAX, -(-est // RG_LANES)))   # ceil(est / RG_LANES)


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
