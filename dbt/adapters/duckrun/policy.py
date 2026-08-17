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
# segment and 16M is the ceiling (kept under 2^24 so one row group stays one segment). This is a
# CEILING, not a size: the 256 MB file roll usually closes the group first, so a write left at RG_MAX
# still lands well inside the band. A SMALL write shrinks the ceiling so the table still yields
# ~RG_LANES groups — each segment transcodes on its own lane, so 2 groups cold-load on 2 lanes.
# Only max_row_group_size moves — never bytes.
#
# NOT a write-memory ceiling, despite what this comment used to claim: delta_rs closes the file as
# soon as its buffered size reaches target_file_size (checked every write batch), so the writer's
# footprint is bounded by the BYTE target no matter how large max_row_group_size is. That is what lets
# the AUTO path below set the row ceiling out of reach entirely (see RG_UNREACHABLE).
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
# the ceiling below 8M — Power BI's default segment size, so a floor-bound write hands Direct Lake
# its native segment rather than a fraction of one (measured on a 144M-row mart: a 9.7x planner
# under-estimate floor-pinned every segment). This costs the ~RG_LANES target below
# RG_LANES × RG_MIN_ESTIMATED rows, which is the deliberate trade: fewer lanes on a small table,
# no floor-pinning on a large one.
#
# A SORTED BY AUTO write is exempt and passes the low RG_MIN floor instead: it stages its source into
# a temp table and the profiler counts every row (sortkey.recommend_sort_key), so its row count is a
# measurement, not a guess, and the whole reason this floor exists does not apply.
RG_MIN_ESTIMATED = 8_000_000
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


# ------------------------------------------------------------ one row group per file (SORTED BY AUTO)
# max_row_group_size for an AUTO write: deliberately out of reach, so the byte target below is the ONLY
# boundary and every file holds exactly one row group. A row ceiling can only fire EARLY here and leave
# a runt trailing group in the same file — the raggedness this path exists to remove. 2^31-1 rather
# than a u64 maximum because engine._writer_properties degrades silently when the pinned wheel rejects
# a parameter, and its last rung drops max_row_group_size altogether, landing Parquet's 1M default.
RG_UNREACHABLE = 2 ** 31 - 1

# Headroom derate for the AUTO rows target. The byte model's landed-rows/target ratio is shape-
# dependent and measured at 0.75x / 1.25x / 1.36x / 1.51x (see AUTO_TFS_FACTOR below — the spread
# is real and NOT tunable away). Mid-band that residual is fine, but a rows target already at the
# TOP of the band (RG_MAX) times any overshoot exits the 16M one-segment band entirely — measured
# on the 591.7M-row nyc fact: a 16M target landed 21.7M-row groups (1.36x), i.e. more than one
# Direct Lake segment per group. So the AUTO path never AIMS at the top: its rows target is capped
# at RG_MAX / AUTO_RG_HEADROOM, so the worst measured overshoot still lands ~<= RG_MAX. This costs
# a big accurate-model table its 16M ideal (it lands ~10.7M nominal), which is the deliberate
# trade: a smaller in-band segment over an out-of-band one. <= 1 disables the derate.
AUTO_RG_HEADROOM = float(os.environ.get("DUCKRUN_AUTO_RG_HEADROOM", "1.5"))


def auto_rg_cap():
    """Rows-target ceiling for the AUTO one-row-group-per-file path (a function, not a constant,
    so tests can monkeypatch ``AUTO_RG_HEADROOM`` and both engine seams stay in agreement)."""
    return int(RG_MAX / AUTO_RG_HEADROOM) if AUTO_RG_HEADROOM > 1.0 else RG_MAX

# Turns the sortkey byte model (sortkey.bytes_per_row) into a target_file_size. It folds TWO effects
# that pull in opposite directions, which is why ONE constant covers both:
#   - the model counts ENCODED bytes — no Snappy, no page headers, no footer — so it reads high;
#   - delta_rs rolls the file on its BUFFERED size, which over-counts the final compressed file
#     (measured 0.50-0.64 actual/target across int, low-cardinality, string and 10-column shapes).
# Calibrated on ROWS PER ROW GROUP landed vs rows_target, never on bytes: rows are what a Direct Lake
# segment is made of, and with one row group per file the Delta log's num_records IS the segment size,
# so engine._log_auto_geometry can measure the ratio for free after every write.
#
# 1.0 — the model's bytes/row IS the target — because the measurements do not support a more precise
# number. Landing 40M-row tables through the real connection API and reading the row groups back out
# of the footers: at this factor a unique-key fact lands 0.75x of its row target, a star-schema fact
# 1.51x and a 10-column mixed fact 1.25x. Sweeping the factor to 0.8 moves those to 0.60 / 1.19 / 0.68,
# and extrapolating each shape onto 1.0x independently lands on 0.89 and 1.02 — i.e. the two estimates
# disagree by more than the correction either would make. So 1.0 it is.
#
# The residual per-shape spread is roughly +/-50% and is NOT reducible by tuning this constant: the
# target sets the row-group size, a bigger row group compresses better, and better compression fits
# more rows under the same byte target. That feedback makes the mapping non-linear and shape-dependent
# (the same shapes measured at 4M rows came out near half their 40M ratio). What the model buys is the
# right order of magnitude — row groups land inside ~0.5-1.5x of target, always exactly one per file,
# and for a realistic fact always inside the 1M-16M segment band. That is the win over ragged
# multi-group files; sub-percent placement was never on offer.
# 0 disables the byte target entirely and the write keeps DEFAULT_TARGET_FILE_SIZE.
AUTO_TFS_FACTOR = float(os.environ.get("DUCKRUN_AUTO_TFS_FACTOR", "1.0"))


# Floor for the computed byte target. NOT a hedge against a slightly-off model — it exists because the
# byte model can collapse toward ZERO and take the file target with it. A perfectly compressible column
# (a cyclic low-cardinality int, sorted) models at ~0 B/row, so rows_target x bpr is a few hundred
# BYTES; measured, that shattered a 4M-row table into 490 files of 8,192 rows. rg_for's RG_MIN bounds
# rows_target, but nothing bounds the byte target, so the collapse is unbounded without this.
# Deliberately small: it is a COLLAPSE guard, not a size policy. Set it at the "healthy file" mark
# (256/RG_LANES = 32 MB) and it stops being a guard and starts being the layout — measured at 40M
# rows, a 32 MB floor governed the well-compressing shapes outright and pinned them to ~2.4x their
# row target, which is the opposite of the exact row group this path exists to produce. 8 MB still
# collapses the pathological case to a single file while leaving every realistic shape to the model.
TFS_MIN = int(os.environ.get("DUCKRUN_AUTO_TFS_MIN", DEFAULT_TARGET_FILE_SIZE // 32))


def tfs_for(rows_target, bytes_per_row):
    """Byte target that makes ONE row group of ~``rows_target`` rows fill exactly one file, or ``None``
    to keep ``DEFAULT_TARGET_FILE_SIZE``.

    Capped at ``DEFAULT_TARGET_FILE_SIZE``: a fact wide enough that ``rows_target`` would need a bigger
    file keeps the global 256 MB policy and lands a smaller — still single — row group. Floored at
    ``TFS_MIN``, which binds only when the byte model has collapsed (see there)."""
    if not rows_target or not bytes_per_row or AUTO_TFS_FACTOR <= 0:
        return None
    want = rows_target * bytes_per_row * AUTO_TFS_FACTOR
    return int(min(DEFAULT_TARGET_FILE_SIZE, max(TFS_MIN, want)))


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
