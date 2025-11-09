import itertools
from typing import List, Dict, Tuple, Optional
import pandas as pd

def analyze_parquet_row_groups(con, parquet_path: str) -> pd.DataFrame:
    """
    Analyze Parquet row group statistics to identify columns with constant values.
    This is much faster than reading all data.
    
    Returns:
        DataFrame with row group stats per column
    """
    try:
        # Get row group metadata
        metadata = con.sql(f"""
            SELECT * FROM parquet_metadata('{parquet_path}')
        """).df()
        
        return metadata
    except Exception as e:
        print(f"Could not read parquet metadata: {e}")
        return None


def estimate_rle_from_row_groups(con, parquet_path: str) -> Dict[str, dict]:
    """
    Estimate RLE potential from Parquet row group statistics.
    If min == max in a row group, that entire group is one RLE run.
    
    Returns:
        Dictionary with column stats: {col: {'constant_groups': N, 'total_groups': M, 'constant_ratio': ratio}}
    """
    try:
        # Get row group statistics - this varies by DuckDB version
        # Try to get column chunk stats
        stats_query = f"""
            SELECT 
                row_group_id,
                column_id,
                file_offset,
                num_values,
                total_compressed_size,
                total_uncompressed_size
            FROM parquet_file_metadata('{parquet_path}')
        """
        
        stats = con.sql(stats_query).df()
        print("Row group metadata available!")
        return stats
        
    except Exception as e:
        print(f"Parquet metadata not available in this DuckDB version: {e}")
        print("Falling back to stratified sampling...")
        return None


def stratified_rle_sampling(con, parquet_path: str, sort_columns: List[str] = None,
                            num_segments: int = 5, segment_size: int = 1000) -> Dict[str, float]:
    """
    Sample RLE density across multiple segments of the file.
    
    Args:
        con: DuckDB connection
        parquet_path: Path to parquet file
        sort_columns: List of columns to sort by before calculating RLE. If None, uses natural order.
        num_segments: Number of segments to sample across the file
        segment_size: Number of rows per segment
    
    Returns:
        Dictionary with estimated RLE runs per column for full file
    """
    # Get total row count
    total_rows = con.sql(f"""
        SELECT COUNT(*) FROM read_parquet('{parquet_path}')
    """).fetchone()[0]
    
    # Get column names
    columns = con.sql(f"""
        SELECT column_name 
        FROM (
            DESCRIBE 
            SELECT * FROM read_parquet('{parquet_path}', file_row_number = TRUE)
        )
        WHERE column_name != 'file_row_number'
    """).fetchall()
    
    column_names = [col[0] for col in columns]
    
    # Build ORDER BY clause
    if sort_columns:
        order_by_clause = "ORDER BY " + ", ".join(sort_columns)
        sort_desc = f"sorted by [{', '.join(sort_columns)}]"
    else:
        order_by_clause = "ORDER BY file_row_number"
        sort_desc = "natural order"
    
    # Calculate segment positions spread across the file
    segment_positions = []
    if num_segments == 1:
        segment_positions = [0]
    else:
        step = total_rows // (num_segments + 1)
        segment_positions = [step * (i + 1) for i in range(num_segments)]
    
    # Sample each segment and calculate RLE density
    all_densities = {col: [] for col in column_names}
    
    for seg_idx, start_pos in enumerate(segment_positions, 1):
        for col in column_names:
            # The key fix: we need to sort the ENTIRE dataset first, then sample from it
            # This is expensive but necessary for accurate results
            rle_count = con.sql(f"""
                WITH sorted_data AS (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER ({order_by_clause}) as sorted_row_num
                    FROM read_parquet('{parquet_path}', file_row_number = TRUE)
                ),
                segment_data AS (
                    SELECT 
                        {col},
                        sorted_row_num
                    FROM sorted_data
                    WHERE sorted_row_num >= {start_pos}
                    ORDER BY sorted_row_num
                    LIMIT {segment_size}
                ),
                runs AS (
                    SELECT 
                        CASE 
                            WHEN LAG({col}) OVER (ORDER BY sorted_row_num) != {col} 
                            OR LAG({col}) OVER (ORDER BY sorted_row_num) IS NULL
                            THEN 1 
                            ELSE 0 
                        END AS new_run
                    FROM segment_data
                )
                SELECT SUM(new_run) AS rle_run_count
                FROM runs
            """).fetchone()[0]
            
            # Calculate density (runs per row)
            density = rle_count / segment_size
            all_densities[col].append(density)
    
    # Estimate total runs for full file
    estimated_runs = {}
    density_stats = {}
    
    for col in column_names:
        avg_density = sum(all_densities[col]) / len(all_densities[col])
        min_density = min(all_densities[col])
        max_density = max(all_densities[col])
        std_density = (sum((d - avg_density)**2 for d in all_densities[col]) / len(all_densities[col]))**0.5
        
        estimated_total = int(avg_density * total_rows)
        estimated_runs[col] = estimated_total
        
        density_stats[col] = {
            'avg_density': avg_density,
            'min_density': min_density,
            'max_density': max_density,
            'std_density': std_density,
            'estimated_runs': estimated_total,
            'variance_coefficient': std_density / avg_density if avg_density > 0 else 0
        }
    
    return estimated_runs, density_stats


def calculate_rle_for_columns(con, parquet_path: str, sort_columns: List[str] = None, limit: int = None) -> Dict[str, int]:
    """
    Calculate RLE runs for all columns in a parquet file, optionally after sorting.
    
    Args:
        con: DuckDB connection
        parquet_path: Path to parquet file
        sort_columns: List of columns to sort by (in order). If None, uses natural file order.
        limit: Optional limit on number of rows to analyze
    
    Returns:
        Dictionary mapping column names to RLE run counts
    """
    # Get all column names
    columns = con.sql(f"""
        SELECT column_name 
        FROM (
            DESCRIBE 
            SELECT * 
            FROM read_parquet('{parquet_path}', file_row_number = TRUE)
        )
        WHERE column_name != 'file_row_number'
    """).fetchall()
    
    column_names = [col[0] for col in columns]
    
    # Build ORDER BY clause
    if sort_columns:
        order_by = "ORDER BY " + ", ".join(sort_columns)
    else:
        order_by = "ORDER BY file_row_number ASC"
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # Calculate RLE for each column
    results = {}
    for column_name in column_names:
        rle_count = con.sql(f""" 
            WITH ordered_data AS (
                SELECT 
                    {column_name},
                    file_row_number
                FROM read_parquet('{parquet_path}', file_row_number = TRUE)
                {order_by}
                {limit_clause}
            ),
            runs AS (
                SELECT 
                    CASE 
                        WHEN LAG({column_name}) OVER (ORDER BY file_row_number) != {column_name} 
                        OR LAG({column_name}) OVER (ORDER BY file_row_number) IS NULL
                        THEN 1 
                        ELSE 0 
                    END AS new_run
                FROM ordered_data
            )
            SELECT SUM(new_run) AS rle_run_count
            FROM runs
        """).fetchone()[0]
        
        results[column_name] = rle_count
    
    return results


def calculate_nfv_score(con, parquet_path: str, limit: int = None) -> Dict[str, float]:
    """
    Calculate Number of Distinct Values (NFV) for each column.
    Lower NFV = better for RLE compression.
    
    Returns:
        Dictionary mapping column names to NFV ratios (0-1, lower is better)
    """
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    columns = con.sql(f"""
        SELECT column_name 
        FROM (
            DESCRIBE 
            SELECT * 
            FROM read_parquet('{parquet_path}', file_row_number = TRUE)
        )
        WHERE column_name != 'file_row_number'
    """).fetchall()
    
    column_names = [col[0] for col in columns]
    nfv_scores = {}
    
    for col in column_names:
        result = con.sql(f"""
            WITH data AS (
                SELECT {col}
                FROM read_parquet('{parquet_path}', file_row_number = TRUE)
                {limit_clause}
            )
            SELECT 
                COUNT(DISTINCT {col})::FLOAT / COUNT(*)::FLOAT as nfv_ratio
            FROM data
        """).fetchone()
        
        nfv_scores[col] = result[0] if result else 1.0
    
    return nfv_scores


def filter_promising_combinations(columns: List[str], nfv_scores: Dict[str, float], 
                                   max_combinations: int = 20) -> List[List[str]]:
    """
    Apply heuristics to filter down to the most promising column orderings.
    
    Heuristics based on research:
    1. Time/date columns first (temporal ordering)
    2. Low cardinality columns before high cardinality
    3. Correlated columns together (e.g., date + time)
    4. Avoid starting with high-cardinality columns
    
    Args:
        columns: List of all column names
        nfv_scores: NFV ratio for each column (lower = fewer distinct values)
        max_combinations: Maximum number of combinations to return
    
    Returns:
        List of promising column orderings to test
    """
    # Sort columns by NFV (lower first = better for RLE)
    sorted_by_nfv = sorted(columns, key=lambda c: nfv_scores[c])
    
    promising = []
    
    # Rule 1: Natural order baseline
    promising.append([])
    
    # Rule 2: NFV-based ordering (lowest to highest)
    promising.append(sorted_by_nfv)
    
    # Rule 3: Single best column (lowest NFV)
    promising.append([sorted_by_nfv[0]])
    
    # Rule 4: Time-based patterns (common column names)
    time_cols = [c for c in columns if any(t in c.lower() for t in ['date', 'time', 'timestamp', 'year', 'month', 'day'])]
    if time_cols:
        promising.append(time_cols)
        # Time columns + low NFV columns
        non_time = [c for c in sorted_by_nfv if c not in time_cols]
        if non_time:
            promising.append(time_cols + non_time[:2])
    
    # Rule 5: Top 2-3 lowest NFV columns in different orders
    top_low_nfv = sorted_by_nfv[:min(3, len(sorted_by_nfv))]
    for perm in itertools.permutations(top_low_nfv, min(2, len(top_low_nfv))):
        promising.append(list(perm))
    
    # Rule 6: ID-like columns first (common patterns)
    id_cols = [c for c in columns if any(t in c.lower() for t in ['id', 'key', 'code'])]
    if id_cols:
        promising.append(id_cols)
    
    # Rule 7: Categorical/enum-like columns (very low NFV < 0.1)
    categorical = [c for c in sorted_by_nfv if nfv_scores[c] < 0.1]
    if categorical:
        promising.append(categorical)
        # Categorical + time
        if time_cols:
            promising.append(categorical + time_cols)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_promising = []
    for combo in promising:
        key = tuple(combo)
        if key not in seen:
            seen.add(key)
            unique_promising.append(combo)
    
    # Limit to max_combinations
    return unique_promising[:max_combinations]


def test_column_orderings_smart(con, parquet_path: str, limit: int = None, 
                                max_combinations: int = 20, use_stratified_sampling: bool = True,
                                num_segments: int = 5, segment_size: int = 1000) -> pd.DataFrame:
    """
    Test column orderings using heuristics to avoid testing all combinations.
    
    This uses research-backed heuristics:
    - Temporal columns (date/time) should be sorted first
    - Low cardinality (NFV) columns compress better
    - Columns with correlation should be grouped
    
    Args:
        con: DuckDB connection
        parquet_path: Path to parquet file
        limit: Optional limit on number of rows to analyze (ignored if use_stratified_sampling=True)
        max_combinations: Maximum number of orderings to test
        use_stratified_sampling: If True, use stratified sampling across entire file
        num_segments: Number of segments for stratified sampling
        segment_size: Size of each segment for sampling
    
    Returns:
        DataFrame with columns: sort_order, total_rle, avg_rle, nfv_weighted_score, and individual column RLE counts
    """
    print("Analyzing column characteristics...")
    
    # Try to get row group metadata first
    print("\nAttempting to read Parquet row group metadata...")
    row_group_stats = analyze_parquet_row_groups(con, parquet_path)
    if row_group_stats is not None:
        print("✓ Row group metadata available")
        print(row_group_stats.head())
    
    # Get NFV scores for all columns (still use sampling for this as it's cheap)
    sample_size = limit if limit else 100000
    nfv_scores = calculate_nfv_score(con, parquet_path, sample_size)
    
    print(f"\nColumn NFV Scores (lower = better for RLE):")
    for col, score in sorted(nfv_scores.items(), key=lambda x: x[1]):
        print(f"  {col}: {score:.4f}")
    
    # Decide whether to use stratified sampling or simple limit
    if use_stratified_sampling and not limit:
        print("\n" + "="*60)
        print("Using STRATIFIED SAMPLING across entire file")
        print("="*60)
        
        # Get total row count
        total_rows = con.sql(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
        print(f"Total rows in file: {total_rows:,}")
        print(f"Sampling strategy: {num_segments} segments of {segment_size} rows each")
        
        # Get baseline with natural order
        print("\nCalculating baseline (natural order)...")
        estimated_runs, density_stats = stratified_rle_sampling(
            con, parquet_path, None, num_segments, segment_size
        )
        
        print("\nBaseline RLE Density Statistics:")
        for col, stats in sorted(density_stats.items(), key=lambda x: x[1]['estimated_runs']):
            cv = stats['variance_coefficient']
            warning = " ⚠️ HIGH VARIANCE" if cv > 0.3 else ""
            print(f"  {col}: {stats['estimated_runs']:,} runs (density: {stats['avg_density']:.4f}, CV: {cv:.2f}){warning}")
        
        use_estimation = True
    else:
        print("\n" + "="*60)
        print(f"Using simple sampling (first {limit or 'all'} rows)")
        print("="*60)
        use_estimation = False
    
    # Get baseline (natural file order)
    if not use_estimation:
        print("\nCalculating baseline (natural file order)...")
        baseline = calculate_rle_for_columns(con, parquet_path, None, limit)
        column_names = list(baseline.keys())
    else:
        column_names = list(nfv_scores.keys())
    
    # Sort columns by NFV for the NFV-based ordering
    sorted_by_nfv = sorted(column_names, key=lambda c: nfv_scores[c])
    
    # Exclude obvious columns (very low NFV < 0.0001) from permutations
    # These are likely constant columns that compress perfectly anywhere
    nfv_threshold = 0.0001
    non_trivial_cols = [c for c in sorted_by_nfv if nfv_scores[c] >= nfv_threshold]
    trivial_cols = [c for c in sorted_by_nfv if nfv_scores[c] < nfv_threshold]
    
    if trivial_cols:
        print(f"\nExcluding trivial columns from permutations (NFV < {nfv_threshold}): {', '.join(trivial_cols)}")
    
    # Define specific orderings to test
    orderings_to_test = [
        ([], 'current_order'),  # Natural file order
        (sorted_by_nfv, 'order_by_nfv')  # Sorted by NFV (low to high)
    ]
    
    # Add permutations of top N lowest NFV columns (excluding trivial ones)
    top_n = min(3, len(non_trivial_cols))  # Top 3 non-trivial or fewer
    print(f"\nGenerating permutations of top {top_n} lowest non-trivial NFV columns...")
    for perm in itertools.permutations(non_trivial_cols[:top_n]):
        orderings_to_test.append((list(perm), f"perm_{', '.join(perm)}"))
    
    print(f"Testing {len(orderings_to_test)} orderings...")
    results = []
    
    for i, (sort_cols, label) in enumerate(orderings_to_test, 1):
        print(f"\n[{i}/{len(orderings_to_test)}] Testing: {label}")
        
        if use_estimation:
            # Use stratified sampling for this ordering
            print(f"    Sort order: {', '.join(sort_cols) if sort_cols else 'natural (file_row_number)'}")
            est_runs, _ = stratified_rle_sampling(
                con, parquet_path, sort_cols if sort_cols else None, num_segments, segment_size
            )
            rle_counts = est_runs
        else:
            # Use regular calculation
            rle_counts = calculate_rle_for_columns(con, parquet_path, sort_cols if sort_cols else None, limit)
        
        # Calculate weighted score (considering both RLE and NFV)
        nfv_weighted = sum(rle_counts[col] * nfv_scores[col] for col in rle_counts.keys())
        
        results.append({
            'sort_order': label,
            'columns_used': ', '.join(sort_cols) if sort_cols else 'file_row_number',
            'total_rle': sum(rle_counts.values()),
            'avg_rle': sum(rle_counts.values()) / len(rle_counts),
            'nfv_weighted_score': nfv_weighted,
            'estimation_method': 'stratified' if use_estimation else 'sequential',
            **rle_counts
        })
    
    # Convert to DataFrame and sort by total RLE
    df = pd.DataFrame(results)
    df = df.sort_values('total_rle')
    
    print(f"\n✓ Analysis complete! Tested {len(orderings_to_test)} orderings.")
    
    if use_estimation:
        print("\n⚠️  Note: RLE counts are ESTIMATES based on stratified sampling.")
        print("   Use these for relative comparison. Run full analysis on best candidate.")
    
    return df


# Example usage:
# parquet_path = 'abfss://tmp@onelake.dfs.fabric.microsoft.com/data.Lakehouse/Tables/unsorted/summary/0-1c557fc2-59fe-487f-a3ee-67b5e63257df-0.parquet'
# 
# # OPTION 1: Fast stratified sampling across entire file (recommended for large files)
# results_df = test_column_orderings_smart(
#     con, 
#     parquet_path, 
#     use_stratified_sampling=True,
#     num_segments=5,      # Sample 5 segments across the file
#     segment_size=1000    # 1000 rows per segment
# )
# 
# # OPTION 2: Traditional approach with limited rows (faster but less accurate)
# results_df = test_column_orderings_smart(
#     con, 
#     parquet_path, 
#     limit=10000,
#     use_stratified_sampling=False
# )
# 
# # Show results
# print("\nTop 5 best orderings:")
# print(results_df[['sort_order', 'columns_used', 'total_rle', 'estimation_method']].head(5))
# 
# # Once you identify the best ordering, verify with full file scan:
# best_ordering = results_df.iloc[0]['columns_used'].split(', ')
# print(f"\nVerifying best ordering on FULL file: {best_ordering}")
# full_rle = calculate_rle_for_columns(con, parquet_path, best_ordering if best_ordering[0] != 'file_row_number' else None, limit=None)