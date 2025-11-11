import itertools
from typing import List, Dict, Tuple, Optional
import pandas as pd
from .stats import get_stats

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


def stratified_rle_sampling(con, delta_path: str, sort_columns: List[str] = None,
                            num_segments: int = 5, segment_size: int = 1000) -> Dict[str, float]:
    """
    Sample RLE density across multiple segments of the file.
    
    Args:
        con: DuckDB connection
        delta_path: Path to Delta table
        sort_columns: List of columns to sort by before calculating RLE. If None, uses natural order.
        num_segments: Number of segments to sample across the file
        segment_size: Number of rows per segment
    
    Returns:
        Dictionary with estimated RLE runs per column for full file
    """
    # Get total row count
    total_rows = con.sql(f"""
        SELECT COUNT(*) FROM delta_scan('{delta_path}')
    """).fetchone()[0]
    
    # Get column names
    columns = con.sql(f"""
        SELECT column_name 
        FROM (
            DESCRIBE 
            SELECT * FROM delta_scan('{delta_path}', file_row_number = TRUE)
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
                    FROM delta_scan('{delta_path}', file_row_number = TRUE)
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


def calculate_rle_for_columns(con, delta_path: str, sort_columns: List[str] = None, limit: int = None) -> Dict[str, int]:
    """
    Calculate RLE runs for all columns in a Delta table, optionally after sorting.
    
    Args:
        con: DuckDB connection
        delta_path: Path to Delta table
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
            FROM delta_scan('{delta_path}', file_row_number = TRUE)
        )
        WHERE column_name != 'file_row_number'
    """).fetchall()
    
    column_names = [col[0] for col in columns]
    
    # Build ORDER BY clause
    if sort_columns:
        order_by = "ORDER BY " + ", ".join(sort_columns)
    else:
        order_by = "ORDER BY filename, file_row_number ASC"
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # Calculate RLE for each column
    results = {}
    for column_name in column_names:
        rle_count = con.sql(f""" 
            WITH ordered_data AS (
                SELECT 
                    {column_name},
                    ROW_NUMBER() OVER ({order_by}) as sort_order
                FROM delta_scan('{delta_path}', filename = TRUE, file_row_number = TRUE)
                {limit_clause}
            ),
            runs AS (
                SELECT 
                    CASE 
                        WHEN LAG({column_name}) OVER (ORDER BY sort_order) != {column_name} 
                        OR LAG({column_name}) OVER (ORDER BY sort_order) IS NULL
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


def calculate_cardinality_ratio(con, source: str, limit: int = None, is_parquet: bool = False, 
                       use_approx: bool = None, approx_threshold: int = 100_000_000) -> Dict[str, dict]:
    """
    Calculate cardinality ratio for each column (distinct_values / total_rows).
    Lower ratio = better for RLE compression (more repetition).
    
    NEVER uses sampling - always scans full dataset with exact distinct counts.
    
    Args:
        con: DuckDB connection
        source: Either a table name (default) or parquet file path
        limit: DEPRECATED - kept for backward compatibility but ignored. Always scans full dataset.
        is_parquet: If True, source is a parquet file path; if False, source is a table name
        use_approx: DEPRECATED - always uses exact COUNT(DISTINCT)
        approx_threshold: DEPRECATED - always uses exact COUNT(DISTINCT)
    
    Returns:
        Dictionary mapping column names to dict with keys:
        - 'cardinality_ratio': distinct/total, range 0-1, lower is better for RLE
        - 'total_rows': total row count
        - 'distinct_values': number of distinct values (exact)
    """
    # Build the FROM clause based on source type
    if is_parquet:
        from_clause = f"read_parquet('{source}', file_row_number = TRUE)"
        column_filter = "WHERE column_name != 'file_row_number'"
    else:
        from_clause = source  # Table name
        column_filter = ""
    
    columns = con.sql(f"""
        SELECT column_name 
        FROM (DESCRIBE SELECT * FROM {from_clause})
        {column_filter}
    """).fetchall()
    
    column_names = [col[0] for col in columns]
    
    if not column_names:
        return {}
    
    # Get row count
    total_rows = con.sql(f"SELECT COUNT(*) FROM {from_clause}").fetchone()[0]
    print(f"   Table has {total_rows:,} rows - using exact COUNT(DISTINCT)")
    
    # Build a single query that calculates all cardinality in one pass
    # This scans the data only ONCE instead of once per column
    select_clauses = []
    for col in column_names:
        select_clauses.append(f"COUNT(DISTINCT {col}) as distinct_{col}")
    
    query = f"""
        SELECT 
            COUNT(*)::BIGINT as total_rows,
            {', '.join(select_clauses)}
        FROM {from_clause}
    """
    
    result = con.sql(query).fetchone()
    
    if not result:
        return {}
    
    total_rows = result[0]
    
    nfv_stats = {}
    
    # Parse results (total_rows, distinct_col1, distinct_col2, ...)
    for idx, col in enumerate(column_names, start=1):
        distinct_values = result[idx]
        cardinality_ratio = (distinct_values / total_rows) if total_rows > 0 else 0.0
        
        nfv_stats[col] = {
            'total_rows': total_rows,
            'distinct_values': distinct_values,
            'cardinality_ratio': cardinality_ratio
        }
    
    return nfv_stats


def filter_promising_combinations(columns: List[str], nfv_scores: Dict[str, float], 
                                   max_combinations: int = 20) -> List[List[str]]:
    """
    Apply heuristics to filter down to the most promising column orderings.
    
    Heuristics based on research:
    1. Time/date columns first (temporal ordering)
    2. High NFV score columns before low NFV score (more repetition = better RLE)
    3. Correlated columns together (e.g., date + time)
    4. Avoid starting with low-NFV columns (high cardinality)
    
    Args:
        columns: List of all column names
        nfv_scores: NFV score for each column (higher = more repetition, better for RLE)
        max_combinations: Maximum number of combinations to return
    
    Returns:
        List of promising column orderings to test
    """
    # Sort columns by NFV (higher first = better for RLE)
    sorted_by_nfv = sorted(columns, key=lambda c: nfv_scores[c], reverse=True)
    
    promising = []
    
    # Rule 1: Natural order baseline
    promising.append([])
    
    # Rule 2: NFV-based ordering (highest to lowest)
    promising.append(sorted_by_nfv)
    
    # Rule 3: Single best column (highest NFV)
    promising.append([sorted_by_nfv[0]])
    
    # Rule 4: Time-based patterns (common column names)
    time_cols = [c for c in columns if any(t in c.lower() for t in ['date', 'time', 'timestamp', 'year', 'month', 'day'])]
    if time_cols:
        promising.append(time_cols)
        # Time columns + high NFV columns
        non_time = [c for c in sorted_by_nfv if c not in time_cols]
        if non_time:
            promising.append(time_cols + non_time[:2])
    
    # Rule 5: Top 2-3 highest NFV columns in different orders
    top_high_nfv = sorted_by_nfv[:min(3, len(sorted_by_nfv))]
    for perm in itertools.permutations(top_high_nfv, min(2, len(top_high_nfv))):
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


def test_column_orderings_smart(con, delta_path: str, table_name: str = None, limit: int = None, 
                                mode: str = "natural",
                                min_distinct_threshold: int = 2,
                                max_cardinality_pct: float = 0.01,
                                max_ordering_depth: int = 3,
                                schema_name: str = None,
                                table_display_name: str = None,
                                duckrun_instance = None) -> pd.DataFrame:
    """
    Test column orderings for RLE optimization.
    
    Modes:
    - "natural": Calculate RLE for natural order only (baseline)
    - "auto": Natural order + cardinality-based ordering (low to high)
    - "advanced": Natural + cardinality + greedy incremental search
    
    Args:
        con: DuckDB connection
        delta_path: Path to Delta table (used for RLE calculation with file_row_number via delta_scan)
        table_name: Optional table name for cardinality calculation on full dataset (if None, uses delta_path)
        limit: Optional limit on number of rows to analyze (for testing only)
        mode: Analysis mode - "natural", "auto", or "advanced" (default: "natural")
        min_distinct_threshold: Exclude columns with fewer distinct values (default: 2, i.e. only exclude constants with 1 value)
        max_cardinality_pct: Exclude columns with cardinality ratio above this % (default: 0.01 = 1%)
        max_ordering_depth: Maximum depth for greedy incremental search in "advanced" mode (default: 3)
        schema_name: Optional schema name to include in results (default: None)
        table_display_name: Optional table name to include in results (default: None)
        duckrun_instance: Optional Duckrun instance to fetch detailed parquet stats (default: None)
    
    Returns:
        DataFrame with columns: schema, table, sort_order, columns_used, total_rle_all, and individual column RLE counts
    """
    print("Analyzing column characteristics...")
    
    # Calculate cardinality ratios first (for all modes)
    print("\nCalculating cardinality ratios on full dataset...")
    if table_name:
        card_stats = calculate_cardinality_ratio(con, table_name, is_parquet=False)
    else:
        # Fallback: use delta_scan directly
        card_stats = calculate_cardinality_ratio(con, f"delta_scan('{delta_path}')", is_parquet=False)
    
    print(f"\nColumn Cardinality Ratios (lower = better for RLE):")
    for col, stats in sorted(card_stats.items(), key=lambda x: x[1]['cardinality_ratio']):
        card_pct = stats['cardinality_ratio'] * 100
        print(f"  {col}: {card_pct:.3f}% (distinct: {stats['distinct_values']:,}, rows: {stats['total_rows']:,})")
    
    # For "natural" mode, just calculate RLE on natural order
    if mode == "natural":
        print("\n" + "="*60)
        print("Mode: NATURAL ORDER (baseline)")
        print("="*60)
        print("Calculating RLE for natural file order (single pass)...")
        
        # Get all column names
        columns = con.sql(f"""
            SELECT column_name 
            FROM (
                DESCRIBE 
                SELECT * FROM delta_scan('{delta_path}', file_row_number = TRUE)
            )
            WHERE column_name != 'file_row_number'
        """).fetchall()
        
        column_names = [col[0] for col in columns]
        
        # Calculate RLE for natural order
        rle_counts = calculate_rle_for_columns(con, delta_path, None, limit)
        
        total_rle_all = sum(rle_counts.values())
        
        print(f"\nResults:")
        print(f"  Total RLE (all columns): {total_rle_all:,}")
        
        results = [{
            'schema': schema_name,
            'table': table_display_name,
            'sort_order': 'natural_order',
            'columns_used': 'file_row_number',
            'total_rle_all': total_rle_all,
            **rle_counts
        }]
        
        df = pd.DataFrame(results)
        print(f"\n{'='*60}")
        print(f"✓ Analysis complete!")
        print(f"{'='*60}")
        
        # Get detailed parquet stats if duckrun_instance is provided
        parquet_stats = None
        vorder_status = False
        table_size_mb = None
        if duckrun_instance and table_display_name:
            print("\nFetching detailed parquet metadata...")
            try:
                # For single-schema connections, just use the table name
                # For multi-schema connections, use schema.table format
                if hasattr(duckrun_instance, 'scan_all_schemas') and duckrun_instance.scan_all_schemas and schema_name:
                    source_param = f"{schema_name}.{table_display_name}"
                else:
                    source_param = table_display_name
                
                parquet_stats = get_stats(duckrun_instance, source=source_param, detailed=True)
                print(f"✓ Retrieved parquet metadata for {len(parquet_stats)} row groups/columns")
                
                # Get vorder status from the stats if available
                if 'vorder' in parquet_stats.columns:
                    vorder_status = parquet_stats['vorder'].iloc[0] if len(parquet_stats) > 0 else False
                
                # Calculate total table size from compressed sizes
                if 'total_compressed_size' in parquet_stats.columns:
                    total_bytes = parquet_stats['total_compressed_size'].sum()
                    table_size_mb = round(total_bytes / (1024 * 1024), 2) if total_bytes else None
            except Exception as e:
                print(f"⚠️  Could not fetch parquet stats: {e}")
                parquet_stats = None
        
        # Transform to long format
        long_format_results = []
        
        for _, row in df.iterrows():
            schema_val = row['schema']
            table_val = row['table']
            sort_order = row['sort_order']
            columns_used = row['columns_used']
            total_rle_all_val = row['total_rle_all']
            
            # Get all column names except metadata columns
            metadata_cols = ['schema', 'table', 'sort_order', 'columns_used', 'total_rle_all']
            data_columns = [col for col in df.columns if col not in metadata_cols]
            
            # Get total rows and NDV from card_stats if available
            total_rows = card_stats[data_columns[0]]['total_rows'] if card_stats and data_columns else None
            
            # Aggregate parquet stats per column if available
            parquet_by_column = {}
            avg_row_group_size = None  # Calculate once for the table
            
            if parquet_stats is not None and not parquet_stats.empty:
                # Calculate average row group size once (same for all columns)
                if 'row_group_num_rows' in parquet_stats.columns:
                    # Get unique row groups to avoid counting duplicates (one entry per column per row group)
                    unique_rg_sizes = parquet_stats.drop_duplicates(subset=['row_group_id'])['row_group_num_rows']
                    avg_row_group_size = int(unique_rg_sizes.mean())
                
                # Determine column name field - can be 'name' or 'path_in_schema'
                col_name_field = 'path_in_schema' if 'path_in_schema' in parquet_stats.columns else 'name'
                
                # Group by column name and aggregate
                for col_name in data_columns:
                    col_stats = parquet_stats[parquet_stats[col_name_field] == col_name] if col_name_field in parquet_stats.columns else pd.DataFrame()
                    
                    if not col_stats.empty:
                        # Aggregate stats across all row groups for this column
                        total_compressed_bytes = col_stats['total_compressed_size'].sum() if 'total_compressed_size' in col_stats.columns else None
                        total_compressed_mb = round(total_compressed_bytes / (1024 * 1024), 2) if total_compressed_bytes else None
                        # Handle both 'encodings' (multiple) and 'encoding' (single) column names
                        if 'encodings' in col_stats.columns:
                            encodings = col_stats['encodings'].unique().tolist()
                        elif 'encoding' in col_stats.columns:
                            encodings = col_stats['encoding'].unique().tolist()
                        else:
                            encodings = []
                        compressions = col_stats['compression'].unique().tolist() if 'compression' in col_stats.columns else []
                        num_row_groups = col_stats['row_group_id'].nunique() if 'row_group_id' in col_stats.columns else len(col_stats)
                        
                        parquet_by_column[col_name] = {
                            'encoding': ', '.join([str(e) for e in encodings if e is not None]),
                            'compression': ', '.join([str(c) for c in compressions if c is not None]),
                            'total_compressed_size_mb': total_compressed_mb,
                            'num_row_groups': num_row_groups
                        }
            
            # Create one row per data column
            for col in data_columns:
                rle_value = row[col]
                
                # Get NDV from card_stats
                ndv_value = card_stats[col]['distinct_values'] if card_stats and col in card_stats else None
                
                # Get parquet stats for this column
                col_parquet = parquet_by_column.get(col, {})
                
                long_format_results.append({
                    'schema': schema_val,
                    'table': table_val,
                    'sort_type': sort_order,
                    'name': col,
                    'order': None,
                    'RLE': rle_value,
                    'NDV': ndv_value,
                    'total_rows': total_rows,
                    'total_RLE': total_rle_all_val,
                    'encoding': col_parquet.get('encoding', ''),
                    'compression': col_parquet.get('compression', ''),
                    'column_size_mb': col_parquet.get('total_compressed_size_mb', None),
                    'avg_row_group_size': avg_row_group_size,
                    'table_size_mb': table_size_mb,
                    'vorder': vorder_status,
                    'comments': ''
                })
        
        long_df = pd.DataFrame(long_format_results)
        
        return long_df
    
    # For "auto" and "advanced" modes, continue with optimization
    # Extract just the ratios for easier handling
    cardinality_ratios = {col: stats['cardinality_ratio'] for col, stats in card_stats.items()}
    column_names = list(card_stats.keys())
    
    # Sort columns by cardinality for ordering (lower cardinality = better for RLE)
    sorted_by_cardinality = sorted(column_names, key=lambda c: cardinality_ratios[c])
    
    # OPTIMIZATION: Filter columns based on configurable thresholds
    # Exclude columns that won't benefit from reordering:
    # 1. Too constant: < min_distinct_threshold (default: 2, only excludes single-value columns)
    # 2. Too fragmented: cardinality_ratio > max_cardinality_pct (default: 10%)
    total_rows = next(iter(card_stats.values()))['total_rows']
    
    constant_cols = [c for c in sorted_by_cardinality 
                    if card_stats[c]['distinct_values'] < min_distinct_threshold]
    
    fragmented_cols = [c for c in sorted_by_cardinality 
                      if cardinality_ratios[c] > max_cardinality_pct]
    
    good_for_reordering = [c for c in sorted_by_cardinality 
                          if c not in constant_cols and c not in fragmented_cols]
    
    if constant_cols:
        print(f"\n✓ Skipping constant columns (< {min_distinct_threshold} distinct values): {', '.join(constant_cols)}")
        print(f"  These compress perfectly regardless of ordering.")
    
    if fragmented_cols:
        print(f"✓ Skipping high-cardinality columns (cardinality > {max_cardinality_pct*100:.0f}%): {', '.join(fragmented_cols)}")
        print(f"  These are too fragmented to benefit from reordering.")
    
    if not good_for_reordering:
        print("\n⚠️  No columns suitable for reordering optimization!")
        print("    All columns are either nearly constant or have too many unique values.")
        return None
    
    print(f"\n✓ Analyzing {len(good_for_reordering)} columns suitable for reordering")
    
    # Get total row count from cardinality calculation
    total_rows = next(iter(card_stats.values()))['total_rows'] if card_stats else 0
    print(f"✓ Table size: {total_rows:,} rows")
    
    # Calculate RLE ONLY on natural order for baseline (single pass - fast!)
    print("\n" + "="*60)
    print("Calculating baseline RLE (natural order - single pass)")
    print("="*60)
    baseline = calculate_rle_for_columns(con, delta_path, None, limit)
    
    # Filter baseline to only include good_for_reordering columns
    baseline_filtered = {col: rle for col, rle in baseline.items() if col in good_for_reordering}
    
    # Show column categorization upfront
    print(f"\nColumn Analysis (baseline RLE in natural order):")
    
    # Show columns worth reordering first
    if baseline_filtered:
        print(f"  Columns included in optimization:")
        for col in sorted(baseline_filtered.keys(), key=lambda c: baseline_filtered[c]):
            print(f"    {col}: {baseline_filtered[col]:,} runs")
        print(f"    ─────────────────────────")
        print(f"    Subtotal: {sum(baseline_filtered.values()):,} runs")
    
    # Show excluded columns (constant or high-cardinality)
    excluded_cols = {col: rle for col, rle in baseline.items() 
                    if col in constant_cols or col in fragmented_cols}
    if excluded_cols:
        print(f"  Columns excluded from optimization:")
        for col in sorted(excluded_cols.keys(), key=lambda c: excluded_cols[c]):
            reason = "constant" if col in constant_cols else "high-cardinality"
            print(f"    {col}: {excluded_cols[col]:,} runs ({reason})")
        print(f"    ─────────────────────────")
        print(f"    Subtotal: {sum(excluded_cols.values()):,} runs")
    
    # Show total baseline RLE
    print(f"\nBaseline Total RLE (all columns): {sum(baseline.values()):,} runs")
    
    # Define only the most promising orderings to test
    orderings_to_test = [
        ([], 'natural_order'),  # Baseline
    ]
    
    # Add cardinality-based ordering for "auto" and "advanced" modes
    if mode in ["auto", "advanced"] and len(good_for_reordering) >= 2:
        orderings_to_test.append((good_for_reordering, 'by_cardinality'))
    
    # Count only the actual reordering tests (exclude natural_order baseline)
    num_tests = len(orderings_to_test) - 1
    
    results = []
    
    for i, (sort_cols, label) in enumerate(orderings_to_test, 1):
        if i == 1:
            # Use baseline for natural order (already calculated and displayed)
            rle_counts = baseline
        else:
            # This is an actual reordering test
            test_num = i - 1
            print(f"\n[{test_num}/{num_tests}] Testing: {label}")
            if sort_cols:
                print(f"    Order: {', '.join(sort_cols)}")
            
            # Calculate RLE for this ordering
            rle_counts = calculate_rle_for_columns(con, delta_path, sort_cols, limit)
        
        # Calculate metrics for ALL columns and optimizable subset
        total_rle_all = sum(rle_counts.values())
        
        # Filter to only good_for_reordering columns for scoring/comparison
        rle_filtered = {col: rle for col, rle in rle_counts.items() if col in good_for_reordering}
        total_rle_optimizable = sum(rle_filtered.values())
        
        # Calculate weighted score (considering both RLE and cardinality - lower cardinality = better)
        cardinality_weighted = sum(rle_filtered[col] * cardinality_ratios[col] for col in rle_filtered.keys())
        
        print(f"    Total RLE: {total_rle_all:,} runs")
        
        results.append({
            'schema': schema_name,
            'table': table_display_name,
            'sort_order': label,
            'columns_used': ', '.join(sort_cols) if sort_cols else 'file_row_number',
            'total_rle_all': total_rle_all,  # All columns (must be >= row_count)
            'optimizable_rle': total_rle_optimizable,  # Only columns we're optimizing
            'avg_rle': total_rle_optimizable / len(rle_filtered),
            'cardinality_weighted_score': cardinality_weighted,
            'method': 'single_pass',
            **rle_counts  # Include individual column RLE counts
        })
    
    # Greedy incremental search (only in "advanced" mode)
    if mode == "advanced" and max_ordering_depth > 0 and len(good_for_reordering) >= 2:
        print(f"\n{'='*60}")
        print(f"ADVANCED MODE: Greedy Incremental Search (max depth: {max_ordering_depth})")
        print(f"{'='*60}")
        print(f"Building optimal ordering column-by-column, testing all positions")
        print(f"at each depth to find the best incremental improvement.\n")
        
        current_best_ordering = []
        current_best_rle = sum(baseline_filtered.values())
        remaining_columns = list(good_for_reordering)
        
        # Get the cardinality-based RLE as the target to beat (both total and optimizable)
        cardinality_rle = results[-1]['optimizable_rle'] if len(results) > 1 else float('inf')
        cardinality_total_rle = results[-1]['total_rle_all'] if len(results) > 1 else float('inf')
        
        for depth in range(1, min(max_ordering_depth + 1, len(good_for_reordering) + 1)):
            num_candidates = len(remaining_columns)
            num_positions = len(current_best_ordering) + 1
            total_tests = num_candidates * num_positions
            print(f"\n--- Depth {depth}: Testing {num_candidates} candidate columns × {num_positions} positions = {total_tests} tests ---")
            print(f"    Target to beat: {cardinality_total_rle:,} runs (cardinality ordering)")
            
            best_depth_ordering = None
            best_depth_rle = float('inf')
            best_depth_col = None
            best_depth_position = None
            early_exit = False
            
            # Sort remaining candidates by baseline RLE (HIGHER first = test worse candidates first)
            # This way we test DUID, time, date before cutoff (which we know is good from cardinality test)
            candidates_sorted = sorted(remaining_columns, key=lambda c: baseline_filtered[c], reverse=True)
            
            test_num = 0
            # Try adding each remaining column (sorted by baseline RLE - worse first)
            for candidate_col in candidates_sorted:
                # Try inserting at each possible position (including end)
                for insert_pos in range(len(current_best_ordering) + 1):
                    test_num += 1
                    
                    # Build test ordering: insert candidate at position
                    test_ordering = current_best_ordering[:insert_pos] + [candidate_col] + current_best_ordering[insert_pos:]
                    
                    print(f"  [{test_num}/{total_tests}] Testing '{candidate_col}' at position {insert_pos}: [{', '.join(test_ordering)}]", end='', flush=True)
                    
                    # Calculate RLE for this ordering
                    rle_counts = calculate_rle_for_columns(con, delta_path, test_ordering, limit)
                    
                    # Sum RLE for optimizable columns only
                    rle_filtered = {col: rle for col, rle in rle_counts.items() if col in good_for_reordering}
                    total_rle = sum(rle_filtered.values())
                    total_rle_all = sum(rle_counts.values())
                    
                    is_best = total_rle < best_depth_rle
                    beats_cardinality = total_rle < cardinality_rle
                    
                    status = ""
                    if beats_cardinality:
                        status = "  🎯 Beats cardinality!"
                    
                    print(f" → Total: {total_rle_all:,}{status}")
                    
                    # Track best at this depth
                    if is_best:
                        best_depth_rle = total_rle
                        best_depth_ordering = test_ordering
                        best_depth_col = candidate_col
                        best_depth_position = insert_pos
                        best_depth_rle_counts = rle_counts
                        
                        # Early exit if we beat cardinality ordering!
                        if beats_cardinality:
                            print(f"\n  ⚡ Early exit! Found ordering better than cardinality. Moving to next depth.")
                            early_exit = True
                            break
                
                if early_exit:
                    break
            
            # Check if we found improvement
            if best_depth_rle < current_best_rle:
                current_total_rle_all = sum(best_depth_rle_counts.values())
                baseline_total_rle_all = sum(baseline.values())
                improvement_pct = ((baseline_total_rle_all - current_total_rle_all) / baseline_total_rle_all) * 100
                print(f"\n✓ Best at depth {depth}: [{', '.join(best_depth_ordering)}]")
                print(f"  Total RLE (all columns): {current_total_rle_all:,} runs")
                print(f"  Optimizable RLE: {best_depth_rle:,} runs")
                print(f"  Improvement: {improvement_pct:.1f}% better than baseline (total RLE)")
                
                # Update for next depth
                current_best_ordering = best_depth_ordering
                current_best_rle = best_depth_rle
                remaining_columns.remove(best_depth_col)
                
                # Store this result
                rle_filtered = {col: rle for col, rle in best_depth_rle_counts.items() if col in good_for_reordering}
                total_rle_all = sum(best_depth_rle_counts.values())
                cardinality_weighted = sum(rle_filtered[col] * cardinality_ratios[col] for col in rle_filtered.keys())
                
                results.append({
                    'schema': schema_name,
                    'table': table_display_name,
                    'sort_order': f'greedy_depth_{depth}',
                    'columns_used': ', '.join(best_depth_ordering),
                    'total_rle_all': total_rle_all,
                    'optimizable_rle': best_depth_rle,
                    'avg_rle': best_depth_rle / len(rle_filtered),
                    'cardinality_weighted_score': cardinality_weighted,
                    'method': 'greedy_incremental',
                    **best_depth_rle_counts
                })
            else:
                print(f"\n✗ No improvement found at depth {depth} - stopping early")
                print(f"  Best RLE (all columns): {sum(best_depth_rle_counts.values()) if best_depth_rle_counts else sum(baseline.values()):,} runs")
                print(f"  Best optimizable RLE: {best_depth_rle if best_depth_rle != float('inf') else current_best_rle:,} runs")
                break
        
        print(f"\n{'='*60}")
        print(f"Greedy Search Complete")
        print(f"{'='*60}")
        if current_best_ordering:
            print(f"Final greedy ordering: {', '.join(current_best_ordering)}")
            print(f"Final optimizable RLE: {current_best_rle:,} runs")

    
    # Convert to DataFrame and sort by optimizable RLE (lower is better)
    df = pd.DataFrame(results)
    df = df.sort_values('optimizable_rle')
    
    print(f"\n{'='*60}")
    print(f"✓ Analysis complete!")
    print(f"{'='*60}")
    print(f"Best ordering: {df.iloc[0]['sort_order']}")
    print(f"Best total RLE: {df.iloc[0]['total_rle_all']:,} runs (lower is better)")

    
    # Calculate improvement using total RLE (all columns) for meaningful comparison
    baseline_total_rle = sum(baseline.values())
    best_total_rle = df.iloc[0]['total_rle_all']
    if len(df) > 1 and baseline_total_rle > 0:
        pct = ((baseline_total_rle - best_total_rle) / baseline_total_rle) * 100
        if pct > 0:
            print(f"Improvement: {pct:.1f}% fewer runs vs natural order")
    
    # Remove confusing internal columns from displayed output
    # Keep: sort_order, columns_used, total_rle_all, and individual column RLE counts
    # Remove: optimizable_rle, avg_rle, cardinality_weighted_score, method
    display_df = df.drop(columns=['optimizable_rle', 'avg_rle', 'cardinality_weighted_score', 'method'], errors='ignore')
    
    # Transform to long format
    long_format_results = []
    
    for _, row in display_df.iterrows():
        schema_val = row['schema']
        table_val = row['table']
        sort_order = row['sort_order']
        columns_used = row['columns_used']
        total_rle_all = row['total_rle_all']
        
        # Get all column names except metadata columns
        metadata_cols = ['schema', 'table', 'sort_order', 'columns_used', 'total_rle_all']
        data_columns = [col for col in display_df.columns if col not in metadata_cols]
        
        # Get total rows and NDV from card_stats if available
        total_rows = card_stats[data_columns[0]]['total_rows'] if card_stats and data_columns else None
        
        # Parse the columns_used to get ordering
        sort_columns_list = []
        if columns_used != 'file_row_number':
            sort_columns_list = [c.strip() for c in columns_used.split(',')]
        
        # Create one row per data column
        for col in data_columns:
            rle_value = row[col]
            
            # Get NDV from card_stats
            ndv_value = card_stats[col]['distinct_values'] if card_stats and col in card_stats else None
            
            # Determine if column was included in the sort and its position
            is_in_sort = col in sort_columns_list
            order_position = sort_columns_list.index(col) + 1 if is_in_sort else None
            comment = '' if is_in_sort or columns_used == 'file_row_number' else 'not included in the sort'
            
            long_format_results.append({
                'schema': schema_val,
                'table': table_val,
                'sort_type': sort_order,
                'name': col,
                'order': order_position,
                'RLE': rle_value,
                'NDV': ndv_value,
                'total_rows': total_rows,
                'total_RLE': total_rle_all,
                'comments': comment
            })
    
    long_df = pd.DataFrame(long_format_results)
    
    return long_df


# Example usage:
# delta_path = 'abfss://tmp@onelake.dfs.fabric.microsoft.com/data.Lakehouse/Tables/unsorted/summary'
# 
# # Fast single-pass analysis (recommended for all table sizes)
# results_df = test_column_orderings_smart(con, delta_path, table_name='summary')
# 
# # Show results
# print("\nBest orderings:")
# print(results_df[['sort_order', 'columns_used', 'optimizable_rle', 'total_rle_all', 'method']].head())
# 
# # The function automatically:
# # - Calculates exact cardinality ratios (or approximate for >100M rows)
# # - Excludes columns that won't benefit from reordering
# # - Tests only 2-3 most promising orderings (low cardinality first, high cardinality first)
# # - Uses single-pass RLE calculation (fast!)