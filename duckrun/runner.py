"""
Pipeline execution functionality for duckrun
"""
import os
import requests
import importlib.util
from typing import List, Tuple, Dict, Optional, Callable, Any
from string import Template
from deltalake import DeltaTable, write_deltalake
# Row Group configuration for optimal Delta Lake performance
RG = 8_000_000


def _build_write_deltalake_args(path, df, mode, schema_mode=None, partition_by=None):
    """
    Build arguments for write_deltalake based on requirements:
    - If schema_mode='merge': use rust engine (no row group params)
    - Otherwise: use pyarrow engine with row group optimization
    """
    args = {
        'table_or_uri': path,
        'data': df,
        'mode': mode
    }
    
    # Add partition_by if specified
    if partition_by:
        args['partition_by'] = partition_by
    
    # Engine selection based on schema_mode
    if schema_mode == 'merge':
        # Use rust engine for schema merging (no row group params supported)
        args['schema_mode'] = 'merge'
        args['engine'] = 'rust'
    else:
        # Use pyarrow engine with row group optimization (default)
        args['max_rows_per_file'] = RG
        args['max_rows_per_group'] = RG
        args['min_rows_per_group'] = RG
    
    return args


def run(duckrun_instance, pipeline: List[Tuple]) -> bool:
    """
    Execute pipeline of tasks.
    
    Task formats:
        - Python: ('function_name', (arg1, arg2, ...))
        - SQL:    ('table_name', 'mode') or ('table_name', 'mode', {sql_params})
        - SQL with Delta options: ('table_name', 'mode', {sql_params}, {delta_options})
    
    Returns:
        True if all tasks succeeded
        False if any task failed (exception) or Python task returned 0 (early exit)
    """
    if duckrun_instance.sql_folder is None:
        raise RuntimeError("sql_folder is not configured. Cannot run pipelines.")
    
    for i, task in enumerate(pipeline, 1):
        print(f"\n{'='*60}")
        print(f"Task {i}/{len(pipeline)}: {task[0]}")
        print('='*60)
        
        try:
            result = None
            
            if len(task) == 2:
                name, second = task
                if isinstance(second, str) and second in {'overwrite', 'append', 'ignore'}:
                    result = _run_sql(duckrun_instance, name, second, {}, {})
                else:
                    args = second if isinstance(second, (tuple, list)) else (second,)
                    result = _run_python(duckrun_instance, name, tuple(args))
                
            elif len(task) == 3:
                table, mode, params = task
                if not isinstance(params, dict):
                    raise ValueError(f"Expected dict for params, got {type(params)}")
                result = _run_sql(duckrun_instance, table, mode, params, {})
                
            elif len(task) == 4:
                table, mode, params, delta_options = task
                if not isinstance(params, dict):
                    raise ValueError(f"Expected dict for SQL params, got {type(params)}")
                if not isinstance(delta_options, dict):
                    raise ValueError(f"Expected dict for Delta options, got {type(delta_options)}")
                result = _run_sql(duckrun_instance, table, mode, params, delta_options)
                
            else:
                raise ValueError(f"Invalid task format: {task}")
            
            # Check if Python task returned 0 (early exit condition)
            # Only check for Python tasks as SQL tasks return table names (strings) and only stop on exceptions
            if (len(task) == 2 and 
                not isinstance(task[1], str) and 
                result == 0):
                print(f"\n⏹️  Python task {i} returned 0 - stopping pipeline execution")
                print(f"   Remaining tasks ({len(pipeline) - i}) will not be executed")
                return False
                
        except Exception as e:
            print(f"\n❌ Task {i} failed: {e}")
            return False

    print(f"\n{'='*60}")
    print("✅ All tasks completed successfully")
    print('='*60)
    return True


def _run_python(duckrun_instance, name: str, args: tuple) -> Any:
    """Execute Python task, return result"""
    duckrun_instance._create_onelake_secret()
    func = _load_py_function(duckrun_instance, name)
    if not func:
        raise RuntimeError(f"Python function '{name}' not found")
    
    print(f"Running Python: {name}{args}")
    result = func(*args)
    print(f"✅ Python '{name}' completed")
    return result


def _run_sql(duckrun_instance, table: str, mode: str, params: Dict, delta_options: Dict = None) -> str:
    """Execute SQL task, write to Delta, return normalized table name"""
    duckrun_instance._create_onelake_secret()
    
    if mode not in {'overwrite', 'append', 'ignore'}:
        raise ValueError(f"Invalid mode '{mode}'. Use: overwrite, append, or ignore")

    sql = _read_sql_file(duckrun_instance, table, params)
    if sql is None:
        raise RuntimeError(f"Failed to read SQL file for '{table}'")

    normalized_table = _normalize_table_name(table)
    path = f"{duckrun_instance.table_base_url}{duckrun_instance.schema}/{normalized_table}"

    # Extract Delta Lake specific options from delta_options
    delta_options = delta_options or {}
    merge_schema = delta_options.get('mergeSchema')
    schema_mode = 'merge' if str(merge_schema).lower() in ('true', '1') else None
    partition_by = delta_options.get('partitionBy') or delta_options.get('partition_by')

    if mode == 'overwrite':
        duckrun_instance.con.sql(f"DROP VIEW IF EXISTS {normalized_table}")
        df = duckrun_instance.con.sql(sql).record_batch()
        
        write_args = _build_write_deltalake_args(
            path, df, 'overwrite', 
            schema_mode=schema_mode, 
            partition_by=partition_by
        )
        write_deltalake(**write_args)
        
        duckrun_instance.con.sql(f"CREATE OR REPLACE VIEW {normalized_table} AS SELECT * FROM delta_scan('{path}')")
        dt = DeltaTable(path)
        dt.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False)
        dt.cleanup_metadata()

    elif mode == 'append':
        df = duckrun_instance.con.sql(sql).record_batch()
        
        write_args = _build_write_deltalake_args(
            path, df, 'append', 
            schema_mode=schema_mode, 
            partition_by=partition_by
        )
        write_deltalake(**write_args)
        
        duckrun_instance.con.sql(f"CREATE OR REPLACE VIEW {normalized_table} AS SELECT * FROM delta_scan('{path}')")
        dt = DeltaTable(path)
        if len(dt.file_uris()) > duckrun_instance.compaction_threshold:
            print(f"Compacting {normalized_table} ({len(dt.file_uris())} files)")
            dt.optimize.compact()
            dt.vacuum(dry_run=False)
            dt.cleanup_metadata()

    elif mode == 'ignore':
        try:
            DeltaTable(path)
            print(f"Table {normalized_table} exists. Skipping (mode='ignore')")
        except Exception:
            print(f"Table {normalized_table} doesn't exist. Creating...")
            duckrun_instance.con.sql(f"DROP VIEW IF EXISTS {normalized_table}")
            df = duckrun_instance.con.sql(sql).record_batch()
            
            write_args = _build_write_deltalake_args(
                path, df, 'overwrite', 
                schema_mode=schema_mode, 
                partition_by=partition_by
            )
            write_deltalake(**write_args)
            
            duckrun_instance.con.sql(f"CREATE OR REPLACE VIEW {normalized_table} AS SELECT * FROM delta_scan('{path}')")
            dt = DeltaTable(path)
            dt.vacuum(dry_run=False)
            dt.cleanup_metadata()

    engine_info = f" (engine=rust, schema_mode=merge)" if schema_mode == 'merge' else " (engine=pyarrow)"
    partition_info = f" partitioned by {partition_by}" if partition_by else ""
    print(f"✅ SQL '{table}' → '{normalized_table}' ({mode}){engine_info}{partition_info}")
    return normalized_table


def _normalize_table_name(name: str) -> str:
    """Extract base table name before first '__'"""
    return name.split('__', 1)[0] if '__' in name else name


def _read_sql_file(duckrun_instance, table_name: str, params: Optional[Dict] = None) -> Optional[str]:
    if duckrun_instance.sql_folder is None:
        raise RuntimeError("sql_folder is not configured. Cannot read SQL files.")
    
    is_url = duckrun_instance.sql_folder.startswith("http")
    if is_url:
        url = f"{duckrun_instance.sql_folder.rstrip('/')}/{table_name}.sql".strip()
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            content = resp.text
        except Exception as e:
            print(f"Failed to fetch SQL from {url}: {e}")
            return None
    else:
        path = os.path.join(duckrun_instance.sql_folder, f"{table_name}.sql")
        try:
            with open(path, 'r') as f:
                content = f.read()
        except Exception as e:
            print(f"Failed to read SQL file {path}: {e}")
            return None

    if not content.strip():
        print(f"SQL file is empty: {table_name}.sql")
        return None

    full_params = {
        'ws': duckrun_instance.workspace,
        'lh': duckrun_instance.lakehouse_name,
        'schema': duckrun_instance.schema,
        'storage_account': duckrun_instance.storage_account
    }
    if params:
        full_params.update(params)

    try:
        template = Template(content)
        content = template.substitute(full_params)
    except KeyError as e:
        print(f"Missing parameter in SQL file: ${e}")
        return None
    except Exception as e:
        print(f"Error during SQL template substitution: {e}")
        return None

    return content


def _load_py_function(duckrun_instance, name: str) -> Optional[Callable]:
    if duckrun_instance.sql_folder is None:
        raise RuntimeError("sql_folder is not configured. Cannot load Python functions.")
    
    is_url = duckrun_instance.sql_folder.startswith("http")
    try:
        if is_url:
            url = f"{duckrun_instance.sql_folder.rstrip('/')}/{name}.py".strip()
            resp = requests.get(url)
            resp.raise_for_status()
            code = resp.text
            namespace = {}
            exec(code, namespace)
            func = namespace.get(name)
            return func if callable(func) else None
        else:
            path = os.path.join(duckrun_instance.sql_folder, f"{name}.py")
            if not os.path.isfile(path):
                print(f"Python file not found: {path}")
                return None
            spec = importlib.util.spec_from_file_location(name, path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            func = getattr(mod, name, None)
            return func if callable(func) else None
    except Exception as e:
        print(f"Error loading Python function '{name}': {e}")
        return None