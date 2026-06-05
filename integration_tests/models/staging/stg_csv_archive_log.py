def model(dbt, session):
    dbt.config(materialized="incremental", incremental_strategy="append", schema="landing")

    import os
    import io
    import gzip
    import zipfile
    import tempfile
    import urllib.request
    from datetime import datetime, timezone
    from concurrent.futures import ThreadPoolExecutor, as_completed

    root_path = os.environ.get("FILES_PATH", "/tmp")
    csv_archive_path = root_path + "/csv"
    csv_log_path = root_path + "/csv_archive_log.parquet"
    download_limit = int(os.environ.get("download_limit", "2"))
    batch_size = 7
    max_workers = 8

    # =========================================================================
    # Load existing log
    # =========================================================================
    log_exists = session.sql(
        f"SELECT count(*) FROM glob('{csv_log_path}')"
    ).fetchone()[0]

    if log_exists > 0:
        # Check if csv_filename column exists in existing parquet
        cols = [row[0] for row in session.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{csv_log_path}')"
        ).fetchall()]
        has_csv_filename = "csv_filename" in cols

        if has_csv_filename:
            session.sql(f"""
                CREATE OR REPLACE TEMP TABLE _csv_archive_log AS
                SELECT source_type, source_filename, archive_path, archived_at,
                       row_count, source_url, etag, csv_filename
                FROM read_parquet('{csv_log_path}')
                WHERE csv_filename IS NOT NULL
            """)
        else:
            # Old-format log without csv_filename — start fresh
            session.sql("""
                CREATE OR REPLACE TEMP TABLE _csv_archive_log (
                    source_type VARCHAR, source_filename VARCHAR,
                    archive_path VARCHAR, archived_at TIMESTAMPTZ,
                    row_count BIGINT, source_url VARCHAR, etag VARCHAR,
                    csv_filename VARCHAR
                )
            """)
    else:
        session.sql("""
            CREATE OR REPLACE TEMP TABLE _csv_archive_log (
                source_type VARCHAR, source_filename VARCHAR,
                archive_path VARCHAR, archived_at TIMESTAMPTZ,
                row_count BIGINT, source_url VARCHAR, etag VARCHAR,
                csv_filename VARCHAR
            )
        """)

    # Get existing source_filenames for dedup
    existing = set()
    for row in session.sql(
        "SELECT source_type || '::' || source_filename FROM _csv_archive_log"
    ).fetchall():
        existing.add(row[0])

    # =========================================================================
    # Helper: download ZIP, extract CSVs to temp dir
    # =========================================================================
    def download_and_extract(url, temp_dir):
        """Download ZIP from url, extract CSV files to temp_dir. Thread-safe."""
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (dbt-aemo)"})
        for attempt in range(3):
            try:
                zip_bytes = urllib.request.urlopen(req, timeout=60).read()
                break
            except urllib.error.HTTPError as e:
                if attempt < 2:
                    import time; time.sleep(2 ** attempt)
                    continue
                raise
        z = zipfile.ZipFile(io.BytesIO(zip_bytes))
        results = []
        for name in z.namelist():
            if name.upper().endswith(".CSV"):
                safe_name = name.replace("/", "_")
                gz_name = safe_name + ".gz"
                gz_path = os.path.join(temp_dir, gz_name)
                with gzip.open(gz_path, "wb") as f:
                    f.write(z.read(name))
                results.append((name, gz_name, gz_path))
        return results

    def copy_to_onelake(temp_path, dest_path):
        """Copy a local file to OneLake via DuckDB COPY."""
        escaped_temp = temp_path.replace("\\", "/")
        if not dest_path.startswith(("az://", "abfss://")):
            dest_dir = dest_path.rsplit("/", 1)[0]
            os.makedirs(dest_dir, exist_ok=True)
        session.sql(
            f"COPY (SELECT content FROM read_blob('{escaped_temp}')) "
            f"TO '{dest_path}' (FORMAT BLOB, COMPRESSION 'none')"
        )

    def save_log():
        """Save current log state to parquet."""
        session.sql(f"COPY _csv_archive_log TO '{csv_log_path}' (FORMAT PARQUET)")

    def process_downloads(rows, source_type, subfolder):
        """Download, extract, copy to OneLake in batches. Saves log after each batch."""
        files_to_process = [(row[0], row[1]) for row in rows]
        for i in range(0, len(files_to_process), batch_size):
            batch = files_to_process[i:i + batch_size]
            with tempfile.TemporaryDirectory() as tmpdir:
                extracted = []
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_meta = {
                        executor.submit(download_and_extract, url, tmpdir): (url, src_fn)
                        for url, src_fn in batch
                    }
                    for future in as_completed(future_to_meta):
                        url, src_fn = future_to_meta[future]
                        try:
                            for csv_name, safe_name, temp_path in future.result():
                                extracted.append((src_fn, safe_name, temp_path, url))
                        except Exception as e:
                            print(f"  WARN: skipping {src_fn}: {e}")

                now = datetime.now(timezone.utc).isoformat()
                for src_fn, csv_name, temp_path, url in extracted:
                    csv_base = csv_name.removesuffix(".gz").removesuffix(".CSV").removesuffix(".csv")
                    dest = f"{csv_archive_path}/{subfolder}/{csv_name}"
                    copy_to_onelake(temp_path, dest)
                    session.sql(f"""
                        INSERT INTO _csv_archive_log VALUES (
                            '{source_type}', '{src_fn}', '/{subfolder}/{csv_name}',
                            '{now}'::TIMESTAMPTZ, NULL, '{url}', NULL, '{csv_base}'
                        )
                    """)
            save_log()

    # =========================================================================
    # DAILY REPORTS (SCADA + PRICE)
    # =========================================================================

    # Fetch file listing from AEMO
    session.sql("""
        CREATE OR REPLACE TEMP TABLE daily_files_web AS
        WITH
          html_data AS (
            SELECT content AS html
            FROM read_text('https://nemweb.com.au/Reports/Current/Daily_Reports/')
          ),
          lines AS (
            SELECT unnest(string_split(html, '<br>')) AS line FROM html_data
          )
        SELECT
          'https://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
          split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\\.zip)"', 1), '.', 1) AS filename
        FROM lines
        WHERE line LIKE '%PUBLIC_DAILY%.zip%'
    """)

    # Check if AEMO has enough new files before hitting GitHub
    aemo_new = session.sql(f"""
        SELECT count(*) FROM daily_files_web
        WHERE 'daily::' || filename NOT IN (
            SELECT source_type || '::' || source_filename FROM _csv_archive_log
        )
    """).fetchone()[0]

    if aemo_new < download_limit:
        # Backfill from GitHub
        session.sql("""
            INSERT INTO daily_files_web
            WITH
              api_responses AS (
                SELECT 2018 AS year, content AS json_content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2018')
                UNION ALL SELECT 2019, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2019')
                UNION ALL SELECT 2020, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2020')
                UNION ALL SELECT 2021, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2021')
                UNION ALL SELECT 2022, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2022')
                UNION ALL SELECT 2023, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2023')
                UNION ALL SELECT 2024, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2024')
                UNION ALL SELECT 2025, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2025')
                UNION ALL SELECT 2026, content FROM read_text('https://api.github.com/repos/djouallah/fabric_demo/contents/data/archive/2026')
              ),
              parsed_files AS (
                SELECT year, unnest(from_json(json_content, '["json"]')) AS file_info
                FROM api_responses
              )
            SELECT
              json_extract_string(file_info, '$.download_url') AS full_url,
              split_part(json_extract_string(file_info, '$.name'), '.', 1) AS filename
            FROM parsed_files
            WHERE json_extract_string(file_info, '$.name') LIKE 'PUBLIC_DAILY%.zip'
              AND split_part(json_extract_string(file_info, '$.name'), '.', 1)
                  NOT IN (SELECT filename FROM daily_files_web)
        """)

    # Get new daily files to download
    daily_to_download = session.sql(f"""
        SELECT full_url, filename FROM daily_files_web
        WHERE 'daily::' || filename NOT IN (
            SELECT source_type || '::' || source_filename FROM _csv_archive_log
        )
        LIMIT {download_limit}
    """).fetchall()

    if daily_to_download:
        process_downloads(daily_to_download, 'daily', 'daily')

    # =========================================================================
    # INTRADAY SCADA
    # =========================================================================

    session.sql("""
        CREATE OR REPLACE TEMP TABLE intraday_scada_web AS
        WITH
          html_data AS (
            SELECT content AS html
            FROM read_text('http://nemweb.com.au/Reports/Current/Dispatch_SCADA/')
          ),
          lines AS (
            SELECT unnest(string_split(html, '<br>')) AS line FROM html_data
          )
        SELECT
          'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
          split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\\.zip)"', 1), '.', 1) AS filename
        FROM lines
        WHERE line LIKE '%PUBLIC_DISPATCHSCADA%'
        ORDER BY full_url DESC
        LIMIT 500
    """)

    scada_to_download = session.sql(f"""
        SELECT full_url, filename FROM intraday_scada_web
        WHERE 'scada_today::' || filename NOT IN (
            SELECT source_type || '::' || source_filename FROM _csv_archive_log
        )
        LIMIT {download_limit}
    """).fetchall()

    if scada_to_download:
        process_downloads(scada_to_download, 'scada_today', 'scada_today')

    # =========================================================================
    # INTRADAY PRICE
    # =========================================================================

    session.sql("""
        CREATE OR REPLACE TEMP TABLE intraday_price_web AS
        WITH
          html_data AS (
            SELECT content AS html
            FROM read_text('http://nemweb.com.au/Reports/Current/DispatchIS_Reports/')
          ),
          lines AS (
            SELECT unnest(string_split(html, '<br>')) AS line FROM html_data
          )
        SELECT
          'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
          split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\\.zip)"', 1), '.', 1) AS filename
        FROM lines
        WHERE line LIKE '%PUBLIC_DISPATCHIS_%.zip%'
        ORDER BY full_url DESC
        LIMIT 500
    """)

    price_to_download = session.sql(f"""
        SELECT full_url, filename FROM intraday_price_web
        WHERE 'price_today::' || filename NOT IN (
            SELECT source_type || '::' || source_filename FROM _csv_archive_log
        )
        LIMIT {download_limit}
    """).fetchall()

    if price_to_download:
        process_downloads(price_to_download, 'price_today', 'price_today')

    # =========================================================================
    # DUID REFERENCE DATA (skip if downloaded less than 24 hours ago)
    # =========================================================================

    duid_sources = [
        (
            "duid_data",
            "duid_data",
            "https://raw.githubusercontent.com/djouallah/aemo_fabric/refs/heads/djouallah-patch-1/duid_data.csv",
            "duid_data.csv",
        ),
        (
            "duid_facilities",
            "facilities",
            "https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv",
            "facilities.csv",
        ),
        (
            "duid_wa_energy",
            "WA_ENERGY",
            "https://raw.githubusercontent.com/djouallah/aemo_fabric/refs/heads/main/WA_ENERGY.csv",
            "WA_ENERGY.csv",
        ),
        (
            "duid_geo_data",
            "geo_data",
            "https://raw.githubusercontent.com/djouallah/aemo_fabric/refs/heads/main/geo_data.csv",
            "geo_data.csv",
        ),
    ]

    # Check if DUID data was downloaded recently (< 24 hours ago)
    last_duid_download = session.sql("""
        SELECT max(archived_at) FROM _csv_archive_log
        WHERE source_type LIKE 'duid_%'
    """).fetchone()[0]

    skip_duid = (
        last_duid_download is not None
        and (datetime.now(last_duid_download.tzinfo) - last_duid_download).total_seconds() < 86400
    )

    if skip_duid:
        print(f"  DUID data is fresh (last download: {last_duid_download}), skipping")
    else:
        duid_dir = f"{csv_archive_path}/duid"
        if not duid_dir.startswith(("az://", "abfss://")):
            os.makedirs(duid_dir, exist_ok=True)

        for source_type, source_filename, url, csv_filename in duid_sources:
            session.sql(f"""
                COPY (
                    SELECT * FROM read_csv_auto('{url}',
                        null_padding=true, ignore_errors=true
                        {", header=true" if source_filename == "WA_ENERGY" else ""})
                ) TO ('{csv_archive_path}/duid/{csv_filename}') (FORMAT CSV, HEADER)
            """)

        # Delete old DUID log entries and re-insert
        session.sql("DELETE FROM _csv_archive_log WHERE source_type LIKE 'duid_%'")
        now = datetime.now(timezone.utc).isoformat()
        for source_type, source_filename, url, csv_filename in duid_sources:
            csv_base = csv_filename.rsplit(".", 1)[0]
            session.sql(f"""
                INSERT INTO _csv_archive_log VALUES (
                    '{source_type}', '{source_filename}',
                    '/duid/{csv_filename}', '{now}'::TIMESTAMPTZ,
                    NULL, '{url}', NULL, '{csv_base}'
                )
            """)

    # =========================================================================
    # Save log to parquet and return
    # =========================================================================
    save_log()

    return session.sql("SELECT * FROM _csv_archive_log")
