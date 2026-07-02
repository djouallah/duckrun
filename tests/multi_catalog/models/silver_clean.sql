-- Default (silver) catalog. Reads bronze_raw, which lives in the lh_bronze catalog, so ref() here
-- resolves ACROSS lakehouses (lh_bronze.main.bronze_raw) — the cross-catalog read path.
{{ config(materialized='table') }}

select id, amount * 2 as double_amount
from {{ ref('bronze_raw') }}
