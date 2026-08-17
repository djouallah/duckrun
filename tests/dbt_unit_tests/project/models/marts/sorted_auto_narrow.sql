-- Wide-DECIMAL narrowing on the dbt sorted-auto path (the port of session._narrow_wide_decimals):
-- wide_price is a DECIMAL(38,2) whose exact max fits DECIMAL(18,2), so the overwrite must narrow
-- it (FLBA -> INT64, dictionary encoding back); wide_keep's max does NOT fit and must land wide.
-- Its own model rather than a column on sorted_layout_auto: that fixture is tuned to sit just
-- above the 10% key-organized threshold, and any extra column's modeled bytes flip its key.
{{ config(materialized='table', sort_by='auto') }}

select
    ['east', 'west', 'north'][(1 + hash(r) % 3)::int] as category,
    ((r % 5) / 4.0)::DECIMAL(38, 2) as wide_price,
    (10000000000000000.0 + r)::DECIMAL(38, 2) as wide_keep
from range(200) t(r)
