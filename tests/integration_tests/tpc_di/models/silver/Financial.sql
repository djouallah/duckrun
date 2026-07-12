-- Company financials, parsed from FINWIRE FIN records and matched to the company
-- version (by name, or by CIK) whose effective window contains the record date.
{% set fin_cols %}
    cast(substr(value, 1, 4) as int)    as fi_year,
    cast(substr(value, 5, 1) as int)    as fi_qtr,
    try_strptime(substr(value, 6, 8), '%Y%m%d')::date as fi_qtr_start_date,
    cast(substr(value, 22, 17) as double) as fi_revenue,
    cast(substr(value, 39, 17) as double) as fi_net_earn,
    cast(substr(value, 56, 12) as double) as fi_basic_eps,
    cast(substr(value, 68, 12) as double) as fi_dilut_eps,
    cast(substr(value, 80, 12) as double) as fi_margin,
    cast(substr(value, 92, 17) as double) as fi_inventory,
    cast(substr(value, 109, 17) as double) as fi_assets,
    cast(substr(value, 126, 17) as double) as fi_liability,
    cast(substr(value, 143, 13) as int)   as fi_out_basic,
    cast(substr(value, 156, 13) as int)   as fi_out_dilut
{% endset %}

select dc.sk_companyid, {{ fin_cols }}
from {{ ref('FinWire') }} f
join {{ ref('DimCompany') }} dc
  on f.rectype = 'FIN_NAME'
 and trim(substr(value, 169, 60)) = dc.name
 and f.recdate >= dc.effectivedate
 and f.recdate < dc.enddate
union all
select dc.sk_companyid, {{ fin_cols }}
from {{ ref('FinWire') }} f
join {{ ref('DimCompany') }} dc
  on f.rectype = 'FIN_COMPANYID'
 and try_cast(trim(substr(value, 169, 60)) as bigint) = dc.companyid
 and f.recdate >= dc.effectivedate
 and f.recdate < dc.enddate
