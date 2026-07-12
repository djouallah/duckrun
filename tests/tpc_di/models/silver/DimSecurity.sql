-- Security SCD2 dimension, parsed from FINWIRE SEC records and clipped to the
-- effective window of the referenced company (by name or CIK).
with sec as (
  select
    recdate as effectivedate,
    trim(substring(value, 1, 15))   as symbol,
    trim(substring(value, 16, 6))   as issue,
    trim(substring(value, 22, 4))   as status,
    trim(substring(value, 26, 70))  as name,
    trim(substring(value, 96, 6))   as exchangeid,
    cast(substring(value, 102, 13) as decimal(38, 0)) as sharesoutstanding,
    try_strptime(substring(value, 115, 8), '%Y%m%d')::date as firsttrade,
    try_strptime(substring(value, 123, 8), '%Y%m%d')::date as firsttradeonexchange,
    cast(substring(value, 131, 12) as double) as dividend,
    trim(substring(value, 143, 60)) as conameorcik
  from {{ ref('FinWire') }}
  where rectype = 'SEC'
),
dc as (
  select sk_companyid, name as conameorcik, effectivedate, enddate
  from {{ ref('DimCompany') }}
  union all
  select sk_companyid, cast(companyid as varchar) as conameorcik, effectivedate, enddate
  from {{ ref('DimCompany') }}
),
sec_prep as (
  select
    sec.* exclude (status, conameorcik),
    coalesce(cast(try_cast(conameorcik as decimal(38, 0)) as varchar), conameorcik) as conameorcik,
    {{ status_longform('status') }} as status,
    coalesce(
      lead(effectivedate) over (partition by symbol order by effectivedate),
      date '9999-12-31') as enddate
  from sec
),
sec_final as (
  select
    sec.symbol, sec.issue, sec.status, sec.name, sec.exchangeid,
    sec.sharesoutstanding, sec.firsttrade, sec.firsttradeonexchange, sec.dividend,
    case when sec.effectivedate < dc.effectivedate then dc.effectivedate else sec.effectivedate end as effectivedate,
    case when sec.enddate > dc.enddate then dc.enddate else sec.enddate end as enddate,
    dc.sk_companyid
  from sec_prep sec
  join dc
    on sec.conameorcik = dc.conameorcik
   and sec.effectivedate < dc.enddate
   and sec.enddate > dc.effectivedate
)
select
  row_number() over (order by effectivedate) as sk_securityid,
  symbol, issue, status, name, exchangeid, sk_companyid,
  sharesoutstanding, firsttrade, firsttradeonexchange, dividend,
  (enddate = date '9999-12-31') as iscurrent,
  1 as batchid,
  effectivedate, enddate
from sec_final
where effectivedate < enddate
