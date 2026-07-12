-- Daily market history with P/E ratio, yield and trailing 52-week high/low.
-- The 52-week extremes are carried with their date via a struct in the window.
with companyfinancials as (
  select
    f.sk_companyid,
    fi_qtr_start_date,
    sum(fi_basic_eps) over (
      partition by d.companyid order by fi_qtr_start_date
      rows between 4 preceding and current row
    ) - fi_basic_eps as sum_fi_basic_eps
  from {{ ref('Financial') }} f
  join {{ ref('DimCompany') }} d on f.sk_companyid = d.sk_companyid
),
dailymarket as (
  select dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, 1 as batchid
  from {{ read_pipe('Batch1/DailyMarket.txt',
    "{'dm_date': 'DATE', 'dm_s_symb': 'VARCHAR', 'dm_close': 'DOUBLE',
      'dm_high': 'DOUBLE', 'dm_low': 'DOUBLE', 'dm_vol': 'INTEGER'}") }}
  union all
  select dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, {{ batchid_from_filename() }} as batchid
  from {{ read_pipe('Batch[23]/DailyMarket.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'dm_date': 'DATE',
      'dm_s_symb': 'VARCHAR', 'dm_close': 'DOUBLE', 'dm_high': 'DOUBLE',
      'dm_low': 'DOUBLE', 'dm_vol': 'INTEGER'}", with_filename=true) }}
),
markethistory as (
  select
    dm.*,
    first_value({'dm_low': dm_low, 'dm_date': dm_date}) over (
      partition by dm_s_symb order by dm_low asc, dm_date asc
      rows between 364 preceding and current row
    ) as fiftytwoweeklow,
    first_value({'dm_high': dm_high, 'dm_date': dm_date}) over (
      partition by dm_s_symb order by dm_high desc, dm_date asc
      rows between 364 preceding and current row
    ) as fiftytwoweekhigh
  from dailymarket dm
)
select
  s.sk_securityid,
  s.sk_companyid,
  cast(strftime(dm_date, '%Y%m%d') as bigint) as sk_dateid,
  coalesce(mh.dm_close / nullif(sum_fi_basic_eps, 0), 0) as peratio,
  coalesce(s.dividend / nullif(mh.dm_close, 0), 0) / 100 as yield,
  fiftytwoweekhigh.dm_high as fiftytwoweekhigh,
  cast(strftime(fiftytwoweekhigh.dm_date, '%Y%m%d') as bigint) as sk_fiftytwoweekhighdate,
  fiftytwoweeklow.dm_low as fiftytwoweeklow,
  cast(strftime(fiftytwoweeklow.dm_date, '%Y%m%d') as bigint) as sk_fiftytwoweeklowdate,
  dm_close as closeprice,
  dm_high as dayhigh,
  dm_low as daylow,
  dm_vol as volume,
  mh.batchid
from markethistory mh
join {{ ref('DimSecurity') }} s
  on s.symbol = mh.dm_s_symb
 and mh.dm_date >= s.effectivedate
 and mh.dm_date < s.enddate
left join companyfinancials f
  on f.sk_companyid = s.sk_companyid
 and extract(quarter from mh.dm_date) = extract(quarter from fi_qtr_start_date)
 and extract(year from mh.dm_date) = extract(year from fi_qtr_start_date)
