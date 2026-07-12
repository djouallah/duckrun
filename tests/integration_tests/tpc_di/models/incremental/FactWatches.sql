-- Watch-list facts: when each (customer, security) watch was placed and removed.
with watchhistory as (
  select customerid, symbol, w_dts, w_action, 1 as batchid
  from {{ read_pipe('Batch1/WatchHistory.txt',
    "{'customerid': 'BIGINT', 'symbol': 'VARCHAR', 'w_dts': 'TIMESTAMP', 'w_action': 'VARCHAR'}") }}
  union all
  select customerid, symbol, w_dts, w_action, {{ batchid_from_filename() }} as batchid
  from {{ read_pipe('Batch[23]/WatchHistory.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'customerid': 'BIGINT',
      'symbol': 'VARCHAR', 'w_dts': 'TIMESTAMP', 'w_action': 'VARCHAR'}", with_filename=true) }}
),
watches as (
  select
    customerid,
    symbol,
    date(min(w_dts)) as dateplaced,
    date(max(case when w_action = 'CNCL' then w_dts end)) as dateremoved,
    min(batchid) as batchid
  from watchhistory
  group by customerid, symbol
)
select
  c.sk_customerid,
  s.sk_securityid,
  cast(strftime(wh.dateplaced, '%Y%m%d') as bigint) as sk_dateid_dateplaced,
  cast(strftime(wh.dateremoved, '%Y%m%d') as bigint) as sk_dateid_dateremoved,
  wh.batchid
from watches wh
join {{ ref('DimSecurity') }} s
  on s.symbol = wh.symbol
 and wh.dateplaced >= s.effectivedate
 and wh.dateplaced < s.enddate
join {{ ref('DimCustomer') }} c
  on wh.customerid = c.customerid
 and wh.dateplaced >= c.effectivedate
 and wh.dateplaced < c.enddate
