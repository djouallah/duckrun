-- Running cash balance per account per day. Daily cash-transaction totals are
-- accumulated over time and joined to the account version effective that day.
with cashtransactionhistorical as (
  select accountid, ct_dts, ct_amt, ct_name, 1 as batchid
  from {{ read_pipe('Batch1/CashTransaction.txt',
    "{'accountid': 'BIGINT', 'ct_dts': 'TIMESTAMP', 'ct_amt': 'DOUBLE', 'ct_name': 'VARCHAR'}") }}
),
cashtransactionincremental as (
  select accountid, ct_dts, ct_amt, ct_name, {{ batchid_from_filename() }} as batchid
  from {{ read_pipe('Batch[23]/CashTransaction.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'accountid': 'BIGINT',
      'ct_dts': 'TIMESTAMP', 'ct_amt': 'DOUBLE', 'ct_name': 'VARCHAR'}", with_filename=true) }}
),
dailytotals as (
  select accountid, date(ct_dts) as datevalue, sum(ct_amt) as account_daily_total, batchid
  from cashtransactionhistorical group by accountid, date(ct_dts), batchid
  union all
  select accountid, date(ct_dts) as datevalue, sum(ct_amt) as account_daily_total, batchid
  from cashtransactionincremental group by accountid, date(ct_dts), batchid
)
select
  a.sk_customerid,
  a.sk_accountid,
  cast(strftime(datevalue, '%Y%m%d') as bigint) as sk_dateid,
  sum(account_daily_total) over (partition by c.accountid order by c.datevalue) as cash,
  c.batchid
from dailytotals c
join {{ ref('DimAccount') }} a
  on c.accountid = a.accountid
 and c.datevalue >= a.effectivedate
 and c.datevalue < a.enddate
