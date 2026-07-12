-- Holdings after each trade, carrying the trade's dimensional keys.
with holdings as (
  select hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty, 1 as batchid
  from {{ read_pipe('Batch1/HoldingHistory.txt',
    "{'hh_h_t_id': 'BIGINT', 'hh_t_id': 'BIGINT', 'hh_before_qty': 'INTEGER', 'hh_after_qty': 'INTEGER'}") }}
  union all
  select hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty, {{ batchid_from_filename() }} as batchid
  from {{ read_pipe('Batch[23]/HoldingHistory.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'hh_h_t_id': 'BIGINT',
      'hh_t_id': 'BIGINT', 'hh_before_qty': 'INTEGER', 'hh_after_qty': 'INTEGER'}", with_filename=true) }}
)
select
  hh.hh_h_t_id as tradeid,
  hh.hh_t_id as currenttradeid,
  dt.sk_customerid,
  dt.sk_accountid,
  dt.sk_securityid,
  dt.sk_companyid,
  dt.sk_closedateid as sk_dateid,
  dt.sk_closetimeid as sk_timeid,
  dt.tradeprice as currentprice,
  hh.hh_after_qty as currentholding,
  hh.batchid
from holdings hh
join {{ ref('DimTrade') }} dt on dt.tradeid = hh.hh_t_id
