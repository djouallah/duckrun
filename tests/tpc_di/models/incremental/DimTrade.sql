-- Trade dimension. Batch1 supplies the full trade + trade-history load; Batch2/3
-- supply CDC (I = insert, U = status update). We resolve each trade's create
-- timestamp and its latest status, then attach broker/security/company/customer/
-- account surrogate keys as of the create time.
with tradeincremental as (
  select
    cdc_flag, tradeid, t_dts, status, t_tt_id, cashflag, t_s_symb, quantity,
    bidprice, t_ca_id, executedby, tradeprice, fee, commission, tax,
    {{ batchid_from_filename() }} as batchid
  from {{ read_pipe('Batch[23]/Trade.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'tradeid': 'BIGINT',
      't_dts': 'TIMESTAMP', 'status': 'VARCHAR', 't_tt_id': 'VARCHAR',
      'cashflag': 'TINYINT', 't_s_symb': 'VARCHAR', 'quantity': 'INTEGER',
      'bidprice': 'DOUBLE', 't_ca_id': 'BIGINT', 'executedby': 'VARCHAR',
      'tradeprice': 'DOUBLE', 'fee': 'DOUBLE', 'commission': 'DOUBLE', 'tax': 'DOUBLE'}",
    with_filename=true) }}
),
tradehistoryraw as (
  select tradeid, th_dts, status
  from {{ read_pipe('Batch1/TradeHistory.txt',
    "{'tradeid': 'BIGINT', 'th_dts': 'TIMESTAMP', 'status': 'VARCHAR'}") }}
),
tradehistory as (
  select
    t_id, t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb, quantity, bidprice,
    t_ca_id, executedby, tradeprice, fee, commission, tax
  from {{ read_pipe('Batch1/Trade.txt',
    "{'t_id': 'BIGINT', 't_dts': 'TIMESTAMP', 't_st_id': 'VARCHAR',
      't_tt_id': 'VARCHAR', 't_is_cash': 'TINYINT', 't_s_symb': 'VARCHAR',
      'quantity': 'INTEGER', 'bidprice': 'DOUBLE', 't_ca_id': 'BIGINT',
      'executedby': 'VARCHAR', 'tradeprice': 'DOUBLE', 'fee': 'DOUBLE',
      'commission': 'DOUBLE', 'tax': 'DOUBLE'}") }}
),
trade_with_latest as (
  select
    tradeid, t_dts, status, t_tt_id, cashflag, t_s_symb, quantity, bidprice,
    t_ca_id, executedby, tradeprice, fee, commission, tax, cdc_flag, batchid,
    row_number() over (partition by tradeid order by t_dts desc) as rn,
    min(t_dts) over (partition by tradeid) as create_ts_raw,
    min(cdc_flag) over (partition by tradeid) as min_cdc_flag,
    min(batchid) over (partition by tradeid) as min_batchid
  from tradeincremental
  qualify rn = 1
),
latest_trades as (
  select
    tradeid, t_dts as latest_t_dts, status as latest_status, t_tt_id, cashflag,
    t_s_symb, quantity, bidprice, t_ca_id, executedby, tradeprice, fee,
    commission, tax, create_ts_raw,
    min_cdc_flag as cdc_flag, min_batchid as batchid
  from trade_with_latest
),
trade_status_history as (
  select tradeid, latest_t_dts as ts, latest_status as status
  from latest_trades where cdc_flag = 'U'
  union all
  select tradeid, th_dts as ts, status from tradehistoryraw
),
current_trade_status as (
  select
    tradeid,
    min(ts) over (partition by tradeid) as create_ts,
    status as current_status,
    ts as last_status_ts,
    row_number() over (partition by tradeid order by ts desc) as status_rn
  from trade_status_history
  qualify status_rn = 1
),
trades_final as (
  select
    t.t_id as tradeid,
    date(cts.create_ts) as create_ts,
    {{ sk_dateid('cts.create_ts') }} as sk_createdateid,
    {{ sk_timeid('cts.create_ts') }} as sk_createtimeid,
    case when cts.current_status in ('CMPT', 'CNCL') then date(cts.last_status_ts) end as close_ts,
    case when cts.current_status in ('CMPT', 'CNCL') then {{ sk_dateid('cts.last_status_ts') }} end as sk_closedateid,
    case when cts.current_status in ('CMPT', 'CNCL') then {{ sk_timeid('cts.last_status_ts') }} end as sk_closetimeid,
    cts.current_status as status,
    t.t_is_cash as cashflag,
    t.t_st_id, t.t_tt_id, t.t_s_symb, t.quantity, t.bidprice, t.t_ca_id,
    t.executedby, t.tradeprice, t.fee, t.commission, t.tax,
    1 as batchid
  from tradehistory t
  join current_trade_status cts on t.t_id = cts.tradeid
  union all
  select
    tradeid,
    date(create_ts_raw) as create_ts,
    {{ sk_dateid('create_ts_raw') }} as sk_createdateid,
    {{ sk_timeid('create_ts_raw') }} as sk_createtimeid,
    case when latest_status in ('CMPT', 'CNCL') then date(latest_t_dts) end as close_ts,
    case when latest_status in ('CMPT', 'CNCL') then {{ sk_dateid('latest_t_dts') }} end as sk_closedateid,
    case when latest_status in ('CMPT', 'CNCL') then {{ sk_timeid('latest_t_dts') }} end as sk_closetimeid,
    latest_status as status,
    cashflag,
    latest_status as t_st_id, t_tt_id, t_s_symb, quantity, bidprice, t_ca_id,
    executedby, tradeprice, fee, commission, tax,
    batchid
  from latest_trades
  where cdc_flag = 'I'
)
select
  trade.tradeid,
  da.sk_brokerid,
  trade.sk_createdateid,
  trade.sk_createtimeid,
  trade.sk_closedateid,
  trade.sk_closetimeid,
  {{ status_longform('trade.status') }} as status,
  case trade.t_tt_id
    when 'TMB' then 'Market Buy'
    when 'TMS' then 'Market Sell'
    when 'TSL' then 'Stop Loss'
    when 'TLS' then 'Limit Sell'
    when 'TLB' then 'Limit Buy'
    else trade.t_tt_id
  end as type,
  (trade.cashflag = 1) as cashflag,
  ds.sk_securityid,
  ds.sk_companyid,
  trade.quantity,
  trade.bidprice,
  da.sk_customerid,
  da.sk_accountid,
  trade.executedby,
  trade.tradeprice,
  trade.fee,
  trade.commission,
  trade.tax,
  trade.batchid
from trades_final as trade
join {{ ref('DimAccount') }} as da
  on trade.t_ca_id = da.accountid
 and trade.create_ts >= da.effectivedate
 and trade.create_ts < da.enddate
join {{ ref('DimSecurity') }} as ds
  on ds.symbol = trade.t_s_symb
 and trade.create_ts >= ds.effectivedate
 and trade.create_ts < ds.enddate
