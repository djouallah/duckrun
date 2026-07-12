-- Account SCD2 dimension. Historical rows come from CustomerMgmt.xml actions,
-- incremental rows from Batch2/3 Account.txt CDC. Attributes are forward-filled,
-- then each account version is intersected with the owning customer's version
-- window so account and customer surrogate keys stay time-consistent.
with account_incremental as (
  select
    accountid, brokerid, customerid, accountdesc, taxstatus, status,
    {{ batchid_from_filename() }} as batchid
  from {{ read_pipe('Batch[23]/Account.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'accountid': 'BIGINT',
      'brokerid': 'BIGINT', 'customerid': 'BIGINT', 'accountdesc': 'VARCHAR',
      'taxstatus': 'TINYINT', 'status': 'VARCHAR'}", with_filename=true) }}
),
account as (
  select
    accountid, customerid, accountdesc, taxstatus, brokerid, status,
    update_ts, 1 as batchid
  from {{ ref('stg_customermgmt') }}
  where actiontype not in ('UPDCUST', 'INACT')
  union all
  select
    accountid, customerid, accountdesc, taxstatus, brokerid,
    {{ status_longform('a.status') }} as status,
    bd.batchdate::timestamp as update_ts,
    a.batchid
  from account_incremental a
  join {{ ref('BatchDate') }} bd on a.batchid = bd.batchid
),
{% set ff = "over (partition by accountid order by update_ts)" %}
accountfinal as (
  select
    accountid,
    customerid,
    coalesce(accountdesc, last_value(accountdesc ignore nulls) {{ ff }}) as accountdesc,
    coalesce(taxstatus,   last_value(taxstatus   ignore nulls) {{ ff }}) as taxstatus,
    coalesce(brokerid,    last_value(brokerid    ignore nulls) {{ ff }}) as brokerid,
    coalesce(status,      last_value(status      ignore nulls) {{ ff }}) as status,
    date(update_ts) as effectivedate,
    coalesce(
      lead(date(update_ts)) over (partition by accountid order by update_ts),
      date '9999-12-31') as enddate,
    batchid
  from account
),
account_customer_updates as (
  select
    a.accountid, a.accountdesc, a.taxstatus, a.brokerid, a.status, a.customerid,
    c.sk_customerid,
    case when a.effectivedate < c.effectivedate then c.effectivedate else a.effectivedate end as effectivedate,
    case when a.enddate > c.enddate then c.enddate else a.enddate end as enddate,
    a.batchid
  from accountfinal a
  full outer join {{ ref('DimCustomer') }} c
    on a.customerid = c.customerid
   and c.enddate > a.effectivedate
   and c.effectivedate < a.enddate
  where a.effectivedate < a.enddate
)
select
  cast(concat(strftime(a.effectivedate, '%Y%m%d'), cast(a.accountid as varchar)) as bigint) as sk_accountid,
  a.accountid,
  b.sk_brokerid,
  a.sk_customerid,
  a.accountdesc,
  a.taxstatus,
  a.status,
  (a.enddate = date '9999-12-31') as iscurrent,
  a.batchid,
  a.effectivedate,
  a.enddate
from account_customer_updates a
join {{ ref('DimBroker') }} b on a.brokerid = b.brokerid
