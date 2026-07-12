-- Customer SCD2 dimension. Historical versions come from CustomerMgmt.xml
-- (stg_customermgmt); incremental changes come from Batch2/3 Customer.txt CDC
-- rows. Each attribute is forward-filled from the last non-null value, and the
-- version window is derived from successive update timestamps.
with customer_incremental as (
  select
    cdc_flag, customerid, taxid, status, lastname, firstname, middleinitial,
    gender, tier, dob, addressline1, addressline2, postalcode, city, stateprov,
    country, c_ctry_1, c_area_1, c_local_1, c_ext_1, c_ctry_2, c_area_2,
    c_local_2, c_ext_2, c_ctry_3, c_area_3, c_local_3, c_ext_3, email1, email2,
    lcl_tx_id, nat_tx_id,
    {{ batchid_from_filename() }} as batchid
  from {{ read_pipe('Batch[123]/Customer.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'customerid': 'BIGINT',
      'taxid': 'VARCHAR', 'status': 'VARCHAR', 'lastname': 'VARCHAR',
      'firstname': 'VARCHAR', 'middleinitial': 'VARCHAR', 'gender': 'VARCHAR',
      'tier': 'TINYINT', 'dob': 'DATE', 'addressline1': 'VARCHAR',
      'addressline2': 'VARCHAR', 'postalcode': 'VARCHAR', 'city': 'VARCHAR',
      'stateprov': 'VARCHAR', 'country': 'VARCHAR',
      'c_ctry_1': 'VARCHAR', 'c_area_1': 'VARCHAR', 'c_local_1': 'VARCHAR', 'c_ext_1': 'VARCHAR',
      'c_ctry_2': 'VARCHAR', 'c_area_2': 'VARCHAR', 'c_local_2': 'VARCHAR', 'c_ext_2': 'VARCHAR',
      'c_ctry_3': 'VARCHAR', 'c_area_3': 'VARCHAR', 'c_local_3': 'VARCHAR', 'c_ext_3': 'VARCHAR',
      'email1': 'VARCHAR', 'email2': 'VARCHAR', 'lcl_tx_id': 'VARCHAR', 'nat_tx_id': 'VARCHAR'}",
    with_filename=true) }}
),
customers as (
  select
    customerid, taxid, status, lastname, firstname, middleinitial, gender, tier,
    dob, addressline1, addressline2, postalcode, city, stateprov, country,
    phone1, phone2, phone3, email1, email2, lcl_tx_id, nat_tx_id,
    1 as batchid, update_ts
  from {{ ref('stg_customermgmt') }}
  where actiontype in ('NEW', 'INACT', 'UPDCUST')
  union all
  select
    customerid,
    nullif(taxid, '') as taxid,
    {{ status_longform('status') }} as status,
    nullif(lastname, '') as lastname,
    nullif(firstname, '') as firstname,
    nullif(middleinitial, '') as middleinitial,
    nullif(gender, '') as gender,
    tier,
    dob,
    nullif(addressline1, '') as addressline1,
    nullif(addressline2, '') as addressline2,
    nullif(postalcode, '') as postalcode,
    nullif(city, '') as city,
    nullif(stateprov, '') as stateprov,
    country,
    case when nullif(c_local_1, '') is not null then
      concat(
        case when nullif(c_ctry_1, '') is not null then concat('+', c_ctry_1, ' ') else '' end,
        case when nullif(c_area_1, '') is not null then concat('(', c_area_1, ') ') else '' end,
        c_local_1, coalesce(c_ext_1, '')) end as phone1,
    case when nullif(c_local_2, '') is not null then
      concat(
        case when nullif(c_ctry_2, '') is not null then concat('+', c_ctry_2, ' ') else '' end,
        case when nullif(c_area_2, '') is not null then concat('(', c_area_2, ') ') else '' end,
        c_local_2, coalesce(c_ext_2, '')) end as phone2,
    case when nullif(c_local_3, '') is not null then
      concat(
        case when nullif(c_ctry_3, '') is not null then concat('+', c_ctry_3, ' ') else '' end,
        case when nullif(c_area_3, '') is not null then concat('(', c_area_3, ') ') else '' end,
        c_local_3, coalesce(c_ext_3, '')) end as phone3,
    nullif(email1, '') as email1,
    nullif(email2, '') as email2,
    nullif(lcl_tx_id, '') as lcl_tx_id,
    nullif(nat_tx_id, '') as nat_tx_id,
    c.batchid,
    bd.batchdate::timestamp as update_ts
  from customer_incremental c
  join {{ ref('BatchDate') }} bd on c.batchid = bd.batchid
),
{% set ff = "over (partition by customerid order by update_ts rows between unbounded preceding and current row)" %}
customerfinal as (
  select
    customerid,
    coalesce(taxid,         last_value(taxid         ignore nulls) {{ ff }}) as taxid,
    status,
    coalesce(lastname,      last_value(lastname      ignore nulls) {{ ff }}) as lastname,
    coalesce(firstname,     last_value(firstname     ignore nulls) {{ ff }}) as firstname,
    coalesce(middleinitial, last_value(middleinitial ignore nulls) {{ ff }}) as middleinitial,
    coalesce(gender,        last_value(gender        ignore nulls) {{ ff }}) as gender,
    coalesce(tier,          last_value(tier          ignore nulls) {{ ff }}) as tier,
    coalesce(dob,           last_value(dob           ignore nulls) {{ ff }}) as dob,
    coalesce(addressline1,  last_value(addressline1  ignore nulls) {{ ff }}) as addressline1,
    coalesce(addressline2,  last_value(addressline2  ignore nulls) {{ ff }}) as addressline2,
    coalesce(postalcode,    last_value(postalcode    ignore nulls) {{ ff }}) as postalcode,
    coalesce(city,          last_value(city          ignore nulls) {{ ff }}) as city,
    coalesce(stateprov,     last_value(stateprov     ignore nulls) {{ ff }}) as stateprov,
    coalesce(country,       last_value(country       ignore nulls) {{ ff }}) as country,
    coalesce(phone1,        last_value(phone1        ignore nulls) {{ ff }}) as phone1,
    coalesce(phone2,        last_value(phone2        ignore nulls) {{ ff }}) as phone2,
    coalesce(phone3,        last_value(phone3        ignore nulls) {{ ff }}) as phone3,
    coalesce(email1,        last_value(email1        ignore nulls) {{ ff }}) as email1,
    coalesce(email2,        last_value(email2        ignore nulls) {{ ff }}) as email2,
    coalesce(lcl_tx_id,     last_value(lcl_tx_id     ignore nulls) {{ ff }}) as lcl_tx_id,
    coalesce(nat_tx_id,     last_value(nat_tx_id     ignore nulls) {{ ff }}) as nat_tx_id,
    lead(update_ts) over (partition by customerid order by update_ts) is null as iscurrent,
    date(update_ts) as effectivedate,
    coalesce(
      lead(date(update_ts)) over (partition by customerid order by update_ts),
      date '9999-12-31') as enddate,
    batchid
  from customers
)
select
  concat(strftime(c.effectivedate, '%Y%m%d'), cast(c.customerid as varchar)) as sk_customerid,
  c.customerid, c.taxid, c.status, c.lastname, c.firstname, c.middleinitial,
  case when c.gender in ('M', 'F') then c.gender else 'U' end as gender,
  c.tier, c.dob, c.addressline1, c.addressline2, c.postalcode, c.city,
  c.stateprov, c.country, c.phone1, c.phone2, c.phone3, c.email1, c.email2,
  r_nat.tx_name as nationaltaxratedesc,
  r_nat.tx_rate as nationaltaxrate,
  r_lcl.tx_name as localtaxratedesc,
  r_lcl.tx_rate as localtaxrate,
  p.agencyid, p.creditrating, p.networth, p.marketingnameplate,
  c.iscurrent, c.batchid, c.effectivedate, c.enddate
from customerfinal c
join {{ ref('TaxRate') }} r_lcl on c.lcl_tx_id = r_lcl.tx_id
join {{ ref('TaxRate') }} r_nat on c.nat_tx_id = r_nat.tx_id
left join {{ ref('ProspectIncremental') }} p
  on upper(p.lastname) = upper(c.lastname)
 and upper(p.firstname) = upper(c.firstname)
 and upper(p.addressline1) = upper(c.addressline1)
 and upper(coalesce(p.addressline2, '')) = upper(coalesce(c.addressline2, ''))
 and upper(p.postalcode) = upper(c.postalcode)
where c.effectivedate < c.enddate
