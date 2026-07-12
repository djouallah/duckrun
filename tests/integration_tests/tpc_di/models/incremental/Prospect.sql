-- Prospect table: current prospects with derived date keys and a flag marking
-- those that matched a current customer by name + address.
with cust as (
  select lastname, firstname, addressline1, addressline2, postalcode
  from {{ ref('DimCustomer') }}
  where iscurrent
)
select
  p.agencyid,
  cast(strftime(recdate.batchdate, '%Y%m%d') as bigint) as sk_recorddateid,
  cast(strftime(origdate.batchdate, '%Y%m%d') as bigint) as sk_updatedateid,
  p.batchid,
  (c.lastname is not null) as iscustomer,
  p.lastname, p.firstname, p.middleinitial, p.gender, p.addressline1,
  p.addressline2, p.postalcode, p.city, p.state, p.country, p.phone, p.income,
  p.numbercars, p.numberchildren, p.maritalstatus, p.age, p.creditrating,
  p.ownorrentflag, p.employer, p.numbercreditcards, p.networth,
  p.marketingnameplate
from {{ ref('ProspectIncremental') }} p
join {{ ref('BatchDate') }} recdate on p.recordbatchid = recdate.batchid
join {{ ref('BatchDate') }} origdate on p.batchid = origdate.batchid
left join cust c
  on upper(p.lastname) = upper(c.lastname)
 and upper(p.firstname) = upper(c.firstname)
 and upper(p.addressline1) = upper(c.addressline1)
 and upper(coalesce(p.addressline2, '')) = upper(coalesce(c.addressline2, ''))
 and upper(p.postalcode) = upper(c.postalcode)
