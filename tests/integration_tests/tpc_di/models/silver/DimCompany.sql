-- Company SCD2 dimension, parsed from FINWIRE CMP records. EndDate is the next
-- posting date for the same CIK (open-ended for the latest version).
with cmp as (
  select
    recdate,
    trim(substr(value, 1, 60))    as companyname,
    trim(substr(value, 61, 10))   as cik,
    trim(substr(value, 71, 4))    as status,
    trim(substr(value, 75, 2))    as industryid,
    trim(substr(value, 77, 4))    as sprating,
    try_strptime(nullif(trim(substr(value, 81, 8)), ''), '%Y%m%d')::date as foundingdate,
    trim(substr(value, 89, 80))   as addrline1,
    trim(substr(value, 169, 80))  as addrline2,
    trim(substr(value, 249, 12))  as postalcode,
    trim(substr(value, 261, 25))  as city,
    trim(substr(value, 286, 20))  as stateprovince,
    trim(substr(value, 306, 24))  as country,
    trim(substr(value, 330, 46))  as ceoname,
    trim(substr(value, 376, 150)) as description
  from {{ ref('FinWire') }}
  where rectype = 'CMP'
),
cmp_transformed as (
  select
    cast(cik as bigint) as companyid,
    {{ status_longform('cmp.status') }} as status,
    companyname as name,
    ind.in_name as industry,
    case
      when sprating in ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D')
      then sprating else null
    end as sprating,
    case
      when sprating in ('AAA','AA','A','AA+','A+','AA-','A-','BBB','BBB+','BBB-') then false
      when sprating in ('BB','B','CCC','CC','C','D','BB+','B+','CCC+','BB-','B-','CCC-') then true
      else null
    end as islowgrade,
    ceoname as ceo,
    addrline1 as addressline1,
    addrline2 as addressline2,
    postalcode,
    city,
    stateprovince as stateprov,
    country,
    description,
    foundingdate,
    1 as batchid,
    recdate as effectivedate,
    coalesce(
      lead(recdate) over (partition by cik order by recdate),
      date '9999-12-31') as enddate
  from cmp
  join {{ ref('Industry') }} ind
    on cmp.industryid = ind.in_id
)
select
  cast(concat(strftime(effectivedate, '%Y%m%d'), cast(companyid as varchar)) as hugeint) as sk_companyid,
  companyid, status, name, industry, sprating, islowgrade, ceo,
  addressline1, addressline2, postalcode, city, stateprov, country,
  description, foundingdate,
  (enddate = date '9999-12-31') as iscurrent,
  batchid, effectivedate, enddate
from cmp_transformed
where effectivedate < enddate
