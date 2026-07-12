-- Broker dimension, from Batch1/HR.csv, keeping only employees whose job code is
-- 314 (brokers). Effective from the first calendar date; never expires in Batch1.
select
  employeeid::bigint as sk_brokerid,
  employeeid::bigint as brokerid,
  managerid::bigint  as managerid,
  firstname,
  lastname,
  middleinitial,
  branch,
  office,
  phone,
  true as iscurrent,
  1 as batchid,
  (select min(datevalue) from {{ ref('DimDate') }}) as effectivedate,
  date '9999-12-31' as enddate
from {{ read_csvfile('Batch1/HR.csv',
  "{'employeeid': 'BIGINT', 'managerid': 'BIGINT', 'firstname': 'VARCHAR',
    'lastname': 'VARCHAR', 'middleinitial': 'VARCHAR', 'jobcode': 'INTEGER',
    'branch': 'VARCHAR', 'office': 'VARCHAR', 'phone': 'VARCHAR'}") }}
where jobcode = 314
