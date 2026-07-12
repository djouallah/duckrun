-- Raw FINWIRE feed (Batch1). Each line is a fixed-width record of one of three
-- types (CMP / SEC / FIN); we only split out rectype, recdate and the payload
-- here. The per-type field layouts are parsed in DimCompany / DimSecurity /
-- Financial. A FIN record is further tagged FIN_COMPANYID vs FIN_NAME depending
-- on whether its company reference (offset 187, len 60) is numeric.
select
  case
    when substr(line, 16, 3) = 'FIN'
      then case
             when try_cast(trim(substr(line, 187, 60)) as integer) is not null
               then 'FIN_COMPANYID'
             else 'FIN_NAME'
           end
    else substr(line, 16, 3)
  end as rectype,
  try_strptime(substr(line, 1, 8), '%Y%m%d')::date as recdate,
  substr(line, 19) as value
from {{ read_fixed('Batch1/FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]') }}
