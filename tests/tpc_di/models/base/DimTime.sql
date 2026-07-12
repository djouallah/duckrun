-- Conformed time-of-day dimension, loaded verbatim from Batch1/Time.txt.
select
  sk_timeid,
  timevalue,
  hourid,
  hourdesc,
  minuteid,
  minutedesc,
  secondid,
  seconddesc,
  markethoursflag_raw in ('1', 'true', 'True', 't', 'Y') as markethoursflag,
  officehoursflag_raw in ('1', 'true', 'True', 't', 'Y') as officehoursflag
from {{ read_pipe('Batch1/Time.txt',
  "{'sk_timeid': 'BIGINT', 'timevalue': 'VARCHAR',
    'hourid': 'INTEGER', 'hourdesc': 'VARCHAR',
    'minuteid': 'INTEGER', 'minutedesc': 'VARCHAR',
    'secondid': 'INTEGER', 'seconddesc': 'VARCHAR',
    'markethoursflag_raw': 'VARCHAR', 'officehoursflag_raw': 'VARCHAR'}") }}
