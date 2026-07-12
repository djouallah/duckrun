-- Industry reference, from Batch1/Industry.txt.
select
  in_id,
  in_name,
  in_sc_id
from {{ read_pipe('Batch1/Industry.txt',
  "{'in_id': 'VARCHAR', 'in_name': 'VARCHAR', 'in_sc_id': 'VARCHAR'}") }}
