-- Status-type reference, from Batch1/StatusType.txt.
select
  st_id,
  st_name
from {{ read_pipe('Batch1/StatusType.txt',
  "{'st_id': 'VARCHAR', 'st_name': 'VARCHAR'}") }}
