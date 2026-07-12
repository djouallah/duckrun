-- One row per batch: the batch's effective date. Read from every Batch*/BatchDate.txt.
select
  batchdate,
  {{ batchid_from_filename() }} as batchid
from {{ read_pipe('Batch[123]/BatchDate.txt', "{'batchdate': 'DATE'}", with_filename=true) }}
