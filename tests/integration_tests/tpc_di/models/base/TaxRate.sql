-- Tax-rate reference, from Batch1/TaxRate.txt.
select
  tx_id,
  tx_name,
  tx_rate
from {{ read_pipe('Batch1/TaxRate.txt',
  "{'tx_id': 'VARCHAR', 'tx_name': 'VARCHAR', 'tx_rate': 'DOUBLE'}") }}
