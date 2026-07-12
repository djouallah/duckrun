-- Trade-type reference, from Batch1/TradeType.txt.
select
  tt_id,
  tt_name,
  tt_is_sell,
  tt_is_mrkt
from {{ read_pipe('Batch1/TradeType.txt',
  "{'tt_id': 'VARCHAR', 'tt_name': 'VARCHAR', 'tt_is_sell': 'INTEGER', 'tt_is_mrkt': 'INTEGER'}") }}
