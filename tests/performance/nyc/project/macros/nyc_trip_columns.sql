{#-- The columns fct_trips reads from the raw TLC yellow parquet, in file order. Ported VERBATIM
     from djouallah/direct-lake-parquet-layout macros/nyc_trip_columns.sql (only the duckdb
     dialect runs here, but the macro is kept whole so the two stay diffable).

     A deliberate 17 rather than TLC's full 19: congestion_surcharge (2019+ only) and airport_fee
     (2022+, ships as `Airport_fee` in some months) are dropped; every column below exists in every
     file from 2011 on. The skewed categoricals — RatecodeID, store_and_fwd_flag, payment_type,
     VendorID at 97-99% single-value, the two Zipfian LocationIDs — are the whole experiment.

     Not here on purpose: `file`. It is derived from the source path (parse_filename), not read
     from the parquet. --#}
{%- macro nyc_trip_columns() -%}
  {{- return([
    'VendorID',
    'tpep_pickup_datetime',
    'tpep_dropoff_datetime',
    'passenger_count',
    'trip_distance',
    'RatecodeID',
    'store_and_fwd_flag',
    'PULocationID',
    'DOLocationID',
    'payment_type',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount'
  ]) -}}
{%- endmacro -%}

{#-- The target type of each column. The raw files are not consistent about integer width or about
     int-vs-double for the count/id columns, so every column is cast explicitly rather than
     inherited — an inherited type would make the stored table's schema depend on which months a
     run happened to land. --#}
{%- macro nyc_trip_type(column, dialect) -%}
  {%- set ints = ['VendorID', 'passenger_count', 'RatecodeID',
                  'PULocationID', 'DOLocationID', 'payment_type'] -%}
  {%- set timestamps = ['tpep_pickup_datetime', 'tpep_dropoff_datetime'] -%}
  {%- set strings = ['store_and_fwd_flag'] -%}
  {%- if column in timestamps -%}
    {{- 'DATETIME2(6)' if dialect == 'fabric' else 'TIMESTAMP' -}}
  {%- elif column in strings -%}
    {%- if dialect == 'fabric' -%}VARCHAR(1)
    {%- elif dialect == 'fabricspark' -%}STRING
    {%- else -%}VARCHAR
    {%- endif -%}
  {%- elif column in ints -%}
    {{- 'INT' -}}
  {%- else -%}
    {{- 'FLOAT' if dialect == 'fabric' else 'DOUBLE' -}}
  {%- endif -%}
{%- endmacro -%}
