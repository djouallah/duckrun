-- Location dimension straight off the CSV over https (1,000 rows). read_csv_auto sniffs the schema;
-- only region (for the fact partition) and location_id / record_id are used downstream.
select * from read_csv_auto('{{ var("coffee_csv_base") }}/Dim_Locations.csv')
