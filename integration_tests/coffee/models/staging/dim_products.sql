-- Raw product dimension over https (26 rows). This is SCD2: product_id repeats across validity
-- windows (from_date / to_date), so it is NOT a unique key yet — the `products` model dedups it.
select * from read_csv_auto('{{ var("coffee_csv_base") }}/Dim_Products.csv')
