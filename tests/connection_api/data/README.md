# Vendored coffee-shop dimension data

`Dim_Locations.csv` (1,000 rows) and `Dim_Products.csv` (26 SCD2 rows) are **verbatim copies** from
**Josue Bogran's** [coffeeshopdatageneratorv2](https://github.com/JosueBogran/coffeeshopdatageneratorv2)
(MIT License, © Josue Bogran). All credit for the dataset goes to him.

They are vendored here (rather than read over `https` at runtime) so the coffee-shop scenario —
`test_coffee.py`, including the **coffee-stress release gate** — runs without any network dependency.
That makes the stress timing/memory numbers legitimate (no upstream-fetch latency or flakiness) and
keeps the release gate from being blocked by a transient `raw.githubusercontent.com` hiccup.

To refresh from upstream:

```bash
base=https://raw.githubusercontent.com/JosueBogran/coffeeshopdatageneratorv2/main
curl -sSL "$base/Dim_Locations.csv" -o Dim_Locations.csv
curl -sSL "$base/Dim_Products.csv"  -o Dim_Products.csv
```
