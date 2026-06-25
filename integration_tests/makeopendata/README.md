# makeopendata — French open data on duckrun

A faithful port of [make-open-data/make-open-data](https://github.com/make-open-data/make-open-data)
— a production-grade dbt project over real French public data (INSEE, Etalab, IGN, DVF/Etalab,
La Poste, Opendatasoft) — running on **duckrun**: every model executes in DuckDB and is
materialized as a **Delta Lake** table via delta-rs.

Upstream targets **Postgres + PostGIS**. This port keeps the upstream layout, macros, seeds and
**tests verbatim**, and rewrites only the model/macro SQL where Postgres/PostGIS differs from
DuckDB + the `spatial` extension.

## What changed from upstream (and why)

**Sources — re-pointed to public origins.** Upstream reads a private S3 mirror; its geography
backbone (the `cog_*` reference tables and the IGN shapefiles) is access-gated (HTTP 403). Each
source is re-pointed to its canonical public origin and read straight over `https` as a duckrun
plugin source (`meta.plugin: duckrun`, see [1_data/sources/schema.yml](1_data/sources/schema.yml)):

| Source(s) | Public origin | Format |
|---|---|---|
| `cog_communes/arrondissements/departements/regions` | Etalab `decoupage-administratif` (unpkg) | json |
| `cog_poste` | La Poste *base officielle des codes postaux* | csv |
| `shape_commune_2024` | IGN ADMIN-EXPRESS COG 2025 communes (data.gouv) | GeoParquet (EPSG:4326) |
| `shape_iris_2024` | Opendatasoft `georef-france-iris` (national; dev scopes to Occitanie) | GeoJSON via json export |
| dvf / logement / filosofi / seveso / bpe / bmo / … | the project's own public bucket objects | csv |

`shape_arrondissement_municipal_2024` has no public source, so the 45 Paris/Lyon/Marseille
arrondissements municipaux carry a NULL contour. The commune **count is unaffected** (it derives
from `cog_communes`); `assert_geo_communes_number` (35074) still passes.

**SQL — PostGIS → DuckDB spatial, Postgres-isms → DuckDB.** Geometry has no Arrow/Delta type, so
duckrun stores it as **WKB BLOB**; any spatial op on a geometry read back from a Delta table wraps it
in `ST_GeomFromWKB`. The mechanical mappings:

- `ST_SetSRID(ST_MakePoint(x,y), 4326)` → `ST_Point(x,y)`
- `ST_Union(geom)` (aggregate) → `ST_Union_Agg(ST_GeomFromWKB(geom))`
- PostGIS KNN `a <-> b` → `ST_Distance(ST_GeomFromWKB(a), ST_GeomFromWKB(b))`
- `ST_Transform(geom, 4674)` dropped — the IGN/Opendatasoft geometry is already EPSG:4326
- `TO_DATE(x,'YYYY')` → `strptime(x,'%Y')::DATE`; `SUBSTRING(x for n)` → `SUBSTRING(x,1,n)`
- `materialized='view'` → `table` (duckrun has no persistent view)
- the PostGIS `GIST` index `post_hook` is dropped (no DuckDB index on a Delta-backed model)

**Adapter — JSON source support.** The duckrun source plugin learned a `json` format
(`read_json_auto`) so the Etalab/Opendatasoft JSON sources read as catalog views like CSV/Parquet.

## Run it

```bash
export WAREHOUSE_PATH=/tmp/makeopendata_wh   # local Delta warehouse (or an abfss:// Tables path)
dbt deps  --project-dir integration_tests/makeopendata --profiles-dir integration_tests/makeopendata
dbt build --project-dir integration_tests/makeopendata --profiles-dir integration_tests/makeopendata
```

`dev` (default) reads the small Hérault/Occitanie samples (dvf, logement) and scopes IRIS to
Occitanie; `--target production` reads all millésimes and all of France. Point `WAREHOUSE_PATH` at an
`abfss://…/Tables` path and set `ONELAKE_TOKEN` to build against Microsoft Fabric OneLake.
