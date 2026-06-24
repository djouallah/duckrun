-- Table: infos_scot
-- Reports basic infos on SCOT (Schéma de Cohérence Territoriale)
-- scot_code, siren, scot_name, population, contour, number_of_communes

{{ config(materialized='table') }}

WITH scot_data as (
    select
        commune_scot."Id SCoT" as scot_code, -- identifiant unique du SCOT
        commune_scot."SCoT" as scot_name,    -- nom du SCOT
        commune_scot."SIREN EPCI" as siren,
        LPAD(CAST(commune_scot."INSEE commune" AS TEXT), 5, '0') as code_commune
    from {{ source('sources', 'communes_to_scot') }} as commune_scot
),

scot_communes as (
    select 
        sd.scot_code,
        sd.siren,
        sd.scot_name,
        sd.code_commune,
        ic.population,
        ic.commune_contour,
        ic.code_departement,
        ic.nom_departement,
        ic.code_region,
        ic.nom_region
    from scot_data sd
    left join {{ ref('infos_communes') }} ic
        on sd.code_commune = ic.code_commune
),

-- Population par (scot, departement) puis département principal = celui avec le plus d'habitants.
-- Remplace les sous-requêtes corrélées de l'original (non supportées dans un SELECT agrégé sous DuckDB)
-- par arg_max, qui retourne le département unique quand il n'y en a qu'un (identique au MIN d'origine).
scot_departement_population as (
    select
        scot_code,
        code_departement,
        nom_departement,
        SUM(CAST(population AS NUMERIC)) as pop
    from scot_communes
    where code_commune is not null
    group by scot_code, code_departement, nom_departement
),
scot_main_departement as (
    select
        scot_code,
        arg_max(code_departement, pop) as code_departement,
        arg_max(nom_departement, pop)  as main_departement
    from scot_departement_population
    group by scot_code
)

select
    scot_communes.scot_code,
    scot_communes.siren,
    scot_communes.scot_name,
    -- Région (on suppose qu'un SCOT n'est que sur une seule région)
    MIN(scot_communes.code_region) as code_region,
    MIN(scot_communes.nom_region) as nom_region,
    scot_main_departement.code_departement as code_departement,
    scot_main_departement.main_departement as main_departement,
    -- Booléen multi_dpt
    (COUNT(DISTINCT scot_communes.code_departement) > 1) as multi_dpt,
    SUM(CAST(scot_communes.population AS NUMERIC)) as population,
    -- ST_Union (aggregate) -> ST_Union_Agg; commune_contour is read back from Delta as a WKB BLOB.
    ST_Union_Agg(ST_GeomFromWKB(scot_communes.commune_contour)) as contour,
    COUNT(scot_communes.code_commune) as number_of_communes
from scot_communes
left join scot_main_departement using (scot_code)
where scot_communes.code_commune is not null
GROUP BY
    scot_communes.scot_code,
    scot_communes.siren,
    scot_communes.scot_name,
    scot_main_departement.code_departement,
    scot_main_departement.main_departement