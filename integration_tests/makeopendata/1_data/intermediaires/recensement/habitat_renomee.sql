{{ 
    config(
        materialized='table',
        schema='intermediaires'
    ) 
}}

WITH logement AS (
    select * from {{ ref('logement_raw') }}
),
logement_renomee AS (
    ( {{ renommer_colonnes_values_logement(logement, 'habitat') }} )
)

SELECT 
    *
FROM 
    logement_renomee