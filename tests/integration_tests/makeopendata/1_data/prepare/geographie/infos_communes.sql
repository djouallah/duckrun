{{ config(materialized='table') }}

with filtre_cog_communes as (
    -- Filter out non-commune rows here to avoid confusion of filtering in the main query
    select * 
    from {{ source('sources', 'cog_communes')}} as cog_communes     
    where cog_communes.type in ('commune-actuelle', 'arrondissement-municipal')

),denomalise_cog as (
    select
        LPAD(CAST(filtre_cog_communes.code as TEXT), 5, '0') as code_commune,
        filtre_cog_communes.nom as nom_commune,
        filtre_cog_communes.arrondissement as code_arrondissement,
        filtre_cog_communes.departement as code_departement,
        filtre_cog_communes.region as code_region,
        "codesPostaux" as codes_postaux,
        filtre_cog_communes.population as population,
        filtre_cog_communes.zone as code_zone,
        
        cog_arrondissements.nom as nom_arrondissement,
        cog_departements.nom as nom_departement,
        cog_regions.nom as nom_region

    from filtre_cog_communes
    left join {{ source('sources', 'cog_arrondissements')}}  cog_arrondissements on cog_arrondissements.code = filtre_cog_communes.arrondissement
    left join {{ source('sources', 'cog_departements')}}  cog_departements on cog_departements.code = filtre_cog_communes.departement
    left join {{ source('sources', 'cog_regions')}}  cog_regions on cog_regions.code = filtre_cog_communes.region  
    
), laposte_gps as (
    select DISTINCT
        LPAD(CAST(cog_poste.code_commune_insee AS TEXT), 5, '0') as code_commune,
        CAST(SPLIT_PART(cog_poste._geopoint, ',', 1) AS FLOAT) as commune_latitude,
        CAST(SPLIT_PART(cog_poste._geopoint, ',', 2) AS FLOAT) as commune_longitude
    from {{ source('sources', 'cog_poste')}}  as cog_poste
), ign_shapes as (
    -- IGN ADMIN-EXPRESS COG communes (GeoParquet, EPSG:4326): codgeo = INSEE code, geom = geometry.
    -- The upstream UNION with shape_arrondissement_municipal is dropped: that layer has no public
    -- source, so the 45 PLM (Paris/Lyon/Marseille) arrondissements municipaux carry a NULL contour.
    -- The commune count is unaffected (it comes from cog_communes, left-joined below).
    select codgeo as code_commune,
           geom   as commune_contour
    from {{ source('sources', 'shape_commune_2024')}}
)

select
    denomalise_cog.*,
    laposte_gps.commune_latitude,
    laposte_gps.commune_longitude,
    ST_Point(laposte_gps.commune_latitude, laposte_gps.commune_longitude) as commune_centre_geopoint,
    ign_shapes.commune_contour
from denomalise_cog
left join laposte_gps on denomalise_cog.code_commune = laposte_gps.code_commune
left join ign_shapes on denomalise_cog.code_commune = ign_shapes.code_commune