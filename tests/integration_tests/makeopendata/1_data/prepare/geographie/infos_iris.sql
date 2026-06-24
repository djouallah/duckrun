{{ config(materialized='table', partition_by='code_departement') }}

-- IRIS contours come from the Opendatasoft "georef-france-iris" GeoParquet export (read over https):
-- com_code / iris_code / iris_name are scalars; geo_shape is native GEOMETRY (EPSG:4326). The 4-digit
-- IRIS suffix (the IGN "IRIS" column) is the tail of the 9-digit iris_code. iris_type is a French
-- label rather than the IGN A/H/D/Z code; coordinates are already EPSG:4326 so the upstream
-- ST_TRANSFORM(...,4674) is dropped (RGF93 ~ WGS84, sub-metre). year is a DATE (one edition: 2024).
with iris_source as (
    select
        iris_code                  as code_iris_2024,
        RIGHT(iris_code, 4)        as suffix_iris_2024,
        iris_name                  as nom_iris,
        com_code                   as code_commune_2024,
        iris_type                  as iris_type_label,
        geo_shape                  as contour_iris
    from {{ source('sources', 'shape_iris_2024')}}
    where extract(year from year) = 2024
),
infos_iris as (
    select
        code_iris_2024,
        suffix_iris_2024,
        nom_iris,
        code_commune_2024,
        contour_iris,
        CASE
            WHEN iris_type_label = 'iris d''activité' THEN 'zone_activite'
            WHEN iris_type_label = 'iris d''habitat'  THEN 'zone_habitat'
            WHEN iris_type_label = 'iris divers'      THEN 'zone_divers'
            WHEN iris_type_label = 'commune'          THEN 'zone_non_iris'
        END as type_iris,
        ST_PointOnSurface(contour_iris) as iris_centre_geopoint,
        ST_X(ST_PointOnSurface(contour_iris)) AS iris_longitude,
        ST_Y(ST_PointOnSurface(contour_iris)) AS iris_latitude
    from iris_source
)

select 
    infos_iris.*,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region
from infos_iris as infos_iris
left join {{ ref('infos_communes') }} as infos_communes on infos_communes.code_commune = infos_iris.code_commune_2024

