-- Pour tester la fonction knn, nous devons créer une table avec des données fictives 
-- (impossible de faire des tests unitaires sur des données qui ne sont pas dans une table). 
-- crée une fausse table avec id, latitude, longitude, et valeur columns
-- S'assurer que le calcul ne se fait pas sur l'objet de la table

{#-- duckrun has no persistent view; materialize as a Delta table so the KNN test can ref() it.
    geopoint is stored as WKB BLOB and rebuilt with ST_GeomFromWKB in calculate_geo_knn. --#}
{{
    config(
        materialized='table',
        schema='simulations'
    )
}}
WITH fake_knn_table AS (
    SELECT 1 AS id, ST_Point(43.7, 3.832) AS geopoint, 100 AS valeur, '2024' AS millesime, '123' AS code_arrondissement UNION ALL -- (3, 2) --> (200 + 300) / 2 = 250
    SELECT 3,       ST_Point(43.7, 3.830),             200,           '2024'             , '123'                        UNION ALL -- (1, 2) --> (100 + 300) / 2 = 200
    SELECT 2,       ST_Point(43.7, 3.831),             300,           '2024'             , '123'                        UNION ALL -- (1, 3) --> (100 + 200) / 2 = 150
    SELECT 4,       ST_Point(43.7, 3.839),             400,           '2024'             , '123'                        UNION ALL -- (6, 1) --> (500 + 100) / 2 = 300
    SELECT 6,       ST_Point(43.7, 3.838),             500,           '2024'             , '123'                                  -- (4, 1) --> (400 + 100) / 2 = 250
)


select * from fake_knn_table