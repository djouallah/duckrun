{{
    config(
        materialized='table',
        schema='intermediaires'
    )
}}
{#-- Materialize the raw logement micro-data (the ~500 MB INSEE census CSV) to Delta ONCE. The four
    *_renomee theme models then read this local Delta table via ref() instead of each re-scanning the
    500 MB source over https — one download/parse instead of four, far less buffer churn. Same rows,
    just staged. dev reads the Occitanie sample; production reads the full national file. --#}
select *
{% if target.name == 'production' %}
    from {{ source('sources', 'logement_2020') }}
{% else %}
    from {{ source('sources', 'logement_2020_dev') }}
{% endif %}
