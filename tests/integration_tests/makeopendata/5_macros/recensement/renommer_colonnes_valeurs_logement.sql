{% macro renommer_colonnes_values_logement(logement, theme) %}    

    {% set listes_colonnes_codes_libelle_liste_liste = lister_colonnes_modaliter_libelles(theme) %}

    {% set colonnes_liste = listes_colonnes_codes_libelle_liste_liste[0] %}
    {% set modalites_liste_de_liste = listes_colonnes_codes_libelle_liste_liste[1] %}
    {% set libelle_liste_de_liste = listes_colonnes_codes_libelle_liste_liste[2] %}


    select
        -- read_csv_auto types numeric-looking columns (COMMUNE) as BIGINT while ARM/IRIS stay VARCHAR;
        -- upstream Postgres read every logement column as text. Cast to VARCHAR so the CASE branches and
        -- CONCAT share one type and the keys join to the VARCHAR commune/iris codes downstream.
        CASE
            WHEN CAST("ARM" AS VARCHAR) != 'ZZZZZ' THEN CAST("ARM" AS VARCHAR)
            ELSE CAST("COMMUNE" AS VARCHAR)
        END AS code_commune_insee,
        "CATL",
        CASE
		    WHEN CAST("IRIS" AS VARCHAR) = 'ZZZZZZZZZ' THEN CONCAT(CAST("COMMUNE" AS VARCHAR), '0000')
		    ELSE CAST("IRIS" AS VARCHAR)
	    END as code_iris,
        {% for colonne_codee, modalite_liste, libelle_liste in  zip(colonnes_liste, modalites_liste_de_liste, libelle_liste_de_liste) %}
            CASE
                {% for modalite, libelle in zip(modalite_liste, libelle_liste) %}
                    when LPAD(CAST("{{ colonne_codee }}" AS TEXT), 3, '0') = '{{ modalite }}' then '{{ libelle }}'
                {% endfor %}
            END AS "{{ colonne_codee }}",
        {% endfor %}
        CAST(CAST("IPONDL" AS NUMERIC) AS INT) AS poids_du_logement
    
    FROM 
        logement

{% endmacro %}
