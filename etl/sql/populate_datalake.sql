DROP TABLE IF EXISTS {{ schema }}_{{ table_name }}_temp;

CREATE TEMP TABLE {{ schema }}_{{ table_name }}_temp (LIKE {{ schema }}.{{ table_name }});

INSERT INTO {{ schema }}_{{ table_name }}_temp (
    {{ fields }}
)
VALUES {{ values }}
;

BEGIN;


DELETE FROM {{ schema }}.{{ table_name }}
USING {{ schema }}_{{ table_name }}_temp
WHERE {{ delete_where }}
;

INSERT INTO {{ schema }}.{{ table_name }} (
    {{ fields }},
    etl_timestamp
)
SELECT --DISTINCT
    {{ fields }},
    '{{ etl_timestamp }}'::TIMESTAMPTZ
FROM {{ schema }}_{{ table_name }}_temp
;


COMMIT;

DROP TABLE IF EXISTS {{ schema }}_{{ table_name }}_temp;


-- Return Result Row Count:
SELECT COUNT(*) FROM {{ schema }}.{{ table_name }};