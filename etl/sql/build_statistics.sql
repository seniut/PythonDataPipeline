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
    {{ fields }}
)
SELECT DISTINCT
    {{ fields }}
FROM {{ schema }}_{{ table_name }}_temp
;


COMMIT;

DROP TABLE IF EXISTS {{ schema }}_{{ table_name }}_temp;