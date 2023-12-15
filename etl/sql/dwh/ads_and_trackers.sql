--TRUNCATE dwh.ads_and_trackers;

DROP TABLE IF EXISTS ads_and_trackers_temp;

DO $$

DECLARE
    dwh_etl_timestamp timestamp;
BEGIN
    dwh_etl_timestamp := (SELECT MAX(etl_timestamp) FROM dwh.ads_and_trackers);


    CREATE TEMP TABLE ads_and_trackers_temp AS
        SELECT DISTINCT
            a.ip,
            a.url
        FROM datalake.ads_and_trackers AS a
        WHERE a.etl_timestamp >= COALESCE(dwh_etl_timestamp, a.etl_timestamp)
        ;

    INSERT INTO dwh.ads_and_trackers (
        hash_key_ip,
        hash_key_url,
        etl_timestamp
    )
    SELECT
        MD5(COALESCE(ip, ''))::UUID AS hash_key_ip,
        MD5(COALESCE(url, ''))::UUID AS hash_key_url,
--         '2023-12-12 14:36:18.302419 +00:00'::TIMESTAMPTZ
        '{{ etl_timestamp }}'::TIMESTAMPTZ AS etl_timestamp
    FROM ads_and_trackers_temp
    ;

END;
$$;

DROP TABLE IF EXISTS ads_and_trackers_temp;

-- Return Result Row Count:
SELECT COUNT(*) FROM dwh.ads_and_trackers;
