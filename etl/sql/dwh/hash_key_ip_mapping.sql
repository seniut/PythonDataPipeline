--TRUNCATE dwh.hash_key_ip_mapping;

DROP TABLE IF EXISTS hash_key_ip_mapping_temp;
DROP TABLE IF EXISTS ads_and_trackers_temp;

DO $$

DECLARE
    dwh_etl_timestamp timestamp;
BEGIN
    dwh_etl_timestamp := (SELECT MAX(etl_timestamp) FROM dwh.hash_key_ip_mapping);


    -----------------------------------------------------------------------------
    -- clear dwh.ads_and_trackers before deleting FK from dwh.hash_key_ip_mapping
    CREATE TEMP TABLE ads_and_trackers_temp AS
        SELECT DISTINCT
            a.ip,
            a.url
        FROM datalake.ads_and_trackers AS a
        WHERE a.etl_timestamp >= COALESCE(dwh_etl_timestamp, a.etl_timestamp)
        ;

    DELETE FROM dwh.ads_and_trackers
    USING ads_and_trackers_temp
    WHERE ads_and_trackers.hash_key_url = MD5(COALESCE(ads_and_trackers_temp.url, ''))::UUID
        AND ads_and_trackers.hash_key_ip = MD5(COALESCE(ads_and_trackers_temp.ip, ''))::UUID
    ;
    -----------------------------------------------------------------------------


    CREATE TEMP TABLE hash_key_ip_mapping_temp AS
        SELECT DISTINCT
            a.ip
        FROM datalake.ads_and_trackers AS a
        WHERE a.etl_timestamp >= COALESCE(dwh_etl_timestamp, a.etl_timestamp)
    ;

    DELETE FROM dwh.hash_key_ip_mapping
    USING hash_key_ip_mapping_temp
    WHERE hash_key_ip_mapping.hash_key = MD5(COALESCE(hash_key_ip_mapping_temp.ip, ''))::UUID
    ;

    INSERT INTO dwh.hash_key_ip_mapping (
        hash_key,
        ip,
        etl_timestamp
    )
    SELECT
        MD5(COALESCE(ip, ''))::UUID AS hash_key,
        NULLIF(ip, '') AS ip,
--         '2023-12-12 14:36:18.302419 +00:00'::TIMESTAMPTZ
        '{{ etl_timestamp }}'::TIMESTAMPTZ AS etl_timestamp
    FROM hash_key_ip_mapping_temp
    ;

END;
$$;

DROP TABLE IF EXISTS hash_key_ip_mapping_temp;
DROP TABLE IF EXISTS ads_and_trackers_temp;

-- Return Result Row Count:
SELECT COUNT(*) FROM dwh.hash_key_ip_mapping;
