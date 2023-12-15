DROP TABLE IF EXISTS ads_and_trackers_temp;
DROP TABLE IF EXISTS malware_temp;

DO $$

DECLARE
    ads_and_trackers_dwh_etl_timestamp timestamp;
    malware_dwh_etl_timestamp timestamp;
BEGIN
    ads_and_trackers_dwh_etl_timestamp := (SELECT MAX(etl_timestamp) FROM dwh.ads_and_trackers);
    malware_dwh_etl_timestamp := (SELECT MAX(etl_timestamp) FROM dwh.malware);


    -----------------------------------------------------------------------------
    -- clear dwh.ads_and_trackers before deleting FK from dwh.hash_key_ip_mapping and dwh.hash_key_url_mapping
    CREATE TEMP TABLE ads_and_trackers_temp AS
        SELECT DISTINCT
            a.ip,
            a.url
        FROM datalake.ads_and_trackers AS a
        WHERE a.etl_timestamp >= COALESCE(ads_and_trackers_dwh_etl_timestamp, a.etl_timestamp)
        ;

    DELETE FROM dwh.ads_and_trackers
    USING ads_and_trackers_temp
    WHERE ads_and_trackers.hash_key_url = MD5(COALESCE(ads_and_trackers_temp.url, ''))::UUID
        AND ads_and_trackers.hash_key_ip = MD5(COALESCE(ads_and_trackers_temp.ip, ''))::UUID
    ;
    -----------------------------------------------------------------------------

    -----------------------------------------------------------------------------
    -- clear dwh.malware before deleting FK from dwh.hash_key_url_mapping
    CREATE TEMP TABLE malware_temp AS
        SELECT DISTINCT
            a.url
        FROM datalake.malware AS a
        WHERE a.etl_timestamp >= COALESCE(malware_dwh_etl_timestamp, a.etl_timestamp)
        ;


    DELETE FROM dwh.malware
    USING malware_temp
    WHERE malware.hash_key_url = MD5(COALESCE(malware_temp.url, ''))::UUID
    ;
    -----------------------------------------------------------------------------


END;
$$;

DROP TABLE IF EXISTS ads_and_trackers_temp;
DROP TABLE IF EXISTS malware_temp;
