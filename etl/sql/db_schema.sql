----- DataLake Part -----

-- DROP TABLE IF EXISTS datalake.build_datalake_statistics;
CREATE TABLE IF NOT EXISTS datalake.build_datalake_statistics (
    etl_timestamp TIMESTAMPTZ,
    stage VARCHAR(20),
    schema VARCHAR(10),
    table_name VARCHAR(20),
    source VARCHAR(255),
    inserting_row_count BIGINT,
    res_row_count BIGINT,
    execution_time_min FLOAT,
    load_timestamp TIMESTAMPTZ,
    PRIMARY KEY (etl_timestamp, stage, schema, table_name, source)
);

--------------------------

-- DROP TABLE IF EXISTS datalake.malware;
CREATE TABLE IF NOT EXISTS datalake.malware (
    url VARCHAR(255),
    etl_timestamp TIMESTAMPTZ
);

-- DROP TABLE IF EXISTS datalake.ads_and_trackers;
CREATE TABLE IF NOT EXISTS datalake.ads_and_trackers (
    ip VARCHAR(20),
    url VARCHAR(255),
    etl_timestamp TIMESTAMPTZ
);


-------------------------------------------------------------------------------

----- DWH Part -----

-- DROP TABLE IF EXISTS dwh.build_dwh_statistics;
CREATE TABLE IF NOT EXISTS dwh.build_dwh_statistics (
    etl_timestamp TIMESTAMPTZ,
    stage VARCHAR(20),
    schema VARCHAR(10),
    table_name VARCHAR(20),
    res_row_count BIGINT,
    execution_time_min FLOAT,
    load_timestamp TIMESTAMPTZ,
    PRIMARY KEY (etl_timestamp, stage, schema, table_name)
);

--------------------------

-- DROP TABLE IF EXISTS dwh.hash_key_url_mapping CASCADE;
CREATE TABLE IF NOT EXISTS dwh.hash_key_url_mapping (
    hash_key UUID PRIMARY KEY,
    url VARCHAR(255),
    etl_timestamp TIMESTAMPTZ
);

-- DROP TABLE IF EXISTS dwh.hash_key_ip_mapping CASCADE;
CREATE TABLE IF NOT EXISTS dwh.hash_key_ip_mapping (
    hash_key UUID PRIMARY KEY,
    ip VARCHAR(255),
    etl_timestamp TIMESTAMPTZ
);

-- DROP TABLE IF EXISTS dwh.malware CASCADE;
CREATE TABLE IF NOT EXISTS dwh.malware (
    hash_key_url UUID PRIMARY KEY,
    etl_timestamp TIMESTAMPTZ,
    CONSTRAINT fk_hash_key_url_mapping FOREIGN KEY(hash_key_url) REFERENCES dwh.hash_key_url_mapping(hash_key)
);

-- DROP TABLE IF EXISTS dwh.ads_and_trackers CASCADE;
CREATE TABLE IF NOT EXISTS dwh.ads_and_trackers (
    hash_key_ip UUID,
    hash_key_url UUID,
    etl_timestamp TIMESTAMPTZ,
    PRIMARY KEY (hash_key_ip, hash_key_url),
    CONSTRAINT fk_hash_key_url_mapping FOREIGN KEY(hash_key_url) REFERENCES dwh.hash_key_url_mapping(hash_key),
    CONSTRAINT fk_hash_key_ip_mapping FOREIGN KEY(hash_key_ip) REFERENCES dwh.hash_key_ip_mapping(hash_key)
);
