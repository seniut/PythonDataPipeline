import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from models import AdsAndTrackers, Malware

DATALAKE_SQL_FILENAME = "populate_datalake"
DATALAKE_SCHEMA = "datalake"

DWH_SCHEMA = "dwh"

ETL_TIMESTAMP = datetime.now(timezone.utc)

if not os.getenv('DB_HOST'):
    load_dotenv(dotenv_path=f'./.env')

# The location of the mount point within the container (docker-compose configs: statement in etl service)
ETL_CONF = "./etl_conf/etl_conf.json"

CONFIGS_DEFAULT = "./configs/etl_conf.json"
DWH_CONF = "./configs/dwh_conf.json"

SCHEMA_MAPPING = {
    "ads_and_trackers": AdsAndTrackers,
    "malware": Malware,
}

SQL_FOLDER_PATH = "./sql/{file_name}.sql"
DWH_SQL_FOLDER_PATH = "./sql/dwh/{file_name}.sql"

DB_SCHEMA_SQL_FILENAME = "db_schema"

STATISTICS_SQL = "build_statistics"
BUILD_DATALAKE_STATISTICS_TABLE = "build_datalake_statistics"
BUILD_DWH_STATISTICS_TABLE = "build_dwh_statistics"
DATALAKE_FIELD_STATISTICS = [
    "etl_timestamp",
    "stage",
    "schema",
    "table_name",
    "source",
    "inserting_row_count",
    "res_row_count",
    "execution_time_min",
    "load_timestamp"
]
DWH_FIELD_STATISTICS = [
    "etl_timestamp",
    "stage",
    "schema",
    "table_name",
    "res_row_count",
    "execution_time_min",
    "load_timestamp"
]
FIELD_STATISTICS_MAPPING = {
    DATALAKE_SCHEMA: DATALAKE_FIELD_STATISTICS,
    DWH_SCHEMA: DWH_FIELD_STATISTICS
}
WHERE_DELETE_DATALAKE_FIELDS = [
    "etl_timestamp",
    "stage",
    "schema",
    "table_name",
    "source",
]
WHERE_DELETE_DWH_FIELDS = [
    "etl_timestamp",
    "stage",
    "schema",
    "table_name",
]
WHERE_DELETE_MAPPING = {
    DATALAKE_SCHEMA: WHERE_DELETE_DATALAKE_FIELDS,
    DWH_SCHEMA: WHERE_DELETE_DWH_FIELDS
}

CLEAR_DWH_SQL_FILE = "clear_before_update_fk"
