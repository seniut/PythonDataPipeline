import re

import requests

from utils import parse_etl_configs, build_params, build_statistics, matcher, get_logger
from db_connector import ConnectorDB
from constants import (
    ETL_TIMESTAMP,
    ETL_CONF,
    DWH_CONF,
    SCHEMA_MAPPING,
    DATALAKE_SQL_FILENAME,
    DATALAKE_SCHEMA,
    DB_SCHEMA_SQL_FILENAME,
    DWH_SCHEMA,
    DWH_SQL_FOLDER_PATH,
    BUILD_DWH_STATISTICS_TABLE,
    CLEAR_DWH_SQL_FILE,
)

logger = get_logger()


class RunETl:
    def __init__(self):
        self.datalake_configs = parse_etl_configs(ETL_CONF)
        self.dwh_configs = parse_etl_configs(DWH_CONF, stage='dwh')
        self.etl_chunk = 1000
        self.db_conn = ConnectorDB(logger=logger)

    def etl_1_build_schema(self):
        logger.info(f"Run ETL 1:'{self.etl_1_build_schema.__name__}'")
        self.db_conn.run_sql(sql_file_name=DB_SCHEMA_SQL_FILENAME)
        logger.info(f"Finish ETL 1:'{self.etl_1_build_schema.__name__}'")

    @build_statistics(schema=DATALAKE_SCHEMA, etl_timestamp=ETL_TIMESTAMP)
    def populate_datalake(self, response: requests.Response, table_name: str, source: str):
        res_row_count = 0
        inserting_row_count = 0
        batch_data = []

        if response.status_code == 200:
            lines = response.text.split("\n")
            pattern = matcher(url=source)
            fields = SCHEMA_MAPPING[table_name].get_field()
            for line in lines:
                parts = None
                if pattern and re.search(pattern, line):
                    parts = re.search(pattern, line).groups()
                elif not pattern and line.strip() and not line.strip().startswith("#"):  # Skip comments and empty lines
                    parts = tuple(line.split()[:2])

                if parts:
                    # handling where ads_and_trackers sources return different result size.
                    if len(parts) != len(fields):
                        parts = ('',) + parts
                    # TODO: In future can be handled additional comments like in row:
                    #  "0.0.0.0 36c4.net # redirect to go.trafficrouter.io"

                    # validation row by schema in models package
                    parts = tuple(
                        i for i in (
                            SCHEMA_MAPPING[table_name].validate_row(
                                dict(zip(fields, parts))
                            )
                        ).values()
                    )
                    # parts.append(url_source)
                    batch_data.append(parts)
                    len_batch_data = len(batch_data)
                    if len_batch_data >= self.etl_chunk:
                        inserting_row_count += len_batch_data
                        res_row_count = (
                            self.db_conn.run_sql(sql_file_name=DATALAKE_SQL_FILENAME,
                                                 sql_params=build_params(table_name=table_name,
                                                                         values=set(batch_data),
                                                                         fields=fields,
                                                                         etl_timestamp=ETL_TIMESTAMP))
                        )
                        batch_data = []

            if batch_data:
                inserting_row_count += len(batch_data)
                res_row_count = (
                    self.db_conn.run_sql(sql_file_name=DATALAKE_SQL_FILENAME,
                                         sql_params=build_params(table_name=table_name,
                                                                 values=set(batch_data),
                                                                 fields=fields,
                                                                 etl_timestamp=ETL_TIMESTAMP))
                )
        else:
            logger.error(f"Failed to retrieve data: {response}")

        return table_name, source, inserting_row_count, res_row_count

    @build_statistics(schema=DWH_SCHEMA, etl_timestamp=ETL_TIMESTAMP, statistics_sql=BUILD_DWH_STATISTICS_TABLE)
    def populate_dwh(self, table: str, schema: str = DWH_SCHEMA):
        res_row_count = (
            self.db_conn.run_sql(sql_file_name=table,
                                 sql_folder_path=DWH_SQL_FOLDER_PATH,
                                 sql_params=build_params(table_name=table,
                                                         schema=schema,
                                                         etl_timestamp=ETL_TIMESTAMP))
        )
        return table, res_row_count

    def etl_2_build_datalake(self):
        logger.info(f"Run ETL 2:'{self.etl_2_build_datalake.__name__}'")
        for url_source, url in self.datalake_configs:
            response = requests.get(url)
            self.populate_datalake(response=response, table_name=url_source, source=url)
        logger.info(f"Finish ETL 2:'{self.etl_2_build_datalake.__name__}'")

    def clear_dwh(self):
        logger.info("Clearing DWH table to avoid violates foreign key constraint...")
        self.db_conn.run_sql(sql_file_name=CLEAR_DWH_SQL_FILE,
                             sql_folder_path=DWH_SQL_FOLDER_PATH)
        logger.info("DWH is cleared...")

    def etl_3_build_dwh(self):
        logger.info(f"Run ETL 3:'{self.etl_3_build_dwh.__name__}'")
        self.clear_dwh()
        for schema, table in self.dwh_configs:
            self.populate_dwh(table=table, schema=schema)
        logger.info(f"Finish ETL 3:'{self.etl_3_build_dwh.__name__}'")

    def data_flow(self):
        logger.info("...Run ETL flow...")
        # Handling DB schema. Updates can be added in the file: .etl/sql/db_schema.sql.
        # Can be replaced with e.g. Alembic
        self.etl_1_build_schema()

        self.etl_2_build_datalake()

        self.etl_3_build_dwh()

        logger.info("...Finish ETL flow...")


if __name__ == "__main__":
    RunETl().data_flow()
