import sys
import json
import logging
from time import time
from datetime import datetime, timezone

from jinja2 import Template

from constants import (
    CONFIGS_DEFAULT,
    DWH_CONF,
    SQL_FOLDER_PATH,
    DATALAKE_SCHEMA,
    ETL_TIMESTAMP,
    STATISTICS_SQL,
    BUILD_DATALAKE_STATISTICS_TABLE,
    FIELD_STATISTICS_MAPPING,
    WHERE_DELETE_MAPPING,
)


def reader(file: str) -> dict:
    with open(file, 'r') as file:
        return json.load(file)


def parse_etl_configs(conf_path: str = CONFIGS_DEFAULT, stage: str = 'datalake'):
    # Open and read the JSON configuration file
    try:
        configs = reader(conf_path)
    except FileNotFoundError:
        if stage == 'datalake':
            configs = reader(CONFIGS_DEFAULT)
        else:
            configs = reader(DWH_CONF)

    # Iterate over the items in the configuration
    for k, v in configs.items():
        for i in v:
            yield k, i


def get_logger(name: str = "ETL_Flow"):
    logger = logging.getLogger(name)

    # Clear existing handlers to prevent duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Set logger level to INFO
    logger.setLevel(logging.INFO)

    # Create console handler with a specific log level
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)

    # Create formatter and add it to the handler
    formatter = logging.Formatter(fmt='%(asctime)s: %(name)s - %(levelname)s - %(message)s',
                                  datefmt='%d-%b-%y %H:%M:%S')
    ch.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(ch)

    return logger


def render_sql_from_file(file_name: str, params_dict: dict = None, sql_folder_path: str = SQL_FOLDER_PATH) -> str:
    # Read the SQL file
    with open(sql_folder_path.format(file_name=file_name), 'r') as file:
        sql_content = file.read()

    # Render the SQL content
    if params_dict:
        sql_content = Template(sql_content).render(params_dict)

    return sql_content


def build_values(values: set) -> str:
    return ", ".join(["(" + ", ".join(
        f"'{val.isoformat()}'" if isinstance(val, datetime) else repr(val) for val in value_tuple
    ) + ")" for value_tuple in values])


def build_params(table_name: str,
                 values: set = None,
                 fields: list = None,
                 delete_where_fields: list = None,
                 schema: str = DATALAKE_SCHEMA,
                 etl_timestamp: datetime = ETL_TIMESTAMP) -> dict:
    if delete_where_fields:
        delete_where = " AND ".join(
            [f"{table_name}.{f} = {schema}_{table_name}_temp.{f}" for f in delete_where_fields]
        )
    elif fields:
        delete_where = " AND ".join(
            [f"{table_name}.{f} = {schema}_{table_name}_temp.{f}" for f in fields]
        )
    else:
        delete_where = ''

    return {
        "table_name": table_name,
        "schema": schema,
        "fields": ", ".join(fields) if fields else None,
        "delete_where": delete_where,
        "values": build_values(values) if values else None,
        "etl_timestamp": etl_timestamp
    }


def build_statistics(
        schema: str, etl_timestamp: datetime = ETL_TIMESTAMP, statistics_sql: str = BUILD_DATALAKE_STATISTICS_TABLE
):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time()
            result = func(*args, **kwargs)
            end_time = time()

            execution_time_min = round((end_time - start_time) / 60, 2)
            function_name = func.__name__

            """
            Statistics fields:
            
            etl_timestamp,
            stage,
            schema,
            table,
            source,
            inserting_row_count,
            res_row_count,
            execution_time_min,
            load_timestamp,
            """

            # Use schema and other data
            if isinstance(result, tuple):
                args[0].db_conn.run_sql(sql_file_name=STATISTICS_SQL,
                                        sql_params=build_params(table_name=statistics_sql,
                                                                schema=schema,
                                                                fields=FIELD_STATISTICS_MAPPING[schema],
                                                                delete_where_fields=WHERE_DELETE_MAPPING[schema],
                                                                values={(etl_timestamp,
                                                                         function_name,
                                                                         schema,
                                                                         *result,
                                                                         execution_time_min,
                                                                         datetime.now(timezone.utc))}))
            # else:
            #     args[0].db_conn.run_sql(schema, function_name, execution_time, result)

            return result

        return wrapper

    return decorator


def matcher(url: str) -> str | None:
    if url.endswith(".txt"):
        # Regular expression for values between '||' and '^'
        return r'\|\|([^|^]+)\^'
    else:
        return None
        # # Regular expression for both IPv4 and IPv6 addresses
        # return r'(\b(?:[0-9a-fA-F:.]{2,39})\b)\s+(\S+)'
