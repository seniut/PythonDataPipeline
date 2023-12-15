import os
import time
import logging

import psycopg2
import psycopg2.extras

from utils import get_logger, render_sql_from_file
from constants import SQL_FOLDER_PATH


class ConnectorDB:
    def __init__(self, logger: logging.Logger = get_logger()):
        self.logger = logger

    def create_db_connection(self) -> psycopg2.connect:
        try:
            return psycopg2.connect(
                dbname=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD'),
                host=os.getenv('DB_HOST'),  # for local run need to use "localhost"
                port=os.getenv('DB_PORT')
            )
        except psycopg2.OperationalError as e:
            self.logger.error(f"Database connection failed: {e}")
            raise e

    def run_sql(self, sql_file_name: str, sql_params: dict = None, sql_folder_path: str = SQL_FOLDER_PATH) -> int:
        max_retries = 5
        retry_count = 1

        res_row_count = None

        self.logger.info(f"DB_HOST: {os.getenv('DB_HOST')}")

        while retry_count < max_retries:
            try:
                with self.create_db_connection() as conn:
                    with conn.cursor() as cur:
                        # Execute SQL query
                        sql = render_sql_from_file(
                            file_name=sql_file_name, params_dict=sql_params, sql_folder_path=sql_folder_path
                        )
                        cur.execute(sql)

                        try:
                            res_row_count = cur.fetchone()[0]

                            self.logger.info(f"Result Row Count of '{sql_file_name}' script: {res_row_count}")
                        except psycopg2.ProgrammingError as e:
                            if e.args[0] == 'no results to fetch' in str(e):
                                self.logger.info(f"Expected error: {e}...continue")
                            else:
                                raise e

                        # columns = ', '.join(batch_data[0].keys())
                        # values = [tuple(data.values()) for data in batch_data]
                        # query = f"TRUNCATE TABLE customer_visits; " \
                        #             f"INSERT INTO customer_visits ({columns}) VALUES %s"
                        # psycopg2.extras.execute_values(cur, query, values)

                        conn.commit()
                        break
            except psycopg2.OperationalError as e:
                self.logger.error(f"Database operation failed: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    self.logger.info(f"Retrying to connect/insert ({retry_count}/{max_retries})...")
                    time.sleep(3)
                else:
                    raise
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                raise

        if retry_count >= max_retries:
            raise psycopg2.OperationalError("Failed to connect to PostgreSQL after several retries.")

        return res_row_count
