import json

import snowflake.connector


class SnowflakeManager:
    def __init__(self, account, user, password, database):
        self.schema = None
        self.account = account
        self.user = user
        self.password = password
        # self.warehouse = warehouse
        self.database = database
        # self.schema = schema
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            # warehouse=self.warehouse,
            database=self.database,
            # schema=self.schema
        )
        self.cursor = self.conn.cursor()

    def create_table(self, table_name, columns):
        columns_str = ', '.join(columns)
        create_table_query = f'CREATE OR REPLACE TABLE "{self.database}"."{self.schema}"."{table_name}" ({columns_str})'
        self.cursor.execute(create_table_query)
        print(f"Table '{table_name}' created successfully.")

    def create_schema(self, schema_name):
        # self.conn.cursor().execute(f'USE DATABASE {self.database}')
        self.schema = schema_name
        self.cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.database}"."{schema_name}"')
        self.cursor.execute(f"USE SCHEMA {self.schema}")

    def create_file_format(self):
        sql_query = (
            f"CREATE OR REPLACE FILE FORMAT {self.database}.{self.schema}.FILE_FORMAT "
            f"ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE "
            f"FIELD_DELIMITER = ',' "
            f"SKIP_HEADER = 1 "
            f"NULL_IF = ('NULL', 'null') "
            f"ESCAPE_UNENCLOSED_FIELD = NONE "
            f"EMPTY_FIELD_AS_NULL = true "
            f"FIELD_OPTIONALLY_ENCLOSED_BY = '\"' "
            f"COMPRESSION = GZIP;"
        )

        self.cursor.execute(sql_query)

    def create_stage(self):
        self.cursor.execute(
            f'CREATE STAGE IF NOT EXISTS "{self.database}"."{self.schema}"."STAGE" '
            f'FILE_FORMAT = "{self.database}"."{self.schema}".FILE_FORMAT;'
        )

    def load_file_into_table(self, table_name, file_path):
        try:
            self.cursor.execute(f'PUT file://{file_path} @"{self.database}"."{self.schema}"."STAGE"')
            self.cursor.execute(
                f'COPY INTO "{self.database}"."{self.schema}"."{table_name}" '
                f'FROM @"{self.database}"."{self.schema}"."STAGE" '
                f'FILE_FORMAT = (FORMAT_NAME = "{self.database}"."{self.schema}"."FILE_FORMAT") '
                f"FILES = ('csv_filename.csv.gz')"
            )
            print(f'Data from "{file_path}" loaded into "{table_name}" table successfully.')
        except snowflake.connector.errors.ProgrammingError as e:
            print(f"Error loading data: {e}")

    def close_connection(self):
        if self.conn:
            self.conn.close()
            print("Connection closed.")

    def convert_to_snowflake_schema(self, json_data):
        table_columns = []

        for key, value in json_data.get('properties', {}).items():
            column_type = value.get('type', '').upper()
            snowflake_type = None

            if column_type == 'STRING':
                snowflake_type = 'STRING'
            elif column_type == 'INTEGER':
                snowflake_type = 'INT'
            elif column_type == 'BOOLEAN':
                snowflake_type = 'BOOLEAN'
            # Add more conditions to handle other types as needed

            if snowflake_type:
                table_columns.append(f'"{key.upper()}" {snowflake_type}')

        return table_columns


