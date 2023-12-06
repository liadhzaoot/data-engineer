# This is a sample Python script.
import json

from snowflake_destination import SnowflakeManager
from stripe_source import Stripe
from dotenv import load_dotenv
import os
load_dotenv()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    stripe = Stripe(token=os.getenv("stripe_token"))
    charges=stripe.extract_data()
    stripe.save_to_json_file(charges)

    # Example usage:
    snowflake_manager = SnowflakeManager(
        account='rc81557.eu-central-1',
        user='yaniredri123',
        password='Yanirliad123',
        # warehouse='your_warehouse',
        database='TEST_DATABASE',
        # schema='your_schema'
    )

    snowflake_manager.connect()

    snowflake_manager.create_schema('STRIPE')
    # Read JSON data from file
    with open('stripe/json_scema.json') as json_file:
        json_data = json.load(json_file)
    # Define table columns
    table_columns = snowflake_manager.convert_to_snowflake_schema(json_data)
    snowflake_manager.create_file_format()
    snowflake_manager.create_stage()
    # Create a table
    snowflake_manager.create_table('example_table', table_columns)

    # Load data from a local file into the table
    snowflake_manager.load_file_into_table('example_table', '/Users/liadhazoot/data-engineer/csv_filename.csv')

    snowflake_manager.close_connection()


