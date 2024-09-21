import aiopg
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Replace with your database connection string
DATABASE_URL = f"postgresql://postgres.jbdrwtqetlpuocblyxtl:{os.environ.get('PASSWORD')}@aws-0-ap-south-1.pooler.supabase.com:6543/postgres"

async def read_data(query: str):
    async with aiopg.connect(DATABASE_URL) as db:
        async with db.cursor() as cursor:
            await cursor.execute(query)
            data = await cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(data, columns=columns)
    
async def push_data(data: pd.DataFrame, table_name: str):
    if data.empty:
        return  # No data to insert

    # Extract columns and values
    columns = data.columns
    column_names = ', '.join(['"' + column + '"' for column in columns])
    values_placeholder = ', '.join([f'%({col})s' for col in columns])
    
    query = f'INSERT INTO "{table_name}" ({column_names}) VALUES ({values_placeholder})'
    
    # Convert DataFrame to a list of dictionaries
    data = data.to_dict(orient='records')

    try:
        async with aiopg.connect(DATABASE_URL) as db:
            async with db.cursor() as cursor:
                for row in data:
                    await cursor.execute(query, row)
    except Exception as e:
        # Handle exceptions (e.g., log the error or re-raise it)
        print(f"An error occurred: {e}")
        raise

async def update_data(data: pd.DataFrame, table_name: str, condition: str):
    if data.empty:
        return  # No data to update

    # Extract columns and values
    columns = data.columns
    set_statements = ', '.join([f'"{col}" = %({col})s' for col in columns])
    
    query = f'UPDATE "{table_name}" SET {set_statements} WHERE {condition}'
    
    # Convert DataFrame to a list of dictionaries
    data = data.to_dict(orient='records')

    try:
        async with aiopg.connect(DATABASE_URL) as db:
            async with db.cursor() as cursor:
                for row in data:
                    await cursor.execute(query, row)
    except Exception as e:
        # Handle exceptions (e.g., log the error or re-raise it)
        print(f"An error occurred: {e}")
        raise

async def drop_data(table_name: str, condition: str):

    # Construct the Delete query
    query = f'DELETE FROM "{table_name}" WHERE {condition}'

    try:
        async with aiopg.connect(DATABASE_URL) as db:
            async with db.cursor() as cursor:
                    await cursor.execute(query)
    except Exception as e:
        # Handle exceptions (e.g., log the error or re-raise it)
        print(f"An error occurred: {e}")
        raise
