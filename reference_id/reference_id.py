import psycopg2
import os
import csv
import glob
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')

def db_conn():
    conn = psycopg2.connect(
        user=os.getenv("RDS_USER"),
        password=os.getenv("RDS_PASSWORD"),
        dbname=os.getenv("RDS_NAME"),
        host=os.getenv("RDS_HOST"),
        port=os.getenv("RDS_PORT")
    )
    
    return conn

def get_id(source_id, conn):
    query = """
        SELECT resource_name, id
        FROM etl.mapping_joins
        WHERE source_id = %s;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (source_id,))
            res = cursor.fetchall()
            res_dict = dict(res)
            return res_dict

    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def process_files(path):
    conn = db_conn()
    data = get_id(807, conn) 
    
    for file_path in path:
        file_name = os.path.basename(file_path)
        table_name = file_name.replace("direct_idx_", "").replace(".sql.csv", "")

        if table_name in data:
            df = pd.read_csv(file_path)
            df['reference_id'] = data[table_name]

            df.to_csv(file_path, index=False)

            print(f"Updated {file_path} with reference_id {data[table_name]}.")


path = r"C:\Users\Personal\OneDrive\Desktop\Serverless\Migrations\Paragon_API\14-PDRA2-888\Loaded"
csv_files = glob.glob(os.path.join(path, "*.csv"))
process_files(csv_files)

