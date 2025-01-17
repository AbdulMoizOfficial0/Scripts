import psycopg2
from concurrent.futures import ThreadPoolExecutor
import threading
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')

def db_conn():
    conn = psycopg2.connect(
        user=os.getenv("home_USER"),
        password=os.getenv("home_PASSWORD"),
        dbname=os.getenv("home_NAME"),
        host=os.getenv("home_HOST"),
        port=os.getenv("home_PORT")
    )
    return conn
    
def fetch_batch_ids(source_id):
    query = f"""
            SELECT DISTINCT batch_id
            FROM listing
            WHERE source_id = {source_id}
            ORDER BY batch_id desc;
    """
    conn = db_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    listing_ids = cursor.fetchall()
    return listing_ids

def deletion(source_id, queries):
    sql_files = glob.glob(os.path.join(directory, "*.sql"))
    batch_ids = fetch_batch_ids(source_id)
    
    for file in sql_files:
        try:
            with open(file, 'r') as f:
                qeury = f.read()
                query = qeury.format(source_id=source_id)

                cursor.execute(query)
                conn.commit()
        except Exception as e:
            print(f"Error executing {sql_file}: {e}")
            conn.rollback()


query_path = r'C:\Users\Personal\OneDrive\Desktop\Serverless\Migrations\Scripts\data_del\table_queries'
print(deletion(807, query_path))