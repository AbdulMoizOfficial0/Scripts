import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
from from_clause import from_clause_change, db_conn


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
    

def from_clause_insertion(path, source_id):
    conn = db_conn()
    cursor = conn.cursor()
    data = from_clause_change(path, source_id)
    for table, clauses in data.items():
        for clause in clauses:
            query = """INSERT INTO etl.mapping_joins (source_id, resource_name, joins_and_conditions)
                       VALUES (%s, %s, %s)"""
            cursor.execute(query, (source_id, table, clause))
    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully.")


path = r"C:\Users\Personal\OneDrive\Desktop\Serverless\Migrations\Paragon_API\14-PDRA2-888"
source_id = 888
print(from_clause_insertion(path, source_id))
