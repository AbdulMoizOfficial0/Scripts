import psycopg2
import os
import csv
import glob
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
            columns = [desc[0] for desc in cursor.description]
            result = cursor.fetchall()
            result_dict = [dict(zip(columns, row)) for row in result]
            return result_dict
    except Exception as e:
        print(f"Error executing query: {e}")
        return None


def file_update(file_path, data, directory_path):
    try:
        print(f"Processing file: {file_path}")
        
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            rows = list(reader)
        
        id_lookup = {entry['resource_name']: entry['id'] for entry in data}

        file_dict = {
            'direct_idx_address.sql.csv': 'address', 
            'direct_idx_agent_1.sql.csv': 'agent_1',
            'direct_idx_agent_2.sql.csv': 'agent_2',
            'direct_idx_office_1.sql.csv': 'office_1',
            'direct_idx_office_2.sql.csv': 'office_2'
        }

        for row in rows:
            resource_name = row['resource_name']
            print(f"Checking resource_name: {resource_name}")

            for file_name, mapped_resource in file_dict.items():
                if resource_name == mapped_resource:
                    print(f"Found matching file for resource_name '{resource_name}': {file_name}")
                    row['reference_id'] = str(id_lookup[resource_name])
                    break
            else:
                print(f"Warning: No matching file found for resource_name: {resource_name}")

        with open(file_path, mode='w', newline='', encoding='utf-8') as outfile:
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"CSV file '{file_path}' updated successfully.")

    except Exception as e:
        print(f"Error updating file: {e}")

def process_all_csv_files(directory_path, conn, source_id):
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
    
    data = get_id(source_id, conn)
    
    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        print(f"\nProcessing CSV file: {file_path}")
        file_update(file_path, data, directory_path)

conn = db_conn()
source_id = 807
directory_path = r"C:\Users\Personal\OneDrive\Desktop\Serverless\Migrations\Paragon_API\15-NMMLS-807\CSV"
process_all_csv_files(directory_path, conn, source_id)