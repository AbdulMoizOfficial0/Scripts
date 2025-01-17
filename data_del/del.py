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
    
def fetch_ids(source_id, offset=0, limit=1000):
    query = f"""
            SELECT id
            FROM listing
            WHERE source_id = {source_id}
            LIMIT {limit} OFFSET {offset};
    """
    conn = db_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    listing_ids = cursor.fetchall()
    return listing_ids

def delete_table_data(table, ids, source_id=None):
    tables_using_source_id = [
        "listing_category", "idx_config.listing_property_type", 
        "listing_property_type", "listing_property_sub_type", 
        "real_estate_office", "real_estate_participant", 
        "listing_property_type_search", "listing_address_standard", 
        "listing_attribute", "listing_attribute_2", "listing_attribute_3",
        "listing_attribute_custom", "listing_attribute_custom_2", 
        "listing_attribute_custom_3", "listing_attribute_custom_4", 
        "listing_address"
    ]
    
    if table in tables_using_source_id:
        query = f"""DELETE FROM {table} WHERE source_id = {source_id}"""
    else:
        query = f"""DELETE FROM {table} WHERE listing_id IN ({', '.join(map(str, ids))})"""
    
    conn = db_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    
    print(f"Deletion completed for table: {table}")

def check_remaining_data(table, source_id):
    """Check if any data remains for the source_id in the table."""
    query = f"""
            SELECT 1
            FROM {table}
            WHERE source_id = {source_id}
            LIMIT 1;
    """
    conn = db_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    return result is not None

def delete_in_parallel(source_id, listing_ids):
    """Delete records concurrently across tables for a batch of IDs."""
    tables_using_source_id = [
        "listing_category", "idx_config.listing_property_type", 
        "listing_property_type", "listing_property_sub_type", 
        "real_estate_office", "real_estate_participant", 
        "listing_property_type_search", "listing_address_standard", 
        "listing_attribute", "listing_attribute_2", "listing_attribute_3",
        "listing_attribute_custom", "listing_attribute_custom_2", 
        "listing_attribute_custom_3", "listing_attribute_custom_4", 
        "listing_address"
    ]
    
    tables_to_process = [
        "listing_description", "listing_photo", "listing_school", 
        "listing_real_estate_office_rel", "listing_participant_rel", 
        "listing_school_district"
    ]
    
    for table in tables_using_source_id:
        if check_remaining_data(table, source_id):
            tables_to_process.append(table)
    
    with ThreadPoolExecutor() as executor:
        futures = []
        for table in tables_to_process:
            futures.append(executor.submit(delete_table_data, table, listing_ids, source_id))
        
        for future in futures:
            future.result()

def delete_listing(source_id):
    """Delete records from the listing table after all deletions."""
    query = f"""DELETE FROM listing WHERE source_id = {source_id}"""
    conn = db_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()

def run_parallel(source_id):
    current_offset = 0
    while True:
        listing_ids = fetch_ids(source_id, offset=current_offset)
        if not listing_ids:
            break
        
        ids = [item[0] for item in listing_ids]
        
        delete_in_parallel(source_id, ids)
        
        current_offset += 1000

    delete_listing(source_id)

run_parallel(807)
