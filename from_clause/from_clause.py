import xml.etree.ElementTree as ET
import re
from collections import defaultdict
import os
import glob
import psycopg2
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
    
def from_clause(folder_path):
    xml_files = glob.glob(os.path.join(folder_path, "*.xml"))
    all_from_clauses = {}
    for xml_file in xml_files:
        sql_text = []
        tree = ET.parse(xml_file)
        root = tree.getroot()
        for step in root.findall(".//step"):
            step_name = step.find("name").text if step.find("name") is not None else "Unnamed"
            sql_tag = step.find("sql")
            if sql_tag is not None and sql_tag.text:
                sql_text.append(sql_tag.text)

        for raw_sql in sql_text:
            clean_text = re.sub(r'\s+', ' ', raw_sql.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")).strip()
            from_index = clean_text.lower().find("from stage.")
            if from_index != -1:
                from_clause_text = clean_text[from_index:]
                file_name = os.path.basename(xml_file)
                if file_name in all_from_clauses:
                    all_from_clauses[file_name].append(from_clause_text)
                else:
                    all_from_clauses[file_name] = [from_clause_text]

    return all_from_clauses

def db_table_name(source_id):
    conn = db_conn()
    query = f"""
                SELECT DISTINCT table_schema, table_name
                FROM information_schema.columns 
                WHERE table_name ~* '{source_id}';
    """
    cursor = conn.cursor()
    cursor.execute(query)
    res = cursor.fetchall()
    return res

def from_clause_change(path, source_id):
    clean_sql = {}
    clauses = from_clause(path)
    db_tables = db_table_name(source_id)
    table_replacements = {}
    for schema, table_name in db_tables:
        if "property" in table_name:
            table_replacements["property"] = table_name
        elif "member" in table_name:
            table_replacements["member"] = table_name
        elif "office" in table_name:
            table_replacements["office"] = table_name
        elif "openhouse" in table_name:
            table_replacements["openhouse"] = table_name
    
    for xml_file, clauses_list in clauses.items():
        file_name = os.path.splitext(os.path.basename(xml_file))[0]
        updated_clauses = []
        for clause in clauses_list:
            def replace_stage_table(match):
                stage_table = match.group(1)
                for keyword, replacement_table in table_replacements.items():
                    if keyword in stage_table.lower():
                        return f"idx_stage.{replacement_table}"
                return stage_table
            modified_clause = re.sub(r"stage\.(\S+)", replace_stage_table, clause)
            modified_clause = re.sub(r"batch_id\s*=\s*\$\{DIRECT_IDX_MAX_BATCH_ID\}", f"source_id = {source_id}", modified_clause)
            updated_clauses.append(modified_clause)

        clean_sql[file_name] = updated_clauses
    
    return clean_sql

