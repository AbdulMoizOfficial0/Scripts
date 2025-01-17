from tables.address import address
from tables.media import media
from tables.office import office
from tables.agent import agent
from tables.rows_count import rows_counts
from tables.test_cases import test_cases

from report import generate_html_report
from save import save_html_report
import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')

def run_checks(source_id):
    # Connect to your database
    conn = psycopg2.connect(
        user=os.getenv("home_USER"),
        password=os.getenv("home_PASSWORD"),
        dbname=os.getenv("home_NAME"),
        host=os.getenv("home_HOST"),
        port=os.getenv("home_PORT")
    )
    cur_stage = conn.cursor()
    rdsConn = psycopg2.connect(
        user=os.getenv("RDS_USER"),
        password=os.getenv("RDS_PASSWORD"),
        dbname=os.getenv("RDS_NAME"),
        host=os.getenv("RDS_HOST"),
        port=os.getenv("RDS_PORT")
    )
    cur_rds = rdsConn.cursor()

    all_results = []

    # Rows Counts checks
    counts_results = rows_counts(cur_stage, cur_rds, source_id)
    all_results.extend(counts_results)

    # Test Cases
    test_cases_results = test_cases(cur_stage, cur_rds, source_id)
    all_results.extend(test_cases_results)

    # Run the address checks and gather the results
    address_results = address(cur_stage, cur_rds, source_id)
    all_results.extend(address_results) 

    # Media Checks
    media_results = media(cur_stage, cur_rds, source_id)
    all_results.extend(media_results)

    # Office checks
    office_results = office(cur_stage, cur_rds, source_id)
    all_results.extend(office_results)

    # Agent checks
    agent_results = agent(cur_stage, cur_rds, source_id)
    all_results.extend(agent_results)

    # Generate the HTML report
    html_report = generate_html_report(all_results, source_id)

    # Save the HTML report
    save_html_report(f'{source_id}_report.html', html_report)

    # Close the connection
    cur_stage.close()
    cur_rds.close()
    conn.close()
    rdsConn.close()

run_checks(807)
