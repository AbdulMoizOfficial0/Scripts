import os

def save_html_report(file_name, html_content):
    save_path = r"C:\Users\Personal\OneDrive\Desktop\Serverless\Migrations\Scripts\devSide_UnitTesting\Reports"
    full_file_path = os.path.join(save_path, file_name)
    
    try:
        with open(full_file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"Report successfully saved as {full_file_path}")
    except Exception as e:
        print(f"Failed to save the report. Error: {str(e)}")
