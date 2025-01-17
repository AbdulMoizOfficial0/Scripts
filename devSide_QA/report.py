import pandas as pd

def generate_html_report(results, source_id):
    # Convert the string to an f-string to dynamically insert the source_id
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .passed {{ color: green; }}
            .failed {{ color: red; }}
        </style>
    </head>
    <body>
        <h1>Data Quality Report for Source ID: {source_id}</h1>
        <table>
            <tr>
                <th>Table Name</th>
                <th>Check Name</th>
                <th>Status</th>
                <th>Description</th>
            </tr>
    """

    for result in results:
        status_class = "passed" if result['status'] == "Passed" else "failed"
        html_content += f"""
            <tr>
                <td>{result['table_name']}</td>
                <td>{result['check_name']}</td>
                <td class="{status_class}">{result['status']}</td>
                <td>{result['results']}</td>
            </tr>
        """

    html_content += f"""
        </table>
        <footer>
            <p>Report generated on: {pd.Timestamp.now()}</p>
        </footer>
    </body>
    </html>
    """

    return html_content
