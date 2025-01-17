import pandas as pd
import psycopg2


def rows_counts(cur_stage, cur_rds, source_id):
    # Get the batch ID for the source
    batch_id_query = f'SELECT DISTINCT batch_id from listing where source_id = {source_id} ORDER BY batch_id DESC;'
    cur_stage.execute(batch_id_query)
    batch_id = cur_stage.fetchone()[0]
    if not batch_id:
        print("There is not batch_id for this source_id")
    counts_report = []

    # Query for row counts from different tables
    counts_query = f'''
    SELECT 'Listing' AS Table_Name, COUNT(1) FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_p_active' AS Table_Name, COUNT(1) FROM listing_p_active WHERE source_id = {source_id} AND batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_p_inactive' AS Table_Name, COUNT(1) FROM listing_p_inactive WHERE source_id = {source_id} AND batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_address' AS Table_Name, COUNT(1) FROM listing_address WHERE source_id = {source_id} AND batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_address_standard' AS Table_Name, COUNT(1) FROM listing_address_standard WHERE source_id = {source_id} AND batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_property_type_search' AS Table_Name, COUNT(1) FROM listing_property_type_search WHERE source_id = {source_id} AND batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_marketing_info' AS Table_Name, COUNT(lmi.listing_id) FROM listing_marketing_info lmi
    JOIN listing l ON l.id = lmi.listing_id WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_attribute' AS Table_Name, COUNT(1) FROM listing_attribute WHERE source_id = {source_id} AND batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_attribute_2' AS Table_Name, COUNT(1) FROM listing_attribute_2 WHERE source_id = {source_id} AND batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_attribute_3' AS Table_Name, COUNT(1) FROM listing_attribute_3 WHERE source_id = {source_id} AND batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_attribute_custom' AS Table_Name, COUNT(1) FROM listing_attribute_custom WHERE source_id = {source_id} AND batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_attribute_custom_2' AS Table_Name, COUNT(1) FROM listing_attribute_custom_2 WHERE source_id = {source_id} AND batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_attribute_custom_3' AS Table_Name, COUNT(1) FROM listing_attribute_custom_3 WHERE source_id = {source_id} AND batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_photo' AS Table_Name, COUNT(DISTINCT lp.listing_id) FROM listing_photo lp
    LEFT JOIN listing l ON l.id = lp.listing_id WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_description' AS Table_Name, COUNT(DISTINCT ld.listing_id) FROM listing_description ld
    LEFT JOIN listing l ON l.id = ld.listing_id WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_real_estate_office_rel' AS Table_Name, COUNT(DISTINCT lreo.listing_id) FROM listing_real_estate_office_rel lreo
    JOIN listing l ON l.id = lreo.listing_id WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
    UNION ALL
    SELECT 'listing_participant_rel' AS Table_Name, COUNT(DISTINCT lpr.listing_id) FROM listing_participant_rel lpr
    JOIN listing l ON l.id = lpr.listing_id WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
    ORDER BY COUNT ASC;
    '''
    cur_stage.execute(counts_query)
    counts = cur_stage.fetchall()

    df = pd.DataFrame(counts, columns=['table_name', 'count'])

    listing_count = df[df['table_name'] == 'Listing']['count'].values[0]

    allowed_diff = 1

    for index, row in df.iterrows():
        table_name = row['table_name']
        count = row['count']
        
        if count == 0:
            continue
        
        diff = abs(listing_count - count)
        
        if diff <= allowed_diff:
            counts_report.append({
            'table_name': table_name,
            'check_name': "Total Rows Counts",
            'status': 'Passed',
            'results': f'difference is: {listing_count}'
        })
        else:
            counts_report.append({
                'table_name': table_name,
                'check_name': "Total Rows Counts",
                'status': 'Failed',
                'results': f'difference is: {count}'
            })


    return counts_report

