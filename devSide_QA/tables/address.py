import pandas as pd
import psycopg2


def address(cur_stage, cur_rds, source_id):
    batch_id_query = f'SELECT DISTINCT batch_id from listing where source_id = {source_id} ORDER BY batch_id DESC;'
    cur_stage.execute(batch_id_query)
    result = cur_stage.fetchone()
    batch_id = result[0]
    address_report = []

    # Query to get the total distinct counts of subdivision_name and community_name in listing_address table in homelistings db
    subdivision_and_community_count = f'''select 'Distinct Count of community_name' as table_name, 
                                  CAST(COUNT(DISTINCT community_name) AS TEXT) 
                                  FROM listing_address WHERE source_id = {source_id} and batch_id = {batch_id} 
                                  union all 
                                  SELECT 'Distinct Count of subdivision_name' as table_name, 
                                  CAST(COUNT(DISTINCT subdivision_name) AS TEXT) 
                                  FROM listing_address WHERE source_id = {source_id} and batch_id = {batch_id};
    '''
    cur_stage.execute(subdivision_and_community_count)
    sub_divison_count = cur_stage.fetchall()
    for col, count in sub_divison_count:
        address_report.append({
            'table_name': 'listing_address',
            'check_name': col,
            'status': 'Passed',
            'results': f'{col} is: {count}'
        })

    # Query to check if there is any sub_division which has 'tbd', 'tba', 'remark', or 'none' value in it
    subdivison_community_amb = f'''select 'subdivision_name' as table_name, 
                          subdivision_name 
                          FROM listing_address WHERE source_id = {source_id} and batch_id = {batch_id} AND
                          (LOWER(subdivision_name) LIKE '%tbd%' 
                          OR LOWER(subdivision_name) LIKE '%tba%' 
                          OR LOWER(subdivision_name) LIKE '%remark%' 
                          OR LOWER(subdivision_name) LIKE '%none%')
                          union all
                          select 'community_name' as table_name, 
                          community_name 
                          FROM listing_address WHERE source_id = {source_id} and batch_id = {batch_id} AND
                          (LOWER(community_name) LIKE '%tbd%' 
                          OR LOWER(community_name) LIKE '%tba%' 
                          OR LOWER(community_name) LIKE '%remark%' 
                          OR LOWER(community_name) LIKE '%none%');
    '''
    cur_stage.execute(subdivison_community_amb)
    results = cur_stage.fetchall()
    if not results:
        div_com_amb = "There is no ambiguity in subdivision_name and community_name"
        address_report.append({
            'table_name': 'listing_address',
            'check_name': 'subdivision_name has ambiguity',
            'status': 'Passed',
            'results': div_com_amb
        })
    else:
        for col, val in results:
            address_report.append({
                'table_name': 'listing_address',
                'check_name': 'subdivision_name has ambiguity',
                'status': 'Failed',
                'results': f'{col} is: {count}'
            })
    
    # Query to check if there is sub_division and community_name which is in lower case
    sub_divison_in_lower_case = f'''SELECT DISTINCT table_name, name
                                    FROM (
    SELECT 'subdivision_name' AS table_name, subdivision_name AS name
    FROM listing_address la 
    WHERE source_id = {source_id} and batch_id = {batch_id} AND subdivision_name = LOWER(subdivision_name)
    
    UNION ALL
    
    SELECT 'community_name' AS table_name, community_name AS name
    FROM listing_address la 
    WHERE source_id = {source_id} and batch_id = {batch_id} AND community_name = LOWER(community_name)
    ) AS combined_names;
                                    '''
    cur_stage.execute(sub_divison_in_lower_case)
    sub_divison_in_lower_case_result = cur_stage.fetchall()
    if not sub_divison_in_lower_case_result:
        no_lower_case = "There is no subdivision_name and community_name which is in lower case."
        address_report.append({
            'table_name': 'listing_address',
            'check_name': 'lower case subdivision_name and community_name',
            'status': 'Passed',
            'results': no_lower_case
        })
    else:
        for col, val in sub_divison_in_lower_case_result:
            address_report.append({
                'table_name': 'listing_address',
                'check_name': 'lower case subdivision_name and community_name',
                'status': 'Failed',
                'results': f'{col}, {val}'
            })

    # Query to check if there is double spaces in full_street_address
    full_street_address = f'''SELECT DISTINCT la.full_street_address, l.mls_number
                              FROM listing_address la
                              JOIN listing l
                              ON l.id = la.listing_id
                              WHERE l.source_id = {source_id} AND l.batch_id = {batch_id} 
                              AND la.full_street_address ~* '  '; 
                              '''
    cur_stage.execute(full_street_address)
    full_street_address_res = cur_stage.fetchall()
    if not full_street_address_res:
        address_res = "There is no double spaces in full_street_address"
        address_report.append({
            'table_name': 'listing_address',
            'check_name': 'double spaces in full_street_address',
            'status': 'Passed',
            'results': address_res
        })
    else:
        for add, mls_num in full_street_address_res:
            address_report.append({
                'table_name': 'listing_address',
                'check_name': 'double spaces in full_street_address',
                'status': 'Failed',
                'results': f'{add}, {mls_num}'
            })
    
    # Query to check if there is triple spaces in full_street_address
    full_street_address = f'''SELECT DISTINCT la.full_street_address, l.mls_number
                              FROM listing_address la
                              JOIN listing l
                              ON l.id = la.listing_id
                              WHERE l.source_id = {source_id} AND l.batch_id = {batch_id} 
                              AND la.full_street_address ~* '   '; 
                              '''
    cur_stage.execute(full_street_address)
    triple_street_address_res = cur_stage.fetchall()
    if not triple_street_address_res:
        address_res = "There is no triple spaces in full_street_address"
        address_report.append({
            'table_name': 'listing_address',
            'check_name': 'triple spaces in full_street_address',
            'status': 'Passed',
            'results': address_res
        })
    else:
        for add, mls_num in triple_street_address_res:
            address_report.append({
                'table_name': 'listing_address',
                'check_name': 'triple spaces in full_street_address',
                'status': 'Failed',
                'results': f'{add}, {mls_num}'
            })
    
    # Query to check duplicate address
    dup_full_street_address = f'''SELECT la.listing_id, 
                                  STRING_AGG(l.mls_number, ', ') AS mls_numbers, 
                                  COUNT(*) AS duplicate_count
                                  FROM listing l
                                  JOIN listing_address la 
                                  ON l.id = la.listing_id
                                  WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
                                  GROUP BY la.listing_id
                                  HAVING COUNT(*) > 1;
    '''
    cur_stage.execute(dup_full_street_address)
    dup_full_street_address_res = cur_stage.fetchall()
    if not dup_full_street_address_res:
        zero_dup = "There is no duplication in listing_address table"
        address_report.append({
            'table_name': 'listing_address',
            'check_name': 'duplication in listing_address table',
            'status': 'Passed',
            'results': zero_dup
        })
    else:
        for mls_number, dup_val in dup_full_street_address_res:
            address_report.append({
                'table_name': 'listing_address',
                'check_name': 'duplication in listing_address table',
                'status': 'Failed',
                'results': f'{mls_number}, {dup_val}'
            })

    # Query to check if the full_street_address is InitCap
    initcap_address = f'''
                        SELECT l.mls_number, la.full_street_address
                        FROM listing_address la
                        JOIN listing l
                        ON l.id = la.listing_id
                        WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
                        AND la.full_street_address != initcap(la.full_street_address);
    '''
    cur_stage.execute(initcap_address)
    initcap_address_res = cur_stage.fetchall()
    if not initcap_address_res:
        zero_lower_address = "full_street_address is initcap"
        address_report.append({
            'table_name': 'listing_address',
            'check_name': 'full_street_address is initcap',
            'status': 'Passed',
            'results': zero_lower_address
        })
    else:
        for mls_num, add in initcap_address_res[:6]:
            address_report.append({
                'table_name': 'listing_address',
                'check_name': 'full_street_address is initcap',
                'status': 'Failed',
                'results': f'{mls_num}, {add}'
            })
    

    # Query to check if there is any NULL full_street_address
    null_address = f'''
                    SELECT la.full_street_address, l.mls_number
                    FROM listing_address la
                    JOIN listing l
                    ON l.id = la.listing_id
                    WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
                    AND la.full_street_address IS NULL;
    '''
    cur_stage.execute(null_address)
    null_address_res = cur_stage.fetchall()
    if not null_address_res:
        null_add_res = "There is no null address in listing_address"
        address_report.append({
            'table_name': 'listing_address',
            'check_name': 'null address in listing_address',
            'status': 'Passed',
            'results': null_add_res
        })
    else:
        for add, mls_num in null_address_res:
            address_report.append({
                'table_name': 'listing_address',
                'check_name': 'null address in listing_address',
                'status': 'Failed',
                'results': f'{add}, {mls_num}'
            })

    #-----------------------------------------------------------------------------------------#
    
    # null_address = f'''
    #                 SELECT la.full_street_address, l.mls_number
    #                 FROM listing_address la
    #                 JOIN listing l
    #                 ON l.id = la.listing_id
    #                 WHERE l.source_id = {source_id} AND l.batch_id = {batch_id}
    #                 AND la.full_street_address IS NULL;
    # '''
    # cur_stage.execute(null_address)
    # null_address_res = cur_stage.fetchall()
    # if null_address_res:
    #     for mls_num in null_address_res:
    #         cur_rds.execute(f"""SELECT business_transformation
    #                             FROM etl.mappings
    #                             WHERE source_id = {source_id} and target_column = 'full_street_address' and resource_name = 'address'""")
    #         mappings = cur_rds.fetchall()[0]
    #         cur_rds.execute(f"""SELECT table_name FROM information_schema.tables WHERE table_name ~* 'property_{source_id}';""")
    #         schema_table = cur_rds.fetchall()[0]

    #         cur_rds.execute(f"""SELECT {mappings}
    #                             FROM idx_stage.ps_spark_{schema_table}_{source_id}""")
    #         address_check = cur_rds.fetchall()
    #         if address_check:
    #             repo = 'There is some issue in your mappings bro...'
    #             address_report.append({
    #                 'table_name': 'listing_address',
    #                 'check_name': 'null address in listing_address',
    #                 'status': 'Failed',
    #                 'results': repo
    #             })
    #         else:
    #             address_report.append({
    #                 'table_name': 'listing_address',
    #                 'check_name': 'null address in listing_address',
    #                 'status': 'Passed',
    #                 'results': 'Everything is perfect bro....jany dy'
    #             })

    
    return address_report



# SELECT table_name FROM information_schema.tables WHERE table_name ~* '894';
