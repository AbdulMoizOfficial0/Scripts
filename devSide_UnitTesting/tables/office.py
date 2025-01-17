import pandas as pd
import psycopg2


def office(cur_stage, cur_rds, source_id):
    batch_id_query = f'SELECT DISTINCT batch_id from listing where source_id = {source_id} ORDER BY batch_id DESC;'
    cur_stage.execute(batch_id_query)
    result = cur_stage.fetchone()
    batch_id = result[0]
    office_report = []

    office_name_double = f'''
                        SELECT source_office_id, office_name
                        FROM real_estate_office
                        WHERE source_id = {source_id} and batch_id = {batch_id}
                        AND office_name ~* '  ';
    '''
    cur_stage.execute(office_name_double)
    office_name_dub_res = cur_stage.fetchall()
    if not office_name_dub_res:
        res = "There is no office_name with double spaces."
        office_report.append({
            'table_name': 'real_estate_office',
            'check_name': 'office_name has double spaces',
            'status': 'Passed',
            'results': res
        })
    else:
        for office_name, office_id in office_name_dub_res:
            office_report.append({
                'table_name': 'real_estate_office',
                'check_name': 'office_name has double spaces',
                'status': 'Failed',
                'results': f'{office_id}, {office_name}'
            })
    
    lis_agent_assoc = f'''
                        SELECT DISTINCT id FROM listing WHERE source_id = {source_id} AND batch_id = {batch_id}
                        EXCEPT
                        SELECT DISTINCT lpr.listing_id FROM listing_real_estate_office_rel lpr
                        JOIN listing l ON l.id = lpr.listing_id
                        WHERE l.source_id = {source_id} AND l.batch_id = {batch_id};
    '''
    cur_stage.execute(lis_agent_assoc)
    lis_agent_assoc_res = cur_stage.fetchall()
    if not lis_agent_assoc_res:
        res = "Listing and Office association is done correctly"
        office_report.append({
            'table_name': 'real_estate_office',
            'check_name': 'Listing and Office association',
            'status': 'Passed',
            'results': res
        })
    else:
        for o_id in lis_agent_assoc_res:
            office_report.append({
                'table_name': 'real_estate_office',
                'check_name': 'Listing and Office association',
                'status': 'Failed',
                'results': f'{o_id}'
            })
    office_duplication = f'''
                            SELECT * 
                            FROM real_estate_office 
                            WHERE source_id = {source_id} and batch_id = {batch_id} 
                            AND source_office_id IN (
                            SELECT source_office_id 
                            FROM real_estate_office 
                            WHERE source_id = {source_id} and batch_id = {batch_id} 
                            GROUP BY source_office_id 
                            HAVING COUNT(source_office_id) > 1);
    '''
    cur_stage.execute(office_duplication)
    office_duplication_res = cur_stage.fetchall()
    if not office_duplication_res:
        res = "There is no duplication in real_estate_office table"
        office_report.append({
            'table_name': 'real_estate_office',
            'check_name': 'Duplication in real_estate_office',
            'status': 'Passed',
            'results': res
        })
    else:
        if office_duplication_res:
            ress = "There is duplication in real_estate_office"
            office_report.append({
            'table_name': 'real_estate_office',
            'check_name': 'Duplication in real_estate_office',
            'status': 'Failed',
            'results': ress
        })

    office_fs_address_dub = f'''
                            SELECT full_street_address
                            FROM real_estate_office
                            WHERE source_id = {source_id} and batch_id = {batch_id}
                            AND full_street_address ~* '  ';
    '''
    cur_stage.execute(office_fs_address_dub)
    office_fs_address_dub_res = cur_stage.fetchall()
    if not office_fs_address_dub_res:
        res = "There is no full_street_address in real_estate_office with double spaces"
        office_report.append({
            'table_name': 'real_estate_office',
            'check_name': 'full_street_address in real_estate_office with double spaces',
            'status': 'Passed',
            'results': res
        })
    else:
        if office_fs_address_dub_res:
            ress = "There is full_street_address in real_estate_office with double spaces"
            office_report.append({
                'table_name': 'real_estate_office',
                'check_name': 'full_street_address in real_estate_office with double spaces',
                'status': 'Failed',
                'results': ress
            })
    return office_report