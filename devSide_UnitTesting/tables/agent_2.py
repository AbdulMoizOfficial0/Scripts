import pandas as pd
import psycopg2


def agent(cur, source_id):
    db_username = "homedbuser"
    db_name = "homelistings"
    db_password = "brUDra8h"
    db_host = "172.25.41.25"
    db_port = "5445"
    conn = psycopg2.connect(
            database=db_name, 
            user=db_username, 
            password=db_password,
            host=db_host, 
            port=int(db_port)
    )
    cursor = conn.cursor()
    batch_id_query = f'SELECT DISTINCT batch_id from listing where source_id = {source_id} ORDER BY batch_id DESC;'
    cursor.execute(batch_id_query)
    result = cursor.fetchone()
    batch_id = result[0]
    agent_report = []


    agent_duplication = f'''
                        SELECT count(source_participant_id), source_participant_id
                        FROM real_estate_participant 
                        WHERE source_id = {source_id} and batch_id = {batch_id}
                        group by source_participant_id
                        having count(source_participant_id)	>1 
    '''
    cursor.execute(agent_duplication)
    agent_duplication_res = cursor.fetchall()
    if not agent_duplication_res:
        res = "There is no duplication in agent table"
        agent_report.append({
            'table_name': 'Agent Table',
            'check_name': 'Duplication in Agent Table',
            'status': 'Passed',
            'results': res
        })
    else:
        for count, s_id in agent_duplication_res:
            agent_report.append({
                'table_name': 'Agent Table',
                'check_name': 'Duplication in Agent Table',
                'status': 'Failed',
                'results': f'{count}, {s_id}'
            })
    list_agent_assoc = f'''
                        SELECT DISTINCT id FROM listing WHERE source_id = {source_id} and batch_id = {batch_id}
                        EXCEPT
                        SELECT DISTINCT lpr.listing_id FROM listing_participant_rel lpr
                        JOIN listing l ON l.id = lpr.listing_id
                        WHERE l.source_id = {source_id} and l.batch_id = {batch_id};    
    '''
    cursor.execute(list_agent_assoc)
    list_agent_assoc_res = cursor.fetchall()
    if not list_agent_assoc_res:
        res = "Listing and Agent association is done correctly"
        agent_report.append({
            'table_name': 'Agent Table',
            'check_name': 'Listing and Agent association',
            'status': 'Passed',
            'results': res
        })
    else:
        for i in list_agent_assoc_res:
            agent_report.append({
                'table_name': 'Agent Table',
                'check_name': 'Listing and Agent association',
                'status': 'Failed',
                'results': i
            })

    agent_full_name = f'''
                        select rep.first_name, rep.last_name, rep.full_name
                        from real_estate_participant rep
                        where  rep.source_id = {source_id} AND batch_id = {batch_id}
                        and lower(rep.full_name) ~* '  ';
    '''
    cursor.execute(agent_full_name)
    agent_fullname_result = cursor.fetchall()
    if not agent_fullname_result:
        zero_agent = "There is no agent with double space"
        agent_report.append({
            'table_name': 'Agent Table',
            'check_name': 'Double space in agent',
            'status': 'Passed',
            'results': zero_agent
        })
    else:
        dub_agent = "There is agent with double spaces"
        agent_report.append({
            'table_name': 'Agent Table',
            'check_name': 'Double space in agent',
            'status': 'Failed',
            'results': dub_agent
        })

    agent_name = f'''
                    SELECT rep.full_name
                    FROM real_estate_participant rep
                    WHERE source_id = {source_id} AND batch_id = {batch_id}
                    AND rep.full_name != initcap(rep.full_name);
    '''
    cursor.execute(agent_name)
    agent_name_res = cursor.fetchall()
    if not agent_name_res:
        agent_cap = "Agent Full Name is initcap."
        agent_report.append({
            'table_name': 'Agent Table',
            'check_name': 'Agent Full Name is initcap',
            'status': 'Passed',
            'results': agent_cap
        })
    else:
        agent_cap_not = "Agent Full Name is not initcap."
        agent_report.append({
            'table_name': 'Agent Table',
            'check_name': 'Agent Full Name is initcap',
            'status': 'Failed',
            'results': agent_cap_not
        })

    agent_name_null = f'''
                        SELECT rep.full_name, rep.first_name, rep.last_name
                        FROM real_estate_participant rep
                        WHERE source_id = {source_id} AND batch_id = {batch_id}
                        AND rep.full_name IS NULL AND rep.first_name IS NULL AND rep.last_name IS NULL;
    '''
    cursor.execute(agent_name_null)
    agent_name_null_res = cursor.fetchall()
    if not agent_name_null_res:
        no_null_agent = "There is no NULL in agent full_name"
        agent_report.append({
            'table_name': 'Agent Table',
            'check_name': 'NULL in agent full_name',
            'status': 'Passed',
            'results': no_null_agent
        })
    else:
        for fl_name, f_name, l_name in agent_name_null_res:
            agent_report.append({
                'table_name': 'Agent Table',
                'check_name': 'NULL in agent full_name',
                'status': 'Failed',
                'results': f'{fl_name}, {f_name}, {l_name}'
            })

    agent_name_amb = f'''
                        SELECT rep.full_name
                        FROM real_estate_participant rep
                        WHERE source_id = {source_id} AND batch_id = {batch_id}
                        AND rep.full_name ~* 'Non Mls';
    '''
    cursor.execute(agent_name_amb)
    agent_name_amb_res = cursor.fetchall()
    if not agent_name_amb_res:
        agent_amb = "There is no ambiguity in agent full_name"
        agent_report.append({
            'table_name': 'listing_address',
            'check_name': 'ambiguity in agent full_name',
            'status': 'Passed',
            'results': agent_amb
        })
    else:
        agent_amb_yes = "There is ambiguity in agent full_name like non mls..."
        agent_report.append({
            'table_name': 'listing_address',
            'check_name': 'ambiguity in agent full_name',
            'status': 'Failed',
            'results': agent_amb_yes
        })


    cursor.close()
    conn.close()
    return agent_report