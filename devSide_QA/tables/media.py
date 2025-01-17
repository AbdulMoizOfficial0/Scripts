import pandas as pd
import psycopg2


def media(cur_stage, cur_rds, source_id):
    batch_id_query = f'SELECT DISTINCT batch_id from listing where source_id = {source_id} ORDER BY batch_id DESC;'
    cur_stage.execute(batch_id_query)
    result = cur_stage.fetchone()
    batch_id = result[0]
    media_report = []

    # Query to get the differnce of photo_count in listing and listing_photo table in homelistings
    photo_count_lis_med = f'''SELECT COUNT(1) 
                              FROM listing l 
                              join listing_photo lp 
                              on l.id=lp.listing_id 
                              WHERE l.source_id = {source_id} and l.batch_id = {batch_id}
                              UNION ALL
                              SELECT SUM(photo_count) 
                              FROM listing WHERE source_id = {source_id} and batch_id = {batch_id};
    '''
    cur_stage.execute(photo_count_lis_med)
    photo_count = cur_stage.fetchall()
    photo_count_list = [i[0] for i in photo_count]
    result = photo_count_list[0] - photo_count_list[1]
    allowed_diff = 10
    if result >= allowed_diff or result == 0:
        media_report.append({
            'table_name': 'media',
            'check_name': "Media difference",
            'status': 'Passed',
            'results': f'difference is: {result}'
        })
    else:
        media_report.append({
            'table_name': 'media',
            'check_name': "Media difference",
            'status': 'Failed',
            'results': f'difference is: {result}'
        })
    
    # Query to check if there is any mls_number which has media_url but photo_count is missing
    media_url_missing = f'''
                            SELECT l.mls_number,l.photo_count,lp.media_url
                            FROM listing l 
                            join listing_photo lp 
                            on l.id=lp.listing_id 
                            WHERE l.source_id = {source_id} and l.batch_id = {batch_id}
                            and (photo_count is not null) and media_url is null
    '''
    cur_stage.execute(media_url_missing)
    media_url_missing_res = cur_stage.fetchall()
    if not media_url_missing_res:
        media_report.append({
            'table_name': 'media',
            'check_name': "mls_number which has media_url but photo_count is missing",
            'status': 'Passed',
            'results': "There is no mls_number which has media_url but photo_count is missing"
        })
    else:
        for mls_num, photo_count, media_url in media_url_missing_res:
            media_report.append({
                'table_name': 'media',
                'check_name': "mls_number which has media_url but photo_count is missing",
                'status': 'Failed',
                'results': f'{mls_num, photo_count, media_url}'
            })
    
    # Query to get the photos difference
    photos_diff = f'''
                    SELECT l.mls_number,l.id,l.photo_count AS P_COUNT , COUNT(lp.Listing_id) AS LPCOUNT ,  COUNT(lp.Listing_id) - l.photo_count AS DIFF FROM listing_photo lp
                    LEFT JOIN listing l ON l.id = lp.listing_id 
                    WHERE l.source_id = {source_id} and l.batch_id = {batch_id}
                    GROUP BY l.mls_number,l.id,l.photo_count, lp.listing_id
                    HAVING l.photo_count <> COUNT(lp.listing_id)
                    ORDER BY DIFF DESC;
    '''
    cur_stage.execute(photos_diff)
    photos_diff_res = cur_stage.fetchall()
    if not photos_diff_res:
        media_report.append({
            'table_name': 'media',
            'check_name': "difference between photos count",
            'status': 'Passed',
            'results': "There is no difference between photos count"
        })
    else:
        for mls_num, id, p_count, lpcount, diff in photos_diff_res:
            media_report.append({
                'table_name': 'media',
                'check_name': "difference between photos count",
                'status': 'Failed',
                'results': f'{mls_num}, {id}, {p_count}, {lpcount}, {diff}'
        })

    # Query to Check duplication in media table
    duplication_media = f'''
                    SELECT count(lp.media_url), lp.media_url
                    FROM listing_photo lp
                    JOIN listing l
                    ON l.id = lp.listing_id
                    WHERE l.source_id = {source_id} and l.batch_id = {batch_id}
                    GROUP BY lp.media_url
                    HAVING COUNT(1) > 1;
    '''
    cur_stage.execute(duplication_media)
    duplication_media_res = cur_stage.fetchall()
    if not duplication_media_res:
        media_report.append({
            'table_name': 'media',
            'check_name': "Duplication in media table",
            'status': 'Passed',
            'results': "There is no duplication in media table"
        })
    else:
        for count, url in duplication_media_res:
            media_report.append({
                'table_name': 'media',
                'check_name': "There is duplication in media table",
                'status': 'Failed',
                'results': f'{count}, {url}'
        })
    
    return media_report