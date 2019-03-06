import os
from datetime import datetime, timedelta

import pandas as pd

from utils.mysql import MySQL
from YoutubeData.YoutubeCrawler import YoutubeCrawler

def gen_split(gen, n, key):
    
    new_list = []
    
    for _ in range(n):
        
        try:
            value = next(gen)
            new_list.append(value[key])

        except StopIteration:
            return 
    
    return ','.join(new_list)
    
def update_check():
    
    created_at_gen = conn.select('select distinct created_at from t_ch_vid_desc;')
    created_at_gen_list = list(created_at_gen)
    
    if len(created_at_gen_list) == 0:
        return {'vid_update':False}
    else:    
        last_create = sorted([i['created_at']for i in created_at_gen_list], reverse=True)[0]
        dt_update = datetime.utcnow() + timedelta(hours=9) - last_create
        update_day = dt_update.days
        return {'vid_update':True, 'update_day': update_day}

def youtube_main_crawler(vid_update=True, update_day=2):
    """
    Youtube Main Crawler. Expected to run once a day
    """
    ch_id_gen = conn.select('select distinct ch_id from t_vid_trend')

    while True:
        id_multi = gen_split(ch_id_gen, 50, 'ch_id')

        # break if no longer id left
        if id_multi is None:
            break    

        # t_ch_desc upsert
        cds = yc.channel_desc(id=id_multi)
        for cd in cds:

            th_de = cd['thumbnails']['default']['url']
            th_med = cd['thumbnails']['medium']['url']
            th_hi = cd['thumbnails']['high']['url']
            ch_thumb = ','.join([th_de, th_med, th_hi])

            ch_desc_dict = {'title': cd['title'], 'ch_id': cd['ch_id'], 
                            'description': cd['description'], 'publishedAt': cd['publishedAt'],
                            'ch_thumb': ch_thumb}

            conn_02.execute(upsert_cd, ch_desc_dict)
            conn_02.conn.commit()

        # t_ch_cstats upsert
        ccs = yc.channel_countstats(id=id_multi)
        for cc in ccs:
            conn_02.execute(upsert_cc, cc)
            conn_02.conn.commit()
        
        if vid_update is True:
            
            # t_ch_video_desc update
            cvds = yc.channel_video_desc(id=id_multi, update=vid_update, days=update_day)
        
        else:
            
            # t_ch_video_desc insert
            cvds = yc.channel_video_desc(id=id_multi)

        for cvd in cvds:      

            ch_vid_dict_list = []

            for vi in cvd['video_info_list']:
                ch_id = cvd['ch_id']
                upload_id = cvd['upload_id']
                vid_id = vi['videoId']
                vid_title = vi['title']
                vid_desc = vi['description']
                vid_published_at = vi['publishedAt'][:-1]

                keys = vi['thumbnails'].keys()
                vid_th = ','.join([vi['thumbnails'][key]['url'] for key in keys])


                ch_vid_desc_dict = {'ch_id': ch_id, 'upload_id': upload_id, 'vid_id': vid_id,
                                    'vid_title': vid_title, 'vid_desc': vid_desc,
                                    'vid_published_at': vid_published_at, 'vid_th': vid_th}

                ch_vid_dict_list.append(ch_vid_desc_dict)

            # insert all video by a channel
            conn_02.connect()
            conn_02.executemany(insert_vid_desc, ch_vid_dict_list)
            conn_02.conn.commit()

if __name__ == '__main__':
    
    api_list = [os.environ['API_1'], os.environ['API_2'], os.environ['API_3'],
                os.environ['API_4'], os.environ['API_5']]
    
    yc = YoutubeCrawler(api_list, processes=50)
    
    conn = MySQL(host=os.environ['host'], 
             db=os.environ['db'], 
             user=os.environ['user'], 
             port=os.environ['port'], 
             passwd=os.environ['pw'],
             charset='utf8mb4',
             auto_commit=False)
    
    conn_02 = MySQL(host=os.environ['host'], 
             db=os.environ['db'], 
             user=os.environ['user'], 
             port=os.environ['port'], 
             passwd=os.environ['pw'],
             charset='utf8mb4',
             auto_commit=False)
    
    upsert_cd = 'INSERT INTO YOUTUBE.t_ch_desc \
          (ch_title, ch_id, ch_desc, ch_published_at, ch_thumb) \
          VALUES (%(title)s, %(ch_id)s, %(description)s, %(publishedAt)s, %(ch_thumb)s) \
          ON DUPLICATE KEY UPDATE ch_title=%(title)s, ch_desc=%(description)s, ch_published_at=%(publishedAt)s, ch_thumb=%(ch_thumb)s'

    upsert_cc = 'INSERT INTO YOUTUBE.t_ch_cstats \
              (ch_id, ch_n_sub, ch_n_view, ch_n_video, ch_n_cmt) \
              VALUES (%(ch_id)s, %(subscriberCount)s, %(viewCount)s, %(videoCount)s, %(comment_count)s) \
              ON DUPLICATE KEY UPDATE ch_n_sub=%(subscriberCount)s, ch_n_view=%(viewCount)s, ch_n_video=%(videoCount)s, ch_n_cmt=%(comment_count)s'

    insert_vid_desc = 'INSERT IGNORE INTO YOUTUBE.t_ch_vid_desc \
                       (ch_id, upload_id, vid_id, vid_title, vid_desc, vid_published_at, vid_th) \
                       VALUES (%(ch_id)s, %(upload_id)s, %(vid_id)s, %(vid_title)s, %(vid_desc)s, %(vid_published_at)s, %(vid_th)s) \
                       '
    check = update_check()
    youtube_main_crawler(**check)
