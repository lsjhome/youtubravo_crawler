import logging
from collections import deque
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
from multiprocessing import Pool

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class YoutubeCrawler(object):
    """
    Youtube data crawler based on Youtube Data Api v3
    """

    def __init__(self, api_key_list, processes=10):
        """
        Args:
            api_key_list (list): developer key list
            processes(int): the number of processes
            thread(bool): Thread use True instead of Process, default False
        """
        self.api_key_iter = iter(api_key_list)
        self.client = build("youtube", "v3", developerKey=next(self.api_key_iter))
        self.processes = processes

    @staticmethod
    def _remove_empty_kwargs(**kwargs):

        good_kwargs = {}

        if kwargs is not None:

            for key, value in kwargs.items():
                if value:
                    good_kwargs[key] = value

        return good_kwargs

    def _response(self, resource, **kwargs):
        """
        Args:
            resource(str): youtube client method resource
            **kwargs: Arbitrary keyword arguments.
        Returns:
            dict: response in dictionary form
        """
        kwargs = self._remove_empty_kwargs(**kwargs)

        response = None

        while not response:

            try:

                if resource == 'channels':
                    response = self.client.channels().list(
                        **kwargs
                    ).execute()

                if resource == 'search':
                    response = self.client.search().list(
                        **kwargs
                    ).execute()

                if resource == 'videos':
                    response = self.client.videos().list(
                        **kwargs
                    ).execute()

                if resource == 'playlistitems':
                    response = self.client.playlistItems().list(
                        **kwargs
                    ).execute()

                if resource == 'videocategories':
                    response = self.client.videoCategories().list(
                        **kwargs
                    ).execute()
                    
                if resource == 'commentThreads':
                    response = self.client.commentThreads().list(
                        **kwargs
                    ).execute()
                    
                if resource == 'comments':
                    response = self.client.comments().list(
                        **kwargs
                    ).execute()
                    
            except HttpError as e:
                
                logger.error("%s" % e)
                
                if b'disabled comments' in e.content:
                    
                    return 
                    
                if e.resp.status == 403:
                    self.client = build("youtube", "v3", developerKey=next(self.api_key_iter))
                pass

        return response

    @staticmethod
    def _split_list(l, n):
        """
        Args:
            l(list): original list to be split
            n(int): split size
        Returns:
            list: n-sized list from list l
        """
        split_list = []

        for i in range(0, len(l), n):
            split_list.append(l[i:i + n])

        return split_list

    def channel_desc(self, id=None):
        """Channel description method
        Args:
            id(str): channel_id
        Returns:
            list: dictionary array
        Examples:
            >>> channel_description(id=channel_id)
            [{'title': channel_title,
            'ch_id': channel_id,
            'description': channel_description,
             'publisehdAt': channel_created_date}, ...]
        """
        responses = self._response('channels', part='snippet', id=id)

        desc_date_list = [{'title': response['snippet']['title'],
                           'ch_id': response['id'],
                           'description': response['snippet']['description'],
                           'publishedAt': response['snippet']['publishedAt'][:10],
                           'thumbnails': response['snippet']['thumbnails']}

                          for response in responses['items']]

        return desc_date_list

    def channel_countstats(self, id=None):
        """Channel count statistics method
        Args:
            id(str): channel_id
        Returns:
            list: dictionary array
        Examples:
            >>> channel_countstats(id=channel_id)
            [{'ch_id': str,
            'subscriberCount': int;None,
            'viewCount': int,
            'videoCount': int,
            'sub_view_ratio': float;None}, ...]
        """
        responses = self._response('channels', part='statistics', id=id)

        result_list = deque()

        for response in responses['items']:

            ch_id = response['id']

            statistics_response = response['statistics']

            view_count = int(statistics_response['viewCount'])
            video_count = int(statistics_response['videoCount'])
            comment_count = int(statistics_response['commentCount'])

            if statistics_response['hiddenSubscriberCount'] is True:

                subscriber_count = None
                sub_view_ratio = None

            else:

                subscriber_count = int(statistics_response['subscriberCount'])

                try:
                    sub_view_ratio = view_count / subscriber_count
                except ZeroDivisionError:
                    sub_view_ratio = None

            result_list.append({'ch_id': ch_id, 'subscriberCount': subscriber_count,
                                'viewCount': view_count, 'videoCount': video_count,
                                'sub_view_ratio': sub_view_ratio, 'comment_count': comment_count})

        return result_list

    def _video_desc(self, ch_id, upload_id, update, days):
        """video description list given by an upload id
        Args:
            ch_id(str): channel_id
            upload_id(str): upload_id
            update(bool): True if requesting video data created after N days ago
            days(int): N days
        Returns:
            dict
        Examples:
            >>> _video_desc(ch_id, upload_id)
            {'ch_id': channel_id,
             'upload_id': upload_id,
             'video_info_list': [{'channelId': channel_id,
                                  'videoId': video_id,
                                  'title': video title,
                                  'description': video description,
                                  'publishedAt': video published time,
                                  'thumbnails': video thumbnail_urls}, ...]}
        """
        next_page_token = ''
        video_dict_list = deque()

        while True:

            response = self._response('playlistitems', playlistId=upload_id,
                                      part='snippet',
                                      maxResults=50,
                                      pageToken=next_page_token)

            video_dict = [{'channelId': item['snippet']['channelId'],
                           'videoId': item['snippet']['resourceId']['videoId'],
                           'title': item['snippet']['title'],
                           'description': item['snippet']['description'],
                           'publishedAt': item['snippet']['publishedAt'],
                           'thumbnails': item['snippet']['thumbnails']
                           }
                          for item in response['items']]

            video_dict_list.extend(video_dict)

            # update를 위한 경우
            if update is True:

                try:

                    vid_pub_at = video_dict[-1]['publishedAt']

                    vid_pub_at_dt = parse(vid_pub_at)

                    utc_now = datetime.now(timezone.utc)
                    stdd = utc_now - timedelta(days=days + 1)

                    if vid_pub_at_dt < stdd:
                        return {
                            'ch_id': ch_id,
                            'upload_id': upload_id,
                            'video_info_list': video_dict_list
                        }
                except IndexError:
                    return {
                        'ch_id': ch_id,
                        'upload_id': upload_id,
                        'video_info_list': video_dict_list
                    }

            if 'nextPageToken' in response.keys():
                next_page_token = response['nextPageToken']

            else:

                return {
                    'ch_id': ch_id,
                    'upload_id': upload_id,
                    'video_info_list': video_dict_list
                }

    def channel_video_desc(self, id=None, update=False, days=0):
        """video description list given by channel ids
        channel ids => upload ids => MultiThreading => video description list by upload ids
        Args:
             id(str): channel_id
             update(bool): True if requesting video data created after N days ago
             days(int): N days
        Returns:
            list: dictionary array
        Examples:
            >>>channel_video_desc(id=channel_id)
            [{'ch_id': channel_id,
              'upload_id': upload_id,
              'video_info_list': [{'channelId': channel_id,
                                   'videoId': video_id,
                                   'title': video title,
                                   'description': video description,
                                   'publishedAt': video published time,
                                   'thumbnails': video thumbnail_urls
                                   }, ...]}, ...]
        """

        responses = self._response('channels', part='contentDetails', id=id)

        ch_uploads_id = [{'ch_id': item['id'],
                          'uploads_id': item['contentDetails']['relatedPlaylists']['uploads']}
                         for item in responses['items']]
        results = deque()

        pool = Pool(self.processes)

        for ch_uploads in ch_uploads_id:
            upload_id = ch_uploads['uploads_id']
            ch_id = ch_uploads['ch_id']

            ready = pool.apply_async(self._video_desc,
                                     kwds={
                                         'ch_id': ch_id,
                                         'upload_id': upload_id,
                                         'update': update,
                                         'days': days
                                     })
            results.append(ready)

        outputs = [p.get() for p in results]

        return outputs

    def _video_trend(self, rc, cid=0):
        """trending video list given by region code and category id
        Args:
             rc(str): region code, 2 Characters
             cid(int): youtube video category code
        Returns:
            list: dictionary array
        Examples:
            >>>_video_trend(rc='KR', cid=0)
            [{'vid_id': videoId, 'vid_published_at': published_at,
              'ch_id': channelId, 'vid_title': vid_title, 'vid_desc': vid_description,
              'vid_th': vid_th, 'ch_title': ch_title, 'vid_tags': vid_tags,
               'vid_cat_id': categoryId, 'region_code': rc, 'vid_rank': rank,'vid_trend_cat': cid
              }, ...]
        """
        pt = ''
        dict_array = deque()
        rank = 1

        while True:

            responses = self._response('videos', part='snippet', chart='mostPopular',
                                       regionCode=rc, pageToken=pt, maxResults=50,
                                       videoCategoryId=cid)

            for item in responses['items']:

                videoId = item['id']
                published_at = item['snippet']['publishedAt']
                channelId = item['snippet']['channelId']
                vid_title = item['snippet']['title']
                vid_description = item['snippet']['description']
                vid_th = item['snippet']['thumbnails']
                ch_title = item['snippet']['channelTitle']

                if 'tags' in item['snippet'].keys():
                    vid_tags = ','.join(item['snippet']['tags'])

                else:
                    vid_tags = ''

                categoryId = item['snippet']['categoryId']

                vid_dict = {'vid_id': videoId, 'vid_published_at': published_at,
                            'ch_id': channelId, 'vid_title': vid_title,
                            'vid_desc': vid_description, 'vid_th': vid_th,
                            'ch_title': ch_title, 'vid_tags': vid_tags,
                            'vid_cat_id': categoryId, 'region_code': rc, 'vid_rank': rank,
                            'vid_trend_cat': cid
                            }
                rank += 1
                dict_array.append(vid_dict)

            if 'nextPageToken' not in responses.keys():

                return dict_array

            else:

                pt = responses['nextPageToken']

    def video_trend(self, rc='KR', top=True):
        """trending video list given by region code and category id
        Args:
             rc(str): region code, 2 Characters
             top(bool): Default True, True if want only top 200 video
                        False if want all possible category popular video
        Returns:
            list: dictionary array
        Examples:
            >>>video_trend(rc='KR', cid=0, top=True)
            [{'vid_id': videoId, 'vid_published_at': published_at,
              'ch_id': channelId, 'vid_title': vid_title, 'vid_desc': vid_description,
              'vid_th': vid_th, 'ch_title': ch_title, 'vid_tags': vid_tags,
               'vid_cat_id': categoryId, 'region_code': rc, 'vid_rank': rank,'vid_trend_cat': cid
              }, ...]
        """
        responses = self._response('videocategories', regionCode='US', part='snippet')

        cat_id_list = {item['id']: item['snippet']['title']
                       for item in responses['items'] if item['snippet']['assignable'] is True}

        cat_id_tot_list = {item['id']: item['snippet']['title'] for item in responses['items']}

        cat_id_list['0'] = 'ALL'

        pool = Pool(self.processes)

        if top is True:

            most_popular_video_list = self._video_trend(rc=rc, cid=0)

            for most_popular_video in most_popular_video_list:
                most_popular_video['vid_cat'] = cat_id_tot_list[most_popular_video['vid_cat_id']]

            return most_popular_video_list

        else:

            results = deque()

            for cat_id in cat_id_list:
                ready = pool.apply_async(self._video_trend,
                                         kwds={'rc': rc, 'cid': cat_id})
                results.append(ready)

            outputs = [p.get() for p in results]

            outputs = [elem for elements in outputs for elem in elements]

            for output in outputs:
                output['vid_cat'] = cat_id_tot_list[output['vid_cat_id']]

            return outputs
        
    def _video_stats(self, vid, **kwargs):
        """Video Statistics by video id(s)
        Args:
             vid(str): Youtube video id(s), maximum 50 ids possible
        Returns:
            deque: dictionary array
        Examples:
            >>>_video_stats(vid=''_S64IMfIod8,_s66WPKCEd8, ...')
                deque([{'viewCount': '17133', 'likeCount': '83', 'dislikeCount': '0',
                        'favoriteCount': '0', 'commentCount': '45', 'vid_id': '_S64IMfIod8'},
                        {'viewCount': '23', 'likeCount': '0', 'dislikeCount': '0',
                        'favoriteCount': '0', 'commentCount': '0', 'vid_id': '_s66WPKCEd8'}, ...])
        """
        responses = self._response('videos', id=vid, part='statistics', **kwargs)
        
        vid_stats_dict_list = deque()
        
        key_require = set(['viewCount', 'likeCount', 'dislikeCount', 'favoriteCount', 'commentCount'])
        
        for item in responses['items']:
            
            vid_stats_dict = item['statistics']
            
            key_has = set(item['statistics'].keys())
            empty_keys = key_require - key_has
            
            # if there is any empty key
            if empty_keys:
                for key in empty_keys:
                    vid_stats_dict[key] = None
                    
            vid_stats_dict['vid_id'] = item['id']
            vid_stats_dict_list.append(vid_stats_dict)
            
        return vid_stats_dict_list
    
    def video_stats(self, vids):
        """Video Statistics by video id(s)
        video ids => split into list with 50 vids elements as string => Multiprocessing
        Args:
             vids(str): Youtube video id(s), no length limit
        Returns:
            deque: dictionary array
        Examples:
            >>>video_stats(vid='_S64IMfIod8,_s66WPKCEd8, ...')
                deque([{'viewCount': '17133', 'likeCount': '83', 'dislikeCount': '0',
                        'favoriteCount': '0', 'commentCount': '45', 'vid_id': '_S64IMfIod8'},
                        {'viewCount': '23', 'likeCount': '0', 'dislikeCount': '0',
                        'favoriteCount': '0', 'commentCount': '0', 'vid_id': '_s66WPKCEd8'}, ...])
        """
        vid_list = vids.split(',')
        
        vid_split_list = self._split_list(vid_list, 50)
        
        vid_split_list_50 = [','.join(item) for item in vid_split_list]
        
        pool = Pool(self.processes)
        
        results = [pool.apply_async(self._video_stats, kwds={'vid': id_join_50}) 
                                            for id_join_50 in vid_split_list_50]
        
        outputs = deque()
        
        for p in results:
            
            outputs.extend(p.get())
        
        return outputs
    
    def _comment(self, vid, **kwargs):
        """Video comments by video id
        Args:
             vid(str): Youtube video id
        Returns:
            deque: dictionary array
        Examples:
            >>>_comment(vid='DxfEdD7JpcE')
                deque([{'author_name': '강성민', 'author_img': 'https://yt3.ggpht.com/-ljpkNvLx6Ng/AAAAAAAAAAI/AAAAAAAAAAA/8dFlKZ5AGWM/s28-c-k-no-mo-rj-c0xffffff/photo.jpg',
                        'author_id': 'UC963kbRm1ZOm68jDXkZSpDQ','vid_id': 'DxfEdD7JpcE', 'comment': '솔직히 말하면 쯔양님이 이긴거아니냐', 'n_like': 0, 'publishedAt': '2019-02-11T12:00:56.000',
                        'replyType': False},
                       {'author_name': '뿌 뿌', 'author_img': 'https://yt3.ggpht.com/-WxJvhks7ZMo/AAAAAAAAAAI/AAAAAAAAAAA/Hts2w2qzm4Q/s28-c-k-no-mo-rj-c0xffffff/photo.jpg',
                        'author_id': 'UCasr8l5uCtxH6zxspy-cg1Q', 'vid_id': 'DxfEdD7JpcE', 'comment': '아니 걍 노답이다', 'n_like': 1, 'publishedAt': '2019-02-11T02:03:16.000',
                        'replyType': False},,])
        """
        pt = ''
        dict_comment_array = deque()
        
        parent_id_array = deque()
        
        key_require = set(['authorDisplayName', 'authorProfileImageUrl', 'authorChannelId', 
                   'textDisplay', 'likeCount', 'publishedAt'])
        
        while True:
            responses = self._response('commentThreads', videoId=vid, part='snippet', 
                                       maxResults=100, pageToken=pt, **kwargs)
            
            if responses is None or not responses['items']:
                
                dict_comment_array.append({'vid_id': vid})
                return dict_comment_array
            
            for item in responses['items']:
                
                if item['snippet']['totalReplyCount'] != 0:
                    parent_id = item['id']
                    parent_id_array.append(parent_id)
                
                snippet = item['snippet']['topLevelComment']['snippet']
                key_has = set(snippet.keys())
                empty_keys = key_require - key_has
                
                dict_comment = dict()
                
                # if there is any empty key
                if empty_keys:
                    for key in empty_keys:
                        dict_comment[key] = None
                        
                key_remain = key_require - empty_keys
                
                
                if key_remain:

                    for key in key_remain:
                    
                        if key == 'authorChannelId':
                            dict_comment[key] = snippet['authorChannelId']['value']
                        
                        else:
                            dict_comment[key] = snippet[key]
                
                dict_comment['replyType'] = False
                dict_comment['vid_id'] = vid
                
                
                dict_comment_array.append(dict_comment)
            
            if 'nextPageToken' not in responses.keys():
                
                break
            
            else:
                pt = responses['nextPageToken']
            
            
        for parent_id in parent_id_array:
                        
            pt = ''
            
            while True:

                responses = self._response('comments', part='snippet', parentId=parent_id, 
                                           maxResults=100, pageToken=pt, **kwargs)
                
                for item in responses['items']:

                    snippet = item['snippet']
                    
                    key_has = set(snippet.keys())
                    empty_keys = key_require - key_has
                    
                    dict_comment = dict()

                    # if there is any empty key
                    if empty_keys:
                        for key in empty_keys:
                            dict_comment[key] = None

                    key_remain = key_require - empty_keys

                    if key_remain:

                        for key in key_remain:

                            if key == 'authorChannelId':
                                dict_comment[key] = snippet['authorChannelId']['value']

                            else:
                                dict_comment[key] = snippet[key]

                    dict_comment['replyType'] = True
                    dict_comment['vid_id'] = vid
                    
                    dict_comment_array.append(dict_comment)
                
                if 'nextPageToken' not in responses.keys():

                    break
                
                else:
                    pt = responses['nextPageToken']
                    
        return dict_comment_array
    
    def comment(self, vids):
        """Video comments by video id(s)
        video ids => split into list with 50 vids elements => Multiprocessing
        Args:
             vids(str): Youtube video id(s)
        Returns:
            deque: dictionary array
        Examples:
            >>>comment(vid='___a64QBUoA,___BxqE6JNY')
               deque([{'vid_id': '___a64QBUoA'},
                      {'textDisplay': '1', 'publishedAt': '2017-06-14T06:21:40.000Z', 'authorDisplayName': 'Nabila Kastella', 'authorChannelId': 'UCgXL5lO-MLjGXOjZEQvEtoA',
                       'likeCount': 1, 'authorProfileImageUrl': 'https://yt3.ggpht.com/-2ETp2uqZBtM/AAAAAAAAAAI/AAAAAAAAAAA/oYtuzdqPRC0/s28-c-k-no-mo-rj-c0xffffff/photo.jpg',
                       'replyType': False, 'vid_id': '___BxqE6JNY'}])
        """
        vid_list = vids.split(',')
        
        vid_split_list = self._split_list(vid_list, 50)
        
        pool = Pool(self.processes)
        
        outputs = deque()
        
        for vid_split in vid_split_list:
            
            results = [pool.apply_async(self._comment, kwds={'vid': vid}) 
                       for vid in vid_split]
            
            for p in results:
                
                outputs.extend(p.get())
                
        return outputs
