import logging
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

            except HttpError as e:
                logger.error("%s" % e)
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

        result_list = []

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
        video_dict_list = []

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

    def video_statistics(self, **kwargs):

        '''
        Returns video statistics by its respective id
        Args:
            **kwargs: Arbitrary keyword arguments
        Returns:
            dictionary array: [{'videoId':id_video, 'statistics': video_statistics}, ..]
        '''

        responses = self._response('videos', **kwargs)

        items = responses['items']

        video_statistics_list = []

        for item in items:
            id_video = item['id']
            video_statistics = item['statistics']
            video_statistics_list.append({'videoId': id_video,
                                          'statistics': video_statistics})
        return video_statistics_list

    def _id_to_stats(self, x):

        id_join = ','.join(x)

        videos_stats = self.video_statistics(id=id_join, part='statistics')

        return videos_stats

    def video_statistics_by_channel(self, **kwargs):

        responses = self.channel_video_desc(**kwargs)

        ch_video_dict_array = []
        for response in responses:
            ch_video_dict = {}
            ch_id = response['ch_id']
            video_ids = [item['videoId'] for item in response['video_info_list']]
            ch_video_dict['ch_id'] = ch_id
            ch_video_dict['video_id'] = video_ids
            ch_video_dict_array.append(ch_video_dict)

        ch_video_info_array = []

        pool = Pool(self.processes)

        for ch_video_dict in ch_video_dict_array:
            ch_id = ch_video_dict['ch_id']
            video_split_list = self._split_list(ch_video_dict['video_id'], 50)

            results = [pool.apply_async(self._id_to_stats, args=([split])).get()
                       for split in video_split_list]

            ch_videos_stats = {}
            ch_videos_stats['ch_id'] = ch_id
            ch_videos_stats['video_stats'] = results

            ch_video_info_array.append(ch_videos_stats)

        return ch_video_info_array

    def statistics_sum(self, *args):

        '''
        Returns the sum of values from video statistics dictionary array
        Args:
            *args: Arbitrary keyword arguments
            Dictionary array with key named 'statistics' and its value in dictionary
        Returns:
            dict: the sum of statistics
        '''

        vsc_sum = {}

        for vs in vsc:

            vs_stat = vs['statistics']
            keys = vs_stat.keys()

            for key in keys:

                if vs['statistics'][key].isdigit():

                    if key in vsc_sum.keys():

                        vsc_sum[key] += int(vs['statistics'][key])

                    else:
                        vsc_sum[key] = int(vs['statistics'][key])

        old_keys = [key for key in vsc_sum.keys()]

        for key in old_keys:
            vsc_sum[key + '_sum'] = vsc_sum[key]
            vsc_sum.pop(key)

        return vsc_sum

    def _video_most_popular(self, rc, cid=0):
        """popular video list given by region code and category id
        Args:
             rc(str): region code, 2 Characters
             cid(int): youtube video category code
        Returns:
            list: dictionary array
        Examples:
            >>>_video_most_popular('KR', 0)
            [{'vid_id': videoId, 'vid_published_at': published_at,
              'ch_id': channelId, 'vid_title': vid_title,
              'vid_description': vid_description, 'vid_th': vid_th,
              'ch_title': ch_title, 'vid_tags': vid_tags,
              'vid_cat_id': categoryId, 'region_code': rc
              }, ...]
        """
        pt = ''

        dict_array = []

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
                            'vid_description': vid_description, 'vid_th': vid_th,
                            'ch_title': ch_title, 'vid_tags': vid_tags,
                            'vid_cat_id': categoryId, 'region_code': rc
                            }

                dict_array.append(vid_dict)

            if 'nextPageToken' not in responses.keys():

                return dict_array

            else:

                pt = responses['nextPageToken']

    def video_most_popular(self, rc='KR', top=True):
        """popular video list given by region code and category id
        Args:
             rc(str): region code, 2 Characters
             top(bool): Default True, True if want only top 200 video
                        False if want all possible category popular video
        Returns:
            list: dictionary array
        Examples:
            >>>video_most_popular('KR', 0)
            [{'vid_id': videoId, 'vid_published_at': published_at,
              'ch_id': channelId, 'vid_title': vid_title,
              'vid_description': vid_description, 'vid_th': vid_th,
              'ch_title': ch_title, 'vid_tags': vid_tags,
              'vid_cat_id': categoryId, 'region_code': rc
              }, ...]
        """
        responses = self._response('videocategories', regionCode=rc, part='snippet')

        cat_id_list = {item['id']: item['snippet']['title']
                       for item in responses['items'] if item['snippet']['assignable'] is True}

        cat_id_list['0'] = 'ALL'

        pool = Pool(self.processes)

        if top is True:

            most_popular = self._video_most_popular(rc=rc, cid=0)

            return most_popular

        else:

            results = deque()

            for cat_id in cat_id_list:
                ready = pool.apply_async(self._video_most_popular,
                                         kwds={'rc': rc, 'cid': cat_id})
                results.append(ready)

            outputs = [p.get() for p in results]

            outputs = [elem for elements in outputs for elem in elements]

            return outputs
