from Queue import Queue

from gevent.ares import channel
from pymongo import MongoClient
import pika
import codecs
import json
import copy
import re
import requests
import threading
import datetime
import random
from langdetect import detect
import time
from random import randint

class VkComsumer():
    def __init__(self):
        stoplist = list()
        stopfile = codecs.open('stopwords.txt', encoding='utf-8')
        for line in stopfile:
            stoplist.extend(line.rstrip('\r\n').split(','))
        stoplist.append('d')
        stoplist.append('rt')
        self.stoplist = set(stoplist)
        lang_file = codecs.open('langcode.txt', encoding='utf-8')
        self.lang_map = {}
        self.config = {}
        dist_server = ''
        dist_db_name = ''
        dist_port = 0
        dist_user = ''
        dist_password = ''

        for line in lang_file:
            lang, code = line.split('=')
            self.lang_map[lang.strip()] = int(code)

        with open('config.json') as json_data_file:
            config = json.load(json_data_file)
            dist_server = config['distServer']
            dist_db_name = config['distDBName']
            dist_port = int(config['distPort'])
            dist_user = config['distUser']
            dist_password = config['distPassword']


        client = MongoClient(dist_server, dist_port)
        self.db = client.tweettracker
        self.db.authenticate(dist_user, dist_password)
        self.api_list = []
        self.get_api_list()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.thread_list=[]

    def get_api_list(self):
        with open('api_keys.txt') as api_key_file:
            for api_key in api_key_file:
                api_key = api_key.strip()
                url = "https://api.vk.com/method/newsfeed.search?q=" + 'test' + "&count=200" + \
                      "&format=JSON&extended=1&v=5.64+&end_time=" + str(
                    int(time.time()) - (60 * 60)) + "&start_time=" + str(
                    int(time.time())) + "&start_from=" + '' + "&access_token=" + api_key

                r = requests.get(url=url)
                r = r.json()
                # print api_key,r,url
                if 'error' not in r:
                    print api_key
                    self.api_list.append(api_key)


    def fetched_vk(self,ch, method, properties, vk):
        thread = threading.Thread(target=self.clean_vk, args=(json.loads(vk),))
        #thread.daemon = True
        thread.start()
        self.thread_list.append(thread)
        if self.check_thread_stop()==5:
            while True:
                if self.check_thread_stop()<3:
                    break
                time.sleep(1)

    def check_thread_stop(self):
        count = 0
        self.remove_list = []
        for th in self.thread_list:
            if th.isAlive():
                count += 1
        for rmth in self.remove_list:
            self.thread_list.remove(rmth)
        return count

    def clean_vk(self,vkf):
        vk = vkf['response']
        post_list = vk['items']
        profile_list = vk['profiles']
        group_list = vk['groups']
        insert_obj = {}
        #print vk
        for post in post_list:
            try:
                date = int(post['date'])
                insert_obj['timestamp'] = date * 1000
                insert_obj['likes'] = post['likes']
                insert_obj['comments'] = post['comments']
                insert_obj['reposts'] = post['reposts']
                postid = int(post['id'])
                urls = []
                if 'attachments' in post:
                    attachment_array = post['attachments']
                    for attachment in attachment_array:
                        if attachment['type'].lower() == "photo" or attachment['type'].lower() == "posted_photo":
                            url_obj = {}
                            photo_obj = attachment['photo']
                            photo_id = photo_obj['id']
                            owner_id = photo_obj['owner_id']
                            url = "https://vk.com/video" + str(owner_id) + "_" + str(photo_id)
                            url_obj['expanded_url'] = url
                            post['text'] = post['text'] + url
                            urls.append(url_obj)
                        elif attachment['type'].lower() == "link":
                            url_obj = {}
                            link_obj = attachment['link']
                            url = link_obj['url']
                            post['text'] = post['text'] + url
                            url_obj['expanded_url'] = url
                            urls.append(url_obj)
                        elif attachment['type'].lower() == "video":
                            url_obj = {}
                            video_obj = attachment['video']
                            video_id = video_obj['id']
                            owner_id = video_obj['owner_id']
                            url = "https://vk.com/video" + str(owner_id) + "_" + str(video_id)
                            url_obj['expanded_url'] = url
                            post['text'] = post['text'] + url
                            urls.append(url_obj)

                text = post['text']

                keywords = [x.lower() for x in text if x.lower() not in self.stoplist  and x.lower()]
                if len(keywords):
                    insert_obj['keywords'] = keywords
                insert_obj['text'] = text
                hashtags = re.findall(r"#(\w+)", text)

                entity_obj = {}
                if len(hashtags):
                    entity_obj['hashtags'] = hashtags
                if len(urls):
                    entity_obj['urls'] = urls
                insert_obj['entities'] = entity_obj
                insert_obj['type'] = 'vk'
                #print post
                ownerId = int(post['owner_id'])
                userObj = {}
                if ownerId > 0:
                    for profile in profile_list:
                        if int(profile['id']) == ownerId:
                            userObj = profile
                            if 'online' in userObj:
                                del userObj['online']
                            userObj['followers_count'] = 0
                else:
                    for group in group_list:
                        if int(group['id']) == -ownerId:
                            userObj = group
                            if "screen_name" in userObj:
                                userObj['userObj'] = userObj['id']
                            userObj['followers_count'] = 0

                if not userObj:
                    if ownerId > 0:
                        url = "https://api.vk.com/method/users.get?user_ids=" + str(
                            ownerId) + "&fields=screen_name,city&format=JSON&access_token=" + self.api_list[randint(0,len(self.api_list)-1)]
                        r = requests.get(url=url)
                        r = r.json()

                        if 'response' in r:
                            r = r['response']
                            userObj = r[0]
                    else:
                        url = "https://api.vk.com/method/groups.getById?group_ids=" + str(
                            -ownerId) + "&format=JSON&access_token=" + self.api_list[randint(0,len(self.api_list)-1)]
                        r = requests.get(url=url)
                        r = r.json()
                        if 'response' in r:
                            r = r['response']
                            r[0]['id'] = r[0]['gid']
                            del r[0]['gid']
                            userObj = r[0]
                    if not userObj:
                        userObj['followers_count'] = 0
                        userObj['screen_name'] = ''
                        userObj['location'] = ''
                        userObj['screen_name_lower'] = ''
                # Location
                if userObj:
                    if 'city' in userObj:
                        cityId = userObj['city']
                        url = "https://api.vk.com/method/places.getCitiesById?city_ids=" + str(cityId) \
                              + "&format=JSON&access_token=" + self.api_list[randint(0,len(self.api_list)-1)]
                        r = requests.get(url=url)
                        r = r.json()
                        if 'response' in r:
                            r = r['response']
                            userObj['location'] = r[0]['title']
                    elif 'country' in userObj:
                        countryid = userObj['country']
                        url = "https://api.vk./method/places.getCountriesById?city_ids=" + str(countryid) \
                              + "&format=JSON&access_token=" + self.api_list[randint(0,len(self.api_list)-1)]
                        r = requests.get(url=url)
                        r = r.json()
                        if 'response' in r:
                            r = r['response']
                            userObj['location'] = r[0]['title']
                    else:
                        userObj['location'] = ''

                if 'id' in userObj:
                    insert_obj['screen_name_lower'] = userObj['screen_name']
                insert_obj['created_at'] = datetime.datetime.fromtimestamp(date).strftime('%a %b %d %H:%M:%S +0000 %Y')
                reposts = insert_obj['reposts']
                insert_obj['retweet_count'] = reposts['count']
                insert_obj['rand'] = random.random()
                userObj['verified'] = False
                insert_obj['user'] = userObj
                if 'reply_owner_id' in post:
                    insert_obj['in_reply_to_user_id'] = int(post['reply_owner_id'])
                if 'reply_post_id' in post:
                    insert_obj['in_reply_to_status_id'] = int(post['reply_post_id'])
                insert_obj['id'] = int(postid)
                insert_obj['tweet-lang'] = 999
                try:
                    lang_val = detect(insert_obj['text'])
                    if lang_val in self.lang_map:
                        insert_obj['tweet-lang'] = self.lang_map[lang_val]
                except:
                    pass
                insert_obj['retweeted'] = False
                if 'copy_history' in post:
                    repost_list = []
                    for reposts in post['copy_history']:
                        repostObj = {}
                        repostObj['timestamp'] = reposts['date']
                        postUserObj = {}
                        postUserObj['id'] = int(reposts['owner_id'])
                        repostObj['user'] = postUserObj
                        repostObj['type'] = reposts['post_type']
                        repostObj['id'] = reposts['id']
                        repostObj['text'] = reposts['text']
                        repost_list.append(repostObj)
                    insert_obj['retweeted_status'] = repost_list
                    insert_obj['retweeted'] = True

                if 'geo' in post:
                    locObj = post['geo']
                    if locObj:
                        location = {}
                        coordinates = locObj['coordinates']
                        lat, lng = coordinates.split(' ')
                        location['lat'] = lat
                        location['lng'] = lng
                        insert_obj['location'] = location
                        insert_obj['geoflag'] = True
                    else:
                        location = {}
                        location['lat'] = 0
                        location['lng'] = 0
                        insert_obj['location'] = location
                        insert_obj['geoflag'] = False

                for realcat in vkf['catlist']:
                    new_insert_obj = copy.deepcopy(insert_obj)
                    new_insert_obj['cat'] = realcat
                    idx = postid % 3
                    shard = [0, 333000, 666000][idx]
                    sharcat = shard + realcat
                    catime = str(sharcat) + str(date)
                    new_insert_obj['catime'] = catime
                    #print new_insert_obj
                    # coll.insert(insert_obj)
                    # insert_obj['inserted_at'] = datetime.datetime()
                    # collram.insert(insert_obj)
                    #self.db.vk_historic.insert(new_insert_obj,{'ordered':False})
            except Exception, e:
                print 'EXCEPTION', e.message
                continue

    def start(self):
        self.channel.basic_consume(self.fetched_vk,
                      queue='VkQueue',
                      no_ack=True)
        try:
            print(' [*] Waiting for messages. To exit press CTRL+C')
            self.channel.start_consuming()
        except Exception, e:
            print 'shobhit',e.message
            self.connection.close()
            while True:
                if self.connection.is_closed:
                    break
                time.sleep(1)
            self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            self.channel = self.connection.channel()
            self.channel.basic_consume(self.fetched_vk,
                                       queue='VkQueue',
                                       no_ack=True)
            self.channel.start_consuming()

vkcon=VkComsumer()
vkcon.start()

