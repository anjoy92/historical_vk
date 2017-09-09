import time
import pika
import json
import threading
import urllib
import requests
import urllib2
import grequests
from tornado import ioloop, httpclient

class MultiThreadProducer():
    def __init__(self, keyword, api,start_time,end_time,key_to_cat,thread_no):
        self.keywords = keyword
        self.key_to_cat = key_to_cat
        self.api_key = api
        self.end_time = str(end_time)
        self.start_time = str(start_time)
        self.start_from=""
        self.thread_no = thread_no
        #print 'Thread No',self.thread_no
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = connection.channel()

    def start(self):
        # url = 'https://api.vk.com/method/newsfeed.search?q=hello&count=200&format=JSON&extended=1&v=5.64+&end_time=1504396203&start_time=1504394403&start_from=&access_token=a8b03c26a8b03c26a8b03c2648a88fb155aa8b0a8b03c26f1bd2b2c35b1e472098ead68'
        #
        # r = requests.get(url=url)
        # print 'sho',r.json()

        self.thread = threading.Thread(target=self.worker, args=())
        #self.thread.daemon = True
        self.thread.start()

    def worker(self):
        ct=0
        for keyword in self.keywords:
            if not keyword:
                continue
            #print keyword
            encoded_keyword = urllib.quote(keyword.encode('utf8'))
            while True:
                url = "https://api.vk.com/method/newsfeed.search?q=" + encoded_keyword + "&count=200" + \
                      "&format=JSON&extended=1&v=5.64+&end_time=" + self.end_time + "&start_time=" + self.start_time + "&start_from=" + self.start_from + "&access_token=" + self.api_key
                r = requests.get(url=url)   
                r=r.json()
                #print self.thread_no
                try:
                    if 'response' in r:
                        if(r['response']['count']!=0):
                            ct+=1
                            #print self.thread_no,ct
                            r['catlist']=self.key_to_cat[keyword]
                            self.channel.basic_publish(exchange='',
                                                  routing_key='VkQueue',
                                                  body=json.dumps(r))
                        if 'next_from' in r:
                            self.startFrom = r['next_from']
                        else:
                            break
                    elif 'error' in r:
                        file = open("crawl_time.txt", "a")
                        file.write('time: '+str(time.time())+' '+json.dumps(r))
                except Exception, e:
                    print e