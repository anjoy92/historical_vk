import json
import requests
from pymongo import *
import time
import pika
import json
from MultiThreadProducer import MultiThreadProducer



class VKTracker():
    def __init__(self):
        self.db = None
        self.db_ram = None
        self.config = {}
        self.key_to_cat = dict()
        self.start_time = None
        self.end_time = None
        self.keywords = []
        self.dist_server = None
        self.dist_db_name = None
        self.dist_port = None
        self.dist_user = None
        self.dist_password = None
        self.ram_server = None
        self.ram_port = None
        self.ram_user = None
        self.ram_password = None
        self.producer_threads=[]
        self.api_list=[]
        self.read_config()
        self.init_db()
        self.get_params()
        self.get_api_list()

    def read_config(self):
        with open('config.json') as json_data_file:
            self.config = json.load(json_data_file)
            self.dist_server = self.config['distServer']
            self.dist_db_name = self.config['distDBName']
            self.dist_port = int(self.config['distPort'])
            self.dist_user = self.config['distUser']
            self.dist_password = self.config['distPassword']

            self.ram_server = self.config['ramServer']
            self.ram_port = int(self.config['ramPort'])
            self.ram_user = self.config['ramUser']
            self.ram_password = self.config['ramPassword']

    def init_db(self):
        # Connect to Mongo Dist/Main server
        #try:
        client = MongoClient(self.dist_server, self.dist_port)
        self.db = client.tweettracker
        self.db.authenticate(self.dist_user, self.dist_password)
        # except:
        #     print 'Main Db auth Failed'

        # Connect to Mongo Ram server
        try:
            client = MongoClient(self.ram_server, self.ram_port)
            self.db = client.tweettracker
            self.db.authenticate(self.ram_user, self.ram_password)
        except:
            print 'Main Db auth Failed'


        # load current list of buffered files
        # dirpath = 'bufferedTweets'
        # bufferedFilesList = ['bufferedTweets/' + f for f in listdir(dirpath) if isfile(join(dirpath,f))]

        # connect all crawlers to the queue

    def get_params(self):
        all_keywords = set()
        query = {'includeincrawl': 1, 'sources': {'$in': ['vk']}}
        result = self.db.categories.find(query,
                                         {'creator': 1, 'categoryID': 1, 'keywords': 1,
                                          'sources': 1, 'oldfield': 1})

        for item in result:
            if 'vk' in item['sources']:
                if item['categoryID'] in [1074,1072,1073,1076,1075]:
                    for keyword in item['keywords']:
                        keyword = keyword.lower().strip()
                        if keyword not in self.key_to_cat:
                            self.key_to_cat[keyword] = [item['categoryID']]
                        else:
                            self.key_to_cat[keyword].append(item['categoryID'])
                        all_keywords.add(keyword)

        #print "%d keywords" % len(all_keywords)
        self.keywords = all_keywords

    def init_multithreads(self,start_time,end_time):
        self.keywords_pool=[]
        for i in range(0, len(self.api_list)):
            self.keywords_pool.append([])
        icount=0
        for keyword in self.keywords:
            self.keywords_pool[icount].append(keyword)
            icount = (icount + 1) % len(self.api_list)

        self.producer_threads=[]
        for i in range(len(self.api_list)):
            self.producer_threads.append(MultiThreadProducer(self.keywords_pool[i],self.api_list[i],start_time,end_time,self.key_to_cat,i))

    def get_api_list(self):
        with open('api_keys.txt') as api_key_file:
            for api_key in api_key_file:
                api_key = api_key.strip()
                url = "https://api.vk.com/method/newsfeed.search?q=" + 'test' + "&count=200" + \
                      "&format=JSON&extended=1&v=5.64+&end_time=" + str(int(time.time())-(60*60)) + "&start_time=" + str(int(time.time())) + "&start_from=" + '' + "&access_token=" + api_key

                r = requests.get(url=url)
                r = r.json()
                #print api_key,r,url
                if 'error' not in r:
                    print api_key
                    self.api_list.append(api_key)

    def check_thread_count(self):
        count = 0
        self.remove_list = []
        for th in self.producer_threads:
            if th.thread.isAlive():
                count += 1
        return count

    def start(self):
        start_time = 1488849108
        end_time = start_time+(60*60)
        while end_time<time.time():
            while True:
                if self.check_thread_count()==0:
                    break
                #print 'sleeping'
                time.sleep(10)
            file = open("crawl_time.txt", "a")
            file.write('STARTING AGAIN '+str(end_time))
            file.write('\n')
            file.close()
            self.init_multithreads(start_time,end_time)
            for obj in self.producer_threads:
                obj.start()
            start_time=end_time
            end_time=end_time+(60*60)


def main():
    vk_obj = VKTracker()
    vk_obj.start()


if __name__ == "__main__":
    main()
