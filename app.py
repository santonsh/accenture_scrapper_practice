import argparse
import urllib.request
import threading 
import queue
import time
from url_normalize import url_normalize
from urllib.error import URLError
import re
from testlib import generate_test_graph

def initial_urls(urls_file):
    urls = []
    urls_file = 'urls'
    with open(urls_file, 'r') as uf:
        for l in uf:
            urls.append(url_normalize(l))
    return urls

class downloaderThread(threading.Thread):
    def __init__(self, threadID, dq, pq, visited_urls, test_graph={}):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.dq = dq
        self.pq = pq
        self.visited_urls = visited_urls
        self.test_graph = test_graph

    def run(self):
        print("Starting " + str(self.threadID))
        while not exitFlag:
            self.dq.acquire()
            # take a job from download queue
            if not self.dq.empty():
                url = self.dq.get()
                self.dq.release()
                self.visited_urls.acquire()
                self.visited_urls.add(url)
                self.visited_urls.release()
                try:
                    if self.test_graph == {}:
                        real_url, page = get_html_page(url)
                    else:
                        # for offline test  
                        page = self.test_graph[url].get_page()
                        real_url = url 
                        #print('pageis '+page)
                except:
                    real_url = ''
                    page = []
                    #print('failed to download')
                if real_url == '':
                    # add to visited DB
                    #visitedURLs[url] = 'error'
                    pass
                else:
                    # add url to visited DB
                    #visitedURLs[url] = 'done'
                    if real_url != url:
                        #visitedURLs[real_url] = 'done'
                        self.visited_urls.acquire()
                        self.visited_urls.add(real_url)
                        self.visited_urls.release()
                    # add page to processor queue
                    self.pq.acquire()
                    self.pq.put(page)
                    self.pq.release()
            # download queue is empty. Lets wait and see if it fills up
            else:
                self.dq.release()
                time.sleep(1)

        print("Exiting " + str(self.threadID))

def get_html_page(url):
    # this function is responsible for downloading a single html page
    req = urllib.request.Request(url)
    response = urllib.request.urlopen(req)
    page = []
    try:
        response = urllib.request.urlopen(req)
    except URLError as e:
        if hasattr(e, 'reason'):
            print('We failed to reach a server.')
            print('Reason: ', e.reason)
        elif hasattr(e, 'code'):
            print('The server couldn\'t fulfill the request.')
            print('Error code: ', e.code)
        raise 
    
    # download successful
    real_url = response.geturl()
    try:
        data_type = re.findall('(.*);', response.info()['Content-Type'])[0]
        if data_type == 'text/html':
            page = response.read().decode('utf-8')
            #print(page)
    except:
        raise
    return real_url, page
    
class processorThread(threading.Thread):
    def __init__(self, threadID, dq, pq, parser, store_sets):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.dq = dq
        self.pq = pq
        self.parser = parser
        self.store_sets = store_sets

    def run(self):
        print("Starting " + str(self.threadID))
        findings = {}
        while not exitFlag:
            print('p is aquiring')
            self.pq.acquire()
            # take a job from processing queue
            if not self.pq.empty():
                page = self.pq.get()
                self.pq.release()
                #print(self.threadID +' processing ' + 'some page')
                #prarse the queued page
                #print(page)
                #self.parser.parse(page) 
                try:
                    findings = self.parser.parse(page)
                    
                except:
                    findings = {}
                for f in findings:
                    print(self.threadID+' found '+ str(len(findings[f]))+' of type '+str(f))

                if findings == {}:
                    # nothing to add to dbs
                    #print(self.threadID+' had a parsing error')
                    pass
                else:
                    new_urls = findings['url']
                    emails = findings['email']
                    self.store_sets['emails'].acquire()
                    for e in emails:
                        self.store_sets['emails'].add(e)
                    self.store_sets['emails'].release() 
                    
                    # filter out visited urls
                    #!!! LOCK !!!
                    #new_urls = urlDB.notIn(new_urls)
                    # add new urls to download queue
                    self.dq.acquire()
                    droped_count = 0
                    for url in new_urls:
                        if url not in self.store_sets['urls'].set:
                            try:  
                                self.dq.put(url, block=False)
                                print('added url')
                            except queue.Full:
                                droped_count +=1
                    if droped_count > 0:
                        print('The download queue is full. Droped '+str(droped_count)+' of '+str(len(new_urls))+' new urls')

                    self.dq.release()

            # processing queue is empty. Lets wait and see if it fills up
            else:
                self.pq.release()
                print('p is waiting')
                time.sleep(1)
                print('p is waiting 2')

        print("Exiting " + str(self.threadID))

class Analizer(threading.Thread):
    def __init__(self, threadID, shared_queues, shared_sets):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.shared_sets = shared_sets
        self.shared_queues = shared_queues

    def run(self):
        print("Starting " + str(self.threadID))
        while not exitFlag:
            #print('counting')
            info = {'q':{}, 's':{}}
            for qname in self.shared_queues:
                q=self.shared_queues[qname]
                q.acquire()
                cnt = q.cnt()
                info['q'][qname] = cnt
                q.release()
            for sname in self.shared_sets:
                s=self.shared_sets[sname]
                s.acquire()
                cnt = s.len()
                s.release()
                info['s'][sname] = cnt
            
            print('Analizer: Elements count = '+str(info))
            time.sleep(10)

class Scrapper():
    def __init__(self, parser, test_graph={}):
        self.max_downloaders = 1
        self.max_processors = 1
        self.urls = []
        self.downloaders = []
        self.processors = []
        self.parser = parser
        self.test_graph = test_graph
        # define queues
        self.downloadQueue   = sharedQueue(size = 1000)
        self.processingQueue = sharedQueue(size = 10)     
        self.analyzer = None
        # define sets
        self.urls_set = sharedSet()
        self.emails_set = sharedSet()

    def set_urls(self, urls):
        self.urls = urls

    def start(self):
        print('start scrapping')
        print('filling the download queue with starting urls')

        self.downloadQueue.acquire()
        for u in self.urls:
            self.downloadQueue.put(u)
            #print(u)
        self.downloadQueue.release()

        # start all threads
        self.analyzer = Analizer('thread_dwn_anl_0', {'down_q': self.downloadQueue, 'proc_q': self.processingQueue}, {'url_s': self.urls_set, 'email_s': self.emails_set})
        self.analyzer.start()

        for i in range(self.max_downloaders):
            thread_id = 'thread_dwn_'+str(i)
            thread = downloaderThread(thread_id, self.downloadQueue, self.processingQueue, self.urls_set, self.test_graph)
            thread.start()
            self.downloaders.append(thread)

        for i in range(self.max_processors):
            thread_id = 'thread_proc_'+str(i)
            thread = processorThread(thread_id, self.downloadQueue, self.processingQueue, self.parser, {'urls':self.urls_set, 'emails':self.emails_set})
            thread.start()
            self.processors.append(thread)

        while not exitFlag:
            time.sleep(10)
        
        for t in self.processors:
            t.join()
        for t in self.downloaders:
            t.join()
        
        self.analyzer.join()

class Parser():
    # this class is used to parse html content
    def __init__(self):
        self.targets = {}
    def parse(self, content):
        # used to parse content
        findings = {}
        if (type(content) != type('')) or len(content)<1:
            return findings
        for t in self.targets:
            try:
                findings[t] = re.findall(self.targets[t], content)
            except:
                print(type(self.targets[t]))
                print(type(content))
        return findings
    def add_target(self, name, regexp):
        # used to add new scrapping targets
        self.targets[name] = regexp

class sharedQueue():
    # this class unifies lock and queue in one instance
    def __init__(self, size):
        self.q = queue.Queue(size)
        self.Lock = threading.Lock()
        self.count = 0
        self.max = size
    def get(self):
        el = self.q.get()
        self.count-=1
        return el
    def cnt(self):
        return self.count
    def empty(self):
        return self.q.empty()
    def put(self, el, block=True):
        self.q.put(el, block=block)
        self.count+=1
    def acquire(self):
        print(str(self)+' queue is being aquired')
        self.Lock.acquire()
        print(str(self)+' queue is aquired')
    def release(self):
        print(str(self)+' queue is being released')
        self.Lock.release()
        print(str(self)+' queue is released')

class sharedSet():
    # this class unifies lock and set in one instance
    def __init__(self):
        self.set = set()
        self.Lock = threading.Lock()
    def add(self, el):
        self.set.add(el)
    def acquire(self):
        self.Lock.acquire()
    def release(self):
        self.Lock.release()
    def len(self):
        return len(self.set)



if __name__ == "__main__":
    # collect options
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url_list", type=str, default='urls',   help="A file of new line separated list of urls to start scrapping from")
    parser.add_argument("-d", "--depth",    type=int, default=50,       help="Max depth of url following")
    parser.add_argument("-m", "--max",      type=int, default=10000,    help="Max number of urls scrapped")
    parser.add_argument("-t", "--test",  action="store_true",           help="run test on offline artifitial graph")
    parser.add_argument("-D", "--domainonly",   action="store_true",    help="Restrict scrapping to initial domain only")
    args = parser.parse_args()

    # get initial urls
    urls = initial_urls(args.url_list)
    # prepare parser
    url_and_email_parser = Parser()
    urlregex = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    emailregex = '([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)'
    

    url_and_email_parser.add_target('url', urlregex)
    url_and_email_parser.add_target('email', emailregex)
    
    test_graph = {}

    exitFlag = 0
    if args.test or True:
        test_graph, test_urls, test_emails = generate_test_graph()
        s = Scrapper(url_and_email_parser, test_graph)
        first_test_url = test_graph[list(test_graph)[0]].url 
        s.set_urls([first_test_url])
    else:
        s = Scrapper(url_and_email_parser)
        s.set_urls(urls)
    
    # start the scrapper
    s.start()
