from parsers import extract_emails, extract_urls, normalize_urls
import time
import json
import argparse
from multiprocessing import Lock, Queue, Process, Value, Manager
import urllib
import queue
import re
from urllib.error import URLError


class sharedSet():
    # this class unifies lock and set in one instance
    def __init__(self):
        self.set = set()
        self.Lock = Lock()

    def add(self, el):
        self.set.add(el)

    def update(self, l):
        self.set.update(l)

    def acquire(self):
        self.Lock.acquire()

    def release(self):
        self.Lock.release()

    def len(self):
        return len(self.set)


def download_and_parse_job(p_id, exitFlag, jobQueue, jobQueueSize, urlDict, emailDict, locality):
    print('p_id: '+p_id+' started')
    process_exitFlag = False
    fail_cnt = 0

    while exitFlag.value == 0 and process_exitFlag == False:
        # plan of action:
        # - look for a job. If no job is available go to sleep
        # - download and get urls and emails if nothing returned loop to beginning
        # - save emails to set
        # - enqueue new urls to queue
        sleep_duration = 0

        try:
            url = jobQueue.get_nowait()
            fail_cnt = 0
            with jobQueueSize.get_lock():
                jobQueueSize.value -= 1
        except queue.Empty:
            # wait when nothing to process
            fail_cnt += 1
            sleep_duration = 2
        else:

            # update processed url set
            urlDict[url] = 1
            # print(len(urlDict))

            # download page
            try:
                [base_url, page] = get_html_page(url)
                if page == '':
                    # bad try. Continue to the next
                    continue
            except:
                # fetch error. Continue to the next
                continue

            # extract data
            new_urls = extract_urls(page, base_url, locality)
            new_emails = {}
            for e in extract_emails(page):
                new_emails[e] = 1

            emailDict.update(new_emails)

            # find urls that has to be queued
            previously_queued_urls = urlDict.keys()
            urlsToQueue = set(new_urls) - set(previously_queued_urls)

            urls_queued = {}
            for u in urlsToQueue:
                try:
                    jobQueue.put_nowait(u)
                    fail_cnt = 0
                    urls_queued[u] = 1
                    with jobQueueSize.get_lock():
                        jobQueueSize.value += 1

                except queue.Full:
                    fail_cnt += 1
                    sleep_duration = 3
                    break

            # update the queued url dict/set
            urlDict.update(urls_queued)

            # small report
            # print('p_id {} found {} email and {} urls of which queued {}'.format(
            #    p_id, len(new_emails), len(new_urls), len(urls_queued)))

        # 5 misses in a row will cause worker to return
        if fail_cnt > 5:
            process_exitFlag = True

        time.sleep(sleep_duration)

    print('p_id: '+p_id+' has finished')
    return True


def get_html_page(url):
    # this function is responsible for downloading a single html page
    req = urllib.request.Request(url)
    page = ''
    real_url = ''
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
        data_type = re.findall(
            '(text/html).*', response.info()['Content-Type'])[0]
        if data_type == 'text/html':
            page = response.read().decode('utf-8')
            # print(page)
    except:
        print('failed to parse a page, probably not an html document')
        raise
    return real_url, page


def initial_urls(urls_file):
    urls = []
    urls_file = 'urls'
    with open(urls_file, 'r') as uf:
        for l in uf:
            urls.append(l)
    return urls


def watch_dog(t_id, exitFlag, taskQueueSize, emailDict, urlDict, max_emails_harvested=None, max_urls_visited=None):
    """the watchdog should send an exit signal when it deetects no progress or an algorithm meats a finish criteria """

    hang_count = 0
    email_cnt = 0
    url_cnt = 0
    job_cnt = 0
    start_t = time.time()
    while exitFlag.value == 0:

        p_email_cnt = email_cnt
        email_cnt = len(emailDict)

        p_url_cnt = url_cnt
        url_cnt = len(urlDict)

        p_job_cnt = job_cnt
        job_cnt = taskQueueSize.value

        if (url_cnt == p_url_cnt) and (email_cnt == p_email_cnt) and (job_cnt == p_job_cnt):
            hang_count += 1
        processing_t = time.time()-start_t
        visited_cnt = url_cnt-taskQueueSize.value
        print('Analizer: harvested urls: {}, harvested emails: {}, visited urls {}, tasks left in queue: {}, processing time {:5.1f} seconds, avg speed {:3.2f} urls/sec'.format(
            url_cnt, email_cnt, visited_cnt, taskQueueSize.value, processing_t, (visited_cnt)/processing_t))

        max_emails_creteria = (max_emails_harvested != None) and (
            max_emails_harvested < email_cnt)
        max_urls_creteria = (max_urls_visited != None) and (
            max_urls_visited < url_cnt)

        if max_emails_creteria or max_urls_creteria:
            exitFlag.value = 1
            print('Analizer: Finish criteria cheached. Exiting...')
            break

        if hang_count > 5:
            exitFlag.value = 1
            print('Analizer: Looks like the algorithm has hanged. Exiting...')
            break

        time.sleep(5)
    return True


class Scrapper():
    def __init__(self, processes_number=2, test_graph={}, locality='any'):
        self.processes_number = processes_number
        self.urls = []
        self.start_time = ''
        self.processes = []
        self.test_graph = test_graph
        self.locality = locality
        self.exitFlag = Value("i", 0)
        # define queues
        self.taskQueue = Queue()
        self.taskQueueSize = Value("i", 0)
        # define shared dicts (acting like sets really)
        self.manager = Manager()
        self.collected_urls_dict = self.manager.dict()
        self.collected_emails_dict = self.manager.dict()

    def set_urls(self, urls):
        self.urls = urls

    def start(self):
        self.start_time = time.time()
        print('start scrapping')
        print('filling the download queue with starting urls')

        for u in self.urls:
            self.taskQueue.put(u)
            with self.taskQueueSize.get_lock():
                self.taskQueueSize.value += 1

        # start all threads
        # first the watch dog
        p = Process(target=watch_dog, args=(
            'watch_dog_0', self.exitFlag, self.taskQueueSize, self.collected_emails_dict, self.collected_urls_dict))
        self.processes.append(p)
        p.start()

        # then the downloaders
        for i in range(self.processes_number):
            p_id = 'process_'+str(i)
            p = Process(target=download_and_parse_job, args=(
                p_id, self.exitFlag, self.taskQueue, self.taskQueueSize, self.collected_urls_dict, self.collected_emails_dict, self.locality))
            self.processes.append(p)
            p.start()

        while self.exitFlag.value == 0:
            #print('Main process is waiting')
            time.sleep(5)

        print('Main process: joining the processes')
        # print(self.collected_urls_dict)
        # print(self.collected_emails_dict)
        for p in self.processes:
            p.join()

        print("--- The scrapping took %s seconds ---" %
              (time.time() - self.start_time))
        return True


if __name__ == '__main__':

    # collect options
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url_list", type=str, default='urls',
                        help="A file of new line separated list of urls to start scrapping from")
    parser.add_argument("-d", "--depth",    type=int,
                        default=50,       help="Max depth of url following")
    parser.add_argument("-m", "--max",      type=int,
                        default=10000,    help="Max number of urls scrapped")
    parser.add_argument("-t", "--test",  action="store_true",
                        help="run test on offline artifitial graph")
    parser.add_argument("-D", "--domainonly",   action="store_true",
                        help="Restrict scrapping to initial domain only")
    args = parser.parse_args()

    # get initial urls
    urls = initial_urls(args.url_list)

    exitFlag = 0
    if args.test:
        pass
        # test_graph, test_urls, test_emails = generate_test_graph()
        # s = Scrapper(test_graph)
        # first_test_url = test_graph[list(test_graph)[0]].url
        # s.set_urls([first_test_url])
    else:
        s = Scrapper(processes_number=8, locality='domain')
        s.set_urls(urls)

    # start the scrapper
    s.start()
