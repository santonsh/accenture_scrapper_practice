from parsers import extract_emails, extract_urls, normalize_urls
import time
import json
import argparse
from multiprocessing import Lock, Queue, Process, Value, Manager
import urllib
import queue
import re
import os
from urllib.error import URLError
import logging
from logging.handlers import QueueHandler, QueueListener
from testlib import generate_test_graph
import unittest

# --------------------------------------------------------------------------
# Auxiliary section
# --------------------------------------------------------------------------
def clean_log_files(files):
        """simple function to remove old log files"""
        for f in files:
            try:
                os.remove(f)
            except:
                pass

def createQueueListener(log_queue):
    """simple function that creates log queue handler/listener """
    formatter = logging.Formatter('%(relativeCreated)d: %(levelname)s: %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    file_handler = logging.FileHandler("scrapper.log")
    file_handler.setFormatter(formatter)
    listener = QueueListener(log_queue, console_handler, file_handler)

    return listener

def get_queue_logger(loggerQueue):
        """this simply returns queue logger based on the provided shared queue. 
        This is needed for multiprocessing logging. The function should be called from each worker to get
         a multiprocessing logger"""
        queue_handler = QueueHandler(loggerQueue)
        logger = logging.getLogger()
        logger.addHandler(queue_handler)
        logger.setLevel(logging.INFO)
        return logger

def append_to_file(fname, content):
    """simple wrapper to append lines to file. Used to store scrapped items and visited urls"""
    with open(fname, 'a+') as f:
        for l in content:
            f.write(l+'\n')

def read_initial_urls(urls_file):
    """simple function to return list of newline-separated initial urls written in urls_file"""
    try:
        with open(urls_file, 'r') as uf:
            urls = [l for l in uf]
        return urls
    except:
        return []

def download_html_page(logger, url):
    """ this function is responsible for downloading a single html page"""
    req = urllib.request.Request(url)
    page = ''
    real_url = ''
    try:
        response = urllib.request.urlopen(req)
    except URLError as e:
        if hasattr(e, 'reason'):
            logger.warning('The server couldn\'t fulfill the request.')
            logger.warning('Reason: '+str(e.reason))
            logger.debug(url)
            pass
        elif hasattr(e, 'code'):
            logger.warning('The server couldn\'t fulfill the request.')
            logger.warning('Error code: '+str(e.code))
            logger.debug(url)
            pass
        raise
    
    # download successful
    real_url = response.geturl()
    try:
        data_type = re.findall(
            '(text/html).*', response.info()['Content-Type'])[0]
        if data_type == 'text/html':
            page = response.read().decode('utf-8')
    except:
        logger.warning('failed to parse a page, probably not an html document')
        raise
    return real_url, page

def download_html_page_simulated(logger, test_graph, url):
    """this function is needed for test purpuses. It returns a test html page that 
       is stored in a test dictionary modeling an internet document network"""
    logger.debug('Returning test page for url: ' + url)
    real_url = url
    try:
        #time.sleep(1)
        page = test_graph[url].get_page()
    except:
        logger.debug('Failed to find a page in a test graph')
        page = ''
    return real_url, page



# --------------------------------------------------------------------------
# Workers section
# --------------------------------------------------------------------------

def watch_dog(t_id, exitFlag, loggerQueue, taskQueueSize, emailDict, urlDict, max_emails_harvested=None, max_urls_visited=None):
    """the watchdog should send an exit signal when it detects no progress or an algorithm meats a finish criteria
    also it is used to monitor and track overall algorithm progress """

    hang_count = 0
    email_cnt = 0
    url_cnt = 0
    job_cnt = 0
    start_t = time.time()
    logger = get_queue_logger(loggerQueue)
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
        logger.info('Stats: harvested urls: {}, harvested emails: {}, visited urls {}, tasks left in queue: {}, processing time {:5.1f} seconds, avg speed {:3.2f} urls/sec'.format(
            url_cnt, email_cnt, visited_cnt, taskQueueSize.value, processing_t, (visited_cnt)/processing_t))

        max_emails_creteria = (max_emails_harvested != None) and (
            max_emails_harvested < email_cnt)
        max_urls_creteria = (max_urls_visited != None) and (
            max_urls_visited < url_cnt)

        if max_emails_creteria:
            exitFlag.value = 1
            logger.info('Finish criteria reached [max emails]. Exiting...')
            break

        if max_urls_creteria:
            exitFlag.value = 1
            logger.info('Finish criteria reached [max urls]. Exiting...')
            break

        if hang_count > 5:
            exitFlag.value = 1
            logger.info('Looks like the algorithm has hanged. Exiting...')
            break

        time.sleep(5)
    return True

def email_storer(p_id, exitFlag, loggerQueue, emailStoreQueue, emailDict):
    """Used to store scrapped items in this case emails"""
    logger = get_queue_logger(loggerQueue)
    logger.debug('p_id: '+p_id+' started')

    while exitFlag.value == 0:
        sleep_duration = 0
        try:
            new_emails = set(emailStoreQueue.get(timeout=3))
        except queue.Empty:
            # wait when nothing to process
            sleep_duration = 2
        else:
            
            # find emails that has to be stored
            previously_found_emails = set(emailDict.keys())
            emailsToStore = set(new_emails) - set(previously_found_emails)

            new_email_dict = {}
            for e in emailsToStore:
                new_email_dict[e] = 1
            emailDict.update(new_email_dict)
            # save on disk as well
            append_to_file('emails.out', emailsToStore)

        time.sleep(sleep_duration)

    logger.debug('p_id: '+p_id+' has finished')
    return True


def job_enqueuer(p_id, exitFlag, loggerQueue, urlResultQueue, jobQueue, jobQueueSize, urlDict, max_urls_visited = None):
    """Used to enque new jobs based of new enqueued urls returned by processors"""
    logger = get_queue_logger(loggerQueue)
    logger.debug('p_id: '+p_id+' started')
    url_count = 0

    while exitFlag.value == 0:
        sleep_duration = 0
        try:
            new_urls = set(urlResultQueue.get(timeout=3))
        except queue.Empty:
            # wait when nothing to process
            sleep_duration = 2
        else:

            # find urls that has to be queued
            previously_queued_urls = set(urlDict.keys())
            urlsToQueue = set(new_urls) - set(previously_queued_urls)

            actual_urls_queued = {}
            for u in urlsToQueue:
                try:
                    if (max_urls_visited==None) or (max_urls_visited<url_count):
                        jobQueue.put_nowait(u)
                        actual_urls_queued[u] = 1
                        with jobQueueSize.get_lock():
                            jobQueueSize.value += 1
                        url_count+=1

                except queue.Full:
                    break

            # update the queued url dict/set
            urlDict.update(actual_urls_queued)
            # save urls on disk as well
            append_to_file('urls.out', actual_urls_queued)

        time.sleep(sleep_duration)

    logger.debug('p_id: '+p_id+' has finished')
    return True

def page_downloader_and_parser(p_id, exitFlag, loggerQueue, test_graph, jobQueue, jobQueueSize, urlResultQueue, emailStoreQueue, urlDict, emailDict, locality):
    """Used to process urls from a jobQueue - download pages, parse them and filter the results"""
    logger = get_queue_logger(loggerQueue)
    logger.debug('p_id: '+p_id+' started')
    process_exitFlag = False
    fail_cnt = 0

    while exitFlag.value == 0 and process_exitFlag == False:

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

            # download page    
            try:
                [base_url, page] = download_html_page(logger, url) if (test_graph == {}) else download_html_page_simulated(logger, test_graph, url)
                if page == '':
                    # bad try. Continue to the next
                    logger.warning('Failed to get a page from a source: '+str(url))
                    continue
            except:
                # fetch error. Continue to the next
                logger.warning('Failed to get a page from a source: '+str(url))
                continue

            # extract data
            new_urls = extract_urls(page, base_url, locality)
            new_emails = extract_emails(page)

            # push urls further to queue new jobs
            try:
                urlResultQueue.put_nowait(new_urls)
            except queue.Full:
                logger.warning('Extracted url queue is full')
                break
            
            # push emails further for storage
            try:
                emailStoreQueue.put_nowait(new_emails)
            except queue.Full:
                logger.warning('Extracted email queue is full')
                break

            # small report
            logger.debug('p_id {} found {} email and {} urls'.format(p_id, len(new_emails), len(new_urls)))

        # 5 misses in a row will cause worker to return
        if fail_cnt > 5:
            process_exitFlag = True

        time.sleep(sleep_duration)

    logger.debug('p_id: '+p_id+' has finished')
    return True

# --------------------------------------------------------------------------
# Scrapper class
# --------------------------------------------------------------------------

class Scrapper():
    def __init__(self, loggerQueue, processes_number=2, test_graph={}, locality='any', max_urls = None):
        self.processes_number = processes_number
        self.urls = []
        self.start_time = ''
        self.processes = []
        self.test_graph = test_graph
        self.locality = locality
        self.max_urls = max_urls
        self.exitFlag = Value("i", 0)
        # define queues
        self.loggerQueue = loggerQueue
        self.taskQueue = Queue()
        self.taskQueueSize = Value("i", 0)
        self.urlResultQueue = Queue()
        self.emailStoreQueue = Queue()
        # define shared dicts (acting like sets really)
        self.manager = Manager()
        self.collected_urls_dict = self.manager.dict()
        self.collected_emails_dict = self.manager.dict()

    def set_urls(self, urls):
        self.urls = urls

    def start(self):
        self.start_time = time.time()
        logger = get_queue_logger(self.loggerQueue)
        logger.info('Starting scrapping')
        logger.info('Filling the download queue with initial urls')
        for u in self.urls:
            self.taskQueue.put(u)
            append_to_file('urls.out', [u])
            with self.taskQueueSize.get_lock():
                self.taskQueueSize.value += 1

        # start all threads
        # first the watch dog
        p = Process(target=watch_dog, args=(
            'watch_dog_0', self.exitFlag, self.loggerQueue, self.taskQueueSize, self.collected_emails_dict, self.collected_urls_dict, self.max_urls, None))
        self.processes.append(p)
        p.start()

        # second the enquer
        p = Process(target=job_enqueuer, args=(
            'job_enqueuer_0', self.exitFlag, self.loggerQueue, self.urlResultQueue, self.taskQueue, self.taskQueueSize, self.collected_urls_dict))

        self.processes.append(p)
        p.start()

        # third the email storer
        p = Process(target=email_storer, args=(
        'email_storer_0', self.exitFlag, self.loggerQueue, self.emailStoreQueue, self.collected_emails_dict))

        self.processes.append(p)
        p.start()

        # then the downloaders
        for i in range(self.processes_number):
            p_id = 'process_'+str(i)
            p = Process(target=page_downloader_and_parser , args=(
                p_id, self.exitFlag, self.loggerQueue, self.test_graph, self.taskQueue, self.taskQueueSize, self.urlResultQueue, self.emailStoreQueue, self.collected_urls_dict, self.collected_emails_dict, self.locality))
            self.processes.append(p)
            p.start()

        while self.exitFlag.value == 0:
            logger.debug('Main process is waiting')
            time.sleep(5)

        logger.debug('Joining the processes...')
        for p in self.processes:
            p.join()

        logger.info("The scrapping is finished")
        logger.info("The scrapping took %s seconds" %(time.time() - self.start_time))
        return True

# --------------------------------------------------------------------------
# Test section
# --------------------------------------------------------------------------

class TestWholeScrapper(unittest.TestCase):

    def test_traversan_on_test_graph(self):

        log_queue = Queue(-1)
        listener = createQueueListener(log_queue)    
        listener.start()
        test_graph, test_urls, test_emails = generate_test_graph(sameDomain = False)
        initial_urls = [test_graph[list(test_graph)[0]].url]

        s = Scrapper(log_queue, processes_number=4, locality='any', test_graph=test_graph, max_urls = 120 )
        s.set_urls(initial_urls)
        s.start()

        # read resulting urls and emails
        found_urls = read_initial_urls('urls.out')
        found_emails = read_initial_urls('emails.out')
        
        # remove new lines
        found_urls = [u[:-1] for u in found_urls]
        found_emails = [e[:-1] for e in found_emails]
        print('found urls:'+str(len(found_urls))+' of them unique: '+str(len(set(found_urls))))
        print('generated '+str(len(test_urls))+' test urls'+' of them unique: '+str(len(set(test_urls))))
        print('found emails:'+str(len(found_emails))+' of them unique: '+str(len(set(found_emails))))
        print('generated '+str(len(test_emails))+' test emails'+' of them unique: '+str(len(set(test_emails))))
        # lets check not found urls
        not_found_urls = set(test_urls)-set(found_urls)
        not_found_emails = set(test_emails)-set(found_emails)

        if len(not_found_urls)+len(not_found_emails) == 0:
            print('Test run completed successfully')
        else:
            print('Some urls or emails were not found. Test run failed')

        self.assertEqual(len(not_found_urls), 0)
        self.assertEqual(len(not_found_emails), 0)



# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

if __name__ == '__main__':

    # collect options
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url_list", type=str, default='urls',
                        help="A file of new line separated list of urls to start scrapping from")
    parser.add_argument("-d", "--depth",    type=int,
                        default=5,       help="Max depth of url following [not implemented in current version]")
    parser.add_argument("-m", "--max",      type=int,
                        default=None,    help="Max number of urls scrapped")
    parser.add_argument("-t", "--test",  action="store_true",
                        help="run test on offline artifitial graph")
    parser.add_argument("-w", "--workers",    type=int,
                        default=4,       help="number of scrapper workers to spawn. Improves scrapping speed")
    parser.add_argument("-D", "--domainonly",   action="store_true",
                        help="Restrict scrapping to initial domain only")
    parser.add_argument("-S", "--subdomainonly",   action="store_true",
                        help="Restrict scrapping to initial subdomain only. Most narrow way of scrapping")
    args = parser.parse_args()

    # remove old files
    clean_log_files(['urls.out', 'emails.out', 'scrapper.log'])

    # Configure loggers
    # configure queue logger listener part 
    log_queue = Queue(-1)
    listener = createQueueListener(log_queue)    
    listener.start()

    # get initial urls
    initial_urls = read_initial_urls(args.url_list)
    test_graph = {}
    
    if args.subdomainonly:
        locality = 'subdomainonly'
    elif args.domainonly:
        locality = 'domainonly'
    else: 
        locality = 'any'

    if args.test:
        unittest.main()

    # initiate and start the scrapper    
    s = Scrapper(log_queue, processes_number=args.workers, locality=locality, test_graph=test_graph, max_urls = args.max )
    s.set_urls(initial_urls)
    s.start()

    # stop the logger queue listener after the scrapper stops
    listener.stop()
