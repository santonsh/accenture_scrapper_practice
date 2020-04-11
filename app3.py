from parsers import extract_emails, extract_urls, normalize_urls
import time
import json
import argparse
from multiprocessing import Lock, Queue, Process, Value, Manager
import urllib
import queue
import re
from urllib.error import URLError
import logging
from logging.handlers import QueueHandler, QueueListener

def append_to_file(fname, content):
    """simple wrapper to append lines to file"""
    with open(fname, 'a+') as f:
        for l in content:
            f.write(l)

def email_storer(p_id, exitFlag, logger, emailStoreQueue, emailDict):
    logger.start('p_id: '+p_id+' started')

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

    logger.info('p_id: '+p_id+' has finished')
    return True


def job_enqueuer(p_id, exitFlag, logger, urlResultQueue, jobQueue, jobQueueSize, urlDict):
    logger.info('p_id: '+p_id+' started')
    process_exitFlag = False
    fail_cnt = 0

    while exitFlag.value == 0 and process_exitFlag == False:
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
                    jobQueue.put_nowait(u)
                    actual_urls_queued[u] = 1
                    with jobQueueSize.get_lock():
                        jobQueueSize.value += 1

                except queue.Full:
                    break

            # update the queued url dict/set
            urlDict.update(actual_urls_queued)
            # save urls on disk as well
            append_to_file('urls.out', actual_urls_queued)


        if fail_cnt > 5:
            process_exitFlag = True

        time.sleep(sleep_duration)

    logger.info('p_id: '+p_id+' has finished')
    return True


def page_downloader_and_parser(p_id, exitFlag, logger, jobQueue, jobQueueSize, urlResultQueue, emailStoreQueue, urlDict, emailDict, locality):
    logger.info('p_id: '+p_id+' started')
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
                [base_url, page] = get_html_page(logger, url)
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
            #logger.debug('p_id {} found {} email and {} urls of which queued {}'.format(p_id, len(new_emails), len(new_urls), len(urls_queued)))

        # 5 misses in a row will cause worker to return
        if fail_cnt > 5:
            process_exitFlag = True

        time.sleep(sleep_duration)

    logger.info('p_id: '+p_id+' has finished')
    return True


def get_html_page(logger, url):
    # this function is responsible for downloading a single html page
    req = urllib.request.Request(url, logger)
    page = ''
    real_url = ''
    try:
        response = urllib.request.urlopen(req)
    except URLError as e:
        if hasattr(e, 'reason'):
            logger.warning('We failed to reach a server.')
            logger.warning('Reason: ', e.reason)
            logger.debug(url)
            pass
        elif hasattr(e, 'code'):
            logger.warning('The server couldn\'t fulfill the request.')
            logger.warning('Error code: ', e.code)
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


def initial_urls(urls_file):
    urls = []
    urls_file = 'urls'
    with open(urls_file, 'r') as uf:
        for l in uf:
            urls.append(l)
    return urls


def watch_dog(t_id, exitFlag, logger, taskQueueSize, emailDict, urlDict, max_emails_harvested=None, max_urls_visited=None):
    """the watchdog should send an exit signal when it detects no progress or an algorithm meats a finish criteria """

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
        logger.info('Stats: harvested urls: {}, harvested emails: {}, visited urls {}, tasks left in queue: {}, processing time {:5.1f} seconds, avg speed {:3.2f} urls/sec'.format(
            url_cnt, email_cnt, visited_cnt, taskQueueSize.value, processing_t, (visited_cnt)/processing_t))

        max_emails_creteria = (max_emails_harvested != None) and (
            max_emails_harvested < email_cnt)
        max_urls_creteria = (max_urls_visited != None) and (
            max_urls_visited < url_cnt)

        if max_emails_creteria or max_urls_creteria:
            exitFlag.value = 1
            logger.info('Finish criteria cheached. Exiting...')
            break

        if hang_count > 5:
            exitFlag.value = 1
            logger.info('Looks like the algorithm has hanged. Exiting...')
            break

        time.sleep(5)
    return True


class Scrapper():
    def __init__(self, logger, processes_number=2, test_graph={}, locality='any'):
        self.processes_number = processes_number
        self.urls = []
        self.start_time = ''
        self.processes = []
        self.test_graph = test_graph
        self.locality = locality
        self.exitFlag = Value("i", 0)
        self.logger = logger
        # define queues
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
        logger = self.logger
        logger.info('Starting scrapping...')
        logger.info('Filling the download queue with initial urls...')
        for u in self.urls:
            self.taskQueue.put(u)
            with self.taskQueueSize.get_lock():
                self.taskQueueSize.value += 1

        # start all threads
        # first the watch dog
        p = Process(target=watch_dog, args=(
            'watch_dog_0', self.exitFlag, logger, self.taskQueueSize, self.collected_emails_dict, self.collected_urls_dict))
        self.processes.append(p)
        p.start()

        # second the enquer
        p = Process(target=job_enqueuer, args=(
            'job_enqueuer_0', self.exitFlag, logger, self.urlResultQueue, self.taskQueue, self.taskQueueSize, self.collected_urls_dict))

        self.processes.append(p)
        p.start()

        # third the email storer
        p = Process(target=email_storer, args=(
        'email_storer_0', self.exitFlag, self.emailStoreQueue, self.collected_emails_dict))

        self.processes.append(p)
        p.start()

        # then the downloaders
        for i in range(self.processes_number):
            p_id = 'process_'+str(i)
            p = Process(target=page_downloader_and_parser, args=(
                p_id, self.exitFlag, self.taskQueue, self.logger, self.taskQueueSize, self.urlResultQueue, self.emailStoreQueue, self.collected_urls_dict, self.collected_emails_dict, self.locality))
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

    # Configure loggers
    log_queue = Queue(-1)
    queue_handler = QueueHandler(log_queue)
    logger = logging.getLogger()
    logger.addHandler(queue_handler)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(relativeCreated)d: %(process)d: %(levelname)s: %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    file_handler = logging.FileHandler("scrapper.log")
    file_handler.setFormatter(formatter)

    listener = QueueListener(log_queue, console_handler, file_handler)
    listener.start()

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
        s = Scrapper(logger, processes_number=8, locality='subdomain')
        s.set_urls(urls)

    # start the scrapper
    s.start()
    # stop the logger queue listener
    listener.stop()