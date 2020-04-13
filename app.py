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
    formatter = logging.Formatter(
        '%(relativeCreated)d: %(levelname)s: %(message)s')
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
            try:
                f.write(l+'\n')
            except:
                pass


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
        # time.sleep(1)
        page = test_graph[url].get_page()
    except:
        logger.debug('Failed to find a page in a test graph')
        page = ''
    return real_url, page


# --------------------------------------------------------------------------
# Workers section
# --------------------------------------------------------------------------

def watch_dog(p_id, shared_resources, config):
    """the watchdog should send an exit signal when it detects no progress or an algorithm meats a finish criteria
    also it is used to monitor and track overall algorithm progress """

    max_emails_harvested = config['max_emails_collected']
    max_urls_visited = config['max_urls_visited']
    hang_count = 0
    email_cnt = 0
    url_cnt = 0
    job_cnt = 0
    start_t = time.time()
    logger = get_queue_logger(shared_resources['loggerQueue'])
    exitFlag = shared_resources['exitFlag']
    while exitFlag.value == 0:

        prev_email_cnt = email_cnt
        email_cnt = len(shared_resources['collected_emails_dict'])

        prev_url_cnt = url_cnt
        url_cnt = len(shared_resources['collected_urls_dict'])

        prev_job_cnt = job_cnt
        job_cnt = shared_resources['taskQueueSize'].value

        # check if any update from previous iteration
        if (url_cnt == prev_url_cnt) and (email_cnt == prev_email_cnt) and (job_cnt == prev_job_cnt):
            hang_count += 1

        processing_t = time.time()-start_t
        visited_cnt = url_cnt-job_cnt
        logger.info('Stats: collected urls: {}, collected emails: {}, visited urls {}, tasks left in queue: {}, processing time {:5.1f} seconds, avg speed {:3.2f} urls/sec'.format(
            url_cnt, email_cnt, visited_cnt, job_cnt, processing_t, (visited_cnt)/processing_t))

        # check finish criteria
        max_emails_creteria = (max_emails_harvested != None) and (
            max_emails_harvested < email_cnt)
        max_urls_creteria = (max_urls_visited != None) and (
            max_urls_visited < visited_cnt)

        if max_emails_creteria:
            exitFlag.value = 1
            logger.info(
                'Finish criteria reached [max emails collected]. Exiting...')
            break

        if max_urls_creteria:
            exitFlag.value = 1
            logger.info(
                'Finish criteria reached [max urls visited]. Exiting...')
            break

        if hang_count > 5:
            exitFlag.value = 1
            logger.info('Looks like the algorithm has hanged. Exiting...')
            break

        time.sleep(2)
    print('watch dog exiting')
    return True


def email_handler(p_id, shared_resources, config):
    """Used to store scrapped items in this case emails"""
    logger = get_queue_logger(shared_resources['loggerQueue'])
    logger.debug('p_id: '+p_id+' started')

    while shared_resources['exitFlag'].value == 0:
        sleep_duration = 0
        try:
            new_emails = set(
                shared_resources['emailStoreQueue'].get(timeout=3))
        except queue.Empty:
            # wait when nothing to process
            sleep_duration = 2
        else:

            # find emails that has to be stored
            previously_found_emails = set(
                shared_resources['collected_emails_dict'].keys())
            emailsToStore = set(new_emails) - set(previously_found_emails)

            new_email_dict = {}
            for e in emailsToStore:
                new_email_dict[e] = 1
            shared_resources['collected_emails_dict'].update(new_email_dict)
            # save on disk as well
            append_to_file('emails.collected', emailsToStore)

        time.sleep(sleep_duration)
    print('email handler exiting')
    logger.info('p_id: '+p_id+' has finished')
    return True


def url_handler(p_id, shared_resources, config):
    """Used to enque new jobs based of new enqueued urls returned by processors"""
    logger = get_queue_logger(shared_resources['loggerQueue'])
    logger.debug('p_id: '+p_id+' started')
    url_count = 0

    while shared_resources['exitFlag'].value == 0:
        sleep_duration = 0
        try:
            new_urls = set(shared_resources['urlResultQueue'].get(timeout=3))
        except queue.Empty:
            # wait when nothing to process
            sleep_duration = 2
        else:

            # find urls that has to be queued
            previously_queued_urls = set(
                shared_resources['collected_urls_dict'].keys())
            urlsToQueue = set(new_urls) - set(previously_queued_urls)

            actual_urls_queued = {}
            for u in urlsToQueue:
                try:
                    max_urls_creteria = (config['max_urls_visited'] != None) and (
                        config['max_urls_visited'] < url_count)
                    if not max_urls_creteria:
                        shared_resources['taskQueue'].put_nowait(u)
                        actual_urls_queued[u] = 1
                        with shared_resources['taskQueueSize'].get_lock():
                            shared_resources['taskQueueSize'].value += 1
                        url_count += 1

                except queue.Full:
                    break

            # update the queued url dict/set
            shared_resources['collected_urls_dict'].update(actual_urls_queued)
            # save urls on disk as well
            append_to_file('urls.collected', actual_urls_queued)

        time.sleep(sleep_duration)
    print('url handler exiting')
    logger.info('p_id: '+p_id+' has finished')
    return True


def page_downloader_and_parser(p_id, shared_resources, config):
    """Used to process urls from a jobQueue - download pages, parse them and filter the results"""
    logger = get_queue_logger(shared_resources['loggerQueue'])
    logger.debug('p_id: '+p_id+' started')
    process_exitFlag = False
    fail_cnt = 0

    while shared_resources['exitFlag'].value == 0 and process_exitFlag == False:

        sleep_duration = 0

        try:
            url = shared_resources['taskQueue'].get_nowait()
            fail_cnt = 0
            with shared_resources['taskQueueSize'].get_lock():
                shared_resources['taskQueueSize'].value -= 1
            append_to_file('urls.visited', [url])
        except queue.Empty:
            # wait when nothing to process
            fail_cnt += 1
            sleep_duration = 2
        else:

            # update processed url set
            shared_resources['collected_urls_dict'][url] = 1

            # download page
            try:
                [base_url, page] = download_html_page(logger, url) if (
                    config['test_graph'] == {}) else download_html_page_simulated(logger, config['test_graph'], url)
                if page == '':
                    # bad try. Continue to the next
                    logger.warning(
                        'Failed to get a page from a source: '+str(url))
                    continue
            except:
                # fetch error. Continue to the next
                logger.warning('Failed to get a page from a source: '+str(url))
                continue

            # extract data
            new_urls = extract_urls(page, base_url, config['locality'])
            new_emails = extract_emails(page)

            # push urls further to queue new jobs
            try:
                shared_resources['urlResultQueue'].put_nowait(new_urls)
            except queue.Full:
                logger.warning('Extracted url queue is full')
                break

            # push emails further for storage
            try:
                shared_resources['emailStoreQueue'].put_nowait(new_emails)
            except queue.Full:
                logger.warning('Extracted email queue is full')
                break

            # small report
            logger.debug('p_id {} found {} email and {} urls'.format(
                p_id, len(new_emails), len(new_urls)))

        # 5 misses in a row will cause worker to return
        if fail_cnt > 5:
            process_exitFlag = True

        time.sleep(sleep_duration)

    print('worker exiting')
    logger.info('p_id: '+p_id+' has finished')
    return True

# --------------------------------------------------------------------------
# Scrapper class
# --------------------------------------------------------------------------


class Scrapper():
    def __init__(self, loggerQueue, config):
        self.config = config
        self.processes_number = config.get('processes_number', 1)
        self.start_time = ''
        self.test_graph = config.get('test_graph', {})
        self.locality = config.get('locality', 'domain')
        self.max_urls = config.get('max_urls_visited', None)
        self.max_emails = config.get('max_emails_collecteed', None)
        self.processes = []
        self.initial_urls = []

        # define shared resources
        # queues are for job piping between processes
        # dicts are acting as sets for checking already processed data)
        sr_manager = Manager()
        self.shared_resources = {
            'exitFlag':             Value("i", 0),
            'loggerQueue':          loggerQueue,
            'taskQueue':            Queue(),
            'taskQueueSize':        Value("i", 0),
            'urlResultQueue':       Queue(),
            'emailStoreQueue':      Queue(),
            'collected_urls_dict':  sr_manager.dict(),
            'collected_emails_dict': sr_manager.dict()
        }

    def set_urls(self, urls):
        self.initial_urls = urls

    def start(self):
        self.start_time = time.time()
        logger = get_queue_logger(self.shared_resources['loggerQueue'])
        logger.info('Starting scrapping from {} urls with {} workers and {} locality'.format(
            len(self.initial_urls), self.processes_number, self.locality))

        logger.info('Filling the download queue with initial urls')
        for u in self.initial_urls:
            self.shared_resources['taskQueue'].put(u)
            append_to_file('urls.collected', [u])
            with self.shared_resources['taskQueueSize'].get_lock():
                self.shared_resources['taskQueueSize'].value += 1

        # start all threads
        # first the watch dog
        p = Process(target=watch_dog, args=(
            'watch_dog_0', self.shared_resources, self.config))

        self.processes.append(p)
        p.start()

        # second the enquer
        p = Process(target=url_handler, args=(
            'url_handler_0', self.shared_resources, self.config))

        self.processes.append(p)
        p.start()

        # third the email storer
        p = Process(target=email_handler, args=(
            'email_handler_0', self.shared_resources, self.config))

        self.processes.append(p)
        p.start()

        # then the downloaders
        for i in range(self.processes_number):
            p_id = 'process_'+str(i)
            p = Process(target=page_downloader_and_parser, args=(
                p_id, self.shared_resources, self.config))
            self.processes.append(p)
            p.start()

        while self.shared_resources['exitFlag'].value == 0:
            logger.debug('Main process is waiting')
            time.sleep(3)

        logger.debug('Joining the processes...')
        for p in self.processes:
            print(p)
            p.terminate()
            # p.join()

        logger.info("The scrapping is finished")
        logger.info("The scrapping took %s seconds" %
                    (time.time() - self.start_time))
        return True

# --------------------------------------------------------------------------
# Test section
# --------------------------------------------------------------------------


class TestWholeScrapper(unittest.TestCase):

    def test_traversan_on_test_graph(self):

        log_queue = Queue(-1)
        listener = createQueueListener(log_queue)
        listener.start()
        test_graph, test_urls, test_emails = generate_test_graph(
            sameDomain=False)
        initial_urls = [test_graph[list(test_graph)[0]].url]

        config = {
            'processes_number':       1,
            'locality':               'any',
            'max_urls_visited':       None,
            'max_emails_collected':   None,
            'test_graph':             test_graph}
        s = Scrapper(log_queue, config)
        s.set_urls(initial_urls)
        s.start()

        # read resulting urls and emails
        found_urls = read_initial_urls('urls.collected')
        found_emails = read_initial_urls('emails.collected')

        # remove new lines
        found_urls = [u[:-1] for u in found_urls]
        found_emails = [e[:-1] for e in found_emails]
        print('found urls:'+str(len(found_urls)) +
              ' of them unique: '+str(len(set(found_urls))))
        print('generated '+str(len(test_urls))+' test urls' +
              ' of them unique: '+str(len(set(test_urls))))
        print('found emails:'+str(len(found_emails)) +
              ' of them unique: '+str(len(set(found_emails))))
        print('generated '+str(len(test_emails))+' test emails' +
              ' of them unique: '+str(len(set(test_emails))))
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
    parser.add_argument("-l", "--locality", type=str, choices=['any', 'domain', 'subdomain'], default='subdomain',
                        help="A locality of scrapper search")

    parser.add_argument("-v", "--max_urls_visited",    type=int,
                        default=None,       help="Max nummber of urls visited")
    parser.add_argument("-e", "--max_emails_collected",      type=int,
                        default=None,    help="Max number of urls scrapped")
    parser.add_argument(
        "-T", "--test_mode", help="run test on offline artifitial graph", action="store_true")
    parser.add_argument("-w", "--workers",    type=int,
                        default=4,       help="number of scrapper workers to spawn. Improves scrapping speed")
    args = parser.parse_args()

    # remove old files
    clean_log_files(['urls.visited', 'urls.collected',
                     'emails.collected', 'scrapper.log'])

    # Configure loggers
    # configure queue logger listener part
    log_queue = Queue(-1)
    listener = createQueueListener(log_queue)
    listener.start()

    # get initial urls
    initial_urls = read_initial_urls(args.url_list)
    test_graph = {}

    # run test case
    if args.test_mode:
        unittest.main()

    # initiate and start the scrapper
    config = {'processes_number':       args.workers,
              'locality':               args.locality,
              'max_urls_visited':       args.max_urls_visited,
              'max_emails_collected':   args.max_emails_collected,
              'test_graph':             test_graph}
    s = Scrapper(log_queue, config=config)
    s.set_urls(initial_urls)
    s.start()

    # stop the logger queue listener after the scrapper stops
    listener.stop()
