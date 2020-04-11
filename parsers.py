import tldextract
from urllib.parse import urlparse, urljoin
import re
from bs4 import BeautifulSoup
from urllib.request import urlopen

# -----------------------------------------------------------------------
# URLs and emails extractors
# -----------------------------------------------------------------------

def extract_pattern_from_text(pattern, content):
    """wrapper for simple re extraction or patterns"""
    if type(content) != type('') or type(pattern) != type(''):
        return []
    try:
        findings = re.findall(pattern, content)
        return findings
    except:
        return []


def extract_emails(content):
    """simple email extractor """
    pattern = '([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)'
    findings = extract_pattern_from_text(pattern, content) 
    #print(findings)
    return findings


def extract_urls(page, base_url, locality='any'):
    """ simple email extractor
    Resieves a page of type str that is later handled by beutiful soup
    For simplicity it is bounded to extract links from a tags only. It does
        - match relative and absolute urls
        - filter out links to common data files
        - turn all links to absolute

    Keyword arguments:
    page -- html page of type str
    base_url -- base url of page of type str
    locality -- locality of link extraction. can be any|domain|subdomian

    """
    # find a domain and a subdomain of page url
    ext = tldextract.extract(base_url)
    base_domain = ext.registered_domain
    base_subdomain = '.'.join(ext[1:3]) if ext[0] == '' else '.'.join(ext[0:3])

    # extract url links from the page
    # url pattern
    url_ptrn = "^(https?:/)?/?.+$"

    # pattern to exclude common files
    excl_f = ['.pdf', '.csv', '.txt', '.gif', '.xml',
              '.jpg', '.img', '.mov', '.mpg', '.mp4', '.mp3', '.ogg', '.avi']

    #excl_f_ptrn = '.*['+''.join(["^(\\" + f + ')' for f in excl_f])+']$'
    excl_f_ptrn = '(?<!('+'|'.join(['\\' + f for f in excl_f])+'))'
    
    # this regex matches http, https, or relative urls filtering out links to common data files
    search_regex = re.compile(url_ptrn+excl_f_ptrn)

    urls = []
    soup = BeautifulSoup(page, features="html.parser")
    for link in soup.findAll('a', attrs={'href': search_regex}):
        u = link.get('href')

        # turn to absolute url
        u = urljoin(base_url, u)
        # check domains
        if (locality == 'domain') or (locality == 'subdomain'):
            ext = tldextract.extract(u)
            domain = ext.registered_domain
            subdomain = '.'.join(
                ext[1:3]) if ext[0] == '' else '.'.join(ext[0:3])
        if locality == 'domain' and domain == base_domain:
            urls.append(u)
        elif locality == 'subdomain' and subdomain == base_subdomain:
            urls.append(u)
        elif locality == 'any':
            urls.append(u)
        else:
            pass
    return [u for u in urls]


def normalize_urls(base_url, urls):
    # super simple function that turns relative urls to absolute
    return [urljoin(base_url, u) for u in urls]



# -----------------------------------------------------------------------
# Tests 
# -----------------------------------------------------------------------

import unittest
from testlib import generate_test_graph
class TestParsersAndTestLibrary(unittest.TestCase):

    def test_graph_generator_and_url_extractor(self):
        nodes, urls, emails = generate_test_graph(sameDomain = False)
        found_urls= set()
        for n in nodes:
            new_urls = extract_urls(nodes[n].get_page(), n, locality='any')
            found_urls.update(new_urls)

        self.assertEqual(found_urls, set(urls))
        
    def test_graph_generator_and_email_extractor(self):
        nodes, urls, emails = generate_test_graph(sameDomain = False)
        found_emails = set()
        for n in nodes:    
            new_emails = extract_emails(nodes[n].get_page()) 
            found_emails.update(new_emails)

        self.assertEqual(found_emails, set(emails))

if __name__ == '__main__':
    unittest.main()

