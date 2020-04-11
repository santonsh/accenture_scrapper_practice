import random
from math import floor

valid_domain_name_chars = [c for c in range(48,57)]+[c for c in range(65,90)]+[c for c in range(97,122)]+[45]
valid_username_chars = [c for c in range(48,57)]+[c for c in range(65,90)]+[c for c in range(97,122)]+[45,46,43,95]

def generate_string(length, valid_set):
    return [chr(random.choice(valid_set)) for _ in range(length)]

def generate_email():
    """generates valid email addresses"""
    valid_domain_name_chars = [c for c in range(48,57)]+[c for c in range(65,90)]+[c for c in range(97,122)]+[45]
    username = ''.join(generate_string(10, valid_username_chars))
    domainname = ''.join(generate_string(10, valid_domain_name_chars))
    domain = random.choice(['com', 'co.il', 'info'])
    return username+'@'+domainname+'.'+domain


def generate_domainname():
    """generates a domain name. Part of url generation chain"""
    domainname = ''.join(generate_string(10, valid_domain_name_chars))
    domain = random.choice(['com', 'co.il', 'info'])
    return domainname+'.'+domain

def generate_url(domainname = None):
    """generates simple url. Possibly with given domainname"""
    path_length = random.choice([1,2,3,4,5])
    path = ''
    for i in range(path_length):
        path = path + '/' +  ''.join(generate_string(5, valid_domain_name_chars))
    if domainname:
        return 'http://www.'+domainname+path
    else: 
        return 'http://www.'+generate_domainname()+path

def generate_link(domainname = None):
    return '<a href="'+generate_url(domainname)+'"></a>'


def generate_page(links, emails):
    """generates random page incorporating given links and emails"""
    s = ''.join(generate_string(30, valid_domain_name_chars))

    for i in links:
        s = s + ' ' + '<a href="'+i+'"></a>' + ' ' +''.join(generate_string(30, valid_domain_name_chars))
    s = s + ' ' +''.join(generate_string(30, valid_domain_name_chars))   

    for i in emails:
        s = s + ' ' + i + ' ' +''.join(generate_string(30, valid_domain_name_chars))
    s = s + ' ' +''.join(generate_string(30, valid_domain_name_chars))   
    

    return s

class testNode():
    """this is a node representing a test internet page with its links and emails"""
    def __init__(self, url, links, emails):
        self.url = url
        self.links = links
        self.emails = emails
        self.page = ''
    def generate_page(self):
        self.page = generate_page(self.links, self.emails)
    def get_page(self):
        return self.page


def generate_test_graph(sameDomain = False):
    """Simple non deterministic algorithm to generate test internet graph to be used in tests. The test graph is very link redundant. Need to generate thinner graph later """
    num = 100

    urls = []
    emails = []
    nodes={}
    if sameDomain:
        domain = generate_domainname()
    else:
        domain = None
    for i in range(num):
        urls.append(generate_url(domain))
        emails.append(generate_email())
    
    used_urls = set()
    used_emails = set()
    for u in urls:
        l = random.choices(urls, k = floor(num/4))
        #l = [u for u in urls]
        e = random.choices(emails, k = floor(num/10))
        #e = [e for e in emails]
        used_urls.update(l)
        used_emails.update(e)
        nodes[u] = testNode(u, l, e)
        nodes[u].generate_page()
    
    return nodes, urls, emails
    


