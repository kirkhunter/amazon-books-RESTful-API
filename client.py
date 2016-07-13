import httplib

# use this server for prod, once it's on ec2
SERVER = ''


def get_most_helpful_review():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/reviews/most_helpful_review')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_least_helpful_review():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/reviews/least_helpful_review')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_most_concise_helpful_review():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/reviews/most_concise_good_review')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_most_expensive_book():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/books/most_expensive_book')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_cheapest_book():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/books/cheapest_book')
    resp = h.getresponse()
    out = resp.read()
    return out

def get_earliest_review():
    out = dict()
    h = httplib.HTTPConnection(SERVER)
    h.request('GET', 'http://'+SERVER+'/reviews/earliest_review')
    resp = h.getresponse()
    out = resp.read()
    return out

if __name__ == '__main__':
    print "*********************************************************"
    print "test of my flask app running at ", SERVER
    print "created by Kirk Hunter"
    print "*********************************************************"
    print " "
    print " "
    print "**********       get most helpful review       **********"
    print get_most_helpful_review()
    print
    print "**********       get least helpful review      **********"
    print get_least_helpful_review()
    print
    print "********** get most concise yet helpful review **********"
    print get_most_concise_helpful_review()
    print
    print "**********       get most expensive book        **********"
    print get_most_expensive_book()
    print 
    print "***********          get cheapest book          **********"
    print get_cheapest_book()
    