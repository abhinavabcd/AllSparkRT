'''
Created on May 27, 2016

@author: abhinav
'''
import time 
import random 
import string
import datetime


def get_random_id(length=10):
    '''returns a 10 character random string containing numbers lowercase upper case'''
    '''http://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits-in-python'''

    key_str = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits+string.ascii_lowercase) for _ in range(length)) + ("%d" % time.time())
    #key_str = hashlib.md5(key_str).hexdigest()
    return key_str


def from_kwargs(cls, **kwargs):
    ret = cls()
    for key in kwargs:
        setattr(ret, key, kwargs[key])
    return ret

EPOCH_DATETIME = datetime.datetime(1970,1,1)

def toUtcTimestamp(dt):
    try:
        td = dt - EPOCH_DATETIME
        # return td.total_seconds()
        return  (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 1e6 
    except:
        return 0
  
def to_utc_timestamp_millis(dt):
    try:
        td = dt - EPOCH_DATETIME
        # return td.total_seconds()
        return  int((td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 1e3)
    except:
        return 0
