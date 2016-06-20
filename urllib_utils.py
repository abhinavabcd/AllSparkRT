'''
Created on May 11, 2016

@author: abhinav
'''
import urllib2
import logging
import zlib
from StringIO import StringIO
import hashlib
import os

http_logger = urllib2.HTTPHandler(debuglevel = 1)
url_loader=urllib2.build_opener(http_logger,urllib2.HTTPCookieProcessor(),urllib2.ProxyHandler(),http_logger, urllib2.HTTPRedirectHandler())
urllib2.install_opener(url_loader)

def get_data(url,post=None,headers={}, method = None):
    headers['Accept-encoding'] ='gzip'
    ret= None
    try:
        req=urllib2.Request(url,post,headers)
        if(method!=None):
            req.get_method = lambda : method
        ret = url_loader.open(req) 
        if ret.info().get('Content-Encoding') == 'gzip':
            ret2 = ret
            try:
                ret = StringIO(zlib.decompress(ret2.read(),16+zlib.MAX_WBITS))
            except:
                decompressor = zlib.decompressobj()
                ret = StringIO(decompressor.decompress(ret2.read()))
            ret2.close()
            
    except urllib2.HTTPError, e: 
        print e
        ret = None
    return ret