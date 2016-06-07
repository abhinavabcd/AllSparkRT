'''
Created on Feb 9, 2016

@author: abhinav
'''

import logging
import urllib
import config
import time
import hmac
import base64
import hashlib

## move it to seperate module ?
## copied from internet sha1 token encode - decode module

if hasattr(hmac, 'compare_digest'):  # python 3.3
    _time_independent_equals = hmac.compare_digest
else:
    def _time_independent_equals(a, b):
        if len(a) != len(b):
            return False
        result = 0
        if isinstance(a[0], int):  # python3 byte strings
            for x, y in zip(a, b):
                result |= x ^ y
        else:  # python2
            for x, y in zip(a, b):
                result |= ord(x) ^ ord(y)
        return result == 0


def utf8(value):
    return value.encode("utf-8")

def create_signed_value(server_secret , name, value):
    timestamp = utf8(str(int(time.time())))
    value = base64.b64encode(utf8(value))
    signature = _create_signature(server_secret, name, value, timestamp)
    value = b"|".join([value, timestamp, signature])
    return value

def decode_signed_value(server_secret, name, value, max_age_days=31):
    if not value:
        return None
    parts = utf8(value).split(b"|")
    if len(parts) != 3:
        return None
    signature = _create_signature(server_secret, name, parts[0], parts[1])
    if not _time_independent_equals(parts[2], signature):
        logging.warning("Invalid cookie signature %r", value)
        return None
#     timestamp = int(parts[1])
#     if timestamp < time.time() - max_age_days * 86400:
#         logging.warning("Expired cookie %r", value)
#         return None
#     if timestamp > time.time() + 31 * 86400:
#         # _cookie_signature does not hash a delimiter between the
#         # parts of the cookie, so an attacker could transfer trailing
#         # digits from the payload to the timestamp without altering the
#         # signature.  For backwards compatibility, sanity-check timestamp
#         # here instead of modifying _cookie_signature.
#         logging.warning("Cookie timestamp in future; possible tampering %r",
#                         value)
#         return None
    if parts[1].startswith(b"0"):
        logging.warning("Tampered cookie %r", value)
        return None
    try:
        return base64.b64decode(parts[0])
    except Exception:
        return None


def _create_signature(secret, *parts):
    hash = hmac.new(utf8(secret), digestmod=hashlib.sha1)
    for part in parts:
        hash.update(utf8(part))
    return utf8(hash.hexdigest())
