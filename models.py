import datetime
import logging
from google.appengine.ext import ndb
import re
import config

class TimeTrackedModel(ndb.Model):
    created_at = ndb.DateTimeProperty(auto_now_add=True, indexed=True)
    updated_at = ndb.DateTimeProperty(auto_now=True, indexed=True)

    @classmethod
    def recently_created_query(cls,*kwargs):
        return cls.query(*kwargs).order(-cls.created_at)

    @classmethod
    def recently_updated_query(cls,*kwargs):
        return cls.query(*kwargs).order(-cls.updated_at)


class NodeEntity(TimeTrackedModel):
    node_id = ndb.StringProperty()
    addr = ndb.StringProperty()
    addr_internal = ndb.StringProperty()
    client_id = ndb.StringProperty()
    port = ndb.IntegerProperty(default=0)
    gcm_key = ndb.StringProperty()
    
    @classmethod
    def flush_counter(cls, expression_key, field_name):
        pass


class ConnectionEntity(TimeTrackedModel):
    connection_id = ndb.StringProperty(indexed = True)
    to_node_key = ndb.KeyProperty(kind="NodeEntity")
    from_node_key = ndb.KeyProperty(kind="NodeEntity")


class SessionEntity(TimeTrackedModel):
    name = ndb.StringProperty()
    description = ndb.StringProperty()
    node_key = ndb.KeyProperty(kind = 'NodeEntity')
    client_id = ndb.StringProperty()

class SessionNodesEntity(TimeTrackedModel):
    session_key = ndb.KeyProperty(kind=SessionEntity)
    node_key = ndb.KeyProperty(kind=NodeEntity)