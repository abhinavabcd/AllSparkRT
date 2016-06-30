import datetime
import logging
from google.appengine.ext import ndb
import re
from config import DBTABLE_PREFIX

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
    addr = ndb.StringProperty(indexed=True)
    addr_internal = ndb.StringProperty()
    client_id = ndb.StringProperty()
    port = ndb.StringProperty(indexed=True)
    gcm_key = ndb.StringProperty()
    
    
    @classmethod
    def _get_kind(cls):
        return 'NodeEntity'+DBTABLE_PREFIX
    
    @classmethod
    def flush_counter(cls, expression_key, field_name):
        pass


class ConnectionEntity(TimeTrackedModel):
    connection_id = ndb.StringProperty(indexed = True)
    to_node_key = ndb.KeyProperty(kind=NodeEntity)
    from_node_key = ndb.KeyProperty(kind=NodeEntity)
    
    @classmethod
    def _get_kind(cls):
        return 'ConnectionEntity'+DBTABLE_PREFIX
    


class SessionEntity(TimeTrackedModel):
    name = ndb.StringProperty()
    description = ndb.StringProperty()
    node_key = ndb.KeyProperty(kind = NodeEntity)
    client_id = ndb.StringProperty()
    
    @classmethod
    def _get_kind(cls):
        return 'SessionEntity'+DBTABLE_PREFIX


class SessionNodesEntity(TimeTrackedModel):
    session_key = ndb.KeyProperty(kind=SessionEntity)
    node_key = ndb.KeyProperty(kind=NodeEntity)
    
    @classmethod
    def _get_kind(cls):
        return 'SessionNodesEntity'+DBTABLE_PREFIX
    
#     
# class UserInboxSeqNumber(base.TimeTrackedModel):
#     seq = ndb.IntegerProperty(default=0)
#     
#     @classmethod
#     def get_sequence_number(cls, user_key):
#         user_seq = UserInboxSeqNumber.get_by_id(user_key.id())
#         if(not user_seq):
#             user_seq = UserInboxSeqNumber(id=user_key.id(), seq=0)
#             user_seq.put()
#             
#         return user_seq
# 
# 
# # we dont index by time stamp
# class UserInboxMessage(base.TimeTrackedModel):
#     user_inbox_sequence_id = ndb.StringProperty(indexed=True)
#     message_payload = ndb.TextProperty()
#     message_type = ndb.IntegerProperty()
#     
#     @classmethod
#     def add_inbox_message(cls, user_key,  message_type , message_payload):
#         user_seq = UserInboxSeqNumber.get_sequence_number(user_key)
#         seq = user_seq.seq
#         message  = UserInboxMessage()
#         message.user_inbox_sequence_id = user_key.id()+"__"+str(seq)
#         message.message_payload = message_payload
#         message.message_type  = message_type
#         
#         to_update = [message]
#         if(datetime.datetime.now() - user_seq.updated_at > timedelta(minutes=30)):
#             user_seq.seq+=1
#             to_update.append(user_seq)
#             
#         ndb.put_multi(to_update)
#         return user_seq.seq
#         
#     
#     
#     
#     #return messages, from_seq, last_seq, has_more
#     @classmethod
#     def get_inbox_messages(cls, user_key , from_seq=-1 , to_seq=-1, from_message_time_stamp=None):
#          
#         ret = []
#         flag = False
#         
#         if(from_seq==-1 and to_seq==-1):
#             to_seq = UserInboxSeqNumber.get_sequence_number(user_key).seq
#         
#         r = range(0) #iterate in reverse 
#         
#         if(from_seq==-1):
#             from_seq = max(to_seq-50 , 0)
#         if(to_seq==-1):
#             to_seq = from_seq+50
#             
#             
#         r = range( from_seq , to_seq+1)
#         
#         more = True
#         for i in r:
#             user_seq_id = user_key.id()+"__"+str(i)
#             temp = UserInboxMessage.query(UserInboxMessage.user_inbox_sequence_id == user_seq_id).fetch()
#             if(from_message_time_stamp):
#                 for i in range(0 , len(temp)):
#                     if(temp[i].created_at <= from_message_time_stamp):
#                         temp  = temp[:i]
#                         more = False
#                         flag=True
#                         break
#                     
#                     
#             ret.extend(temp)
#             if(len(temp)==0):
#                 flag = True
#                 more = False
#             if(len(ret)>500):
#                 flag  = True #no more than 500 messages
#             if(flag):
#                 break
#         return ret, from_seq, to_seq, more
# 
# class UserFriendship(base.TimeTrackedModel):
#     #implies user_id1 send a request to  user_id2 || it mean user_id1 following user_id2
#     
#     user_id1 = ndb.KeyProperty(kind='UserEntity')# key of first user
#     user_id2 = ndb.KeyProperty(kind='UserEntity')# key of second user
#     is_mutual = ndb.BooleanProperty()# for querying friends only
#     relation_type = ndb.StringProperty()
#     added_through = ndb.IntegerProperty(default=0) #0=> contancts , 1=>facebook 
#       
#     @classmethod
#     def get_user_friendships(cls, user_id, cursor, page_size=500 , friends_only = False, include_friends_who_friended_me = True, exclude_unapproved_requests=False):
#         #initially only , we will get all friends and followers , this can be overloading if user has lot of folowers carefully monitor
#         query = None
#         if(friends_only):
#             query = UserFriendship.query(ndb.AND(UserFriendship.user_id2 == user_id, UserFriendship.is_mutual==True))
#         else:
#             #include pending request
#             if(include_friends_who_friended_me):
#                 query = UserFriendship.query(ndb.OR(UserFriendship.user_id2 == user_id, UserFriendship.user_id1 == user_id)).order(UserFriendship._key)
#             else:
#                 query = UserFriendship.query(UserFriendship.user_id1 == user_id)                
#         if(cursor):
#             cursor = ndb.Cursor.from_websafe_string(cursor)
#             
#         friendships, next_cursor, more = query.fetch_page(page_size, start_cursor=cursor, produce_cursors=True)
#     
#         if next_cursor:
#             next_cursor = next_cursor.to_websafe_string()
#             
#         if(exclude_unapproved_requests):# request i sent but not yet accepted by other fellas
#             filter(lambda x: not x.is_mutual and x.user_id1.id()==user_id.key() ,  friendships  )
#             
#             
#         return friendships , next_cursor, more
#     
# 
#     @classmethod
#     def set_relation_type(cls, user_id1, user_id2 , relation_type):
#         user_friendship = ndb.Key(UserFriendship, user_id1.id()+"__"+user_id2.id()).get()
#         if(user_friendship):
#             user_friendship.relation_type = relation_type
#             user_friendship.put()
#         return user_friendship
#     
#     @classmethod
#     def get_friendships(cls, user_id1, friend_ids):
#         keys = map(lambda friend_id : ndb.Key(UserFriendship,user_id1.id()+"__"+friend_id.id() )  , friend_ids)
#         friendships = ndb.get_multi(keys)
#         return friendships
#     
#     @classmethod
#     def add_friendship(cls, user_id1, user_id2, save=True, added_through=0):#both args are keys
#         user_friendship = UserFriendship()
#         user_friendship.key = ndb.Key(UserFriendship, user_id1.id()+"__"+user_id2.id())
#         user_friendship.user_id1 = user_id1
#         user_friendship.user_id2 = user_id2
#         if(user_friendship.added_through==None):
#             user_friendship.added_through = added_through
#         
#         user_friendship_other_way = UserFriendship.get_by_id(user_id2.id()+"__"+user_id1.id())
# 
#         
#         user_friendship.is_mutual = user_friendship_other_way!=None
#         
#         to_update=  [user_friendship]
#         
#         if(user_friendship_other_way!=None):
#             user_friendship_other_way.is_mutual = True
#             to_update.append(user_friendship_other_way)
#             
#         if save:
#             ndb.put_multi(to_update)
#         
#         return user_friendship, to_update
#     
# 
#             
