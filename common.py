from pysage import Message
from pymongo import MongoClient
import gridfs
import json
import bson

def get_database():
    connection = MongoClient('137.215.137.225', 27017)
    database = connection.test_pleiades
    database.authenticate('admin', '12345')
    return database, connection

def get_file(name):
    #TODO: make this use a read-only user
    db, con = get_database()
    grid = gridfs.GridFS(db)
    _id = db.fs.files.find_one({'id':name})['_id']
    r = grid.get(_id).read()
    con.close()
    return r

def put_file(fname, fid):
    db, con = get_database()
    grid = gridfs.GridFS(db)
    r = grid.put(open(fname, 'rb'), id=fid)
    con.close()
    return r

class JobRequestMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 101
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class ResultMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 102
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class JobMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 103
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class AckResultMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 104
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class NoJobMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 105
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class NewJobMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 106
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)
