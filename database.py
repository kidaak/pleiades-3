from pymongo import *
from settings import *
import gridfs

def mongo_connect(user, pwd):
    connection = Connection(MONGO_IP, MONGO_PORT)
    database = connection[MONGO_DB]
    database.authenticate(user, pwd)
    return database, connection

def get_file(job_id, user_id):
    db, con = mongo_connect(MONGO_RO_USER, MONGO_RO_PWD)
    grid = gridfs.GridFS(db)
    r = grid.get(db.fs.files.find_one({'job_id':job_id, 'user_id':user_id})['_id']).read()
    con.close()
    return r

def put_file(fname, user_id, job_id):
    db, con = mongo_connect(MONGO_RW_USER, MONGO_RW_PWD)
    grid = gridfs.GridFS(db)
    r = grid.put(fname, user_id=user_id, job_id=job_id)
    con.close()
    return r
