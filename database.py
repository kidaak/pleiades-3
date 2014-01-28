from pymongo import *
from settings import *
import gridfs
import MySQLdb as sql
import sys

def mongo_connect(user, pwd):
    connection = Connection(MONGO_IP, MONGO_PORT)
    database = connection[MONGO_DB]
    try:
        database.authenticate(user, pwd)
    except:
        print 'Mongo authentication failed. Exiting.'
        connection.close()
        sys.exit(1)
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

def get_user_email(user):
    con = None
    try:
        con = sql.connect(host=SQL_HOST, port=SQL_PORT, user=SQL_RO_USER, passwd=SQL_RO_PWD, db=SQL_DB)
        cur = con.cursor()
        cur.execute("SELECT email FROM users WHERE username = '" + user + "'")
        email = cur.fetchone()
        return email[0]
    except sql.Error, e:
        print e
    finally:
        if con:
            con.close()

