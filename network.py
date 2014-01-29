from netifaces import *
from smtplib import *
from email.MIMEText import *
from pymongo import *
from settings import *
import MySQLdb as sql
import gridfs
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


def get_local_ip():
    ip_list = []
    for interface in interfaces():
        try:
            for link in ifaddresses(interface)[AF_INET]:
                ip_list.append(link['addr'])
        except:
            continue
    return [ip for ip in ip_list if not (ip.startswith('127.') or ip.startswith('10.'))][0]


def recvall(sock, size):
    data = ''
    while len(data) < size:
        d = sock.recv(size - len(data))
        if not d:
            return None
        data += d
    return data


def sendall(sock, data):
    while data:
        sent = sock.send(data)
        data = data[sent:]


def sendmail(user, msg):
    #TODO: put these details in settings.py
    email_addr = get_user_email(user)

    msg = MIMEText(msg)
    msg['Subject'] = 'Pleiades Cluster Notification'
    msg['From'] = 'Pleiades Cluster<no-reply@cs.up.ac.za>'
    msg['To'] = email_addr

    s = SMTP('student.up.ac.za', 25, 'Pleiades')
    s.sendmail('no-reply@cs.up.ac.za', [email_addr], msg.as_string())
    s.quit()

