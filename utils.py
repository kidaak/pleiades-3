from pysage import *
from netifaces import *
from smtplib import *
from email.MIMEText import *
from pymongo import *
from settings import *
import socket
import logging
import logging.handlers
import MySQLdb as sql
import gridfs
import sys
import os.path
import hashlib

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


def get_file(jar_hash):
    db, con = mongo_connect(MONGO_RO_USER, MONGO_RO_PWD)
    grid = gridfs.GridFS(db)
    r = grid.get(db.fs.files.find_one({'jar_hash':jar_hash})['_id']).read()
    con.close()
    return r


def put_file(jar, jar_hash):
    db, con = mongo_connect(MONGO_RW_USER, MONGO_RW_PWD)
    grid = gridfs.GridFS(db)
    
    if grid.exists({"jar_hash": jar_hash}):
        return None

    r = grid.put(jar, jar_hash=jar_hash)
    con.close()
    return r


def get_port(server):
    db, con = mongo_connect(MONGO_RO_USER, MONGO_RO_PWD)
    port = db.info.find_one({server: { '$exists': True }})
    con.close()
    if port:
        return port[server]
    return None


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


def get_logger(name):
    if not os.path.exists('logs'):
        os.mkdir('logs')
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)

    handler = logging.handlers.RotatingFileHandler(os.path.join('logs', name + '.log'), maxBytes=10240*100, backupCount=10)
    handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s - %(message)s'))

    log.addHandler(handler)
    return log

class ReconnectingTCPTransport(transport.SelectTCPTransport):
    def connect(self, host, port, d_handler):
        transport.SelectTCPTransport.connect(self, host, port)
        self.disconnect_handler = d_handler

    def poll(self, packet_handler):
        try:
            transport.SelectTCPTransport.poll(self, packet_handler)
        except socket.error:
            self.disconnect_handler()
            self.poll(packet_handler)

