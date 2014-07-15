#!/usr/bin/python2
import os, sys
from pysage import *
from cStringIO import *
from shutil import *
from xml_uploader import *
from settings import *
from messages import *
from traceback import *
from gatherer import *
from utils import *
from socket import *

class UploadServer(Actor):
    subscriptions = ['NewJobMessage']

    def __init__(self):
        print 'Starting upload server...'
        self.db, self.connection = mongo_connect(MONGO_RW_USER, MONGO_RW_PWD)

        self.mgr = ActorManager.get_singleton()
        self.mgr.register_actor(self)
        self.listen()

        self.running = True


    def listen(self):
        if self.mgr.transport and self.mgr.transport._is_connected:
            self.mgr.disconnect()

        self.mgr.listen(transport.SelectTCPTransport, host=SERVER_IP, port=0)

        new_port = self.mgr.transport.address[1]
        port = self.db.info.find_one({'upload_server_port': { '$exists': True }})
        if port:
            print 'Upload server is already running.'
            sys.exit(1)
        else:
            self.db.info.insert({'upload_server_port': new_port})


    def handle_NewJobMessage(self, msg):
        try:
            print 'New job received from', msg.sender
            job = msg.get_property('msg')
            user = job['user_id']
            job_id = max([j['job_id'] for j in self.db.jobs.find({'user_id': user})] + [0]) + 1
            jar_hash = job['jar_hash']

            # Upload XML
            xml = job['xml'].decode('base64').decode('zlib')
            if os.path.isfile(xml):
                with open(xml, 'r') as xml_file:
                    xml = xml_file.read()
            jobs = upload_xml(StringIO(xml), job_id, user)

            if jobs != None:
                # Transfer jar
                sock = socket(AF_INET, SOCK_STREAM)
                sock.connect(tuple(job['socket']))
                jar_file = recvall(sock, job['m_size']).decode('base64').decode('zlib')
                sock.close()

                put_file(StringIO(jar_file), jar_hash)

                # Upload jobs
                self.db.jobs.insert([{
                    'user_id': user,
                    'job_name': job['name'],
                    'file_name': j[2],
                    'samples': range(1, int(j[1]) + 1),
                    'total_samples': j[1],
                    'job_id': job_id,
                    'sim_id': j[0],
                    'jar_hash': jar_hash,
                    'results': []
                } for j in jobs])

                self.mgr.send_message(AckNewJobMessage(msg={
                    'status':'Uploaded ' + str(len(jobs)) + ' simulations successfully.'
                }), msg.sender)
                print
        except Exception, e:
            print 'New job error: ', e
            print 'Stack trace:'
            print_exc(file=sys.stdout)
            print

            self.mgr.send_message(AckNewJobMessage(msg={'status':'Error: could not upload job.'}), msg.sender)

        return True


    def run(self):
        print 'Ready to receive new jobs...'
        while self.running:
            try:
                self.mgr.tick()

            except error:
                self.listen()

            except KeyboardInterrupt, SystemExit:
                port = self.db.info.remove({'upload_server_port': { '$exists': True }})
                self.running = False

            except Exception, e:
                print 'ERROR:', e

if __name__ == '__main__':
    UploadServer().run()

