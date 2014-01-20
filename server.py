#!/bin/env python2
from pysage import *
from common import *
import time
import uuid
import sys
<<<<<<< HEAD
from bson.binary import Binary

import uu

class Server(Actor):
    subscriptions = ['JobRequestMessage', 'ResultMessage', 'NewJobMessage']


    def __init__(self):
        self.db, self.connection = get_database()

        self.mgr = ActorManager.get_singleton()
        self.mgr.listen(transport.SelectTCPTransport, host=HOST, port=PORT)
        self.mgr.register_actor(self)

        # TODO: remove this at some point
        '''x = open('/home/filipe/src/cilib/simulator/xml/ga.xml', 'r')

        self.db.jobs.insert({
            'samples':100,
            'id':1,
            'xml':x.read(),
            'jar':'filinep_1',
            'owner':'filinep',
            'results':[]
        })

        j.close()'''


    def handle_JobRequestMessage(self, msg):
        try:
            print 1
            job = self.db.jobs.find_one({'samples': {'$gt':0}})
            job['samples'] -= 1
            self.db.jobs.save(job)
            print 2

            output_filename = str(uuid.uuid4())

            print 3
            self.mgr.send_message(JobMessage(msg={
                'id': job['id'],
                #'xml': job['xml'].replace('$OUTPUT', output_filename),
                'xml': job['xml'].replace('data/ga-ackley.txt', output_filename),
                'jar': job['jar'],
                'output_filename': output_filename
            }), msg.sender)
            print 4
        except Exception, e:
            print e
            self.mgr.send_message(NoJobMessage(msg=0), msg.sender)

        return True


    def handle_ResultMessage(self, msg):
        self.mgr.send_message(AckResultMessage(msg=0), msg.sender)
        result = msg.get_property('msg')

        job = self.db.jobs.find_one({'id': result['id']})
        file_name = job['id'] + '_' + str(job['samples']) + '.txt'

        with open(file_name, 'w') as result_file:
            result_file.write(result['result'])

        job['results'].append(file_name)

        if job['samples'] == 0:
            # gather results here
            pass

        return True


    def handle_NewJobMessage(self, msg):
        job = msg.get_property('msg')

        user = job['user']
        xml = cStringIO.StringIO(job['xml'])
        first_job_id = max([j['id'] for j in self.db.jobs.find({'owner': user})] + [0]) + 1

        # Transfer jar
        try:
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            sock.connect(tuple(job['socket']))

            jar_file = ''

            while len(jar_file) < job['m_size']:
                data = sock.recv(65536)
                jar_file += data

            sock.close()

            put_file(cStringIO.StringIO(jar_file), 'filinep_1')
        except Exception, e:
            print "New job error: ", e

        # Upload XML
        p = XML_Uploader()
        jobs = p.upload_xml(cStringIO.StringIO(xml), job_id, user)

        for j in jobs:
            

        self.mgr.send_message(AckResultMessage(msg=0), msg.sender)

        return True

    def run(self):
        while True:
            self.mgr.tick()

s = Server()
s.run()

