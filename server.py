#!/bin/env python2
from pysage import *
from common import *
import time
import uuid
import sys
from bson.binary import Binary

import uu

class Server(Actor):
    subscriptions = ['JobRequestMessage', 'ResultMessage', 'NewJobMessage']


    def __init__(self):
        self.db, self.connection = get_database()

        self.mgr = ActorManager.get_singleton()
        self.mgr.listen(transport.SelectTCPTransport, host='localhost', port=8001)
        self.mgr.register_actor(self)

        # TODO: remove this at some point
        x = open('/home/filipe/src/cilib/simulator/xml/ga.xml', 'r')
        j = open('/home/filipe/src/cilib/simulator/target/cilib-simulator-assembly-0.8-SNAPSHOT.jar', 'rb')

        self.db.jobs.insert({
            'samples':100,
            'id':'filinep_1_0',
            'xml':x.read(),
            'jar':'filinep_1',
            'owner':'filinep',
            'results':[]
        })

        put_file('/home/filipe/src/cilib/simulator/target/cilib-simulator-assembly-0.8-SNAPSHOT.jar', 'filinep_1')

        x.close()
        j.close()


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


    def handle_JobRequestMessage(self, msg):
        job = msg.get_property('msg')

        user = job['user']
        xml = job['xml']
        jar = job['jar']

        return True

    def run(self):
        while True:
            self.mgr.tick()

s = Server()
s.run()

