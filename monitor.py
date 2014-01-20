#!/bin/env python2
from pysage import *
from common import *
import time
import os
from subprocess import *
from multiprocessing import Process

class Worker(Actor):
    subscriptions = ['RequestStatusMessage']

    def __init__(self):
        self.mgr = ActorManager.get_singleton()
        self.mgr.register_actor(self)
        self.mgr.connect(transport.SelectTCPTransport, host='localhost', port=8001)
        self.mgr.broadcast_message(JobRequestMessage(a=0))

        self.status = ''

    def get_status_string(self):
        return self.status

    def handle_JobMessage(self, msg):
        self.status = 'job received'
        job = msg.get_property('msg')

        # execute
        with open(job['id'] + '.xml', 'w') as xml_file:
            xml_file.write(job['xml'])

        with open(job['id'] + '.run', 'wb') as jar_file:
            jar = get_file(job['jar'])
            jar_file.write(jar)

        process = Popen(['java', '-jar', job['id'] + '.run', job['id'] + '.xml'], stdout=PIPE)
        while process.poll() == None:
            l = process.stdout.read(256)
            print l,

        os.remove(job['id'] + '.xml')
        os.remove(job['id'] + '.run')

        # send back result
        with open(job['output_filename'], 'r') as result_file:
            self.mgr.broadcast_message(ResultMessage(msg={'id': job['id'], 'result':result_file.read()}))

        return True

    def handle_NoJobMessage(self, msg):
        time.sleep(1)
        self.mgr.broadcast_message(JobRequestMessage(msg=0))
        self.status = 'request resent'

        return True

    def handle_AckResultMessage(self, msg):
        print 'ack received'
        self.mgr.broadcast_message(JobRequestMessage(msg=0))
        self.status = 'ACK Received...'

        return True

    def run(self):
        while True:
            self.mgr.tick()

w = Worker()
w.run()
