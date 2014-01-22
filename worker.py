#!/bin/env python2
from pysage import *
from settings import *
from messages import *
from database import *
from subprocess import *
from traceback import *
import time
import uuid
import os
import sys

template = '''<?xml version='1.0'?>
<!DOCTYPE simulator [
<!ATTLIST algorithm id ID #IMPLIED>
<!ATTLIST problem id ID #IMPLIED>
<!ATTLIST measurements id ID #IMPLIED>
]>
<simulator>
<algorithms>%s</algorithms>
<problems>%s</problems>
%s
<simulations>%s</simulations>
</simulator>'''

class Worker(Actor):
    subscriptions = ['JobMessage', 'NoJobMessage', 'AckResultMessage', 'KillMessage']

    def __init__(self):
        self.mgr = ActorManager.get_singleton()
        self.mgr.register_actor(self)
        self.mgr.connect(transport.SelectTCPTransport, host=SERVER_IP, port=SERVER_PORT)
        self.mgr.broadcast_message(JobRequestMessage(msg={'allowed_users':ALLOWED_USERS}))

        self.db, self.connection = mongo_connect(MONGO_RO_USER, MONGO_RO_PWD)
        self.status = ''
        self.running = True

    def get_status_string(self):
        return self.status

    def handle_JobMessage(self, msg):
        try:
            self.status = 'job received'
            sim = msg.get_property('msg')

            user_id = sim['user_id']
            sim_id = sim['sim_id']
            print "executing: " + str(sim_id)
            job_id = sim['job_id']
            base_name = '%s_%s_%s_%s' % (user_id, str(job_id), str(sim_id), str(sim['sample']))

            self.status = 'getting xml'

            xml_name = base_name +'.xml'
            meas_xml = self.db.xml.find_one({
                'type': 'meas',
                'idref': sim['meas'],
                'job_id': job_id,
                'user_id': user_id
            })
            prob_xml = self.db.xml.find_one({
                'type': 'prob',
                'idref': sim['prob'],
                'job_id': job_id,
                'user_id': user_id
            })
            algs_xml = list(self.db.xml.find({
                'type':'alg',
                'job_id':job_id,
                'user_id':user_id
            }))
            sim_xml = self.db.xml.find_one({
                'type':'sim',
                'job_id':job_id,
                'sim_id':sim_id,
                'user_id':user_id
            })

            self.status = 'writing files'

            output_file_name = str(uuid.uuid4())
            with open(xml_name, 'w') as xml_file:
                xml_string = template % (
                    '\n'.join([a['value'] for a in algs_xml]),
                    prob_xml['value'],
                    meas_xml['value'],
                    sim_xml['value'].replace('_output_', output_file_name)
                )
                xml_file.write(xml_string)

            # Execute job
            jar_name = base_name +'.run'
            with open(jar_name, 'wb') as jar_file:
                jar = get_file(job_id, user_id)
                jar_file.write(jar)

            self.status = 'executing'

            process = Popen(['java', '-jar', jar_name, xml_name], stdout=PIPE)
            while process.poll() == None:
                self.status = process.stdout.read(256)
                print self.status,

            self.status = 'posting results'

            # send back result
            if not process.returncode:
                with open(output_file_name, 'r') as result_file:
                    self.mgr.broadcast_message(ResultMessage(msg={
                        'job_id': sim['job_id'],
                        'sim_id': sim['sim_id'],
                        'user_id':sim['user_id'],
                        'result':result_file.read(),
                        'sample':sim['sample']
                    }))

                os.remove(output_file_name)
            else:
                self.mgr.broadcast_message(JobErrorMessage(msg={
                    'job_id': sim['job_id'],
                    'sim_id': sim['sim_id'],
                    'user_id':sim['user_id']
                }))

            os.remove(xml_name)
            os.remove(jar_name)
        except Exception, e:
            print 'Worker error: ', e
            print 'Stack trace:'
            print_exc(file=sys.stdout)
            print
            #TODO: maybe send an error message to replenish the lost sample

        return True


    def handle_NoJobMessage(self, msg):
        time.sleep(1)
        self.mgr.broadcast_message(JobRequestMessage(msg={'allowed_users':ALLOWED_USERS}))
        self.status = 'request resent'

        return True


    def handle_AckResultMessage(self, msg):
        print 'ack received'
        self.mgr.broadcast_message(JobRequestMessage(msg={'allowed_users':ALLOWED_USERS}))
        self.status = 'ACK Received...'

        return True


    def handle_KillMessage(self, msg):
        self.running = False
        return True


    def run(self):
        while self.running:
            try:
                self.mgr.tick()
            except KeyboardInterrupt, SystemExit:
                self.running = False

if __name__ == '__main__':
    Worker().run()

