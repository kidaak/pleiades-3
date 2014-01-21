#!/bin/env python2
from pysage import *
from common import *
import time, uuid
import os
from subprocess import *

class Worker(Actor):
    subscriptions = ['JobMessage', 'NoJobMessage', 'AckResultMessage']

    def __init__(self):
        self.mgr = ActorManager.get_singleton()
        self.mgr.register_actor(self)
        self.mgr.connect(transport.SelectTCPTransport, host=HOST, port=PORT)
        self.mgr.broadcast_message(JobRequestMessage(a=0))

        self.db, self.connection = get_database()
        self.status = ''

    def get_status_string(self):
        return self.status

    def handle_JobMessage(self, msg):
        self.status = 'job received'
        sim = msg.get_property('msg')

        user_id = sim['user_id']
        sim_id = sim['sim_id']
        job_id = sim['job_id']

        # execute
        
        #TODO: construct xml from db here
        meas_id = sim['meas']
        prob_id = sim['prob']
        
        xml_name = user_id + '_' + str(job_id) + '_' + str(sim_id) + '.xml'
        print user_id
        meas_xml = self.db.xml.find_one({'type':'meas', 'idref':meas_id, 'job_id':job_id, 'user_id':user_id})
        prob_xml = self.db.xml.find_one({'type':'prob', 'idref':prob_id, 'job_id':job_id, 'user_id':user_id})
        algs_xml = list(self.db.xml.find({'type':'alg', 'job_id':job_id, 'user_id':user_id}))
        sim_xml = self.db.xml.find_one({'type':'sim', 'job_id':job_id, 'sim_id':sim_id, 'user_id':user_id})

        #print sim_xml['value']

        output_file_name = str(uuid.uuid4())
        with open(xml_name, 'w') as xml_file:
            xml_string = '''<?xml version="1.0"?>
<!DOCTYPE simulator [
<!ATTLIST algorithm id ID #IMPLIED>
<!ATTLIST problem id ID #IMPLIED>
<!ATTLIST measurements id ID #IMPLIED>
]>
<simulator><algorithms>'''
            xml_string += '\n'.join([a['value'] for a in algs_xml])
            xml_string += "</algorithms><problems>"
            xml_string += prob_xml['value']
            xml_string += "</problems>"
            xml_string += meas_xml['value']
            xml_string += "<simulations>"
            xml_string += sim_xml['value'].replace('_output_', output_file_name)
            xml_string += "</simulations></simulator>"
            xml_file.write(xml_string)
        
        jar_name = user_id + '_' + str(job_id) + '_' + str(sim_id) + '.run'
        with open(jar_name, 'wb') as jar_file:
            jar = get_file(job_id, user_id)
            jar_file.write(jar)

        process = Popen(['java', '-jar', jar_name, xml_name], stdout=PIPE)
        while process.poll() == None:
            self.status = process.stdout.read(256)
            print self.status,

        os.remove(xml_name)
        os.remove(jar_name)

        # send back result
        with open(output_file_name, 'r') as result_file:
            self.mgr.broadcast_message(ResultMessage(msg={'id': sim['sim_id'], 'result':result_file.read()}))

        os.remove(output_file_name)

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


