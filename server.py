#!/bin/env python2
import os
from pysage import *
from socket import *
from cStringIO import *
from xml_uploader import *
from settings import *
from messages import *
from database import *
from traceback import *

class Server(Actor):
    subscriptions = ['JobRequestMessage', 'ResultMessage', 'NewJobMessage', 'KillMessage']

    def __init__(self):
        self.db, self.connection = mongo_connect(MONGO_RW_USER, MONGO_RW_PWD)

        self.mgr = ActorManager.get_singleton()
        self.mgr.listen(transport.SelectTCPTransport, host=SERVER_IP, port=SERVER_PORT)
        self.mgr.register_actor(self)

        self.running = True

        print "Server started..."


    def handle_JobRequestMessage(self, msg):
        print "Request from:", msg.sender

        try:
            print "Getting job"

            job = self.db.jobs.find_one({'samples': {'$gt':0}})

            if not job:
                # No job was found
                print 'No jobs to run'
                self.mgr.send_message(NoJobMessage(msg=0), msg.sender)
                return True

            sample = job['samples']
            job['samples'] -= 1
            self.db.jobs.save(job)

            print "Constructing job message"

            job = self.db.xml.find_one({'job_id':job['job_id'], 'type':'sim', 'user_id':job['user_id']})
            job['sample'] = sample
            del(job['_id'])

            print "Sending job"

            self.mgr.send_message(JobMessage(msg=job), msg.sender)

            print 'Job sent'
            print
        except Exception, e:
            print "Job request error: ", e
            print 'Stack trace:'
            print_exc(file=sys.stdout)
            print

        return True

    def handle_ResultMessage(self, msg):
        self.mgr.send_message(AckResultMessage(msg=0), msg.sender)
        
        result = msg.get_property('msg')

        user_id = result['user_id']
        job_id = result['job_id']
        sim_id = result['sim_id']
        sample = result['sample']

        job = self.db.jobs.find_one({'job_id':job_id, 'sim_id':sim_id, 'user_id':user_id})
        job_name = job['job_name']

        #TODO: append output directory here
        output_dir = os.path.join(RESULTS_DIR, str(user_id), str(job_name), str(sim_id))
        file_name = os.path.join(output_dir, str(sample) + '.txt')

        if not os.path.exists(output_dir):
            info = os.path.join(output_dir, '.info')
            os.makedirs(output_dir)
            with open(info, 'w+') as info_file:
                info_file.write('user_id: ' + str(job['user_id']) + '\n')
                info_file.write('job_name: ' + str(job['job_name']) + '\n')
                info_file.write('file_name: ' + str(job['file_name']) + '\n')
                info_file.write('job_id: ' + str(job['job_id']) + '\n')
                info_file.write('sim_id: ' + str(job['sim_id']) + '\n')

        with open(file_name, 'w+') as result_file:
            result_file.write(result['result'])

        job['results'].append(file_name)
        self.db.jobs.save(job)

        if job['samples'] == 0:
            #TODO: gather results here
            print 'Gathering results'

        return True

    def handle_NewJobMessage(self, msg):
        try:
            print 'New job received'
            job = msg.get_property('msg')

            user = job['user']
            job_id = max([j['job_id'] for j in self.db.jobs.find({'user_id': user})] + [0]) + 1

            # Upload XML
            p = XML_Uploader()
            jobs = p.upload_xml(StringIO(job['xml'].decode('base64').decode('zlib')), job_id, user)

            # Upload jobs
            self.db.jobs.insert([{
                'user_id': user,
                'job_name': job['name'],
                'file_name': j[2],
                'samples': j[1],
                'job_id': job_id,
                'sim_id': j[0],
                'results': []
            } for j in jobs])

            # Transfer jar
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(tuple(job['socket']))

            jar_file = ''

            while len(jar_file) < job['m_size']:
                data = sock.recv(65536)
                jar_file += data

            sock.close()

            put_file(StringIO(jar_file.decode('base64').decode('zlib')), user, job_id)

            #TODO: Send status with message
            self.mgr.send_message(AckResultMessage(msg=0), msg.sender)
        except Exception, e:
            print "New job error: ", e
            print 'Stack trace:'
            print_exc(file=sys.stdout)
            print

            #TODO: Send status with message
            self.mgr.send_message(AckResultMessage(msg=0), msg.sender)

        return True

    def run(self):
        while self.running:
            self.mgr.tick()

s = Server()
s.run()

