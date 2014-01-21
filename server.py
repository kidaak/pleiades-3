#!/bin/env python2
import socket, os
from pysage import *
from common import *
from cStringIO import StringIO
from xml_uploader import XML_Uploader

class Server(Actor):
    subscriptions = ['JobRequestMessage', 'ResultMessage', 'NewJobMessage']

    def __init__(self):
        self.db, self.connection = get_database()

        self.mgr = ActorManager.get_singleton()
        self.mgr.listen(transport.SelectTCPTransport, host=HOST, port=PORT)
        self.mgr.register_actor(self)

        print("Server started...")

    def handle_JobRequestMessage(self, msg):
        print ("Request from: " + str(msg.sender))
        
        try:
            job = self.db.jobs.find_one({'samples': {'$gt':0}})
            sample = job['samples']
            job['samples'] -= 1

            self.db.jobs.save(job)

            job_id = job['job_id']
            sim_id = job['sim_id']
            user_id = job['user_id']
            job = self.db.xml.find_one({'type':'sim', 'job_id':job_id, 'sim_id':sim_id, 'user_id':user_id})
            job['sample'] = sample
            del(job['_id'])

            self.mgr.send_message(JobMessage(msg=job), msg.sender)

        except Exception, e:
            print e
            self.mgr.send_message(NoJobMessage(msg=0), msg.sender)

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
        job = msg.get_property('msg')

        try:
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
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            sock.connect(tuple(job['socket']))

            jar_file = ''

            while len(jar_file) < job['m_size']:
                data = sock.recv(65536)
                jar_file += data

            sock.close()

            put_file(StringIO(jar_file.decode('base64').decode('zlib')), user, job_id)
        except Exception, e:
            #TODO: Send status with message
            print e
        
        self.mgr.send_message(AckResultMessage(msg=0), msg.sender)

        return True

    def run(self):
        while True:
            self.mgr.tick()

s = Server()
s.run()

