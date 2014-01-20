#!/bin/env python2
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


    def handle_JobRequestMessage(self, msg):
        try:
            job = self.db.jobs.find_one({'samples': {'$gt':0}})
            job['samples'] -= 1

            self.db.jobs.save(job)

            output_filename = job['user_id'] + '_' + job['job_id'] + '_' + job['user_id'] + '.tmp'

            self.mgr.send_message(JobMessage(msg={
                'job_id': job['id'],
                #'xml': job['xml'].replace('$OUTPUT', output_filename),
                'xml': job['xml'].replace('data/ga-ackley.txt', output_filename),
                'jar': job['jar'],
                'output_filename': output_filename
            }), msg.sender)

        except Exception, e:
            print "Job request error:", e
            self.mgr.send_message(NoJobMessage(msg=0), msg.sender)

        return True


    def handle_ResultMessage(self, msg):
        #TODO: Send status with message
        self.mgr.send_message(AckResultMessage(msg=0), msg.sender)
        result = msg.get_property('msg')

        job = self.db.jobs.find_one({'id': result['id']})
        #TODO: append output directory here
        file_name = job['job_id'] + '_' + str(job['samples']) + '.txt'

        with open(file_name, 'w') as result_file:
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
            jobs = p.upload_xml(StringIO(job['xml']), job_id, user)

            # Upload jobs
            self.db.jobs.insert([{
                'samples': jobs[j],
                'user_id': user,
                'results': [],
                'job_id': job_id,
                'sim_id': j
            } for j in jobs.keys()])

            # Transfer jar
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            sock.connect(tuple(job['socket']))

            jar_file = ''

            while len(jar_file) < job['m_size']:
                data = sock.recv(65536)
                jar_file += data

            sock.close()

            put_file(StringIO(jar_file), user)
        except Exception, e:
            print "New job error:", e

        #TODO: Send status with message
        self.mgr.send_message(AckResultMessage(msg=0), msg.sender)

        return True

    def run(self):
        while True:
            self.mgr.tick()

s = Server()
s.run()

