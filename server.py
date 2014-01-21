#!/bin/env python2
import socket
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

        '''# TODO: remove this at some point
        #x = open('/home/filipe/src/cilib/simulator/xml/ga.xml', 'r')
        #j = open('/home/filipe/src/cilib/simulator/target/cilib-simulator-assembly-0.8-SNAPSHOT.jar', 'rb')

        self.db.jobs.insert({
            'samples':100,
            'job_id':'0',
            'user_id':'bennie',
            'results':[]
        })

        #put_file('/home/filipe/src/cilib/simulator/target/cilib-simulator-assembly-0.8-SNAPSHOT.jar', 'filinep_1')

        #x.close()
        #j.close()'''

    def handle_JobRequestMessage(self, msg):
        print ("Request from: " + str(msg.sender))
        
        try:
            job = self.db.jobs.find_one({'samples': {'$gt':0}})
            job['samples'] -= 1

            self.db.jobs.save(job)

            print 2
            job_id = job['job_id']
            user_id = job['user_id']
            job = self.db.xml.find_one({'job_id':job_id, 'type':'sim', 'user_id':user_id})
            del(job['_id'])
            
            #TODO: move file naming to worker
            #output_filename = str(uuid.uuid4())

            print ">>>" + str(job)
            print 3
            self.mgr.send_message(JobMessage(msg=job), msg.sender)
            '''self.mgr.send_message(JobMessage(msg={
                'id': job['id'],
                #'xml': job['xml'].replace('$OUTPUT', output_filename),
                'xml': job['xml'].replace('data/ga-ackley.txt', output_filename),
                'jar': job['jar'],
                'output_filename': output_filename
            }), msg.sender)'''

            print 4
        except Exception, e:
            print "D: " + str(e)
            self.mgr.send_message(NoJobMessage(msg=0), msg.sender)

        return True

    def handle_ResultMessage(self, msg):
        self.mgr.send_message(AckResultMessage(msg=0), msg.sender)
        '''#TODO: Send status with message
        
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
        '''

        return True

    def handle_NewJobMessage(self, msg):
        job = msg.get_property('msg')

        #try:
        user = job['user']
        job_id = max([j['job_id'] for j in self.db.jobs.find({'user_id': user})] + [0]) + 1

        # Upload XML
        p = XML_Uploader()
        jobs = p.upload_xml(StringIO(job['xml'].decode('base64').decode('zlib')), job_id, user)######<----Problem lies here with StringIO...

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

        put_file(StringIO(jar_file.decode('base64').decode('zlib')), user, job_id)
        #except Exception, e:
        #print "New job error:", e

        #TODO: Send status with message
        self.mgr.send_message(AckResultMessage(msg=0), msg.sender)

        return True

    def run(self):
        while True:
            self.mgr.tick()

s = Server()
s.run()

