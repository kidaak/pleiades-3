#!/usr/bin/python2
import os, sys
from pysage import *
from socket import *
from cStringIO import *
from shutil import *
from xml_uploader import *
from settings import *
from messages import *
from database import *
from traceback import *
from gatherer import *
from network import *

class Server(Actor):
    subscriptions = ['JobRequestMessage', 'ResultMessage', 'NewJobMessage', 'KillMessage', 'JobErrorMessage']

    def __init__(self):
        self.db, self.connection = mongo_connect(MONGO_RW_USER, MONGO_RW_PWD)

        self.mgr = ActorManager.get_singleton()
        self.mgr.listen(transport.SelectTCPTransport, host=SERVER_IP, port=SERVER_PORT)
        self.mgr.register_actor(self)

        self.running = True

        print 'Starting server...'


    def handle_JobRequestMessage(self, msg):
        print 'Request from:', msg.sender

        try:
            print 'Getting job'
            allowed_users = msg.get_property('msg')['allowed_users']

            if not allowed_users:
                job = self.db.jobs.find_one({'samples': {'$gt':0}})
            else:
                job = self.db.jobs.find_one({'samples': {'$gt':0}, 'user_id':{'$all':allowed_users}})

            if not job:
                # No job was found
                print 'No jobs to run'
                self.mgr.send_message(NoJobMessage(msg=0), msg.sender)
                return True

            sample = job['samples']
            job['samples'] -= 1
            self.db.jobs.save(job)

            print 'Constructing job message (' + str(job['sim_id']) + ')'

            job = self.db.xml.find_one({'job_id':job['job_id'], 'sim_id':job['sim_id'], 'type':'sim', 'user_id':job['user_id']})
            job['sample'] = sample
            del(job['_id'])

            print 'Sending job'

            self.mgr.send_message(JobMessage(msg=job), msg.sender)

            print 'Job sent'
            print
        except Exception, e:
            print 'Job request error: ', e
            print 'Stack trace:'
            print_exc(file=sys.stdout)
            print

        return True

    def handle_ResultMessage(self, msg):
        try:
            result = msg.get_property('msg')

            user_id = result['user_id']
            job_id = result['job_id']
            sim_id = result['sim_id']
            sample = result['sample']

            try:
                sim = self.db.jobs.find_one({'job_id':job_id, 'sim_id':sim_id, 'user_id':user_id})
                job_name = sim['job_name']
            except:
                raise Exception('Job not found')

            # Get results file
            try:
                sock = socket(AF_INET, SOCK_STREAM)
                sock.connect(tuple(result['socket']))
                result_file_contents = recvall(sock, result['m_size']).decode('base64').decode('zlib')
                sock.close()
            except:
                raise Exception('Error transferring result file')

            print "Received result for: ", user_id, job_id, sim_id

            output_dir = os.path.join(RESULTS_DIR, str(user_id), str(job_name), str(job_id), str(sim_id))
            file_name = os.path.join(output_dir, str(sample) + '.txt')

            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                with open(os.path.join(output_dir, '.info'), 'w+') as info_file:
                    info_file.write('user_id: ' + str(sim['user_id']) + '\n')
                    info_file.write('job_name: ' + str(sim['job_name']) + '\n')
                    info_file.write('file_name: ' + str(sim['file_name']) + '\n')
                    info_file.write('samples: ' + str(sim['total_samples']) + '\n')
                    info_file.write('job_id: ' + str(sim['job_id']) + '\n')
                    info_file.write('sim_id: ' + str(sim['sim_id']) + '\n')

            with open(file_name, 'w+') as result_file:
                result_file.write(result_file_contents)

            sim['results'].append(file_name)
            self.db.jobs.save(sim)

            if sim['total_samples'] == len(sim['results']):
                print "Gathering ", job_name, 'into', sim['file_name']

                # Gather, clear temp files and remove sim
                Gatherer().gather_results(sim)
                rmtree(output_dir)
                self.db.jobs.remove(sim)

                # if job is complete clear everything
                to_find = {'job_id':job_id, 'user_id':user_id}
                if self.db.jobs.find_one(to_find) == None:
                    rmtree(output_dir[:output_dir.rfind(os.path.sep)])

                    self.db.xml.remove(to_find)
                    self.db.fs.files.remove(to_find)

                    self.mgr.broadcast_message(RmJarMessage(msg={
                        'jar_file': '%s_%i.run' % (user_id, job_id)
                    }))
                    #TODO: Better message
                    sendmail(user_id, 'Your job, ' + job_name + ', is done.')

            #TODO: Send status with message
            self.mgr.send_message(AckResultMessage(msg=0), msg.sender)
            print
        except Exception, e:
            print 'Result received error: ', e
            print 'Stack trace:'
            print_exc(file=sys.stdout)
            print
            #TODO couldnt gather or some such, do something here

            #TODO: Send status with message
            self.mgr.send_message(AckResultMessage(msg=0), msg.sender)

        return True

    def handle_NewJobMessage(self, msg):
        try:
            print 'New job received from', msg.sender
            job = msg.get_property('msg')
            user = job['user_id']
            job_id = max([j['job_id'] for j in self.db.jobs.find({'user_id': user})] + [0]) + 1

            # Upload XML
            jobs = XML_Uploader().upload_xml(StringIO(job['xml'].decode('base64').decode('zlib')), job_id, user)

            # Upload jobs
            self.db.jobs.insert([{
                'user_id': user,
                'job_name': job['name'],
                'file_name': j[2],
                'samples': j[1],
                'total_samples': j[1],
                'job_id': job_id,
                'sim_id': j[0],
                'results': []
            } for j in jobs])

            # Transfer jar
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(tuple(job['socket']))
            jar_file = recvall(sock, job['m_size']).decode('base64').decode('zlib')
            sock.close()

            put_file(StringIO(jar_file), user, job_id)

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


    def handle_JobErrorMessage(self, msg):
        try:
            print 'Received job error'

            job = msg.get_property('msg')
            user_id = job['user_id']
            sim_id = job['sim_id']
            job_id = job['job_id']
            sim = self.db.jobs.find_one({'user_id': user_id, 'sim_id': sim_id, 'job_id': job_id})
            self.db.jobs.remove(sim)

            to_find = {'job_id':job_id, 'user_id':user_id}
            if self.db.jobs.find_one(to_find) == None:
                # leave the directory in case there is salvageable stuff? it could get removed though
                self.db.xml.remove(to_find)
                self.db.fs.files.remove(to_find)

                self.mgr.broadcast_message(RmJarMessage(msg={
                    'jar_file': '%s_%i.run' % (user_id, job_id)
                }))
                #TODO: Better message
                sendmail(user_id, 'Error with simulation number ' + str(sim_id) + ' in job ' + job_name)

            print 'Removed job: user', job['user_id'], 'job', job['job_id'], 'sim', job['sim_id']
            #TODO: email user

            #TODO: Send status with message
            self.mgr.send_message(AckResultMessage(msg=0), msg.sender)
            print
        except Exception, e:
            print 'Job error error: ', e
            print 'Stack trace:'
            print_exc(file=sys.stdout)
            print

        return True


    def run(self):
        print 'Ready to receive/distribute...'
        while self.running:
            try:
                self.mgr.tick()
            except KeyboardInterrupt, SystemExit:
                self.running = False
                for j in self.db.jobs.find():
                    rem = j['total_samples'] - j['samples']
                    if rem > len(j['results']):
                        #TODO: replenish 'rem' jobs somehow taking into account the samples that have already arrived
                        pass

if __name__ == '__main__':
    Server().run()

