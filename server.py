#!/usr/bin/python2
import os, sys
import signal
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
from random import *

class DistributionServer(Actor):
    subscriptions = ['JobRequestMessage', 'JobErrorMessage', 'ResultMessage']

    def __init__(self):
        print 'Starting distribution server...'
        self.db, self.connection = mongo_connect(MONGO_RW_USER, MONGO_RW_PWD)

        self.mgr = ActorManager.get_singleton()
        self.mgr.register_actor(self)

        self.listen()

        self.running = True


    def listen(self):
        self.mgr.listen(transport.SelectTCPTransport, host=SERVER_IP, port=0)

        port = self.db.info.find_one({'server_port': { '$exists': True }})
        if port:
            print 'Distribution server is already running'
            sys.exit(1)

        self.db.info.insert({'server_port': self.mgr.transport.address[1]})


    def send_no_job(self, sender):
        print '\rNo jobs for worker', sender
        self.mgr.send_message(NoJobMessage(msg=0), sender)


    def handle_JobRequestMessage(self, msg):
        try:
            print 'Request from:', msg.sender,

            allowed_users = msg.get_property('msg')['allowed_users']
            possible_users = self.db.jobs.distinct('user_id')

            try:
                actual_user = [choice([i for i in possible_users if not allowed_users or i in allowed_users])]
            except:
                self.send_no_job(msg.sender)
                return True

            job = self.db.jobs.find_one({'samples': {'$gt':0}, 'user_id':{'$all':actual_user}})

            if not job:
                self.send_no_job(msg.sender)
                return True

            sample = job['samples']
            job['samples'] -= 1
            self.db.jobs.save(job)

            print '\rConstructing job: ', job['user_id'], job['job_id'], job['sim_id'], sample,

            job = self.db.xml.find_one({'job_id':job['job_id'], 'sim_id':job['sim_id'], 'type':'sim', 'user_id':job['user_id']})
            job['sample'] = sample
            del(job['_id'])

            print '\rSending job: ', job['user_id'], job['job_id'], job['sim_id'], sample,

            self.mgr.send_message(JobMessage(msg=job), msg.sender)

            print '\rSent job: ', job['user_id'], job['job_id'], job['sim_id'], sample
            print
        except Exception, e:
            print 'Job request error: ', e
            print 'Stack trace:'
            print_exc(file=sys.stdout)
            print

            # No job was found
            self.mgr.send_message(NoJobMessage(msg=0), msg.sender)

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
                    'user_id': user_id,
                    'job_id': job_id
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
                        'user_id': user_id,
                        'job_id': job_id
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


    def run(self):
        print 'Ready to distribute jobs...'
        while self.running:
            try:
                self.mgr.tick()
            except (KeyboardInterrupt, SystemExit):
                self.db.info.remove({'server_port': { '$exists': True }})
                self.running = False

                for j in self.db.jobs.find():
                    rem = j['total_samples'] - j['samples']
                    if rem > len(j['results']):
                        #TODO: replenish 'rem' jobs somehow taking into account the samples that have already arrived
                        pass


if __name__ == '__main__':
    DistributionServer().run()

