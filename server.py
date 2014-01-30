#!/usr/bin/python2
import os, sys, time as timer
import logging as log
from pysage import *
from socket import *
from cStringIO import *
from shutil import *
from xml_uploader import *
from settings import *
from messages import *
from traceback import *
from gatherer import *
from network import *
from random import *

if os.path.exists('error.log'):
    os.remove('error.log')

log.basicConfig(level=log.DEBUG, filename='error.log')

class DistributionServer(Actor):
    subscriptions = ['JobRequestMessage', 'JobErrorMessage', 'ResultMessage', 'DyingMessage']

    def __init__(self):
        print 'Starting distribution server...'
        self.db, self.connection = mongo_connect(MONGO_RW_USER, MONGO_RW_PWD)

        #clear running queue
        for job in self.db.running.find():
            self.replenish_job(job['user_id'], job['job_id'], job['sim_id'], job['sample'])
        self.db.running.remove()

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
        #print '\rNo jobs for worker', sender
        self.mgr.send_message(NoJobMessage(msg=0), sender)


    def replenish_job(self, user_id, job_id, sim_id, sample_id):
        job = self.db.jobs.find_one({
            'user_id':user_id,
            'job_id':job_id,
            'sim_id':sim_id
        })

        if job:
            job['samples'].append(sample_id)
            self.db.jobs.save(job)
            return True

        return False


    def handle_JobRequestMessage(self, msg):
        try:
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

            sample = job['samples'].pop()
            self.db.jobs.save(job)

            job = self.db.xml.find_one({'job_id':job['job_id'], 'sim_id':job['sim_id'], 'type':'sim', 'user_id':job['user_id']})
            job['sample'] = sample
            del(job['_id'])

            #TODO: maybe check if worker is in running queue (is it possible?)
            self.mgr.send_message(JobMessage(msg=job), msg.sender)

            self.db.running.insert({
                'job_id': job['job_id'],
                'sim_id': job['sim_id'],
                'user_id': job['user_id'],
                'sample': job['sample'],
                'worker_id': msg.get_property('msg')['worker_id']
            })

            print '\rSent job:', job['user_id'], job['job_id'], job['sim_id'], sample, 'to', msg.sender, msg.get_property('msg')['worker_id']
            print
        except Exception, e:
            t = time('Job error')
            print t
            log.exception(t)

            # No job was found
            self.send_no_job(msg.sender)

        return True


    def handle_JobErrorMessage(self, msg):
        try:
            job = msg.get_property('msg')
            user_id = job['user_id']
            sim_id = job['sim_id']
            job_id = job['job_id']

            print time(), 'Received job error', user_id, job_id, sim_id

            sim = self.db.jobs.find_one({'user_id': user_id, 'sim_id': sim_id, 'job_id': job_id})

            if sim:
                self.db.jobs.remove(sim)
                #TODO: Better message
                sendmail(user_id, 'Error with simulation number ' + str(sim_id) + ' in job ' + sim['job_name'] + '.')
                print 'Removed job:', user_id, job_id, sim_id

                to_find = {'job_id':job_id, 'user_id':user_id}
                if self.db.jobs.find_one(to_find) == None:
                    # leave the directory in case there is salvageable stuff? it could get removed though
                    self.db.xml.remove(to_find)
                    self.db.fs.files.remove(to_find)

                    self.mgr.broadcast_message(RmJarMessage(msg={
                        'user_id': user_id,
                        'job_id': job_id
                    }))
                    sendmail(user_id, 'Your job, ' + sim['job_name'] + ', is complete.')

            print
        except:
            t = time('Job error error')
            print t
            log.exception(t)
        finally:
            #TODO: Send status with message
            self.mgr.send_message(AckResultMessage(msg=0), msg.sender)

        return True


    def handle_ResultMessage(self, msg):
        try:
            result = msg.get_property('msg')

            user_id = result['user_id']
            job_id = result['job_id']
            sim_id = result['sim_id']
            sample = result['sample']

            self.db.running.remove({'worker_id': result['worker_id']})

            sim = self.db.jobs.find_one({'job_id':job_id, 'sim_id':sim_id, 'user_id':user_id})
            if not sim:
                print 'Job not found in database:', user_id, job_id, sim_id
                return True

            job_name = sim['job_name']

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

            print
        except Exception, e:
            t = time('Result error')
            print t
            log.exception(t)
        finally:
            #TODO: Send status with message
            self.mgr.send_message(AckResultMessage(msg=0), msg.sender)

        return True


    def handle_DyingMessage(self, msg):
        try:
            print 'Man down!'
            worker = msg.get_property('msg')['worker_id']
            rjob = self.db.running.find_one({'worker_id':worker})

            if rjob:
                self.replenish_job(rjob['user_id'], rjob['job_id'], rjob['sim_id'], rjob['sample'])
                self.db.running.remove(rjob)
        except:
            t = time('Dying error')
            print t
            log.exception(t)

        return True


    def run(self):
        print 'Ready to distribute jobs...'
        t = timer.time()
        while self.running:
            try:
                if timer.time() - t > 300:
                    print time('Alive')
                    t = timer.time()

                self.mgr.tick()
            except (KeyboardInterrupt, SystemExit):
                print 'Clearing port information.'
                self.db.info.remove({'server_port': { '$exists': True }})
                self.running = False

            #except Exception:
            #    log.exception(time('Main error'))


if __name__ == '__main__':
    DistributionServer().run()

