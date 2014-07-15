#!/usr/bin/python2
import re, os, sys, time as timer
import logging as log
from pysage import *
from socket import *
from cStringIO import *
from shutil import *
from xml_uploader import *
from settings import *
from messages import *
from traceback import *
from utils import *
from random import *

ERROR_TEMPLATE = '''
Dear %s,

There was the following error with simulation number %i in job %s
running on worker %s.

The status of the error is:
%s

Have a nice day!

Regards,
Pleiades
'''

class DistributionServer(Actor):
    subscriptions = ['JobRequestMessage', 'JobErrorMessage', 'ResultMessage', 'DyingMessage']

    def __init__(self):
        print 'Starting distribution server...'
        self.log = get_logger('server')
        self.log.info('New run')
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


    def write_info(self, output_dir, sim):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            with open(os.path.join(output_dir, '.info'), 'w+') as info_file:
                info_file.write('user_id: ' + str(sim['user_id']) + '\n')
                info_file.write('job_name: ' + str(sim['job_name']) + '\n')
                info_file.write('file_name: ' + str(sim['file_name']) + '\n')
                info_file.write('samples: ' + str(sim['total_samples']) + '\n')
                info_file.write('job_id: ' + str(sim['job_id']) + '\n')
                info_file.write('sim_id: ' + str(sim['sim_id']) + '\n')


    def handle_JobRequestMessage(self, msg):
        try:
            worker_id = msg.get_property('msg')['worker_id']
            worker_host = worker_id.split('-')[0]
            #self.log.info('Request from ' + worker_id)

            allowed_users = msg.get_property('msg')['allowed_users']
            available_users = self.db.jobs.find({'samples': {'$not': {'$size': 0}}}).distinct('user_id')
            possible_users = [i for i in available_users if not allowed_users or i in allowed_users]

            actual_user = self.db.running.aggregate([
                {'$match': {'user_id': {'$in': possible_users}, 'worker_id': re.compile('^' + worker_host + '.*')}},
                {'$group': {'_id': '$user_id', 'count': {'$sum': 1}}}
            ])['result']

            for i in possible_users:
                if i not in [j['_id'] for j in actual_user]:
                    actual_user.append({'_id':i, 'count':0})

            try:
                min_count = sorted(actual_user, key=lambda x:x['count'])[0]['count']
                actual_user = choice([i['_id'] for i in actual_user if i['count'] == min_count])
            except:
                try:
                    actual_user = choice(possible_users)
                except:
                    self.send_no_job(msg.sender)
                    return True

            job = self.db.jobs.find({
                    'samples': {'$not': {'$size': 0}},
                    'user_id': actual_user
                }).sort([
                    ('job_id', 1),
                    ('sim_id', 1)
                ]).limit(1)

            if not job.count():
                #self.log.info('No job')
                self.send_no_job(msg.sender)
                return True

            job = job.next()
            sample = job['samples'].pop()
            jar_hash = job['jar_hash']
            self.db.jobs.save(job)

            job = self.db.xml.find_one({'job_id':int(job['job_id']), 'sim_id':int(job['sim_id']), 'type':'sim', 'user_id':job['user_id']})
            job['sample'] = int(sample)
            job['jar_hash'] = jar_hash
            del(job['_id'])

            try:
                self.mgr.send_message(JobMessage(msg=job), msg.sender)

                for in_running in self.db.running.find({'worker_id': worker_id}):
                    self.replenish_job(in_running['user_id'], in_running['job_id'], in_running['sim_id'], in_running['sample'])
                    self.db.running.remove(in_running)
            except transport.NotConnectedException:
                # Couldn't send job? Prolly cos of constant reconnecting
                self.log.info('Job not sent. Worker not connected: ' + worker_id)
                print 'Job not sent. Worker not connected: ' + worker_id

                self.replenish_job(job['user_id'], job['job_id'], job['sim_id'], job['sample'])
                return True

            self.db.running.insert({
                'job_id': job['job_id'],
                'sim_id': job['sim_id'],
                'user_id': job['user_id'],
                'sample': job['sample'],
                'worker_id': worker_id,
                'custom': bool(allowed_users)
            })

            print 'Sent job:', job['user_id'], job['job_id'], job['sim_id'], sample, 'to', worker_id
            self.log.info('Sent job: %s %i %i %i %s' % (job['user_id'], job['job_id'], job['sim_id'], sample, worker_id))
        except Exception, e:
            print 'Job error'
            self.log.exception('Job error')

            # No job was found
            self.send_no_job(msg.sender)

        return True


    def handle_JobErrorMessage(self, msg):
        try:
            job = msg.get_property('msg')

            user_id = job['user_id']
            sim_id = job['sim_id']
            job_id = job['job_id']
            sample = job['sample']
            jar_hash = job['jar_hash']
            worker_id = job['worker_id']
            replenish = job['replenish']
            error = job['error'].decode('base64').decode('zlib')

            self.db.running.remove({'worker_id': worker_id})

            self.log.info('Received job error %s %i %i %i from %s' % (user_id, job_id, sim_id, sample, worker_id))
            self.log.info(error)
            print 'Received job error %s %i %i %i from %s' % (user_id, job_id, sim_id, sample, worker_id)
            print error

            if replenish:
                self.replenish_job(user_id, job_id, sim_id, sample)
                return True

            sim = self.db.jobs.find_one({'user_id': user_id, 'sim_id': sim_id, 'job_id': job_id})

            if sim:
                job_name = sim['job_name']
                jar_hash = sim['jar_hash']
                self.db.jobs.remove(sim)
                sendmail(user_id, ERROR_TEMPLATE % (user_id, sim_id, job_name, worker_id, error), self.log)
                print 'Removed job:', user_id, job_id, sim_id
                self.log.info('Removed job: %s %i %i %i' % (user_id, job_id, sim_id, sample))

                to_find = {'job_id':job_id, 'user_id':user_id}
                if self.db.jobs.find_one(to_find) == None:
                    # leave the directory in case there is salvageable stuff? it could get removed though
                    self.db.xml.remove(to_find)

                    sendmail(user_id, 'Your job, ' + sim['job_name'] + ', is complete.', self.log)
                    self.log.info('Email sent: Your job, ' + sim['job_name'] + ', is complete.')

                if self.db.jobs.find_one({'jar_hash':jar_hash}) == None:
                    self.db.fs.files.remove({'jar_hash':jar_hash})
        except:
            print 'Job error error'
            self.log.exception('Job error error')

        finally:
            #TODO: Send status with message
            self.log.info('JobError complete')
            self.mgr.send_message(AckResultMessage(msg=0), msg.sender)
            self.log.info('ACK sent')

        return True


    def handle_ResultMessage(self, msg):
        try:
            result = msg.get_property('msg')

            user_id = result['user_id']
            job_id = result['job_id']
            sim_id = result['sim_id']
            sample = result['sample']
            jar_hash = result['jar_hash']
            worker_id = result['worker_id']

            self.log.info("Received result for: %s %i %i %i from %s" % (user_id, job_id, sim_id, sample, worker_id))
            self.db.running.remove({'worker_id': worker_id})

            sim = self.db.jobs.find_one({'job_id':int(job_id), 'sim_id':int(sim_id), 'user_id':user_id})
            if not sim:
                print 'Job not found in database:', user_id, job_id, sim_id
                self.log.info('Job not found (already removed?)')
                return True

            job_name = sim['job_name']

            print "Received result for: ", user_id, job_id, sim_id, sample, 'from', worker_id

            tmp_file = os.path.join(TMP_RESULTS_DIR, '%s_%i_%i_%i.txt' % (user_id, job_id, sim_id, sample))
            output_dir = os.path.join(RESULTS_DIR, str(user_id), str(job_name), str(job_id), str(sim_id))
            file_name = os.path.join(output_dir, str(sample) + '.txt')

            self.write_info(output_dir, sim)

            if not os.path.exists(tmp_file):
                #TODO: remove job?
                print 'Result file not found in filesystem:', tmp_file
                self.log.info('Result file not found in filesystem:' + tmp_file)
                return True

            move(tmp_file, file_name)
            if os.path.exists(file_name):
                self.log.info('Result file moved')
            else:
                self.log.info('ERROR: Result file not moved')

            if not file_name in sim['results']:
                sim['results'].append(file_name)
            self.db.jobs.save(sim)

        except Exception, e:
            print 'Result error'
            log.exception('Result error')

        finally:
            #TODO: Send status with message
            self.log.info('ResultMessage complete')
            self.mgr.send_message(AckResultMessage(msg=0), msg.sender)
            self.log.info('ACK sent')

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
            print 'Dying error'
            log.exception('Dying error')

        return True


    def run(self):
        print 'Ready to distribute jobs...'

        print_timer = timer.time()

        while self.running:
            try:
                if timer.time() - print_timer > 300:
                    print time('Alive')
                    print_timer = timer.time()

                #TODO: check if db is consistent every n min

                self.mgr.tick()
            except (KeyboardInterrupt, SystemExit):
                print 'Clearing port information.'
                self.db.info.remove({'server_port': { '$exists': True }})
                self.running = False
                self.log.info('You killed it')

            except error:
                self.db.info.remove({'server_port': { '$exists': True }})
                self.listen()

            except Exception:
                log.exception(time('Main error'))


if __name__ == '__main__':
    DistributionServer().run()

