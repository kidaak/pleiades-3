#!/usr/bin/python2
from pysage import *
from settings import *
from messages import *
from utils import *
from xml_uploader import *
from subprocess import *
from traceback import *
from socket import *
from smb.SMBConnection import SMBConnection
import logging
import curses
import time
import uuid
import os, os.path
import sys
import random
import re

HEADER = '''
  _____  _      _           _
 |  __ \\| |    (_)         | |
 | |__) | | ___ _  __ _  __| | ___ ___
 |  ___/| |/ _ \\ |/ _` |/ _` |/ _ | __| * Cluster *
 | |    | |  __/ | (_| | (_| |  __|__ \\
 |_|    |_|\\___|_|\\__,_|\\__,_|\\___|___/
'''

class Worker(Actor):
    subscriptions = ['JobMessage', 'NoJobMessage', 'AckResultMessage', 'RmJarMessage']

    def __init__(self, worker_id):
        self.log = get_logger(worker_id)
        self.req_log = get_logger(worker_id + '_request')
        self.log.info('Starting worker')

        self.mgr = mgr
        self.mgr.register_actor(self)

        self.worker_id = worker_id
        self.connect()
        self.send_job_request()


    def connect(self):
        if self.mgr.transport and self.mgr.transport._is_connected:
            self.mgr.disconnect()

        connected = False

        while not connected:
            try:
                self.log.info('Connecting...')
                port = get_port('server_port')

                self.mgr.connect(transport.SelectTCPTransport, host=SERVER_IP, port=port)
                if self.mgr.transport._is_connected:
                    connected = True
            except:
                time.sleep(RECONNECT_TIME)

        self.log.info('Connected to distribution server.')

        return True


    def send_status(self, status, job_id, sim_id, user_id, sample):
        try:
            self.mgr.queue_message_to_group(self.mgr.PYSAGE_MAIN_GROUP,StatusMessage(msg={
                'status': status,
                'job_id': job_id,
                'sim_id': sim_id,
                'user_id':user_id,
                'sample': sample,
                'worker_id': self.worker_id
            }))
        except:
            pass


    def send_job_request(self):
        self.ack_timer = time.time()
        self.req_log.info('')

        self.mgr.broadcast_message(JobRequestMessage(msg={
            'allowed_users': ALLOWED_USERS,
            'worker_id': self.worker_id
        }))


    def handle_JobMessage(self, msg):
        try:
            self.errors = ''
            def set_status(s):
                self.status = s
                self.log.info(s)

            set_status('Received job')
            sim = msg.get_property('msg')

            user_id = sim['user_id']
            sim_id = sim['sim_id']
            job_id = sim['job_id']
            sample = sim['sample']

            self.send_status('Starting job', job_id, sim_id, user_id, sample)

            out_name = os.path.join('results', self.worker_id)
            jar_name = os.path.join('jars', '%s_%i_%s.run' % (user_id, job_id, self.worker_id))
            xml_name = os.path.join('xmls', '%s_%i_%i_%i.xml' % (user_id, job_id, sim_id, sample))

            set_status('Writing input files')
            self.send_status('Writing input files', job_id, sim_id, user_id, sample)

            xml = construct_xml(sim, out_name)
            set_status('1')
            if not xml:
                self.errors = 'Error constructing XML (check idrefs?)'
                set_status(self.errors)
                raise Exception(self.errors)
            set_status('2')

            with open(xml_name, 'w') as xml_file:
                xml_file.write(xml)
            set_status('3')

            if not os.path.exists(jar_name):
                set_status('4')
                with open(jar_name, 'wb') as jar_file:
                    jar_file.write(get_file(job_id, user_id))
            set_status('5')

            process = Popen(['java', '-jar', jar_name, xml_name], stdout=PIPE, stderr=PIPE)
            p_timer = time.time()
            set_status('6')

            while process.poll() == None:
                status = process.stdout.read(128)

                if time.time() - p_timer > 0.3:
                    try:
                        s = re.findall('\(.*?\)', status)[-1]
                    except:
                        s = ''

                    self.send_status(s, job_id, sim_id, user_id, sample)
                    p_timer = time.time()

            os.remove(xml_name)
            set_status('Execution finished')

            p1, p2 = process.communicate()
            self.errors = p2
            if self.errors:
                set_status(self.errors)
                raise Exception('CIlib error')

            else:
                set_status('Posting results')
                with open(out_name, 'r') as result_file:
                    conn = SMBConnection(SMB_USER, SMB_PWD, '', '', use_ntlm_v2=True)
                    assert conn.connect(SMB_IP, timeout=SMB_TIMEOUT)
                    conn.storeFile(SMB_SHARE, "%s_%i_%i_%i.txt" % (user_id, job_id, sim_id, sample), result_file, timeout=SMB_TIMEOUT)
                    conn.close()

                os.remove(out_name)

                set_status('Result notification')
                self.connect()
                self.mgr.broadcast_message(ResultMessage(msg={
                    'job_id': job_id,
                    'sim_id': sim_id,
                    'user_id': user_id,
                    'sample': sample,
                    'worker_id': self.worker_id
                }))

                set_status('Status update')
                self.send_status('Done', job_id, sim_id, user_id, sample)
                set_status('Job Done')
        except error:
            self.log.info('con error')
            self.connect()
            #TODO: and then?

        except Exception, e:
            try:
                print self.status
                self.log.exception('Worker job error')
                self.connect()
                self.mgr.broadcast_message(JobErrorMessage(msg={
                    'job_id': job_id,
                    'sim_id': sim_id,
                    'user_id': user_id,
                    'sample': sample,
                    'error': self.status.encode('zlib').encode('base64'),
                    'replenish': not self.errors,
                    'worker_id': self.worker_id
                }))

                set_status('Sending error progress thingy')
                self.send_status('Error', job_id, sim_id, user_id, sample)
            except:
                self.log.info('Sending error error')
                self.connect()

        finally:
            set_status('JobMessage complete')
            self.ack_timer = time.time()

        return True


    def handle_RmJarMessage(self, msg):
        m = msg.get_property('msg')
        jar_file = os.path.join('jars', '%s_%i_%s.run' % (m['user_id'], m['job_id'], self.worker_id))

        if os.path.exists(jar_file):
            self.log.info('Removing jar: %s' % jar_file)
            os.remove(jar_file)

        return False


    def handle_NoJobMessage(self, msg):
        self.send_status('No job', -1, -1, '', -1)
        time.sleep(5)
        self.send_job_request()
        return True


    #TODO: maybe just resend without wating for ack
    def handle_AckResultMessage(self, msg):
        self.log.info('ACK received')
        time.sleep(random.uniform(1, 5))
        self.connect()
        self.send_job_request()
        return True


    def update(self):
        if time.time() - self.ack_timer > ACK_TIMEOUT:
            self.log.info('ACK timeout: reconnecting')
            self.connect()


class WorkerProgress(Actor):
    subscriptions = ['StatusMessage', 'JoinMessage', 'DyingMessage']

    def __init__(self, w_count, quiet=False):
        self.mgr = mgr
        self.workers = {}
        self.quiet = quiet
        self.log = get_logger('progress')
        self.ex_log = get_logger('progress_ex')
        self.w_count = w_count

        for i in ['jars', 'xmls']:
            if not os.path.exists(i):
                os.mkdir(i)

        for i in range(w_count):
            worker_id = get_local_ip() + '-' + str(i)
            self.workers[worker_id] = {
                'status': 'Init',
                'user_id': '',
                'sim_id': -1,
                'job_id': -1,
                'sample': -1,
                'time': time.time()
            }
            self.mgr.add_process_group(worker_id, lambda: Worker(worker_id))


    def handle_StatusMessage(self, msg):
        status = msg.get_property('msg')
        worker_id = status['worker_id']
        del(status['worker_id'])
        status['time'] = time.time()
        if not status['status']:
            status['status'] = self.workers[worker_id]['status']
        self.workers[worker_id] = status
        return False


    def handle_DyingMessage(self, msg):
        status = msg.get_property('msg')
        worker_id = status['worker_id']
        self.log.info(worker_id + ' died')
        return False


    def run(self, scr):
        if scr:
            screen = curses.newpad(1024, 1024)
            screen.scrollok(True)

        print_timer = progress_timer = timeout_timer = time.time()
        while True:
            try:
                # If worker has not sent status in last WORKER_RESTART_TIMEOUT min, restart it
                '''if time.time() - timeout_timer > 60:
                    for k, v in self.workers.iteritems():
                        if time.time() - v['time'] > WORKER_RESTART_TIMEOUT:
                            self.log.info('Periodic check: Restarting ' + k)
                            self.mgr.remove_process_group(k)
                            self.workers[k] = {
                                'status': 'Init',
                                'user_id': '',
                                'sim_id': -1,
                                'job_id': -1,
                                'sample': -1,
                                'time': time.time()
                            }
                            self.mgr.add_process_group(k, lambda: Worker(k))
                    self.log.info(str(len(self.mgr.groups)) + ' workers')
                    timeout_timer = time.time()'''

                # Print to screen
                if time.time() - print_timer > 0.3 and not self.quiet:
                    try:
                        max_y, max_x = scr.getmaxyx()

                        screen.clear()
                        screen.addstr(HEADER)
                        screen.addstr('\n%s %s %s %s %s %s' % (
                                    'Worker ID'.ljust(28), 'User ID'.ljust(10), 'Job', 'Sim', 'Sam', 'Status'
                                ))

                        line_len = 53 #length of longest line in HEADER
                        for k,v in sorted(self.workers.iteritems(), key=lambda x:int(x[0].split('-')[-1])):
                            if v:
                                try:
                                    percent = int(float(v['status'][1:-1]))
                                    s = v['status'].ljust(6) + ' |' + ('='*(percent / 10)).ljust(10) + '|'
                                except:
                                    s = v['status'].replace('\n', '')

                                line = '\nWorker %s: %s %3i %3i %3i %s' % (
                                    k.ljust(20), v['user_id'].ljust(10), v['job_id'], v['sim_id'], v['sample'], s
                                )

                                screen.addstr(line)
                                line_len = max(line_len, len(line))

                        screen.refresh(0, 0, 0, 0, max_y - 1, min(max_x - 1, line_len))
                        curses.doupdate()
                        print_timer = time.time()

                    except curses.error:
                        self.ex_log.exception('Curses error')

                # Upload progress every minute
                if time.time() - progress_timer > 60:
                    con = None
                    try:
                        db, con = mongo_connect(MONGO_RW_USER, MONGO_RW_PWD)

                        for k, v in self.workers.iteritems():
                            db.progress.update({'worker_id': k}, {'$set': v}, upsert=True, w=0)

                    except Exception, e:
                        self.log.exception('Writing progress')
                        self.ex_log.exception('Writing progress')
                    finally:
                        if con:
                            con.close()
                        progress_timer = time.time()

                self.mgr.tick()
            except KeyboardInterrupt, SystemExit:
                self.mgr.clear_process_group()
                self.log.info('You killed it!')
                sys.exit(1)

            except error:
                self.ex_log.exception('Con Error')
                self.log.info('Con Error')
                for g, (p, _id, b) in self.mgr.groups.items():
                    if not p.is_alive():
                        self.log.info('Con error: Restarting ' + g)
                        self.mgr.remove_process_group(g)
                        self.mgr.add_process_group(g)

            except EOFError, e:
                self.ex_log.exception('Con Error (IPC recv)')
                for g, (p, _id, b) in self.mgr.groups.items():
                    if not p.is_alive():
                        self.log.info('Con Error (IPC recv): Restarting ' + g)
                        self.mgr.remove_process_group(g)
                        self.mgr.add_process_group(g)

            except IOError:
                self.ex_log.exception('Worker died (IPC poll)')
                self.log.info('Worker died (IPC poll)')
                for g, (p, _id, b) in self.mgr.groups.items():
                    if not p.is_alive():
                        self.log.info('Worker died (IPC poll): Restarting ' + g)
                        self.mgr.remove_process_group(g)
                        self.mgr.add_process_group(g)

            except GroupFailed, e:
                self.ex_log.exception('Worker died (group)')
                self.log.info('Worker died (group): ' + e.group_name)
                self.mgr.remove_process_group(e.group_name)
                self.mgr.add_process_group(e.group_name, lambda: Worker(e.group_name))

            except Exception, e:
                self.ex_log.exception('ERRORORROR')
                self.log.info('Exception: ' + e)


mgr = ActorManager.get_singleton()
if __name__ == '__main__':
    try:
        w_count = int(sys.argv[1])
    except:
        w_count = 1

    quiet = 'q' in sys.argv

    mgr.enable_groups()
    p = WorkerProgress(w_count, quiet)
    mgr.register_actor(p)

    if quiet:
        p.run(None)
    else:
        curses.wrapper(p.run)

