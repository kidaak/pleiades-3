import sys
import os
import time
from shutil import *
from utils import *
from settings import *

COMPLETE_TEMPLATE = '''
Dear %s,

Your job, %s, is complete.

Have a nice day!

Regards,
Pleiades
'''

class Gatherer():

    def __init__(self):
        self.log = get_logger('gatherer')


    def gather_results(self, sim):
        try:
            self.log.info("Started gathering %s %d %d into %s" % (sim['user_id'], sim['job_id'], sim['sim_id'], sim['file_name']))
            results = []
            measurements = []

            with open(sim['results'][0]) as sample:
                comments = 0
                for i, l in enumerate(sample):
                    if l.startswith('#'):
                        comments += 1
                        measurements.append(l[l.find(' - ') + 3:l.find('(0)') - 1])

            noMeasurements = comments - 1

            samples = int(sim['total_samples'])

            # create header
            self.log.info('Header')
            header = '# 0 - Iterations\n'
            column = 1
            for m in measurements[1:]:
                for i in range(samples):
                    header += '# ' + str(column) + ' - ' + m + ' (' + str(i) + ')\n'
                    column += 1

            # Get samples
            self.log.info('Samples')
            measurements = []
            for f in sim['results']:
                self.log.info(f)
                with open(f, 'r') as sample:
                    measurements.append([line for line in sample if not line.startswith('#')])

            measurements = sorted(measurements, key=lambda x: -len(x))
            max_measurement = measurements[0]
            max_size = len(max_measurement)

            for m in range(len(measurements)):
                l_m = len(measurements[m])
                if l_m < max_size:
                    new_m = [i.split(' ') for i in max_measurement[l_m:]]
                    new_m = [i[0] + ' -' * (len(i) - 1) for i in new_m]
                    measurements[m] += new_m

            # Group lines by iteration
            self.log.info('Grouping')
            lines = [ [ i[j].strip().replace('\n', '') for i in measurements ] 
                        for j in range(len(measurements[0])) ]

            # Join data
            self.log.info('Joining')
            self.log.info(str(lines))
            joined = '\n'.join([i[0].split(' ')[0] + ' ' + ' '.join([j.split(' ')[k]
                                           for k in range(1, noMeasurements + 1)
                                           for j in i])
                                           for i in lines])

            file_path, file_name = os.path.split(sim['file_name'])
            output_dir = os.path.join(RESULTS_DIR, sim['user_id'], sim['job_name'], file_path)

            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            self.log.info('creating ' + os.path.join(output_dir, file_name))
            output = open(os.path.join(output_dir, file_name), 'w+')

            # Write data to file and close
            output.write(header)
            output.write(joined)
            output.close()
            self.log.info('Result file exists:' + str(os.path.exists(os.path.join(output_dir, file_name))))
        except Exception, e:
            self.log.exception('Gathering error')

        self.log.info('Gathering complete')


    def run(self):
        con = None

        while True:
            try:
                db, con = mongo_connect(MONGO_RW_USER, MONGO_RW_PWD)

                sims = db.jobs.find({'$where': 'this.results.length == this.total_samples'})
                for sim in sims:

                    user_id = sim['user_id']
                    job_id = sim['job_id']
                    sim_id = sim['sim_id']
                    job_name = sim['job_name']
                    jar_hash = sim['jar_hash']
                    output_dir = os.path.join(RESULTS_DIR, user_id, job_name, str(job_id), str(sim_id))

                    print "Gathering ", job_name, 'into', sim['file_name']
                    self.log.info("Gathering " + job_name + ' into ' + sim['file_name'])

                    # Gather, clear temp files and remove sim
                    self.gather_results(sim)
                    try:
                        rmtree(output_dir)
                    except OSError:
                        self.log.info('Folder does not exist: ' + output_dir) #TODO: would this affect the db, should we just remove it and alert the user?
                    db.jobs.remove(sim)

                    # if job is complete clear everything, distributor will notify to remove jar
                    to_find = {'job_id':job_id, 'user_id':user_id}
                    if db.jobs.find_one(to_find) == None:
                        self.log.info('Removing stuff from db and tmp files')
                        rmtree(output_dir[:output_dir.rfind(os.path.sep)])

                        db.xml.remove(to_find)

                    if db.jobs.find_one({'jar_hash':jar_hash}) == None:
                        db.fs.files.remove({'jar_hash':jar_hash})

                    if db.jobs.find_one({'job_name':job_name}) == None:
                        try:
                            sendmail(user_id, COMPLETE_TEMPLATE % (user_id, job_name))
                        except:
                            print "Could not send email."

            except Exception, e:
                print "ERROR", e
                self.log.exception('Gatherer error.')

            finally:
                if con:
                    print 'Closing mongo connection'
                    con.close()
                print 'Sleepy time'
                self.log.info('Sleepy time')
                time.sleep(30)

if __name__ == '__main__':
    Gatherer().run()

