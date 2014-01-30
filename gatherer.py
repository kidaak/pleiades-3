import sys, os
from network import *
from settings import *

class Gatherer:
    def gather_results(self, sim):

        results = []
        measurements = []
        
        with open(sim['results'][0]) as sample:
            comments = 0
            for i, l in enumerate(sample):
                if l.startswith('#'):
                    comments += 1
                    measurements.append(l[l.find(' - ') + 3:l.find('(0)') - 1])

        for f in sim['results']:
            with open(f, 'r') as sample:
                [results.append(line) for line in sample if not line.startswith('#')]

        linesPerSample = i - comments + 1
        noMeasurements = comments - 1

        file_path, file_name = os.path.split(sim['file_name'])
        
        output_dir = os.path.join(RESULTS_DIR, sim['user_id'], sim['job_name'], file_path)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output = open(os.path.join(output_dir, file_name), 'w+')

        samples = int(sim['total_samples'])

        # create header
        header = '# 0 - Iterations\n'
        column = 1
        for m in measurements[1:]:
            for i in range(samples):
                header += '# ' + str(column) + ' - ' + m + ' (' + str(i) + ')\n'
                column += 1

        # Get samples
        measurements = [ results[i * (linesPerSample): (i + 1) * (linesPerSample)] 
                             for i in range(samples) ]

        # Group lines by iteration
        lines = [ [ i[j].strip().replace('\n', '') for i in measurements ] 
                    for j in range(0, len(measurements[0])) ]

        # Join data
        joined = '\n'.join([i[0].split(' ')[0] + ' ' + ' '.join([j.split(' ')[k]
                                       for k in range(1, noMeasurements + 1)
                                       for j in i])
                                       for i in lines])

        # Write data to file and close
        output.write(header)
        output.write(joined)
        output.close()

