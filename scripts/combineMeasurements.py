import sys

"""
Arguments: output file name, input file name, number of samples, measurements per sample, no. of measurements
"""

output = open(sys.argv[1], 'w')

resultFile = open(sys.argv[2], 'r')
results = resultFile.readlines()
resultFile.close()

samples = int(sys.argv[3])
linesPerSample = int(sys.argv[4])
noMeasurements = int(sys.argv[5])

# Print header
headerArray = [i for i in results if i[0] == '#']
header = ''.join(headerArray)
output.writelines(header)

del(results[:len(headerArray)])

# Get samples
measurements = [ results[i * (linesPerSample + 1): (i + 1) * (linesPerSample + 1)] 
                     for i in range(samples) ]

# Group lines by iteration
lines = [ [ i[j].strip().replace('\n', '') for i in measurements ] 
            for j in range(1, len(measurements[0])) ]

# Join data
joined = '\n'.join([i[0].split(' ')[0] + ' ' + ' '.join([j.split(' ')[k]
                               for k in range(1, noMeasurements)
                               for j in i])
                               for i in lines])

# Write data to file and close
output.write(joined)
output.close()
#print(open(sys.argv[1], 'r').readlines())
