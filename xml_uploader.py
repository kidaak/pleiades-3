from lxml import etree
from pymongo import Connection

class XML_Uploader:
    def upload_xml(self, file, job, user):
        try:
            parser = etree.XMLParser(remove_blank_text=True)
            tree = etree.parse(file, parser)
        except etree.XMLSyntaxError as e:
            print('XML Syntax Error in {0}:\n{1}'.format(file, e.message))

        #find all <algorithm id=""/> elements
        algorithms = []
        alg_idrefs = []
        algs = tree.findall('.//algorithm[@id]')
        [algorithms.append(etree.tostring(e)) for e in algs]
        [alg_idrefs.append(e.get('id')) for e in algs]

        #find all <problem id=""/> elements
        problems = []
        prob_idrefs = []
        probs = tree.findall('.//problem[@id]')
        [problems.append(etree.tostring(e)) for e in probs]
        [prob_idrefs.append(e.get('id')) for e in probs]

        #find all <measurements id=""/> elements
        measurements = []
        meas_idrefs = []
        meas = tree.findall('.//measurements[@id]')
        [measurements.append(etree.tostring(e)) for e in meas]
        [meas_idrefs.append(e.get('id')) for e in meas]

        #find all <simulation samples=""/> elements
        #record samples and replace with 1
        #replace output filename with '_output_' placeholder
        #simulations = []
        samples = []
        sims = tree.findall('.//simulation[@samples]')
        [samples.append(e.get('samples')) for e in sims]
        [e.set('samples', '1') for e in sims]
        [e.find('./output').set('file', '_output_') for e in sims]
        #[simulations.append(etree.tostring(e)) for e in sims]

        print samples

        #upload to db
        _id = uuid.uuid4()
        self.upload_xml_strings(alg_idrefs, algorithms, 'alg', job, user, _id)
        self.upload_xml_strings(prob_idrefs, problems, 'prob', job, user, _id)
        self.upload_xml_strings(meas_idrefs, measurements, 'measure', job, user, _id)
        self.upload_simulations(sims, 'sim', job, user, _id)

        #construct jobs
        jobs = {}
        i = job
        for s in samples:
            jobs[i] = int(samples[i])
            i += 1

        return jobs

    def upload_xml_strings(self, id_list, xml_list, type, job, user, _id):
        db = get_database()[0]
        
        i = 0
        for e in xml_list:
            db.xml.insert({
                'id': _id,
                'type': type,
                'user_id': user,
                'idref': id_list[i],
                'value': e
            })
            i += 1

    def upload_simulations(self, sims, type, job, user, _id):
        db = get_database()[0]

        i = 0
        for e in sims:
            db.xml.insert({
                'id': _id,
                'type': type,
                'user_id': user,
                'job_id': job + i,
                'alg': e.find('./algorithm').get('idref'),
                'prob': e.find('./problem').get('idref'),
                'meas': e.find('./measurements').get('idref'),
                'value': etree.tostring(e)
            })
            i += 1

def get_database():
    connection = Connection('137.215.137.225', 27017)
    database = connection.test_pleiades
    database.authenticate('admin', '12345')
    return database, connection

p = XML_Uploader()
f = 'gbestPSO.xml'
jobs = p.upload_xml(f, '0', 'bennie')
print jobs
