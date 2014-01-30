from lxml import etree
from network import *

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
        #record output filename and replace with '_output_' placeholder
        #simulations = []
        samples = []
        filenames = []
        sims = tree.findall('.//simulation[@samples]')
        [samples.append(e.get('samples')) for e in sims]
        [e.set('samples', '1') for e in sims]
        [filenames.append(e.find('./output').get('file')) for e in sims]
        [e.find('./output').set('file', '_output_') for e in sims]
        #[simulations.append(etree.tostring(e)) for e in sims]

        #upload to db
        self.upload_xml_strings(alg_idrefs, algorithms, 'alg', job, user)
        self.upload_xml_strings(prob_idrefs, problems, 'prob', job, user)
        self.upload_xml_strings(meas_idrefs, measurements, 'meas', job, user)
        self.upload_simulations(sims, job, user)

        #construct jobs
        jobs = []
        i = 0
        for s in samples:
            jobs.append((i, int(samples[i]), filenames[i]))
            i += 1

        return jobs

    def upload_xml_strings(self, id_list, xml_list, type, job, user):
        db, con = mongo_connect(MONGO_RW_USER, MONGO_RW_PWD)
        
        i = 0
        for e in xml_list:
            db.xml.insert({
                'job_id': job,
                'type': type,
                'user_id': user,
                'idref': id_list[i],
                'value': e
            })
            i += 1
        con.close()

    def upload_simulations(self, sims, job, user):
        db, con = mongo_connect(MONGO_RW_USER, MONGO_RW_PWD)

        i = 0
        for e in sims:
            db.xml.insert({
                'job_id': job,
                'sim_id': i,
                'type': 'sim',
                'user_id': user,
                'alg': e.find('./algorithm').get('idref'),
                'prob': e.find('./problem').get('idref'),
                'meas': e.find('./measurements').get('idref'),
                'value': etree.tostring(e)
            })
            i += 1
        con.close()

