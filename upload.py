#!/usr/bin/python2
from pysage import *
from settings import *
from messages import *
from socket import *
from network import *
from optparse import OptionParser
import sys, os

class Uploader(Actor):
    subscriptions = ['AckNewJobMessage', 'AckResultMessage']

    def __init__(self, args):
        self.db, self.connection = mongo_connect(MONGO_RO_USER, MONGO_RO_PWD)

        self.mgr = ActorManager.get_singleton()
        self.mgr.register_actor(self)
        self.connect()

        self.running = True
        self.args = args


    def connect(self):
        port = self.db.info.find_one({'upload_server_port': { '$exists': True }})
        if not port:
            print 'Upload server is not running'
            return False

        self.mgr.connect(transport.SelectTCPTransport, host=SERVER_IP, port=port['upload_server_port'])
        return True


    def handle_AckNewJobMessage(self, msg):
        status = msg.get_property('msg')['status']
        print status
        self.running = False
        return True


    def handle_AckResultMessage(self, msg):
        print 'Received ACK'
        #TODO: get status from message
        self.running = False
        return True


    def upload(self):
        if not options.type.lower() in ['custom', 'master', 'official']:
            print 'Jar type must be "custom", "master" or "official"'
            sys.exit(1)

        xmlfile = options.xmlfile
        name = xmlfile[xmlfile.rfind(os.path.sep) + 1:xmlfile.rindex('.')]

        try:
            with open(xmlfile, 'r') as xml:
                x = xml.read().encode('zlib').encode('base64')
        except:
            print 'Error reading XML file'
            sys.exit(1)

        try:
            with open(options.jarfile, 'rb') as jar:
                j = jar.read().encode('zlib').encode('base64')
        except:
            print 'Error reading JAR file'
            sys.exit(1)

        print 'Setting up file transfer...'
        sock = socket(AF_INET, SOCK_STREAM)
        local_ip = get_local_ip()
        sock.bind((local_ip, 0))
        sock.listen(1)

        print 'Sending files...'
        self.mgr.broadcast_message(NewJobMessage(msg={
            'user_id':self.args.user,
            'type':self.args.type,
            'socket':sock.getsockname(),
            'm_size':len(j),
            'xml':x,
            'name':name
        }))

        print sock.getsockname()
        try:
            s,a = sock.accept()
            sendall(s, j)
            sock.close()
        except:
            print 'Error sending JAR file'
            sys.exit(1)

        print 'Wating for reply...'
        while self.running:
            try:
                self.mgr.tick()
            except KeyboardInterrupt, SystemExit:
                self.running = False

        print 'Completing...'


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-j', '--jar', dest='jarfile',
                      help='cilib jar file to use', metavar='JAR')
    parser.add_option('-i', '--xml', dest='xmlfile',
                      help='xml input file to use', metavar='XML')
    parser.add_option('-u', '--user', dest='user',
                      help='username', metavar='USER')
    parser.add_option('-t', '--type', dest='type',
                      help='type of jar file: custom, master, official', metavar='TYPE')

    (options, args) = parser.parse_args()

    if not (options.jarfile and options.xmlfile and options.user and options.type):
        print 'Not enough arguments'
        parser.print_help()
        sys.exit(1)

    Uploader(options).upload()

