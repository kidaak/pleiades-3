#!/bin/env python2
from pysage import *
from settings import *
from messages import *
from socket import *
from optparse import OptionParser
import sys, os

class Uploader(Actor):
    subscriptions = ['AckResultMessage']

    def __init__(self, args):
        self.mgr = ActorManager.get_singleton()
        self.mgr.register_actor(self)
        self.mgr.connect(transport.SelectTCPTransport, host=SERVER_IP, port=SERVER_PORT)
        self.running = True
        self.args = args

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
        local_ip = [ip for ip in gethostbyname_ex(gethostname())[2] if not ip == '127.0.0.1'][:1][0]
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
            s.sendall(j)
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
