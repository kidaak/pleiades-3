from pysage import Message
import json
import datetime

def time(m):
    return m + ' : ' + str(datetime.datetime.now().isoformat())

class PleiadesMessage(Message):
    properties = ['msg']
    types = ['S']
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class JobRequestMessage(PleiadesMessage):
    packet_type = 101

class ResultMessage(PleiadesMessage):
    packet_type = 102

class JobMessage(PleiadesMessage):
    packet_type = 103

class AckResultMessage(PleiadesMessage):
    packet_type = 104

class NoJobMessage(PleiadesMessage):
    packet_type = 105

class NewJobMessage(PleiadesMessage):
    packet_type = 106

class JoinMessage(PleiadesMessage):
    packet_type = 107

class StatusMessage(PleiadesMessage):
    packet_type = 108

class KillMessage(PleiadesMessage):
    packet_type = 109

class JobErrorMessage(PleiadesMessage):
    packet_type = 110

class AckNewJobMessage(PleiadesMessage):
    packet_type = 111

class RmJarMessage(PleiadesMessage):
    packet_type = 112

class DyingMessage(PleiadesMessage):
    packet_type = 113

