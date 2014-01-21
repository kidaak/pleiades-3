from pysage import Message
import json

class JobRequestMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 101
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class ResultMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 102
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class JobMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 103
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class AckResultMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 104
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class NoJobMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 105
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class NewJobMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 106
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class RequestStatusMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 107
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class StatusMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 108
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

class KillMessage(Message):
    properties = ['msg']
    types = ['S']
    packet_type = 109
    def pack_msg(self, msg):
        return json.dumps(msg)
    def unpack_msg(self, msg_s):
        return json.loads(msg_s)

