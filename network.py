from database import *
from netifaces import *
from smtplib import *
from email.MIMEText import *

def get_local_ip():
    ip_list = []
    for interface in interfaces():
        try:
            for link in ifaddresses(interface)[AF_INET]:
                ip_list.append(link['addr'])
        except:
            continue
    return [ip for ip in ip_list if not (ip.startswith('127.') or ip.startswith('10.'))][0]


def recvall(sock, size):
    data = ''
    while len(data) < size:
        d = sock.recv(size - len(data))
        if not d:
            return None
        data += d
    return data


def sendall(sock, data):
    while data:
        sent = sock.send(data)
        data = data[sent:]


def sendmail(user, msg):
    email_addr = get_user_email(user)

    msg = MIMEText(msg)
    msg['Subject'] = 'Pleiades Cluster Notification'
    msg['From'] = 'Pleiades Cluster<no-reply@cs.up.ac.za>'
    msg['To'] = email_addr

    s = SMTP('student.up.ac.za', 25, 'Pleiades')
    s.sendmail('no-reply@cs.up.ac.za', [email_addr], msg.as_string())
    s.quit()
