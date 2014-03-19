from utils import *
from time import sleep
import psutil

db, con = mongo_connect(MONGO_RW_USER, MONGO_RW_PWD)

print("Connected to database.\nNow reporting status...")

while True:
    ip = get_local_ip()
    cpu = psutil.cpu_percent()
    mem = psutil.phymem_usage()[3]
    disk = psutil.disk_usage('/home')[3]

    db.health.update({'node_id':ip}, {'$push': {'cpu': {'$each':[cpu], '$slice': -120}}}, True)
    db.health.update({'node_id':ip}, {'$push': {'mem': {'$each':[mem], '$slice': -120}}}, True)
    db.health.update({'node_id':ip}, {'$push': {'disk': {'$each':[disk], '$slice': -120}}}, True)
    sleep(10)
