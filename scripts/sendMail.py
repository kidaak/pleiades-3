import smtplib
from email.MIMEText import MIMEText
import sys

toSend = sys.argv[2]

msg = MIMEText(toSend)
msg['Subject'] = 'Pleiades Cluster Job Complete'
msg['From'] = 'Pleiades Cluster<no-reply@cs.up.ac.za>'
msg['To'] = sys.argv[1]

s = smtplib.SMTP('student.up.ac.za', 25, 'Pleiades')
s.sendmail('no-reply@cs.up.ac.za', [sys.argv[1]], msg.as_string())
s.quit()
