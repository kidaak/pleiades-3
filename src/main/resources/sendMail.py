import smtplib
from email.mime.text import MIMEText
import sys

toSend = sys.argv[2]

msg = MIMEText(toSend)
msg['Subject'] = 'enterSubjectHere'
msg['From'] = 'fromEmailAddressHere'
msg['To'] = sys.argv[1]

s = smtplib.SMTP('student.up.ac.za', 25, 'usernameHere')
s.sendmail('fromEmailAddressHere@somewhere.com', [sys.argv[1]], msg.as_string())
s.quit()
