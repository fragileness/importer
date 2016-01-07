import sys
import csv, StringIO
from kafka import KafkaConsumer
from collections import defaultdict
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText

mail_from_addr = "SW3_Postman <SW3_Postman@pegatroncorp.com>"
mail_to_addrs = []
mail_to_addrs += ['Onegin_Liang@pegatroncorp.com']
mail_to_addrs += ['Tommy_Chu@pegatroncorp.com']
mail_to_addrs += ['Paul1_Huang@pegatroncorp.com']
EMAIL_FROM = mail_from_addr
EMAIL_RECEIVERS = mail_to_addrs
EMAIL_SUBJECT = "FACTORY ALERT"

def listToStr(lst):
	"""This method makes comma separated list item string"""
	return ','.join(lst)

def notify_mail(project, tsp):
	msg = "FACTORY ALERT!!! Project: %s, TSP: %s" % (project, tsp)
	print msg
	mail_obj = MIMEMultipart()
	mail_obj['Subject'] = EMAIL_SUBJECT
	mail_obj['From'] = EMAIL_FROM
	mail_obj['To'] = listToStr(EMAIL_RECEIVERS)
	mail_obj.preamble = "This is a multi-part message in MIME format."
	mail_obj.epilogue = ''
	msg_txt = MIMEText(msg)
	msg_txt.replace_header('Content-Type', 'text/html; charset="big5"')
	mail_obj.attach(msg_txt)
	msg_body = mail_obj.as_string()
	smtpObj = smtplib.SMTP('relay-b.pegatroncorp.com')
	smtpObj.sendmail(EMAIL_FROM, EMAIL_RECEIVERS, msg_body)
	smtpObj.quit()

def parse_str(str, expected_project):
	fieldnames = ("TEST", "STATUS", "VALUE", "U_LIMIT", "L_LIMIT", "TEST_TIME")
	reader = csv.DictReader(StringIO.StringIO(str), fieldnames)
	reader.next()
	rows = list(reader)
	tsp = "NOT_FOUND"
	res = "NOT_FOUND"
	val = 1
	project = "NOT_FOUND"
	for row in rows:
		if (row['TEST'] == "TSP"):
			tsp = row['VALUE']
		elif (row['TEST'] == "Test Status"):
			res = row['VALUE']
			if (row['VALUE'] == "PASS"):
				val = 0
		elif (row['TEST'] == "PROJECT"):
			project = row['VALUE']
	if ((expected_project != None) and (project != expected_project)):
		print("Project: %s, TSP: %s, result: %s" % (project, tsp, res))
		return;
	total_fail[tsp] += val
	if (val == 0):
		continuous_fail[tsp] = 0
	else:
		continuous_fail[tsp] += val
	print("Project: %s, TSP: %s, result: %s, (t, c)=(%d/5, %d/3)" % (project, tsp, res, total_fail[tsp], continuous_fail[tsp]))
	if (total_fail[tsp] > 5) or (continuous_fail[tsp] >= 3):
		total_fail[tsp] = 0
		continuous_fail[tsp] = 0
		notify_mail(project, tsp)

expected_project = None
if (len(sys.argv) > 1):
	expected_project = sys.argv[1]

total_fail = defaultdict(int)
continuous_fail = defaultdict(int)

consumer = KafkaConsumer('test', bootstrap_servers=['172.22.248.49:9092'])

print("Waiting project %s..." % (expected_project))
for message in consumer:
	print("%s:%d:%d: key=%s" % (message.topic, message.partition, message.offset, message.key))
	parse_str(message.value, expected_project)
