import sys
import csv, StringIO
from kafka import KafkaConsumer
from collections import defaultdict
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
import urllib
import urllib2
import json
import os
import time
import datetime
import traceback
import logging
import private
LOG_PREFIX = 'mla_complex_v2_consumer'
LOG_POSTFIX = '.log'

EMAIL_SUBJECT = "[MLA] Factory Alert"
MO_LIMIT = 50

def listToStr(lst):
	"""This method makes comma separated list item string"""
	return ','.join(lst)

def notify_mail(project, mo, tsp, tests):
	msg = "Project: %s\nMO: %s\nTSP: %s\n\n%s" % (project, mo, tsp, tests)
	print msg
	mail_obj = MIMEMultipart()
	mail_obj['Subject'] = EMAIL_SUBJECT
	mail_obj['From'] = private.mail_from_addr
	mail_obj['To'] = listToStr(private.get_mail_list())
	mail_obj.preamble = "This is a multi-part message in MIME format."
	mail_obj.epilogue = ''
	msg_txt = MIMEText(msg.replace("\n", "<br>"))
	msg_txt.replace_header('Content-Type', 'text/html; charset="big5"')
	mail_obj.attach(msg_txt)
	msg_body = mail_obj.as_string()
	smtpObj = smtplib.SMTP(private.mail_server_addr)
	smtpObj.sendmail(private.mail_from_addr, private.get_mail_list(), msg_body)
	smtpObj.quit()

def notify_rocket(addr, user, password, room, msg):
	#====login====
	print ">>>> login"

	url = addr + '/api/login'
	data = urllib.urlencode({'password' : password, 'user' : user})
	print data
	resp = urllib2.urlopen(url, data=data).read()
	print resp

	jdata = json.loads(resp)
	status = jdata["status"]
	print "status=" + status
	authToken = jdata["data"]["authToken"]
	print "authToken=" + authToken
	userId = jdata["data"]["userId"]
	print "userId=" + userId

	#====send message====
	print ">>>> send message"

	url = addr + '/api/rooms/' + room + '/send'
	req = urllib2.Request(url)
	req.add_header('X-Auth-Token', authToken)
	req.add_header('X-User-Id', userId)
	req.add_header('Content-Type', 'application/json')
	data = "{\"msg\" : \"" + msg + "\"}"
	resp = urllib2.urlopen(req, data=data).read()
	print resp

	#====logout====
	print ">>>> logout"

	url = addr + '/api/logout'

	req = urllib2.Request(url)
	req.add_header('X-Auth-Token', authToken)
	req.add_header('X-User-Id', userId)

	resp = urllib2.urlopen(req).read()
	print resp
	jdata = json.loads(resp)
	status = jdata["status"]
	print "status=" + status
	message = jdata["data"]["message"]
	print "status=" + message

def notify_im(project, mo, item, reason, timestamp):
	msg = str(os.getpid()) + ", " + timestamp + ", Alert, " + project + ", " + mo + ", " + item + " " + reason
	notify_rocket(private.rocket_url, private.rocket_user, private.rocket_password, private.rocket_room, msg)

def reinit_counter(index):
	total_fail.pop(index)
	total_fail.insert(index, defaultdict(int))
	continuous_fail.pop(index)
	continuous_fail.insert(index, defaultdict(int))
	return

def mo_dump():
	logger.info("dump ordered_mo_list:")
	for i in range(len(ordered_mo_list)):
		logger.info(" [%s] %s" % (i, ordered_mo_list[i]))

def add_mo(mo):
	have_new_mo = False
	if mo in ordered_mo_list:
		#We only need to change the order of mo in ordered_mo_list
		ordered_mo_list.remove(mo)
	elif mo not in mo_list:
		have_new_mo = True
		#We need to add new mo to mo_list
		if len(ordered_mo_list) >= MO_LIMIT:
			poped_mo = ordered_mo_list.pop(0)
			logger.info("%s in, %s out" % (mo, poped_mo))
			poped_index = mo_list.index(poped_mo)
			mo_list.pop(poped_index)
			mo_list.insert(poped_index, mo)
			#We need to reset the counter[poped_index]
			reinit_counter(poped_index)
		else:
			mo_list.append(mo)
			total_fail.append(defaultdict(int))
			continuous_fail.append(defaultdict(int))
	ordered_mo_list.append(mo)
	if have_new_mo:
		mo_dump()
	res = mo_list.index(mo)
	return res

def reset(counter_index, tsp, rows):
	for row in rows:
		if ((row['TEST'] == "TSP") or (row['TEST'] == "PROJECT")):
			continue
		test = row['TEST']
		field = tsp + "." + test
		total_fail[counter_index].pop(field, None)
		continuous_fail[counter_index].pop(field, None)

def parse_pkt(pkt, expected_project):
	fieldnames = ("TEST", "STATUS", "VALUE", "U_LIMIT", "L_LIMIT", "TEST_TIME")
	reader = csv.DictReader(StringIO.StringIO(pkt), fieldnames)
	reader.next()
	rows = list(reader)
	datasource = "NOT_FOUND"
	tsp = "NOT_FOUND"
	project = "NOT_FOUND"
	isn = "NOT_FOUND"
	test = "NOT_FOUND"
	field = "NOT_FOUND"
	mo = "NOT_FOUND"
	reason = ""
	err_msg = ""

	for row in rows:
		if (row['TEST'] == "DATASOURCE"):
			datasource = row['VALUE']
		elif (row['TEST'] == "TSP"):
			tsp = row['VALUE']
		elif (row['TEST'] == "PROJECT"):
			project = row['VALUE']
		elif (row['TEST'] == "ISN"):
			isn = row['VALUE']
		elif (row['TEST'] == "MO_From_SFIS"):
			mo = row['VALUE']
	if (((expected_project != None) and (project != expected_project)) or (datasource != "ON-LINE")):
		print("Project: %s, TSP: %s, DATASOURCE: %s" % (project, tsp, datasource))
		return

	counter_index = add_mo(mo)
	for row in rows:
		if ((row['TEST'] == "TSP") or (row['TEST'] == "PROJECT")):
			continue
		test = row['TEST']
		field = tsp + "." + test
		if (row['STATUS'] == "1"):
			logger.warning(isn + ": " + mo + ", " + field)
			total_fail[counter_index][field] += 1
			continuous_fail[counter_index][field] += 1
			if ((total_fail[counter_index][field] >= 5) or (continuous_fail[counter_index][field] >= 3)):
				if (total_fail[counter_index][field] >= 5):
					reason = "total fail = 5"
				elif (continuous_fail[counter_index][field] >= 3):
					reason = "continuous fail = 3"
				logger.error(mo + ", " + field + ": " + reason)
				total_fail[counter_index].pop(field, None)
				continuous_fail[counter_index].pop(field, None)
				if (err_msg != ""):
					err_msg += "\n"
				err_msg += test
				err_msg += " "
				err_msg += reason
			#assume that the test will stop when meet first fail item, so we can skip other counters
			break
		elif (row['STATUS'] == "0"):
			continuous_fail[counter_index].pop(field, None)
	if (err_msg != ""):
		reset(counter_index, tsp, rows)
		try:
			notify_im(project, mo, field, reason, str(datetime.datetime.now()))
		except:
			except_msg = traceback.format_exc()
			print except_msg
		try:
			notify_mail(project, mo, tsp, err_msg)
		except:
			except_msg = traceback.format_exc()
			print except_msg

def dump_to_csv(id, data):
	try:
		filename = LOG_PREFIX + "_" + str(expected_project) + "_" + id + ".csv"
		with open(filename, 'w') as csvfile:
			csvfile.write(data)
	except:
		msg = traceback.format_exc()
		print(msg)

expected_project = None
if (len(sys.argv) > 1):
	expected_project = sys.argv[1]
logger = logging.getLogger('mylogger')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_PREFIX + "_" + str(expected_project) + "_" + time.strftime("%Y%m%d-%H%M%S") + LOG_POSTFIX)
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(levelname)8s] %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
#logger.addHandler(ch)

total_fail = []
continuous_fail = []
#for i in range(MO_LIMIT):
	#total_fail.append(defaultdict(int))
	#continuous_fail.append(defaultdict(int))
mo_list = []
ordered_mo_list = []

consumer = KafkaConsumer('test', bootstrap_servers=[private.kafka_server_addr])

print("%s: waiting project %s..." % (LOG_PREFIX, expected_project))
logger.info("%s: waiting project %s..." % (LOG_PREFIX, expected_project))
for message in consumer:
	print("%s:%d:%d: key=%s" % (message.topic, message.partition, message.offset, message.key))
	try:
		parse_pkt(message.value, expected_project)
	except _csv.Error, e:
		msg = traceback.format_exc()
		print(msg)
		logger.error(msg)
		dump_to_csv(message.offset, message.value)
