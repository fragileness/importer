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

LOG_PREFIX = 'mla_demo_consumer'
LOG_POSTFIX = '.log'

def notify_rocket(addr, user, password, room, msg):
	#====login====
	print ">>>> login"

	url = addr + '/api/login'
	data = urllib.urlencode({'password' : password, 'user' : user})
	#print data
	resp = urllib2.urlopen(url, data=data).read()
	#print resp

	jdata = json.loads(resp)
	status = jdata["status"]
	print "status=" + status
	authToken = jdata["data"]["authToken"]
	#print "authToken=" + authToken
	userId = jdata["data"]["userId"]
	#print "userId=" + userId

	#====send message====
	#print ">>>> send message"
	logger.info(">>>> send message")

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
	#print resp
	jdata = json.loads(resp)
	status = jdata["status"]
	print "status=" + status
	message = jdata["data"]["message"]
	#print "status=" + message

def notify_im(project, item, reason, timestamp):
	msg = str(os.getpid()) + ", " + timestamp + ", " + project + ", " + item + ", " + reason
	logger.info(msg)
	notify_rocket(private.rocket_url, private.rocket_user, private.rocket_password, private.rocket_room_demo_consumer, msg)

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

	for row in rows:
		if (row['TEST'] == "DATASOURCE"):
			datasource = row['VALUE']
		elif (row['TEST'] == "TSP"):
			tsp = row['VALUE']
		elif (row['TEST'] == "PROJECT"):
			project = row['VALUE']
		elif (row['TEST'] == "ISN"):
			isn = row['VALUE']
	if (((expected_project != None) and (project != expected_project)) or (datasource != "ON-LINE")):
		logger.warning("Project: %s, TSP: %s, DATASOURCE: %s" % (project, tsp, datasource))
		return

	reason = isn + ", PASS"
	
	for row in rows:
		if ((row['TEST'] == "TSP") or (row['TEST'] == "PROJECT")):
			continue
		test = row['TEST']
		field = tsp + "." + test
		if (row['STATUS'] == "1"):
			#logger.warning(isn + ": " + field)
			reason = isn + ", FAIL at " + test
			#assume that the test will stop when meet first fail item, so we can skip other counters
			break

	try:
		notify_im(project, tsp, reason, str(datetime.datetime.now()))
	except:
		except_msg = traceback.format_exc()
		print except_msg

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
logger.addHandler(ch)

consumer = KafkaConsumer('test', bootstrap_servers=[private.kafka_server_addr])

logger.info("%s: waiting project %s..." % (LOG_PREFIX, expected_project))
for message in consumer:
	logger.info("%s:%d:%d: key=%s" % (message.topic, message.partition, message.offset, message.key))
	parse_pkt(message.value, expected_project)
