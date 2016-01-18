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
from smb.SMBConnection import SMBConnection

LOG_PREFIX = 'mla_processmining_consumer'
LOG_POSTFIX = '.log'
OUTPUT_LOCATION = "//172.18.212.211/sz_sfis_event_log"

def notify_csv(isn, timestamp, msg):
	filename = "%s_%s.csv" % (isn, timestamp)
	with open(OUTPUT_LOCATION + "/" + filename, 'w') as csvfile:
		csvfile.write(msg)

def notify_smb(isn, timestamp, msg):
	server_name = ""
	client_machine_name = "Onegin"
	server_ip = "172.18.212.211"
	userID = "szsfis"
	password = "szsfis"
	conn = SMBConnection(userID, password, client_machine_name, server_name, use_ntlm_v2 = True)
	assert conn.connect(server_ip, 139)
	filename = "%s_%s.csv" % (isn, timestamp)
	conn.storeFile("sz_sfis_event_log", filename, StringIO.StringIO(msg))

def notify(project, tsp, isn, fail_symptom, test_start_time, total_testing_time, test_end_time, operator_id):
	msg = "%s, %s, %s, %s, %s, %s, %s, %s" % (project, tsp, isn, fail_symptom, test_start_time, total_testing_time, test_end_time, operator_id)
	timestamp = time.strftime("%Y%m%d-%H%M%S")
	logger.info(str(os.getpid()) + ", " + timestamp + ", " + msg)
	#notify_csv(isn, timestamp, msg)
	notify_smb(isn, timestamp, msg)

def parse_pkt(pkt, expected_project):
	fieldnames = ("TEST", "STATUS", "VALUE", "U_LIMIT", "L_LIMIT", "TEST_TIME")
	reader = csv.DictReader(StringIO.StringIO(pkt), fieldnames)
	reader.next()
	rows = list(reader)
	datasource = None
	project = None
	tsp = None
	isn = None
	fail_symptom = None
	test_start_time = None
	total_testing_time = None
	test_end_time = None
	operator_id = None

	for row in rows:
		if (row['TEST'] == "DATASOURCE"):
			datasource = row['VALUE']
		elif (row['TEST'] == "PROJECT"):
			project = row['VALUE']
		elif (row['TEST'] == "TSP"):
			tsp = row['VALUE']
		elif (row['TEST'] == "ISN"):
			isn = row['VALUE']
		elif (row['TEST'] == "Test Start Time"):
			test_start_time = row['VALUE']
		elif (row['TEST'] == "Total Testing Time"):
			total_testing_time = row['VALUE']
		elif (row['TEST'] == "Test end Time"):
			test_end_time = row['VALUE']
		elif (row['TEST'] == "Operator ID"):
			operator_id = row['VALUE']
	if (((expected_project != None) and (project != expected_project)) or (datasource != "ON-LINE")):
		logger.warning("Project: %s, TSP: %s, DATASOURCE: %s" % (project, tsp, datasource))
		return

	for row in rows:
		if ((row['TEST'] == "TSP") or (row['TEST'] == "PROJECT")):
			continue
		if (row['STATUS'] == "1"):
			fail_symptom = row['TEST']
			#assume that the test will stop when meet first fail item, so we can skip other counters
			break

	try:
		notify(project, tsp, isn, fail_symptom, test_start_time, total_testing_time, test_end_time, operator_id)
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
