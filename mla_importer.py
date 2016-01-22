"""
Import log to elasticsearch server

Usage:  mla_importer.py [flags] index [es_server_addr]

-i --input_location
-s --input_subfolder
-o --output_location
-f --fail_location
-l --loop

"""

from elasticsearch import Elasticsearch
from elasticsearch import RequestError
from elasticsearch import ConnectionTimeout
from elasticsearch.client import IndicesClient
import csv
import os
import time
import codecs
import zipfile
import tarfile
import json
import sys
import getopt
import shutil
import datetime
import logging
import smtplib
import traceback
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText

import mla_setup
import private

LOG_PREFIX = 'mla_importer'
LOG_POSTFIX = '.log'
TEMP_PREFIX = "_temp"
TIME_BUFFER = 5 * 60
DEFAULT_FAIL_PATH = "./_fail"
ES_DOC_TYPE = 'ats'

####################################################################
# Code here borrowed from json encoder
import re
ESCAPE = re.compile(r'[\x00-\x1f\\"\b\f\n\r\t]')
ESCAPE_ASCII = re.compile(r'([\\"]|[^\ -~])')
HAS_UTF8 = re.compile(r'[\x80-\xff]')
ESCAPE_DCT = {
    '\\': '\\\\',
    '"': '\\"',
    '\b': '\\b',
    '\f': '\\f',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
}
for i in range(0x20):
    ESCAPE_DCT.setdefault(chr(i), '\\u{0:04x}'.format(i))
    #ESCAPE_DCT.setdefault(chr(i), '\\u%04x' % (i,))

def py_encode_basestring_ascii(s):
    """Return an ASCII-only JSON representation of a Python string

    """
    if isinstance(s, str) and HAS_UTF8.search(s) is not None:
        #s = s.decode('utf-8')
        s = s
    def replace(match):
        s = match.group(0)
        try:
            return ESCAPE_DCT[s]
        except KeyError:
            n = ord(s)
            if n < 0x10000:
                return '\\u{0:04x}'.format(n)
                #return '\\u%04x' % (n,)
            else:
                # surrogate pair
                n -= 0x10000
                s1 = 0xd800 | ((n >> 10) & 0x3ff)
                s2 = 0xdc00 | (n & 0x3ff)
                return '\\u{0:04x}\\u{1:04x}'.format(s1, s2)
                #return '\\u%04x\\u%04x' % (s1, s2)
    return str(ESCAPE_ASCII.sub(replace, s))

####################################################################
def replace_for_json1(a):
	if (a is not None):
		a = a.replace('\\', '\\\\')
		# this case should be first
		a = a.replace('\"', '\\\"')
		a = a.replace('\b', '\\b')
		a = a.replace('\f', '\\f')
		a = a.replace('\n', '\\n')
		a = a.replace('\r', '\\r')
		a = a.replace('\t', '\\t')
		a = a.replace('\/', '\\/')
	return a

def replace_for_json(a):
	if (a is not None):
		a = py_encode_basestring_ascii(a)
	return a

def parse_read_file(filepath):
	a = ""
	with open(filepath) as f:
		a = f.read()
	return a

EMAIL_SUBJECT = "[MLA] Importer Alert"

def listToStr(lst):
	"""This method makes comma separated list item string"""
	return ','.join(lst)

def notify_mail(msg):
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

def parse_ats_log(dirPath, filename):
	logger = logging.getLogger('mla_logger')
	data = ""
	atslog_filepath = os.path.join(dirPath, os.path.splitext(filename)[0] + ".log")
	if (os.path.isfile(atslog_filepath)):
		logger.info("Ats log: %s" % (atslog_filepath))
		a = parse_read_file(atslog_filepath)
		if a[:3] == codecs.BOM_UTF8:
			a = a[3:]
		a = replace_for_json(a)
		data += "\"ats_log\": \"%s\", " %(a)
	return data

def parse_log(dirPath, filename):
	logger = logging.getLogger('mla_logger')
	data = ""
	zip_filepath = os.path.join(dirPath, os.path.splitext(filename)[0] + ".zip")
	if (os.path.isfile(zip_filepath)):
		num_of_logs = 0
		num_of_mainlogs = 0
		num_of_eventlogs = 0
		num_of_radiologs = 0
		num_of_dmesglogs = 0
		not_first = 0
		data += "\"logs\": [ "
		print zip_filepath
		with zipfile.ZipFile(zip_filepath, 'r') as myzip:
			for name in myzip.namelist():
				if (name == "all_log.tar.bz2"):
					myzip.extract(name, dirPath)
					with tarfile.open(os.path.join(dirPath, name), "r:bz2") as t:
						for tar_info in t:
							if (os.path.splitext(tar_info.name)[0].endswith("adb-main-system.log")):
								#if (num_of_mainlogs > 0):
									#continue
								if (num_of_logs):
									data += ","
								logger.info("Log %s" % (tar_info.name))
								data += "{\"adb-main-system.log\": \"%s\"}" %(replace_for_json(t.extractfile(tar_info).read()))
								num_of_logs += 1
								num_of_mainlogs += 1
							elif (os.path.splitext(tar_info.name)[0].endswith("adb-events.log")):
								#if (num_of_eventlogs > 0):
									#continue
								if (num_of_logs):
									data += ","
								logger.info("Log %s" % (tar_info.name))
								data += "{\"adb-events.log\": \"%s\"}" %(replace_for_json(t.extractfile(tar_info).read()))
								num_of_logs += 1
								num_of_eventlogs += 1
							elif (os.path.splitext(tar_info.name)[0].endswith("adb-radio.log")):
								#if (num_of_radiologs > 0):
									#continue
								if (num_of_logs):
									data += ","
								logger.info("Log %s" % (tar_info.name))
								data += "{\"adb-radio.log\": \"%s\"}" %(replace_for_json(t.extractfile(tar_info).read()))
								num_of_logs += 1
								num_of_radiologs += 1
							elif (os.path.splitext(tar_info.name)[0].endswith("dmesg.log")):
								#if (num_of_dmesglogs > 0):
									#continue
								if (num_of_logs):
									data += ","
								logger.info("Log %s" % (tar_info.name))
								data += "{\"dmesg.log\": \"%s\"}" %(replace_for_json(t.extractfile(tar_info).read()))
								num_of_logs += 1
								num_of_dmesglogs += 1
					os.remove(os.path.join(dirPath, name))
				elif (os.path.splitext(name)[0].endswith("adb-main-system.log")):
					logger.info("Log %s" % (name))
					if (num_of_logs):
						data += ","
					data += "{\"adb-main-system.log\": \"%s\"}" %(replace_for_json(myzip.read(name)))
					num_of_logs += 1
				elif (os.path.splitext(name)[0].endswith("adb-events.log")):
					logger.info("Log %s" % (name))
					if (num_of_logs):
						data += ","
					data += "{\"adb-events.log\": \"%s\"}" %(replace_for_json(myzip.read(name)))
					num_of_logs += 1
				elif (os.path.splitext(name)[0].endswith("adb-radio.log")):
					logger.info("Log %s" % (name))
					if (num_of_logs):
						data += ","
					data += "{\"adb-radio.log\": \"%s\"}" %(replace_for_json(myzip.read(name)))
					num_of_logs += 1
				elif (os.path.splitext(name)[0].endswith("dmesg.log")):
					logger.info("Log %s" % (name))
					if (num_of_logs):
						data += ","
					data += "{\"dmesg.log\": \"%s\"}" %(replace_for_json(myzip.read(name)))
					num_of_logs += 1
		data += "],"
	return data

def parse_runinlog(dirPath, filename):
	logger = logging.getLogger('mla_logger')
	data = "\"RunInLog\": {"
	zip_filepath = os.path.join(dirPath, os.path.splitext(filename)[0] + ".zip")
	if (os.path.isfile(zip_filepath)):
		with zipfile.ZipFile(zip_filepath, 'r') as myzip:
			for name in myzip.namelist():
				if (name == "all_log.tar.bz2"):
					myzip.extract(name, dirPath)
					with tarfile.open(os.path.join(dirPath, name), "r:bz2") as t:
						row_no = 0
						for tar_info in t:
							if (tar_info.name.endswith("Run_In_test_log.csv")):
								if (row_no):
									break
								logger.info("Run-in log %s" % (tar_info.name))
								fieldnames = ("TEST", "STATUS", "VALUE", "U_LIMIT", "L_LIMIT")
								reader = csv.DictReader(t.extractfile(tar_info), fieldnames)
								reader.next()
								rows = list(reader)
								row_counter = len(rows)
								#row_no = 0
								for row in rows:
									row_no += 1
									test = row['TEST']
									test = replace_for_json(test)
									status = row['STATUS']
									status = replace_for_json(status)
									value = row['VALUE']
									value = replace_for_json(value)
									u_limit = row['U_LIMIT']
									u_limit = replace_for_json(u_limit)
									l_limit = row['L_LIMIT']
									l_limit = replace_for_json(l_limit)
									data += "\"%s\": { \"STATUS\": \"%s\", \"VALUE\": \"%s\", \"U_LIMIT\": \"%s\", \"L_LIMIT\": \"%s\" }" %(test, status, value, u_limit, l_limit)
									if row_no < row_counter :
										data += ", "
						if (row_no > 0):
							for tar_info in t:
								if (tar_info.name.endswith("old_log.txt")):
									logger.info("Run-in log %s" % (tar_info.name))
									data += ", \"old_log\": \"%s\"" % (replace_for_json1(t.extractfile(tar_info).read()))
									break
					os.remove(os.path.join(dirPath, name))
	data +="},"
	return data

def mla_import_csv(es, project, doctype, file_location, filename, es_id, payload=""):
	res = False
	logger = logging.getLogger('mla_logger')
	if (es.exists(index=project, doc_type=doctype, id=es_id)):
		logger.warning("id %s already exists!" % (es_id))
		return False
	file_path = os.path.join(file_location, filename)
	logger.info("Csv: %s" % (file_path))
	with open(file_path, 'r') as csvfile:
		fieldnames = ("TEST", "STATUS", "VALUE", "U_LIMIT", "L_LIMIT", "TEST_TIME")
		reader = csv.DictReader(csvfile, fieldnames)
		reader.next()
		rows = list(reader)
		data="{"
		fail_symptom = None
		for row in rows:
			test = row['TEST']
			test = replace_for_json(test)
			status = row['STATUS']
			status = replace_for_json(status)
			if ((fail_symptom == None) and (status == "1")):
				fail_symptom = test
			value = row['VALUE']
			value = replace_for_json(value)
			if (test == "Test Start Time" or test == "Test end Time"):
				value += "+08:00"
			u_limit = row['U_LIMIT']
			u_limit = replace_for_json(u_limit)
			l_limit = row['L_LIMIT']
			l_limit = replace_for_json(l_limit)
			test_time = row['TEST_TIME']
			test_time = replace_for_json(test_time)
			data += "\"%s\": { \"STATUS\": \"%s\", \"VALUE\": \"%s\", \"U_LIMIT\": \"%s\", \"L_LIMIT\": \"%s\", \"TEST_TIME\": \"%s\" }, " %(test, status, value, u_limit, l_limit, test_time)
		data += "\"fail_symptom\": \"%s\", " %(fail_symptom)
		data += parse_ats_log(file_location, filename)
		data += parse_log(file_location, filename)
		data += parse_runinlog(file_location, filename)
		parse_runinlog(file_location, filename)
		data += "\"file_path\": \"%s\"" %(os.path.normpath(payload).replace('\\','\\\\'))
		data +="}"
		res = True
		try:
			es.create(index=project, doc_type=doctype, id=es_id, body=data)
		except:
			logger.error("Exception on es.create()" + str(sys.exc_info()[0]))
			res = False
	return res

def mla_import_log(es, project, doctype, log_location, es_id, payload):
	res = False
	for dirPath, dirNames, fileNames in os.walk(log_location):
		for filename in fileNames:
			if ((".csv" == os.path.splitext(filename)[-1]) and es_id.split('_', 1)[0] == filename.split('_', 1)[0]):
				res = mla_import_csv(es, project, doctype, dirPath, filename, es_id, payload)
				break
	return res

def mla_import_zip(es, project, doctype, dirPath, filename, payload):
	res = False
	logger = logging.getLogger('mla_logger')
	file_path = os.path.join(dirPath, filename)
	logger.info("Import zip: %s" % (file_path))
	logger.info("Relative path: %s" % (payload))
	extract_to_path = TEMP_PREFIX + "_" + project
	try:
		shutil.rmtree(extract_to_path)
	except:
		pass
	with zipfile.ZipFile(file_path, 'r') as myzip:
		try:
			myzip.extractall(extract_to_path)
		except:
			logger.error("Exception on extractall():" + str(sys.exc_info()[0]))
			return res
		res = mla_import_log(es, project, doctype, extract_to_path, filename, payload)
	return res

def mla_import(es, project, doctype, input_location, input_subfolder, output_location, fail_location, delay=0):
	logger = logging.getLogger('mla_logger')
	scan_location = input_location
	if (input_subfolder != None):
		scan_location = os.path.join(input_location, input_subfolder)
	i = 0
	j = 0
	start = time.time()
	for dirPath, dirNames, fileNames in os.walk(scan_location):
		for filename in fileNames:
			if (".zip" != os.path.splitext(filename)[-1]):
				continue
			if (delay > 0):
				try:
					file_create_time = os.path.getctime(os.path.join(dirPath, filename))
				except:
					logger.error("Exception on os.path.getctime():" + str(sys.exc_info()[0]))
					#The file disappears...
					continue
				if ((start - file_create_time) < delay):
					continue
			i += 1
			reletive_path = os.path.relpath(dirPath, input_location)
			res = mla_import_zip(es, project, doctype, dirPath, filename, os.path.join(reletive_path, filename))
			if (res):
				j += 1
			if (output_location != None):
				if (res):
					move_to_path = output_location
				else:
					move_to_path = fail_location
				move_to_path = os.path.join(move_to_path, reletive_path)
				logger.info("Move to %s" % (move_to_path))
				if (os.path.lexists(os.path.join(move_to_path, filename))):
					logger.error("File already exists: " + os.path.join(move_to_path, filename))
					try:
						os.remove(os.path.join(dirPath, filename))
					except:
						logger.error("Exception on deleting file:" + str(sys.exc_info()[0]))
				else:
					if (False == os.path.lexists(move_to_path)):
						os.makedirs(move_to_path)
					try:
						shutil.move(os.path.join(dirPath, filename), move_to_path)
					except:
						logger.error("Exception on moving file:" + str(sys.exc_info()[0]))
	end = time.time()
	elapsed = end - start
	logger.info(str(j) + "/" + str(i) + " documents created; Time taken: " + str(elapsed) + " seconds.")

def mla_import_loop(es, project, doctype, input_location, input_subfolder, output_location, fail_location, is_looping):
	delay = 0
	if is_looping:
		delay = TIME_BUFFER
	while True:
		print "Importing..."
		mla_import(es, project, doctype, input_location, input_subfolder, output_location, fail_location, delay)
		if (False == is_looping):
			break
		print "Waiting..."
		time.sleep(10)

def usage():
	print "Usage:  mla_importer.py [flags] index [es_server_addr]"
	print ""
	print "  -i (--input_location) <path>"
	print ""
	print "  -s (--input_subfolder) <relative path>"
	print ""
	print "  -o (--output_location) <path>"
	print ""
	print "  -f (--fail_location) <path>"
	print ""
	print "  -d (--doc_type) <doc_type>"
	print ""
	print "  -i (--loop)"

def main(argv):
	es_server_addr = 'localhost'
	input_location = os.path.abspath(".")
	input_subfolder = None
	output_location = None
	fail_location = os.path.abspath(DEFAULT_FAIL_PATH)
	doctype = ES_DOC_TYPE
	is_looping = False
	try:
		opts, args = getopt.getopt(argv, "i:s:o:f:t:l", ["input_location=", "input_subfolder=", "output_location=", "fail_location=", "doc_type=", "loop"])
	except getopt.GetoptError:
		usage()
		sys.exit(2)
	if len(args) < 1:
		usage()
		sys.exit(2)
	elif len(args) > 1:
		es_server_addr = args[1]
	project = args[0]
	for opt, arg in opts:
		if opt in ("-i", "--input_location"):
			input_location = os.path.abspath(arg)
		elif opt in ("-s", "--input_subfolder"):
			input_subfolder = arg
		elif opt in ("-o", "--output_location"):
			output_location = os.path.abspath(arg)
		elif opt in ("-f", "--fail_location"):
			fail_location = os.path.abspath(arg)
		elif opt in ("-t", "--doc_type"):
			doctype = arg
		elif opt in ("-l", "--loop"):
			is_looping = True
	logger = logging.getLogger('mla_logger')
	logger.setLevel(logging.INFO)
	fh = logging.FileHandler(LOG_PREFIX + "_" + project + "_" + time.strftime("%Y%m%d-%H%M%S") + LOG_POSTFIX)
	fh.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	formatter = logging.Formatter('[%(asctime)s] [%(levelname)8s] %(message)s')
	fh.setFormatter(formatter)
	logger.addHandler(fh)
	logger.addHandler(ch)
	logger.info("%s run with es_server_addr=%s, project=%s, input_location=%s, input_subfolder=%s, output_location=%s, fail_location=%s, doctype=%s, is_looping=%s" % (LOG_PREFIX, es_server_addr, project, input_location, input_subfolder, output_location, fail_location, doctype, is_looping))
	print "Press Enter key to continue"
	a	=	raw_input()
	es = Elasticsearch([{'host': es_server_addr, 'port': 9200}], max_retries=10, retry_on_timeout=True)
	idx_client = IndicesClient(es)
	if (False == idx_client.exists(index=project)):
		logger.info("Index %s not exist, press enter key to create schema" % project) 
		a	=	raw_input()
		mla_setup.schema_setup(es, project, False, logger)
		print "Press enter key to continue"
		a	=	raw_input()
	try:
		mla_import_loop(es, project, doctype, input_location, input_subfolder, output_location, fail_location, is_looping)
	except:
		msg = traceback.format_exc()
		print msg
		notify_mail(msg)

if __name__ == '__main__':
	main(sys.argv[1:])
