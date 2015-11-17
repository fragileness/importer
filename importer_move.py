from elasticsearch import Elasticsearch
from elasticsearch import RequestError
from elasticsearch import ConnectionTimeout
import csv
import os
import time
import codecs
import zipfile
import tarfile
import json
import sys
import shutil
import datetime
import logging

TEMP_PATH = "./_temp"
TIME_BUFFER = 60 * 5
URL_PREFIX = "files://10.193.95.185/mla/atslog/"

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

# function for test
def parse_read_file_lines(filepath):
	a = ""
	i = 1
	f = open(filepath)
	for l in f:
		i += 1
		if i < 1:
			continue
		a += l
		if i > 100:
			break
	f.close()
	return a

def parse_read_file(filepath):
	a = ""
	f = open(filepath)
	a = f.read()
	f.close()
	return a

# function for test
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

def parse_ats_log(data, dirPath, filename):
	atslog_filepath = os.path.join(dirPath, os.path.splitext(filename)[0] + ".log")
	if (os.path.isfile(atslog_filepath)):
		print atslog_filepath
		a = parse_read_file(atslog_filepath)
		if a[:3] == codecs.BOM_UTF8:
			a = a[3:]
		a = replace_for_json(a)
		data += "\"ats_log\": \"%s\", " %(a)
	return data

def parse_runinlog(data, dirPath, filename):
	data += "\"RunInLog\": {"
	zip_filepath = os.path.join(dirPath, os.path.splitext(filename)[0] + ".zip")
	if (os.path.isfile(zip_filepath)):
		with zipfile.ZipFile(zip_filepath, 'r') as myzip:
			for name in myzip.namelist():
				if (name == "all_log.tar.bz2"):
					myzip.extract(name)
					with tarfile.open(name, "r:bz2") as t:
						row_no = 0
						for tar_info in t:
							if (tar_info.name.endswith("Run_In_test_log.csv")):
								if (row_no):
									break
								print tar_info.name
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
									print tar_info.name
									data += ", \"old_log\": \"%s\"" % (replace_for_json1(t.extractfile(tar_info).read()))
									break
					os.remove(name)
	data +="},"
	return data

def parse_log(data, dirPath, filename):
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
					myzip.extract(name)
					with tarfile.open(name, "r:bz2") as t:
						for tar_info in t:
							if (os.path.splitext(tar_info.name)[0].endswith("adb-main-system.log")):
								#if (num_of_mainlogs > 0):
									#continue
								if (num_of_logs):
									data += ","
								print tar_info.name
								data += "{\"adb-main-system.log\": \"%s\"}" %(replace_for_json(t.extractfile(tar_info).read()))
								num_of_logs += 1
								num_of_mainlogs += 1
							elif (os.path.splitext(tar_info.name)[0].endswith("adb-events.log")):
								#if (num_of_eventlogs > 0):
									#continue
								if (num_of_logs):
									data += ","
								print tar_info.name
								data += "{\"adb-events.log\": \"%s\"}" %(replace_for_json(t.extractfile(tar_info).read()))
								num_of_logs += 1
								num_of_eventlogs += 1
							elif (os.path.splitext(tar_info.name)[0].endswith("adb-radio.log")):
								#if (num_of_radiologs > 0):
									#continue
								if (num_of_logs):
									data += ","
								print tar_info.name
								data += "{\"adb-radio.log\": \"%s\"}" %(replace_for_json(t.extractfile(tar_info).read()))
								num_of_logs += 1
								num_of_radiologs += 1
							elif (os.path.splitext(tar_info.name)[0].endswith("dmesg.log")):
								#if (num_of_dmesglogs > 0):
									#continue
								if (num_of_logs):
									data += ","
								print tar_info.name
								data += "{\"dmesg.log\": \"%s\"}" %(replace_for_json(t.extractfile(tar_info).read()))
								num_of_logs += 1
								num_of_dmesglogs += 1
					os.remove(name)
				elif (os.path.splitext(name)[0].endswith("adb-main-system.log")):
					print name
					if (num_of_logs):
						data += ","
					data += "{\"adb-main-system.log\": \"%s\"}" %(replace_for_json(myzip.read(name)))
					num_of_logs += 1
				elif (os.path.splitext(name)[0].endswith("adb-events.log")):
					print name
					if (num_of_logs):
						data += ","
					data += "{\"adb-events.log\": \"%s\"}" %(replace_for_json(myzip.read(name)))
					num_of_logs += 1
				elif (os.path.splitext(name)[0].endswith("adb-radio.log")):
					print name
					if (num_of_logs):
						data += ","
					data += "{\"adb-radio.log\": \"%s\"}" %(replace_for_json(myzip.read(name)))
					num_of_logs += 1
				elif (os.path.splitext(name)[0].endswith("dmesg.log")):
					print name
					if (num_of_logs):
						data += ","
					data += "{\"dmesg.log\": \"%s\"}" %(replace_for_json(myzip.read(name)))
					num_of_logs += 1
		data += "],"
	return data

def if_mapping_exception():
	try:
		if ("MapperParsingException" in sys.exc_info()[1][1]):
			logger.error(str(sys.exc_info()[1][1]))
	except:
		pass

def parse_csv(client, dirPath, filename, url_path):
	res = True
	file_path = os.path.join(dirPath, filename)
	print file_path
	if (client.exists(index='max1', doc_type='mp', id=filename)):
		logger.warning("Index already exists!")
		return False
	csvfile = open(file_path, 'r')
	fieldnames = ("TEST", "STATUS", "VALUE", "U_LIMIT", "L_LIMIT", "TEST_TIME")
	reader = csv.DictReader(csvfile, fieldnames)
	reader.next()
	rows = list(reader)
	row_counter = len(rows)
	row_no = 1
	data="{"
	for row in rows:
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
		test_time = row['TEST_TIME']
		test_time = replace_for_json(test_time)
		data += "\"%s\": { \"STATUS\": \"%s\", \"VALUE\": \"%s\", \"U_LIMIT\": \"%s\", \"L_LIMIT\": \"%s\", \"TEST_TIME\": \"%s\" }, " %(test, status, value, u_limit, l_limit, test_time)
	data = parse_ats_log(data, dirPath, filename)
	data = parse_log(data, dirPath, filename)
	data = parse_runinlog(data, dirPath, filename)
	data += "\"file_path\": \"%s\"" %(URL_PREFIX + os.path.normpath(url_path).replace('\\','/'))
	data +="}"
	try:
		client.create(index='max1', doc_type='mp', id=filename, body=data)
	except:
		logger.error("Exception on client.create()" + str(sys.exc_info()[0]))
		if_mapping_exception()
		res = False

	return res

def parser(client, root_path, url_path):
	res = True
	if (False == os.path.lexists(root_path)):
		print "Path Error!"
		return False
	for dirPath, dirNames, fileNames in os.walk(root_path):
		for f in fileNames:
			if (".csv" == os.path.splitext(f)[-1]):
				try:
					res = res and parse_csv(client, dirPath, f, url_path)
				except:
					logger.error("Exception on parse_csv()" + str(sys.exc_info()[0]))
					res = False
					#raise
	return res

def parse_zip(client, dirPath, filename):
	res = True
	file_path = os.path.join(dirPath, filename)
	logger.info(file_path)
	with zipfile.ZipFile(file_path, 'r') as myzip:
		myzip.extractall(TEMP_PATH)
		try:
			res = parser(client, TEMP_PATH, file_path)
		except:
			logger.error("Exception on parse():" + str(sys.exc_info()[0]))
			res = False
		shutil.rmtree(TEMP_PATH)
	return res

def parser_findzip(is_looping):
	if (os.path.lexists(TEMP_PATH)):
		shutil.rmtree(TEMP_PATH)
	is_moving = False
	root_path = "./"
	move_path = "./"
	if (len(sys.argv) > 1):
		root_path += sys.argv[1]
	if (len(sys.argv) > 2):
		move_path += sys.argv[2]
		if (os.path.lexists(move_path)):
			is_moving = True
	if (is_moving):
		print root_path, "->" ,move_path
	else:
		print root_path
	if (False == os.path.lexists(root_path)):
		print "Path Error!"
		return
	i = 0
	j = 0
	start = time.time()
	es = Elasticsearch([{'host': 'localhost', 'port': 9200}], max_retries=10, retry_on_timeout=True)
	for dirPath, dirNames, fileNames in os.walk(root_path):
		for f in fileNames:
			file_create_time = os.path.getctime(os.path.join(dirPath, f))
			process_this_file = True
			if (is_looping):
				process_this_file = ((start - file_create_time) > TIME_BUFFER)
			if (process_this_file and (".zip" == os.path.splitext(f)[-1])):
				i += 1
				res = False
				try:
					res = parse_zip(es, dirPath, f)
				except:
					logger.error("Exception on parse_zip():" + str(sys.exc_info()[0]))
					#raise
				if (res):
					j += 1
				if (is_moving):
					move_full_path = os.path.join(move_path, dirPath)
					print "Move to" , move_full_path
					if (False == os.path.lexists(move_full_path)):
						os.makedirs(move_full_path)
					shutil.move(os.path.join(dirPath, f), move_full_path)
	end = time.time()
	elapsed = end - start
	logger.info(str(j) + "/" + str(i) + " indices created; Time taken: " + str(elapsed) + " seconds.")

if __name__ == '__main__':
	logger = logging.getLogger('mylogger')
	logger.setLevel(logging.INFO)
	fh = logging.FileHandler('test.log')
	fh.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	ch.setLevel(logging.DEBUG)
	formatter = logging.Formatter('[%(asctime)s] [%(levelname)8s] %(message)s')
	fh.setFormatter(formatter)
	logger.addHandler(fh)
	logger.addHandler(ch)

	is_looping = False
	if (len(sys.argv) > 3):
		is_looping = True
		print "Looping..."
	while True:
		print "Importing..."
		parser_findzip(is_looping)
		if (False == is_looping):
			break
		print "Waiting..."
		time.sleep(10)
