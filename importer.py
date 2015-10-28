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

def parse_csv(client, dirPath, filename):
	res = True
	file_path = os.path.join(dirPath, filename)
	print file_path
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
	data += "\"file_path\": \"%s\"" %(file_path.replace('\\','/'))
	#print os.path.abspath(file_path)
	data +="}"
	#return res
	try:
		client.index(index='max1', doc_type='mp', id=filename, body=data)
	except RequestError:
		print "ERROR: Request Error"
		res = False
	except ConnectionTimeout:
		print "ERROR: Connection Timeout"
		res = False
	except UnicodeDecodeError:
		print "ERROR: Unicode Decode Error"
		res = False

	return res

def parser():
	root_path = "./"
	if (len(sys.argv) > 1):
		root_path += sys.argv[1]
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
			if (".csv" == os.path.splitext(f)[-1]):
				i += 1
				res = False
				try:
					res = parse_csv(es, dirPath, f)
				except:
					print "Unexpected error:", sys.exc_info()[0]
					#raise
				if (res):
					j += 1
	end = time.time()
	elapsed = end - start
	print j,"/", i, " indices created; Time taken: ", elapsed, "seconds."

if __name__ == '__main__':
	parser()
