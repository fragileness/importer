from elasticsearch import Elasticsearch
from elasticsearch import RequestError
from elasticsearch import ConnectionTimeout
from elasticsearch.client import IndicesClient
import json
import sys
import getopt

def add_unique_mapping(properties, name, property):
	try:
		properties[name]
	except:
		properties[name] = {"properties" : property}
		return
	raise NameError('field "%s" configured twice' % (name))

def setup(forced):
	properties = {}
	add_unique_mapping(properties, "Test Start Time", {"VALUE" : {"type" : "date", "format": "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd"}})
	add_unique_mapping(properties, "Test End Time", {"VALUE" : {"type" : "date", "format": "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd"}})

	es = Elasticsearch([{'host': 'localhost', 'port': 9200}], max_retries=10, retry_on_timeout=True)
	idx_client = IndicesClient(es)
	if (idx_client.exists(index='max1')):
		if (forced):
			idx_client.delete(index='max1')
		else :
			print "Index already exists!"
			return

	csv_status = {"csv_status" : {"path_match": "*.STATUS", "mapping": {"index": "not_analyzed"}}}
	csv_value = {"csv_value" : {"path_match": "*.VALUE", "mapping": {"index": "not_analyzed", "fields" : {"double" : {"type" : "double"}}}}}
	csv_u_limit = {"csv_u_limit" : {"path_match": "*.U_LIMIT", "mapping": {"index": "not_analyzed", "fields" : {"double" : {"type" : "double"}}}}}
	csv_l_limit = {"csv_l_limit" : {"path_match": "*.L_LIMIT", "mapping": {"index": "not_analyzed", "fields" : {"double" : {"type" : "double"}}}}}
	csv_test_time = {"csv_test_time" : {"path_match": "*.TEST_TIME", "mapping": {"index": "not_analyzed", "fields" : {"double" : {"type" : "double"}}}}}
	dynamic_templates = [csv_status, csv_value, csv_u_limit, csv_l_limit, csv_test_time]

	mappings = {"dynamic_templates" : dynamic_templates, "properties" : properties}
	data = {"settings" : {"index.mapping.ignore_malformed": True}, "mappings" : {"mp": mappings}}
	print json.dumps(data)
	idx_client.create(index='max1', body=data)

def usage():
	return

def main(argv):
	forced = 0
	try:
		opts, args = getopt.getopt(argv, "f")
	except getopt.GetoptError:
		usage()
		sys.exit(2)
	for opt, arg in opts:
		if opt == "-f":
			forced = 1
	setup(forced)

if __name__ == '__main__':
	main(sys.argv[1:])
