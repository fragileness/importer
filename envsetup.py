from elasticsearch import Elasticsearch
from elasticsearch import RequestError
from elasticsearch import ConnectionTimeout
from elasticsearch.client import IndicesClient
import json
import sys
import getopt

def setup(forced):
	es = Elasticsearch([{'host': 'localhost', 'port': 9200}], max_retries=10, retry_on_timeout=True)
	idx_client = IndicesClient(es)
	if (idx_client.exists(index='max1')):
		if (forced):
			idx_client.delete(index='max1')
		else :
			print "Index already exists!"
			return
	csv_status = {"csv_status" : {"path_match": "*.STATUS", "mapping": {"index": "not_analyzed"}}}
	csv_value = {"csv_value" : {"path_match": "*.VALUE", "mapping": {"index": "not_analyzed"}}}
	csv_u_limit = {"csv_u_limit" : {"path_match": "*.U_LIMIT", "mapping": {"index": "not_analyzed"}}}
	csv_l_limit = {"csv_l_limit" : {"path_match": "*.L_LIMIT", "mapping": {"index": "not_analyzed"}}}
	csv_test_time = {"csv_test_time" : {"path_match": "*.TEST_TIME", "mapping": {"index": "not_analyzed"}}}
	dynamic_templates = [csv_status, csv_value, csv_u_limit, csv_l_limit, csv_test_time]
	total_test_time = {"Total Testing Time" : {"properties" : {"VALUE" : {"type" : "double"}}}}
	properties = total_test_time
	mappings = {"dynamic_templates" : dynamic_templates, "properties" : properties}
	data = {"mappings" : {"mp": mappings}}
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
