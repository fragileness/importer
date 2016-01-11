from elasticsearch import Elasticsearch
from elasticsearch import RequestError
from elasticsearch import ConnectionTimeout
from elasticsearch.client import IndicesClient
import json
import sys
import getopt

PROJECT = "max1"
STAGE = "mp"

def add_unique_mapping(properties, name, property):
	try:
		properties[name]
	except:
		properties[name] = {"properties" : property}
		return
	raise NameError('field "%s" configured twice' % (name))

def setup(forced):
	properties = {}
	properties["fail_symptom"] = {"type" : "string", "index": "not_analyzed"}
	properties["ats_log"] = {"type" : "string"}
	properties["file_path"] = {"type" : "string", "analyzer": "path-analyzer"}
	add_unique_mapping(properties, "Test Start Time", {"VALUE" : {"type" : "date", "format": "yyyy/MM/dd HH:mm:ssZ||yyyy/MM/ddZ"}})
	add_unique_mapping(properties, "Test end Time", {"VALUE" : {"type" : "date", "format": "yyyy/MM/dd HH:mm:ssZ||yyyy/MM/ddZ"}})

	es = Elasticsearch([{'host': 'localhost', 'port': 9200}], max_retries=10, retry_on_timeout=True)
	idx_client = IndicesClient(es)
	if (idx_client.exists(index=PROJECT)):
		if (forced):
			idx_client.delete(index=PROJECT)
		else :
			print "Index already exists!"
			return

	runin_csv_status = {"runin_csv_status" : {"path_match": "RunInLog.*.STATUS", "mapping": {"index": "not_analyzed"}}}
	runin_csv_value = {"runin_csv_value" : {"path_match": "RunInLog.*.VALUE", "mapping": {"index": "not_analyzed", "fields" : {"double" : {"type" : "double"}}}}}
	runin_csv_u_limit = {"runin_csv_u_limit" : {"path_match": "RunInLog.*.U_LIMIT", "mapping": {"index": "not_analyzed", "fields" : {"double" : {"type" : "double"}}}}}
	runin_csv_l_limit = {"runin_csv_l_limit" : {"path_match": "RunInLog.*.L_LIMIT", "mapping": {"index": "not_analyzed", "fields" : {"double" : {"type" : "double"}}}}}
	runin_csv_test_time = {"runin_csv_test_time" : {"path_match": "RunInLog.*.TEST_TIME", "mapping": {"index": "not_analyzed", "fields" : {"double" : {"type" : "double"}}}}}
	csv_status = {"csv_status" : {"path_match": "*.STATUS", "mapping": {"index": "not_analyzed"}}}
	csv_value = {"csv_value" : {"path_match": "*.VALUE", "mapping": {"index": "not_analyzed", "fields" : {"double" : {"type" : "double"}}}}}
	csv_u_limit = {"csv_u_limit" : {"path_match": "*.U_LIMIT", "mapping": {"index": "not_analyzed", "fields" : {"double" : {"type" : "double"}}}}}
	csv_l_limit = {"csv_l_limit" : {"path_match": "*.L_LIMIT", "mapping": {"index": "not_analyzed", "fields" : {"double" : {"type" : "double"}}}}}
	csv_test_time = {"csv_test_time" : {"path_match": "*.TEST_TIME", "mapping": {"index": "not_analyzed", "fields" : {"double" : {"type" : "double"}}}}}
	dynamic_templates = [runin_csv_status, runin_csv_value, runin_csv_u_limit, runin_csv_l_limit, runin_csv_test_time, csv_status, csv_value, csv_u_limit, csv_l_limit, csv_test_time]

	analysis = {}
	analysis["analyzer"] = {}
	analysis["tokenizer"] = {}
	analysis["analyzer"]["path-analyzer"] = {"type": "custom", "tokenizer": "path-tokenizer"}
	analysis["tokenizer"]["path-tokenizer"] = {"type": "path_hierarchy"}

	mappings = {"dynamic_templates" : dynamic_templates, "properties" : properties}
	data = {"settings" : {"index.mapping.ignore_malformed": True, "number_of_replicas": 1, "analysis": analysis}, "mappings" : {STAGE: mappings}}
	print json.dumps(data)
	idx_client.create(index=PROJECT, body=data)

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
