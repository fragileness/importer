from elasticsearch import Elasticsearch
from elasticsearch import RequestError
from elasticsearch import ConnectionTimeout
from elasticsearch.client import IndicesClient
import json

def setup():
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
	es = Elasticsearch([{'host': 'localhost', 'port': 9200}], max_retries=10, retry_on_timeout=True)
	idx_client = IndicesClient(es)
	idx_client.create(index='max1', body=data)

if __name__ == '__main__':
	setup()
