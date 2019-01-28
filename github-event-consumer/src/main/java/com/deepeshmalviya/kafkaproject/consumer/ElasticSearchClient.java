package com.deepeshmalviya.kafkaproject.consumer;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchClient {
	
	private static final String ELASTICSEARCH_HOSTNAME = "127.0.0.1";
	private static final int ELASTICSEARCH_PORT = 9200;
	
	private static final RestHighLevelClient elasticSearchClient = new RestHighLevelClient(
			RestClient.builder(
					new HttpHost(
							ELASTICSEARCH_HOSTNAME, ELASTICSEARCH_PORT)));
			
	private ElasticSearchClient() {}
	
	public static RestHighLevelClient getInstance() {
		return elasticSearchClient;
	}
	
}
