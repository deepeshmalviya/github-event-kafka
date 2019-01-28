package com.deepeshmalviya.kafkaproject.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GitHubEventKafkaConsumer implements Runnable {
	
	Logger logger = LoggerFactory.getLogger(GitHubEventKafkaConsumer.class);
	
	private final int id;
	private final String topic;
	
	private static final String KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
	
	private final KafkaConsumer<String, String> kafkaConsumer;
	private final RestHighLevelClient elasticSearchClient;
	
	public GitHubEventKafkaConsumer(int id, String topic, String groupId, RestHighLevelClient elasticSearchClient) {
		
		this.id = id;
		this.topic = topic;
		this.elasticSearchClient = elasticSearchClient;
	
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		
		kafkaConsumer = new KafkaConsumer<String, String>(properties);
	}
	
	public void run() {
		
		try {
			kafkaConsumer.subscribe(Arrays.asList(topic));
			
			while(true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(30000));
				int count = records.count();
				logger.info("Records received " + count);
				
				if(count > 0) {
					processSingleRecord(records);
				}
			}
		} catch(WakeupException we) {
			logger.info("Consumer " + id + " is interrupted");
		} finally {
			kafkaConsumer.close();
		}
	}
	
	private void processSingleRecord(ConsumerRecords<String, String> records) {
		for(ConsumerRecord<String, String> record: records) {
			IndexRequest request = new IndexRequest("github", "event");
			request.source(record.value(), XContentType.JSON);
			
			try {
				IndexResponse response = elasticSearchClient.index(request, RequestOptions.DEFAULT);
				logger.info("Written with id " + response.getId() + " for record " + record.value());
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
	}
	
	public void shutdown() {
		kafkaConsumer.wakeup();
	}
}
