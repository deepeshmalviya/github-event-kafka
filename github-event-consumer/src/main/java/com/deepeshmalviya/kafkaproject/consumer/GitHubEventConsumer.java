package com.deepeshmalviya.kafkaproject.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GitHubEventConsumer {
	
	Logger logger = LoggerFactory.getLogger(GitHubEventConsumer.class);
	
	private static final int NUMBER_OF_CONSUMERS = 3;
	
	public static void main(String[] args) {
		
		final ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_CONSUMERS);
		
		RestHighLevelClient elasticSearchClient = ElasticSearchClient.getInstance();
		
		final List<GitHubEventKafkaConsumer> consumers = new ArrayList<GitHubEventKafkaConsumer>();
		
		for(int i=0; i<NUMBER_OF_CONSUMERS; i++) {
			GitHubEventKafkaConsumer consumer = new GitHubEventKafkaConsumer(i, "github_events", "github-event-consumer", elasticSearchClient);
			
			consumers.add(consumer);
			executor.submit(consumer);
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			
			public void run() {
				for(GitHubEventKafkaConsumer consumer: consumers) {
					consumer.shutdown();
				}
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch(InterruptedException ie) {
					ie.printStackTrace();
				}
			}
		});
	}

}
