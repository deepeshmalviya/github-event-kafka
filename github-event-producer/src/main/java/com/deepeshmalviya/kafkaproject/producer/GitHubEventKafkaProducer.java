package com.deepeshmalviya.kafkaproject.producer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GitHubEventKafkaProducer implements Runnable {

	Logger logger = LoggerFactory.getLogger(GitHubEventKafkaProducer.class);
	
	private BlockingQueue<String> messageQueue;
	
	private static final String KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
	private static final String KAFKA_TOPIC = "github_events";
	private static KafkaProducer<String, String> kafkaProducer;
	
	public GitHubEventKafkaProducer(BlockingQueue<String> messageQueue) {
		this.messageQueue = messageQueue;
		
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		kafkaProducer = new KafkaProducer<String, String>(properties);
	}
	
	public void run() {
		try {
			while(true) {
				String event = this.messageQueue.take();
				sendToKafka(event);
			}
		} catch(InterruptedException ie) {
			logger.info("Kafka producer is interrupted");
			Thread.currentThread().interrupt();
		} finally {
			kafkaProducer.flush();
			kafkaProducer.close();
		}
	}
	
	private void sendToKafka(String event) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(KAFKA_TOPIC, event);
		
		kafkaProducer.send(record, new Callback() {
			
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(exception != null) {
					logger.error("Error while sending message to Kafka", exception);
					exception.printStackTrace();
				}
				
			}
		});
		
	}
}
