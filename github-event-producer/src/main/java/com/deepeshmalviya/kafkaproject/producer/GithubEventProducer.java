package com.deepeshmalviya.kafkaproject.producer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GithubEventProducer {
	
	Logger logger = LoggerFactory.getLogger(GithubEventProducer.class);
	
	public static void main(String[] args) {
		
		BlockingQueue<String> events = new LinkedBlockingQueue<String>(100);
		
		GithubEventReader githubEventReader = new GithubEventReader(events);
		GitHubEventKafkaProducer gitHubEventKafkaProducer = new GitHubEventKafkaProducer(events);
		
		new Thread(githubEventReader).start();
		new Thread(gitHubEventKafkaProducer).start();
		
	}
	
}
