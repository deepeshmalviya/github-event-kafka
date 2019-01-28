package com.deepeshmalviya.kafkaproject.producer;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class GithubEventReader implements Runnable {

	Logger logger = LoggerFactory.getLogger(GithubEventReader.class);
	
	private BlockingQueue<String> messageQueue;
	private static final String URL = "https://api.github.com/events";
	private static final OkHttpClient httpClient = new OkHttpClient();
	
	public GithubEventReader(BlockingQueue<String> messageQueue) {
		this.messageQueue = messageQueue;
	}
	
	public void run() {
		try {
			while(true) {
				requestGitHubEvents();
				Thread.sleep(60*1000);
			}
		} catch(InterruptedException ie) {
			logger.info("Github event reading is interrupted");
			Thread.currentThread().interrupt();
		}
	} 
	
	private void requestGitHubEvents() {
		
		Request request = new Request.Builder()
				.url(URL)
				.build();
		
		Call call = httpClient.newCall(request);
		Response response = null;
		
		try {
			response = call.execute();
			String responseData = response.body().string();
			parseResponse(responseData);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(response != null) {
				response = null;
			}
		}
		
	}
	
	private void parseResponse(String data) {
		
		Gson gson = new Gson();
		
		JsonArray events = gson.fromJson(data, JsonArray.class);
		
		for(JsonElement event: events) {
			event.getAsJsonObject().remove("payload");
			String jsonEvent = event.getAsJsonObject().toString();
			this.messageQueue.add(jsonEvent);
			logger.info(jsonEvent);
		}
	}
	
}
