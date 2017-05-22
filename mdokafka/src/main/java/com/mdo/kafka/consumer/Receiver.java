package com.mdo.kafka.consumer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.mdo.kafka.service.StreamService;

public class Receiver {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(Receiver.class);

	private CountDownLatch latch = new CountDownLatch(1);
	
	@Autowired
	protected StreamService streamService ;

	public CountDownLatch getLatch() {
		return latch;
	}

	@KafkaListener(topics = "${topic.mdo}")
	public void receive(String message) {
		LOGGER.info("received message='{}'", message);
		streamService.insertTask(message);
		//streamService.insertTaskToRedis(message);
		
		latch.countDown();
	}
}
