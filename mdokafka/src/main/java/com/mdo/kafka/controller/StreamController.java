package com.mdo.kafka.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mdo.kafka.producer.Sender;

@RestController
public class StreamController {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(StreamController.class);

	@Autowired
	private Sender sender;

	@RequestMapping("/taskcreate")
	public String taskCreate() {
		String topic = "mdotopic.t";
		String message = "welcome to AO MDO Microservices kafka messages";
		LOGGER.info("welcome to AO MDO messages");
		sender.send(topic, message);
		return "Successfully created Task!!";
	}

}
