package com.mdo.kafka.controller;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mdo.kafka.domain.Tasks;
import com.mdo.kafka.messagehub.MessageHub;
import com.mdo.kafka.messagehub.ProducerHub;
import com.mdo.kafka.producer.Sender;

@RestController
public class StreamController {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(StreamController.class);

	@Value("${topic.mdo}")
	private String topicname;

	@Autowired
	private Sender sender;
	
	
	@RequestMapping("/taskcreate")
	public String taskCreate() throws JsonProcessingException {
		// String topic = "mdotopic.t";
		// String message = "Welcome to MDO STREAM TEST MESSAGES";
		// String message =
		// "{\"taskId\":1,\"documentId\":10,\"type\":\"Customs Manifest\",\"status\":\"PENDING\",\"taskItem\":\"Customs Manifest\",\"taskItemStatus\":\"false\",\"taskItemUser\":\"ADIDAS\",\"taskItemTimeStamp\":\"2011-01-18 00:00:00.0\",\"urgent\":false}";
		Tasks t1Tasks = new Tasks(1, 10, "Customs Manifest", "PENDING", false,
				"Customs Manifest", "false", "ADIDAS", "2011-01-18 00:00:00.0");
		ObjectMapper mapper = new ObjectMapper();
		String jsonInString = mapper.writeValueAsString(t1Tasks);
		LOGGER.info("sending message to task topic " + jsonInString);
		// sender.send(topicname, message);
		sender.send(topicname, jsonInString);
		LOGGER.info("Second task ==========");

		Tasks t2Tasks = new Tasks(1, 10, "testing Manifest", "PENDING", false,
				"testing Manifest", "false", "ADIDAS", "2011-01-18 00:00:00.0");
		ObjectMapper mapper2 = new ObjectMapper();
		String jsonInString2 = mapper2.writeValueAsString(t2Tasks);
		LOGGER.info("sending message to task topic " + jsonInString2);
		// sender.send(topicname, message);
		sender.send(topicname, jsonInString2);

		return "Successfully created Task!!";
	}

	@RequestMapping(value = "/taskupdate", method = RequestMethod.POST)
	public String taskUpdate(@RequestBody Tasks tasks) {

		// Tasks taskObj = new Tasks();
		ObjectMapper mapper = new ObjectMapper();
		try {
			String jsonInString = mapper.writeValueAsString(tasks);
			sender.send(topicname, jsonInString);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return "Successfully updated Task!!";
	}

	@RequestMapping("/testupdate")
	public String taskUpdateTest() {

		final String uri = "http://localhost:8078/taskupdate";
		Tasks t1Tasks = new Tasks(1, 10, "Customs Manifest test", "PENDING",
				false, "Customs Manifest test", "false", "ADIDAS",
				"2011-01-18 00:00:00.0");

		RestTemplate restTemplate = new RestTemplate();
		String postForObject = restTemplate.postForObject(uri, t1Tasks,
				String.class);

		return "Success";
	}

	@RequestMapping("/inserttask")
	public String insertTask() {

		final String uri = "http://localhost:8058/taskinsert";
		//final String uri = "http://ao-stream.mybluemix.net/taskinsert";
		List<Tasks> tasklst = new ArrayList();
		Tasks t1Tasks = new Tasks(1, 10, "Customs Manifest1", "PENDING", false,
				"Customs Manifest1", "false", "ADIDAS", "2011-01-18 00:00:00.0");
		tasklst.add(t1Tasks);
		Tasks t2Tasks = new Tasks(2, 10, "Customs Manifest2", "PENDING", false,
				"Customs Manifest2", "false", "ADIDAS", "2011-01-18 00:00:00.0");
		tasklst.add(t2Tasks);
		Tasks t3Tasks = new Tasks(3, 10, "Customs Manifest3", "PENDING", false,
				"Customs Manifest3", "false", "ADIDAS", "2011-01-18 00:00:00.0");
		tasklst.add(t3Tasks);

		RestTemplate restTemplate = new RestTemplate();
		String postForObject = restTemplate.postForObject(uri, tasklst,
				String.class);

		return postForObject;
	}

	@RequestMapping(value = "/taskinsert", method = RequestMethod.POST)
	public String taskInsert(@RequestBody List<Tasks> tasklst) {

		List<Tasks> tasklsts = tasklst;
		for (Tasks tasks : tasklsts) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				String jsonInString = mapper.writeValueAsString(tasks);
				System.out.println("jsonInString =========" + jsonInString);
				sender.send(topicname, jsonInString);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return "Successfully inserted Task!!";
	}
	
	/* for bluemix starts */
	
	
	@RequestMapping("/inserttaskbm")
	public String insertTaskBM() {

		final String uri = "http://localhost:8058/taskinsertbm";
		//final String uri = "http://ao-stream.mybluemix.net/taskinsert";
		List<Tasks> tasklst = new ArrayList();
		Tasks t1Tasks = new Tasks(1, 10, "Customs Manifest1", "PENDING", false,
				"Customs Manifest1", "false", "ADIDAS", "2011-01-18 00:00:00.0");
		tasklst.add(t1Tasks);
		Tasks t2Tasks = new Tasks(2, 10, "Customs Manifest2", "PENDING", false,
				"Customs Manifest2", "false", "ADIDAS", "2011-01-18 00:00:00.0");
		tasklst.add(t2Tasks);
		Tasks t3Tasks = new Tasks(3, 10, "Customs Manifest3", "PENDING", false,
				"Customs Manifest3", "false", "ADIDAS", "2011-01-18 00:00:00.0");
		tasklst.add(t3Tasks);

		RestTemplate restTemplate = new RestTemplate();
		String postForObject = restTemplate.postForObject(uri, tasklst,
				String.class);

		return postForObject;
	}

	@RequestMapping(value = "/taskinsertbm", method = RequestMethod.POST)
	public String taskInsertBM(@RequestBody List<Tasks> tasklst) {

		List<Tasks> tasklsts = tasklst;
		for (Tasks tasks : tasklsts) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				String jsonInString = mapper.writeValueAsString(tasks);
				System.out.println("jsonInString =========" + jsonInString);
				sender.send(topicname, jsonInString);
				
				//producerHub.sendMsg(jsonInString);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return "Successfully inserted Task!!";
	}
	
	/* for bluemix ends */

}
