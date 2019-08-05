package com.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("kafka")
public class KafkaResource {

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	KafkaTemplate<String, User> kafkaJsonTemplate;

	@GetMapping("publish/{message}")
	public String post(@PathVariable("message") String message) {
		kafkaTemplate.send("test", message);
		return "published successfully";
	}

	@GetMapping("publish/name/{name}/message/{message}")
	public String post(@PathVariable("name") String name, @PathVariable("message") String message) {
		kafkaJsonTemplate.send("testjson", new User(name, message));
		return "published successfully";
	}

}
