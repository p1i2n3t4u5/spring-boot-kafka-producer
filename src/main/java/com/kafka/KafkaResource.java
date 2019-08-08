package com.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
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
		ListenableFuture<SendResult<String, User>> future = kafkaJsonTemplate.send("testjson", new User(name, message));
		future.addCallback(new ListenableFutureCallback<SendResult<String, User>>() {

			@Override
			public void onSuccess(SendResult<String, User> result) {
				System.out.println(result.getProducerRecord());
				System.out.println(result.getRecordMetadata().offset());
				System.out.println(result.getRecordMetadata().partition());
				System.out.println(result.getRecordMetadata().topic());
			}

			@Override
			public void onFailure(Throwable ex) {
				System.out.println(ex.getMessage());
			}
		});
		return "published successfully";
	}

}
