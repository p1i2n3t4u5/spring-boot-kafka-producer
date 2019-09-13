package com.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

	private static final Logger logger = LoggerFactory.getLogger(KafkaResource.class);

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
		// kafkaJsonTemplate.send(topic, key, data);
		// providing a key ensures that a particular key always go to 1 specific
		// partition

		future.addCallback(new ListenableFutureCallback<SendResult<String, User>>() {

			@Override
			public void onSuccess(SendResult<String, User> result) {
				logger.info("result.getProducerRecord():" + result.getProducerRecord());
				logger.info("offset:" + result.getRecordMetadata().offset() + "  Partition:"
						+ result.getRecordMetadata().partition() + " topic:" + result.getRecordMetadata().topic());
			}

			@Override
			public void onFailure(Throwable ex) {
				logger.error(ex.getMessage());
			}
		});
		return "published successfully";
	}

	@GetMapping("publish/bulk/name/{name}/message/{message}")
	public String postBulk(@PathVariable("name") String name, @PathVariable("message") String message) {

		for (int i = 0; i < 10; i++) {
			ListenableFuture<SendResult<String, User>> future = kafkaJsonTemplate.send("testjson",
					new User(name, message + i));
			// kafkaJsonTemplate.send(topic, key, data);
			// providing a key ensures that a particular key always go to 1 specific
			// partition

			future.addCallback(new ListenableFutureCallback<SendResult<String, User>>() {

				@Override
				public void onSuccess(SendResult<String, User> result) {
					logger.info("result.getProducerRecord():" + result.getProducerRecord());
					logger.info("offset:" + result.getRecordMetadata().offset() + "  Partition:"
							+ result.getRecordMetadata().partition() + " topic:" + result.getRecordMetadata().topic());
				}

				@Override
				public void onFailure(Throwable ex) {
					logger.error(ex.getMessage());
				}
			});

		}

		return "published successfully";
	}

}
