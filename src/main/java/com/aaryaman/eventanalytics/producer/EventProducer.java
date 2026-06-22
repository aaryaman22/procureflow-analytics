package com.aaryaman.eventanalytics.producer;

import com.aaryaman.eventanalytics.dto.EventRequest;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
public class EventProducer {

	private static final Logger log = LoggerFactory.getLogger(EventProducer.class);

	private final KafkaTemplate<String, EventRequest> kafkaTemplate;
	private final String topic;

	public EventProducer(
			KafkaTemplate<String, EventRequest> kafkaTemplate,
			@Value("${app.kafka.topic.procurement-events}") String topic) {
		this.kafkaTemplate = kafkaTemplate;
		this.topic = topic;
	}

	public CompletableFuture<SendResult<String, EventRequest>> publish(EventRequest event) {
		String key = event.aggregateId().toString();

		log.info(
				"Publishing event to Kafka: topic={}, key={}, eventId={}, eventType={}",
				topic,
				key,
				event.eventId(),
				event.eventType());

		ProducerRecord<String, EventRequest> record = new ProducerRecord<>(topic, key, event);

		return kafkaTemplate.send(record).whenComplete((result, ex) -> {
			if (ex != null) {
				log.error(
						"Failed to publish event to Kafka: topic={}, eventId={}, eventType={}",
						topic,
						event.eventId(),
						event.eventType(),
						ex);
				return;
			}

			log.info(
					"Successfully published event to Kafka: topic={}, eventId={}, partition={}, offset={}",
					topic,
					event.eventId(),
					result.getRecordMetadata().partition(),
					result.getRecordMetadata().offset());
		});
	}

}
