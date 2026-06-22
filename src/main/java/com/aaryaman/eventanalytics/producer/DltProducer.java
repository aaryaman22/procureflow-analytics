package com.aaryaman.eventanalytics.producer;

import com.aaryaman.eventanalytics.dto.EventRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;

@Component
public class DltProducer {

	private static final Logger log = LoggerFactory.getLogger(DltProducer.class);

	private final KafkaTemplate<String, String> dltKafkaTemplate;
	private final ObjectMapper objectMapper;
	private final String dltTopic;

	public DltProducer(
			KafkaTemplate<String, String> dltKafkaTemplate,
			ObjectMapper objectMapper,
			@Value("${app.kafka.topic.procurement-events-dlq}") String dltTopic) {
		this.dltKafkaTemplate = dltKafkaTemplate;
		this.objectMapper = objectMapper;
		this.dltTopic = dltTopic;
	}

	public void publish(EventRequest event, Exception exception) {
		try {
			String payload = objectMapper.writeValueAsString(event);
			String key = event.aggregateId().toString();
			send(key, payload, exception);
		}
		catch (JsonProcessingException ex) {
			log.atError()
					.addKeyValue("action", "DLQ_PUBLISH_FAILED")
					.addKeyValue("eventId", event.eventId())
					.setCause(ex)
					.log("Failed to serialize event for DLQ");
		}
	}

	public void publishFailedRecord(ConsumerRecord<?, ?> record, Exception exception) {
		String key = record.key() != null ? record.key().toString() : "unknown";
		String payload = extractPayload(record, exception);
		send(key, payload, exception);
	}

	private void send(String key, String payload, Exception exception) {
		log.atError()
				.addKeyValue("action", "SEND_TO_DLQ")
				.addKeyValue("dltTopic", dltTopic)
				.addKeyValue("key", key)
				.addKeyValue("failureType", exception.getClass().getSimpleName())
				.addKeyValue("failureMessage", exception.getMessage())
				.setCause(exception)
				.log("Publishing failed event to DLQ");

		dltKafkaTemplate.send(new ProducerRecord<>(dltTopic, key, payload));
	}

	private String extractPayload(ConsumerRecord<?, ?> record, Exception exception) {
		DeserializationException deserEx = findDeserializationException(exception);
		if (deserEx != null && deserEx.getData() != null) {
			return new String(deserEx.getData(), StandardCharsets.UTF_8);
		}

		Object value = record.value();
		if (value instanceof byte[] bytes) {
			return new String(bytes, StandardCharsets.UTF_8);
		}
		if (value instanceof String str) {
			return str;
		}
		if (value != null) {
			try {
				return objectMapper.writeValueAsString(value);
			}
			catch (JsonProcessingException ex) {
				return value.toString();
			}
		}
		return "";
	}

	private DeserializationException findDeserializationException(Throwable exception) {
		Throwable current = exception;
		while (current != null) {
			if (current instanceof DeserializationException deserEx) {
				return deserEx;
			}
			current = current.getCause();
		}
		return null;
	}

}
