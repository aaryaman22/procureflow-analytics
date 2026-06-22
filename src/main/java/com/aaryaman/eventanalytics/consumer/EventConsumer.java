package com.aaryaman.eventanalytics.consumer;

import com.aaryaman.eventanalytics.dto.EventRequest;
import com.aaryaman.eventanalytics.entity.EventEntity;
import com.aaryaman.eventanalytics.producer.DltProducer;
import com.aaryaman.eventanalytics.repository.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EventConsumer {

	private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

	private final EventRepository eventRepository;
	private final DltProducer dltProducer;

	public EventConsumer(EventRepository eventRepository, DltProducer dltProducer) {
		this.eventRepository = eventRepository;
		this.dltProducer = dltProducer;
	}

	@KafkaListener(
			topics = "${app.kafka.topic.procurement-events}",
			groupId = "event-analytics-group")
	public void consume(EventRequest event) {
		try {
			if (isDuplicateEvent(event)) {
				return;
			}

			persistEvent(event);
		}
		catch (Exception ex) {
			log.atError()
					.addKeyValue("action", "CONSUME_FAILED")
					.addKeyValue("eventId", event.eventId())
					.addKeyValue("eventType", event.eventType())
					.addKeyValue("aggregateId", event.aggregateId())
					.addKeyValue("failureType", ex.getClass().getSimpleName())
					.setCause(ex)
					.log("Unexpected failure while consuming event");

			dltProducer.publish(event, ex);
		}
	}

	private boolean isDuplicateEvent(EventRequest event) {
		if (!eventRepository.existsByEventId(event.eventId())) {
			return false;
		}

		log.atInfo()
				.addKeyValue("action", "SKIP_DUPLICATE")
				.addKeyValue("eventId", event.eventId())
				.addKeyValue("eventType", event.eventType())
				.addKeyValue("aggregateId", event.aggregateId())
				.addKeyValue("reason", "event_id already exists")
				.log("Duplicate event detected — skipping persistence");

		return true;
	}

	private void persistEvent(EventRequest event) {
		EventEntity entity = toEntity(event);

		try {
			EventEntity saved = eventRepository.save(entity);

			log.atInfo()
					.addKeyValue("action", "PERSISTED")
					.addKeyValue("eventId", saved.getEventId())
					.addKeyValue("eventType", saved.getEventType())
					.addKeyValue("aggregateId", saved.getAggregateId())
					.addKeyValue("id", saved.getId())
					.log("Event persisted successfully");
		}
		catch (DataIntegrityViolationException ex) {
			log.atInfo()
					.addKeyValue("action", "SKIP_DUPLICATE")
					.addKeyValue("eventId", event.eventId())
					.addKeyValue("eventType", event.eventType())
					.addKeyValue("aggregateId", event.aggregateId())
					.addKeyValue("reason", "unique constraint on event_id")
					.log("Duplicate event detected — skipping persistence");
		}
	}

	private EventEntity toEntity(EventRequest event) {
		return new EventEntity(
				event.eventId(),
				event.eventType(),
				event.aggregateId(),
				event.userId(),
				event.payload(),
				event.occurredAt());
	}

}
