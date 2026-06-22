package com.aaryaman.eventanalytics.service;

import com.aaryaman.eventanalytics.dto.EventRequest;
import com.aaryaman.eventanalytics.producer.EventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class EventService {

	private static final Logger log = LoggerFactory.getLogger(EventService.class);

	private final EventProducer eventProducer;

	public EventService(EventProducer eventProducer) {
		this.eventProducer = eventProducer;
	}

	public void acceptEvent(EventRequest event) {
		log.atInfo()
				.addKeyValue("eventId", event.eventId())
				.addKeyValue("eventType", event.eventType())
				.addKeyValue("aggregateId", event.aggregateId())
				.log("Accepting event for publishing");

		eventProducer.publish(event);
	}

}
