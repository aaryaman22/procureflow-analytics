package com.aaryaman.eventanalytics.controller;

import com.aaryaman.eventanalytics.dto.EventRequest;
import com.aaryaman.eventanalytics.service.EventService;
import jakarta.validation.Valid;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {

	private static final Logger log = LoggerFactory.getLogger(EventController.class);

	private final EventService eventService;

	public EventController(EventService eventService) {
		this.eventService = eventService;
	}

	@PostMapping
	public ResponseEntity<EventAcceptedResponse> ingestEvent(@Valid @RequestBody EventRequest request) {
		log.atInfo()
				.addKeyValue("eventId", request.eventId())
				.addKeyValue("eventType", request.eventType())
				.addKeyValue("aggregateId", request.aggregateId())
				.log("Received event ingestion request");

		eventService.acceptEvent(request);

		return ResponseEntity.accepted()
				.body(new EventAcceptedResponse(request.eventId(), "Event accepted"));
	}

	public record EventAcceptedResponse(UUID eventId, String message) {
	}

}
