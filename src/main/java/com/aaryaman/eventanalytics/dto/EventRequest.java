package com.aaryaman.eventanalytics.dto;

import com.aaryaman.eventanalytics.enums.EventType;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record EventRequest(

		@NotNull
		@JsonProperty("event_id")
		UUID eventId,

		@NotNull
		@JsonProperty("event_type")
		EventType eventType,

		@NotNull
		@JsonProperty("aggregate_id")
		UUID aggregateId,

		@NotBlank
		@JsonProperty("user_id")
		String userId,

		@NotNull
		Map<String, Object> payload,

		@NotNull
		@JsonProperty("occurred_at")
		Instant occurredAt

) {
}
