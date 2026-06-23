package com.aaryaman.eventanalytics.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.aaryaman.eventanalytics.enums.EventType;
import com.aaryaman.eventanalytics.repository.EventRepository;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(
		partitions = 1,
		topics = {"procurement-events", "procurement-events-dlq"})
class EventIngestionIntegrationTest {

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private EventRepository eventRepository;

	private UUID eventIdUnderTest;

	@AfterEach
	void cleanup() {
		if (eventIdUnderTest != null) {
			eventRepository.findByEventId(eventIdUnderTest).ifPresent(eventRepository::delete);
		}
	}

	@Test
	void shouldAcceptEventAndPersistViaKafka() throws Exception {
		eventIdUnderTest = UUID.randomUUID();
		UUID aggregateId = UUID.randomUUID();

		String requestBody = """
				{
				  "event_id": "%s",
				  "event_type": "RFP_CREATED",
				  "aggregate_id": "%s",
				  "user_id": "integration-test-user",
				  "payload": { "title": "Integration Test RFP" },
				  "occurred_at": "2026-06-23T10:00:00Z"
				}
				""".formatted(eventIdUnderTest, aggregateId);

		mockMvc.perform(post("/api/v1/events")
						.contentType(MediaType.APPLICATION_JSON)
						.content(requestBody))
				.andExpect(status().isAccepted())
				.andExpect(jsonPath("$.eventId").value(eventIdUnderTest.toString()))
				.andExpect(jsonPath("$.message").value("Event accepted"));

		Awaitility.await()
				.atMost(Duration.ofSeconds(15))
				.pollInterval(Duration.ofMillis(250))
				.untilAsserted(() -> {
					var entity = eventRepository.findByEventId(eventIdUnderTest);
					assertThat(entity).isPresent();
					assertThat(entity.get().getEventType()).isEqualTo(EventType.RFP_CREATED);
					assertThat(entity.get().getAggregateId()).isEqualTo(aggregateId);
					assertThat(entity.get().getUserId()).isEqualTo("integration-test-user");
					assertThat(entity.get().getPayload()).containsEntry("title", "Integration Test RFP");
				});
	}

}
