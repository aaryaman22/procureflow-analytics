package com.aaryaman.eventanalytics.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.aaryaman.eventanalytics.repository.EventRepository;
import java.time.Duration;
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
class IdempotencyIntegrationTest {

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
	void shouldPersistOnlyOnceWhenSameEventIsSubmittedTwice() throws Exception {
		eventIdUnderTest = UUID.randomUUID();
		UUID aggregateId = UUID.randomUUID();

		String requestBody = """
				{
				  "event_id": "%s",
				  "event_type": "VENDOR_INVITED",
				  "aggregate_id": "%s",
				  "user_id": "idempotency-test-user",
				  "payload": { "vendor": "Acme Corp" },
				  "occurred_at": "2026-06-23T11:00:00Z"
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
				.untilAsserted(() -> assertThat(eventRepository.findByEventId(eventIdUnderTest)).isPresent());

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
					assertThat(eventRepository.existsByEventId(eventIdUnderTest)).isTrue();
					assertThat(countRowsWithEventId(eventIdUnderTest)).isEqualTo(1);
				});
	}

	private long countRowsWithEventId(UUID eventId) {
		return eventRepository.findAll().stream()
				.filter(event -> eventId.equals(event.getEventId()))
				.count();
	}

}
