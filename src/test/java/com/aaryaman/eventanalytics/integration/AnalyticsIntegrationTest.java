package com.aaryaman.eventanalytics.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.aaryaman.eventanalytics.entity.EventEntity;
import com.aaryaman.eventanalytics.enums.EventType;
import com.aaryaman.eventanalytics.repository.EventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(
		partitions = 1,
		topics = {"procurement-events", "procurement-events-dlq"})
class AnalyticsIntegrationTest {

	private static final Instant OCCURRED_AT = Instant.parse("2026-06-23T12:00:00Z");

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private EventRepository eventRepository;

	@Autowired
	private ObjectMapper objectMapper;

	private final List<UUID> seededEventIds = new ArrayList<>();

	@AfterEach
	void cleanup() {
		for (UUID eventId : seededEventIds) {
			eventRepository.findByEventId(eventId).ifPresent(eventRepository::delete);
		}
		seededEventIds.clear();
	}

	@Test
	void shouldReturnEventCountsAndFunnelMetrics() throws Exception {
		Map<EventType, Long> baseline = loadCountsByType();

		seedEvents(EventType.RFP_CREATED, 3);
		seedEvents(EventType.VENDOR_INVITED, 2);
		seedEvents(EventType.PROPOSAL_SUBMITTED, 1);
		seedEvents(EventType.CONTRACT_AWARDED, 1);

		long expectedRfpCreated = baseline.get(EventType.RFP_CREATED) + 3;
		long expectedVendorInvited = baseline.get(EventType.VENDOR_INVITED) + 2;
		long expectedProposalSubmitted = baseline.get(EventType.PROPOSAL_SUBMITTED) + 1;
		long expectedContractAwarded = baseline.get(EventType.CONTRACT_AWARDED) + 1;
		long expectedProposalApproved = baseline.get(EventType.PROPOSAL_APPROVED);
		double expectedConversionRate = expectedRfpCreated == 0
				? 0.0
				: (expectedContractAwarded * 100.0) / expectedRfpCreated;

		mockMvc.perform(get("/api/v1/analytics/event-counts"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.RFP_CREATED").value(expectedRfpCreated))
				.andExpect(jsonPath("$.VENDOR_INVITED").value(expectedVendorInvited))
				.andExpect(jsonPath("$.PROPOSAL_SUBMITTED").value(expectedProposalSubmitted))
				.andExpect(jsonPath("$.PROPOSAL_APPROVED").value(expectedProposalApproved))
				.andExpect(jsonPath("$.CONTRACT_AWARDED").value(expectedContractAwarded));

		MvcResult funnelResult = mockMvc.perform(get("/api/v1/analytics/funnel"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.RFP_CREATED").value(expectedRfpCreated))
				.andExpect(jsonPath("$.VENDOR_INVITED").value(expectedVendorInvited))
				.andExpect(jsonPath("$.PROPOSAL_SUBMITTED").value(expectedProposalSubmitted))
				.andExpect(jsonPath("$.CONTRACT_AWARDED").value(expectedContractAwarded))
				.andReturn();

		JsonNode funnel = objectMapper.readTree(funnelResult.getResponse().getContentAsString());
		assertThat(funnel.get("conversionRate").asDouble()).isEqualTo(expectedConversionRate);
	}

	private void seedEvents(EventType eventType, int count) {
		for (int i = 0; i < count; i++) {
			UUID eventId = UUID.randomUUID();
			seededEventIds.add(eventId);
			eventRepository.save(new EventEntity(
					eventId,
					eventType,
					UUID.randomUUID(),
					"analytics-test-user",
					Map.of("seed", eventType.name(), "index", i),
					OCCURRED_AT));
		}
	}

	private Map<EventType, Long> loadCountsByType() {
		Map<EventType, Long> counts = new EnumMap<>(EventType.class);
		for (EventType eventType : EventType.values()) {
			counts.put(eventType, 0L);
		}
		for (Object[] row : eventRepository.countEventsByEventType()) {
			EventType eventType = (EventType) row[0];
			long count = ((Number) row[1]).longValue();
			counts.put(eventType, count);
		}
		return counts;
	}

}
