package com.aaryaman.eventanalytics.service;

import com.aaryaman.eventanalytics.enums.EventType;
import com.aaryaman.eventanalytics.repository.EventRepository;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class AnalyticsService {

	private static final Logger log = LoggerFactory.getLogger(AnalyticsService.class);

	private static final List<EventType> FUNNEL_STAGES = List.of(
			EventType.RFP_CREATED,
			EventType.VENDOR_INVITED,
			EventType.PROPOSAL_SUBMITTED,
			EventType.CONTRACT_AWARDED);

	private final EventRepository eventRepository;

	public AnalyticsService(EventRepository eventRepository) {
		this.eventRepository = eventRepository;
	}

	public Map<EventType, Long> getEventCountsByType() {
		Map<EventType, Long> counts = loadEventCountsByType();

		log.atInfo().addKeyValue("resultCount", counts.size()).log("Fetched event counts by type");

		return counts;
	}

	public Map<String, Object> getProcurementFunnel() {
		Map<EventType, Long> counts = loadEventCountsByType();

		Map<String, Object> funnel = new LinkedHashMap<>();
		for (EventType stage : FUNNEL_STAGES) {
			funnel.put(stage.name(), counts.get(stage));
		}

		long rfpCreated = counts.get(EventType.RFP_CREATED);
		long contractAwarded = counts.get(EventType.CONTRACT_AWARDED);
		double conversionRate = rfpCreated == 0 ? 0.0 : (contractAwarded * 100.0) / rfpCreated;
		funnel.put("conversionRate", conversionRate);

		log.atInfo()
				.addKeyValue("rfpCreated", rfpCreated)
				.addKeyValue("contractAwarded", contractAwarded)
				.addKeyValue("conversionRate", conversionRate)
				.log("Fetched procurement funnel");

		return funnel;
	}

	private Map<EventType, Long> loadEventCountsByType() {
		Map<EventType, Long> counts = new EnumMap<>(EventType.class);
		for (EventType eventType : EventType.values()) {
			counts.put(eventType, 0L);
		}

		List<Object[]> results = eventRepository.countEventsByEventType();
		for (Object[] row : results) {
			EventType eventType = (EventType) row[0];
			Long count = (Long) row[1];
			counts.put(eventType, count);
		}

		return counts;
	}

}
