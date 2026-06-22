package com.aaryaman.eventanalytics.controller;

import com.aaryaman.eventanalytics.enums.EventType;
import com.aaryaman.eventanalytics.service.AnalyticsService;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/analytics")
public class AnalyticsController {

	private static final Logger log = LoggerFactory.getLogger(AnalyticsController.class);

	private final AnalyticsService analyticsService;

	public AnalyticsController(AnalyticsService analyticsService) {
		this.analyticsService = analyticsService;
	}

	@GetMapping("/event-counts")
	public Map<EventType, Long> getEventCounts() {
		log.atInfo().log("Received event counts request");

		return analyticsService.getEventCountsByType();
	}

	@GetMapping("/funnel")
	public Map<String, Object> getProcurementFunnel() {
		log.atInfo().log("Received procurement funnel request");

		return analyticsService.getProcurementFunnel();
	}

}
