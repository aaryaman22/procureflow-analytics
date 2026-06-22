package com.aaryaman.eventanalytics.entity;

import com.aaryaman.eventanalytics.enums.EventType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "events")
public class EventEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "event_id", nullable = false, unique = true)
	private UUID eventId;

	@Enumerated(EnumType.STRING)
	@Column(name = "event_type", nullable = false, length = 50)
	private EventType eventType;

	@Column(name = "aggregate_id", nullable = false)
	private UUID aggregateId;

	@Column(name = "user_id", nullable = false, length = 100)
	private String userId;

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name = "payload", nullable = false, columnDefinition = "jsonb")
	private Map<String, Object> payload;

	@Column(name = "occurred_at", nullable = false)
	private Instant occurredAt;

	@CreationTimestamp
	@Column(name = "created_at", nullable = false, updatable = false)
	private Instant createdAt;

	protected EventEntity() {
	}

	public EventEntity(
			UUID eventId,
			EventType eventType,
			UUID aggregateId,
			String userId,
			Map<String, Object> payload,
			Instant occurredAt) {
		this.eventId = eventId;
		this.eventType = eventType;
		this.aggregateId = aggregateId;
		this.userId = userId;
		this.payload = payload;
		this.occurredAt = occurredAt;
	}

	public Long getId() {
		return id;
	}

	public UUID getEventId() {
		return eventId;
	}

	public EventType getEventType() {
		return eventType;
	}

	public UUID getAggregateId() {
		return aggregateId;
	}

	public String getUserId() {
		return userId;
	}

	public Map<String, Object> getPayload() {
		return payload;
	}

	public Instant getOccurredAt() {
		return occurredAt;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}
}
