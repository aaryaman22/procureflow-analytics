package com.aaryaman.eventanalytics.repository;

import com.aaryaman.eventanalytics.entity.EventEntity;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface EventRepository extends JpaRepository<EventEntity, Long> {

	Optional<EventEntity> findByEventId(UUID eventId);

	boolean existsByEventId(UUID eventId);

	@Query("SELECT e.eventType, COUNT(e) FROM EventEntity e GROUP BY e.eventType")
	List<Object[]> countEventsByEventType();

}
