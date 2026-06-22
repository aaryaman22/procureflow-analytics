package com.aaryaman.eventanalytics.config;

import com.aaryaman.eventanalytics.dto.EventRequest;
import com.aaryaman.eventanalytics.producer.DltProducer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${spring.kafka.consumer.group-id}")
	private String consumerGroupId;

	@Bean
	public ProducerFactory<String, EventRequest> producerFactory() {
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		config.put(ProducerConfig.ACKS_CONFIG, "all");
		config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		config.put(ProducerConfig.RETRIES_CONFIG, 3);
		config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

		return new DefaultKafkaProducerFactory<>(config);
	}

	@Bean
	public KafkaTemplate<String, EventRequest> kafkaTemplate(ProducerFactory<String, EventRequest> producerFactory) {
		return new KafkaTemplate<>(producerFactory);
	}

	@Bean
	public ProducerFactory<String, String> dltProducerFactory() {
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.ACKS_CONFIG, "all");

		return new DefaultKafkaProducerFactory<>(config);
	}

	@Bean
	public KafkaTemplate<String, String> dltKafkaTemplate(ProducerFactory<String, String> dltProducerFactory) {
		return new KafkaTemplate<>(dltProducerFactory);
	}

	@Bean
	public ConsumerFactory<String, EventRequest> consumerFactory() {
		Map<String, Object> config = new HashMap<>();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.aaryaman.eventanalytics.dto");
		config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EventRequest.class.getName());
		config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

		return new DefaultKafkaConsumerFactory<>(config);
	}

	@Bean
	public DefaultErrorHandler kafkaErrorHandler(DltProducer dltProducer) {
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(
				(record, exception) -> dltProducer.publishFailedRecord(record, exception),
				new FixedBackOff(0L, 0L));
		errorHandler.setCommitRecovered(true);
		return errorHandler;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, EventRequest> kafkaListenerContainerFactory(
			ConsumerFactory<String, EventRequest> consumerFactory,
			DefaultErrorHandler kafkaErrorHandler) {
		ConcurrentKafkaListenerContainerFactory<String, EventRequest> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		factory.setCommonErrorHandler(kafkaErrorHandler);
		return factory;
	}

}
