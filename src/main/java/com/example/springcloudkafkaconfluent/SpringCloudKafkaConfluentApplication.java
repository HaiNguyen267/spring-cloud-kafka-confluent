package com.example.springcloudkafkaconfluent;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@SpringBootApplication
public class SpringCloudKafkaConfluentApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringCloudKafkaConfluentApplication.class, args);
	}


	static final String TOPIC_NAME = "hobbit";

	@Bean
	NewTopic hobbit2() {
		return TopicBuilder.name("hobbit2")
				.partitions(12)
				.replicas(1) // this must be <= number of broker in the Kafka Cluster
				.build();
	}
}


@Configuration
class KafkaConfiguration {


	@Value("${spring.kafka.properties.bootstrap.servers}")
	private String configServer;


	@Bean
	public ProducerFactory<Integer, String> producerFactory() {
		Map<String, Object> properties = new HashMap<String, Object>();;
		properties.put(BOOTSTRAP_SERVERS_CONFIG, configServer);
		properties.put(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return new DefaultKafkaProducerFactory<>(properties);
	}

	@Bean
	public KafkaTemplate<Integer, String> kafkaTemplate(ProducerFactory<Integer, String> producerFactory) {
		return new KafkaTemplate<>(producerFactory);
	}


	@Bean
	public ConsumerFactory<Integer, String> consumerFactory() {
		Map<String, Object> properties = new HashMap<>();
		properties.put(BOOTSTRAP_SERVERS_CONFIG, configServer);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		return new DefaultKafkaConsumerFactory<>(properties);
	}

}



@RequiredArgsConstructor
@Component
class Producer {
	private final KafkaTemplate<Integer, String> template;

	Faker faker;


	@EventListener(ApplicationStartedEvent.class)
	public void generate() {
		faker = Faker.instance();
		faker.hobbit().quote();
		final Flux<Long> interval = Flux.interval(Duration.ofMillis(1_000));

		final Flux<String> quotes = Flux.fromStream(Stream.generate(() -> faker.hobbit().quote()));

		Flux.zip(interval, quotes).map(objects -> template.send(SpringCloudKafkaConfluentApplication.TOPIC_NAME, faker.random().nextInt(42), objects.getT2())).blockLast();

	}
}


@RequiredArgsConstructor
@Component
class Consumer {


	@KafkaListener(
			topics = SpringCloudKafkaConfluentApplication.TOPIC_NAME,
			groupId = "spring-boot-kafka"
	)
	public void consume(String quote) {
		System.out.println("received = " + quote);
	}
}

