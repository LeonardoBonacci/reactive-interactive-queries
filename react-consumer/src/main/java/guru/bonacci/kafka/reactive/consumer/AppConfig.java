package guru.bonacci.kafka.reactive.consumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

@Slf4j
@Configuration
public class AppConfig {

	private static final String BOOTSTRAP_SERVERS = "localhost:9092";

	@Bean
	WebClient webclient() {
		return WebClient.create("http://localhost:8080");
	}
	
	@Bean
	KafkaReceiver<String, String> receiver() {
		String groupId = "sample-group";
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		return KafkaReceiver.create(ReceiverOptions.<String, String>create(props)
				.addAssignListener(p -> log.warn("Group {} partitions assigned {}", groupId, p))
				.addRevokeListener(p -> log.warn("Group {} partitions assigned {}", groupId, p))
				.subscription(Collections.singleton(ReactiveKafkaApp.TOPIC_IN)));
	}

	@Bean
	KafkaSender<String, Long> sender() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
		return KafkaSender.create(SenderOptions.create(props));
	}
}
