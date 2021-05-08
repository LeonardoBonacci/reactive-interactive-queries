package guru.bonacci.kafka.reactive.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
public class SampleConsumer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "count-us";


    public static void main(String[] args) {
        new SampleConsumer(BOOTSTRAP_SERVERS).consumeMessages(TOPIC);
    }
    
    private final ReceiverOptions<String, String> receiverOptions;
    private final WebClient client;

    public SampleConsumer(String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        receiverOptions = ReceiverOptions.create(props);
        
		client = WebClient.create("http://localhost:8080");
    }

    public Disposable consumeMessages(String topic) {
        ReceiverOptions<String, String> options = receiverOptions.subscription(Collections.singleton(topic));
        Flux<ReceiverRecord<String, String>> kaFlux = KafkaReceiver.create(options).receive();

        Flux<CountBean> cbs = 
        		kaFlux.delayElements(Duration.ofMillis(10))
        		.doOnNext(record -> record.receiverOffset().acknowledge())
        		.flatMap(record -> { log.info(record.value());
        	return client.get().uri("/count/" + record.value()).retrieve()
        				.onStatus(HttpStatus::is4xxClientError, resp -> Mono.empty())
        				.onStatus(HttpStatus::is5xxServerError, resp -> Mono.error(new IllegalStateException("oops")))
        				.bodyToMono(CountBean.class);
        });        
        return cbs.checkpoint("about to subscribe...")
        		  .subscribe(cb -> log.info(cb.toString()), t -> log.error(t.getMessage()));
    }
}
