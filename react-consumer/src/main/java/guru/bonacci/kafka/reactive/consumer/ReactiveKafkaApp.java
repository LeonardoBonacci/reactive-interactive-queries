package guru.bonacci.kafka.reactive.consumer;

import java.time.Duration;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Slf4j
@SpringBootApplication
@RequiredArgsConstructor
public class ReactiveKafkaApp {

	static final String TOPIC_IN = "count-us";
	static final String TOPIC_OUT = "counted";

	public static void main(String[] args) {
        SpringApplication.run(ReactiveKafkaApp.class, args);
    }

	private final KafkaReceiver<String, String> receiver;
	private final KafkaSender<String, Long> sender;
	private final WebClient client;
	
    public Flux<?> flux() {
        return receiver
        		.receive()
        		.doOnNext(rec -> log.info("Received {}", rec.value()))
        		.delayElements(Duration.ofSeconds(1))
        		.flatMap(m -> transform(m.key(), m.value()).map(j -> SenderRecord.create(j, m.receiverOffset())))
                .as(sender::send)
                .doOnNext(m -> m.correlationMetadata().acknowledge())
                .doOnCancel(() -> close());
    }
    
    public Mono<ProducerRecord<String, Long>> transform(String key, String value) {
    	return client.get().uri("/count/" + value).retrieve()
    				.onStatus(HttpStatus::is4xxClientError, resp -> Mono.empty())
    				.onStatus(HttpStatus::is5xxServerError, resp -> Mono.error(new IllegalStateException("oops")))
    				.bodyToMono(CountBean.class)
    			.map(cb -> new ProducerRecord<>(TOPIC_OUT, cb.getId(), cb.getCount()));    	
    }
    
    public void close() {
        if (sender != null)
            sender.close();
    }
    
    @Bean
	CommandLineRunner runner() {
		return args -> {
		    flux().blockLast();
            close();
		};
    }
}	