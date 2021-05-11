package guru.bonacci.kafka.reactive.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

public class LoadProducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "count-us";

    private final KafkaSender<String, String> sender;

    public LoadProducer(String bootstrapServers) {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        SenderOptions<String, String> senderOptions = SenderOptions.create(props);

        sender = KafkaSender.create(senderOptions);
    }

    public void sendMessages(String topic, int count, CountDownLatch latch) throws InterruptedException {
        sender.send(Flux.range(1, count)
                        .map(i -> {
                        	String str = String.valueOf(i);
                        	return SenderRecord.create(new ProducerRecord<>(topic, str, str), i);
                        }))
              .doOnError(e -> {
            	  e.printStackTrace();
              })
              .subscribe(r -> {
            	  RecordMetadata metadata = r.recordMetadata();
            	  System.out.printf("Message %d sent successfully, topic-partition=%s-%d offset=%d\n",
                          r.correlationMetadata(),
                          metadata.topic(),
                          metadata.partition(),
                          metadata.offset());
                  latch.countDown();
              });
    }

    public void close() {
        sender.close();
    }

    public static void main(String[] args) throws Exception {
        int count = 1000;
        CountDownLatch latch = new CountDownLatch(count);
        LoadProducer producer = new LoadProducer(BOOTSTRAP_SERVERS);
        producer.sendMessages(TOPIC, count, latch);
        latch.await(10, TimeUnit.SECONDS);
        producer.close();
    }
}