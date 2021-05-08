package kafka.streams.interactive.query;

import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;

import reactor.core.publisher.Mono;

@SpringBootApplication
public class App {

	static final String STORE_NAME = "prod-id-count-store";

	@Autowired
	private InteractiveQueryService interactiveQueryService;
	
	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}

	public static class KStreamApp {

		ReadOnlyKeyValueStore<Integer, Long> keyValueStore;

		@Bean
		public Consumer<KStream<Integer, Product>> process() {
			return input -> input
					.map((key, value) -> new KeyValue<>(value.id, value))
					.groupByKey(Grouped.with(Serdes.Integer(), new JsonSerde<>(Product.class)))
					.count(Materialized.<Integer, Long, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
						.withKeySerde(Serdes.Integer())
						.withValueSerde(Serdes.Long()))
					.toStream().print(Printed.toSysOut());
		}
	}

	static class Product {

		Integer id;

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}
	}


	@RestController
	public class FooController {

		private final Log logger = LogFactory.getLog(getClass());

		@RequestMapping("/counter/{id}")
		public Mono<CountBean> products(@PathVariable("id") Integer id) {
			HostInfo hostInfo = interactiveQueryService.getHostInfo(App.STORE_NAME, id, new IntegerSerializer());
			
			return interactiveQueryService.getCurrentHostInfo().equals(hostInfo) ? 			
					findLocal(hostInfo, id) : findRemote(hostInfo, id);
		}

		private Mono<CountBean> findLocal(final HostInfo hostInfo, final Integer id) {
			logger.info("Request served from same host: " + hostInfo);
			final ReadOnlyKeyValueStore<Integer, Long> productStore =
					interactiveQueryService.getQueryableStore(App.STORE_NAME, QueryableStoreTypes.<Integer, Long>keyValueStore());

			final Long count = productStore.get(id);
			if (count == null) {
				throw new IllegalArgumentException("hi");
			}
			return Mono.just(new CountBean(id, count));
		}
		
		private Mono<CountBean> findRemote(final HostInfo hostInfo, final Integer id) {
			logger.info("Request is served from different host: " + hostInfo);

			// cache me please...
			WebClient client = WebClient.create(String.format("http://%s:%d", hostInfo.host(), hostInfo.port()));
			return client.get().uri("/counter/" + id).retrieve().bodyToMono(CountBean.class);
		}
	}
}
