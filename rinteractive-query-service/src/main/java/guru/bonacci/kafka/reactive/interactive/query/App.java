package guru.bonacci.kafka.reactive.interactive.query;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;
import static org.springframework.web.reactive.function.server.ServerResponse.notFound;
import static org.springframework.web.reactive.function.server.ServerResponse.ok;

import java.util.function.Consumer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import reactor.blockhound.BlockHound;

@SpringBootApplication
public class App {

	static final String STORE_NAME = "count-store";

	static {
		BlockHound.install();
	}

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}

	public static class KStreamApp {

		ReadOnlyKeyValueStore<String, Long> keyValueStore;

		@Bean
		public Consumer<KStream<String, String>> process() {
			return input -> input
					.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
					.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
						.withKeySerde(Serdes.String())
						.withValueSerde(Serdes.Long()))
					.toStream().print(Printed.toSysOut());
		}
	}

	@Bean
	RouterFunction<ServerResponse> endpoint(CountService counter) {
	  return route(GET("/count/{id}"), 
	    req -> {
	  		return ok().body(counter.bitte(req.pathVariable("id")), CountBean.class)
	  				.onErrorResume(NotFoundException.class, e -> notFound().build());
	    });
	}
}
