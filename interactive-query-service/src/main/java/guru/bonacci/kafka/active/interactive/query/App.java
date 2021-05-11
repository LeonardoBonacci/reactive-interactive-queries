package guru.bonacci.kafka.active.interactive.query;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@SpringBootApplication
public class App {

	static final String STORE_NAME = "act-count-store";

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}

	@Autowired
	CountService counter;

	public static class KStreamApp {

		ReadOnlyKeyValueStore<String, Long> keyValueStore;

		@Bean
		public Consumer<KStream<String, String>> process() {
			return input -> input.map((key, value) -> new KeyValue<>(value, value))
					.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
					.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
							.withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
					.toStream().print(Printed.toSysOut());
		}
	}

	@RestController
	public class Countroller {

		@GetMapping("/count/{id}")
		public CountBean count(@PathVariable(value = "id") String id) {
			try {
				return counter.bitte(id);
			} catch (NotFoundException e) {
				throw new ResponseStatusException(HttpStatus.NOT_FOUND);
			}
		}
	}
}
