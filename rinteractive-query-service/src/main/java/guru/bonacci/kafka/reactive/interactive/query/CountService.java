package guru.bonacci.kafka.reactive.interactive.query;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Service
@RequiredArgsConstructor
public class CountService {

	private final InteractiveQueryService interactiveQueryService;
	
	
	public Mono<CountBean> bitte(String id) {
		log.info("Request for: " + id);

		HostInfo hostInfo = interactiveQueryService.getHostInfo(App.STORE_NAME, id, new StringSerializer());
		
		return findLocal(hostInfo, id)
				.switchIfEmpty(findRemote(hostInfo, id));
	}

	private Mono<CountBean> findLocal(final HostInfo hostInfo, final String id) {
		log.info("Request served from same host: " + hostInfo);

		if (!interactiveQueryService.getCurrentHostInfo().equals(hostInfo))
			return Mono.empty();

		return Mono.fromCallable(() -> {
			final ReadOnlyKeyValueStore<String, Long> store =
					interactiveQueryService.getQueryableStore(App.STORE_NAME, QueryableStoreTypes.<String, Long>keyValueStore());

			final Long count = store.get(id);
			if (count == null) {
				throw new NotFoundException("Abusing not found acception...");
			}
			return new CountBean(id, count);
		}).subscribeOn(Schedulers.boundedElastic());
	}
	
	private Mono<CountBean> findRemote(final HostInfo hostInfo, final String id) {
		log.info("Request is served from different host: " + hostInfo);

		WebClient client = WebClient.create(String.format("http://%s:%d", hostInfo.host(), hostInfo.port()));
		return client.get().uri("/count/" + id).retrieve().bodyToMono(CountBean.class);
	}
}
