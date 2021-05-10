package guru.bonacci.kafka.active.interactive.query;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class CountService {

	private final InteractiveQueryService interactiveQueryService;
	
	
	public CountBean bitte(String id) {
		HostInfo hostInfo = interactiveQueryService.getHostInfo(App.STORE_NAME, id, new StringSerializer());

		if (interactiveQueryService.getCurrentHostInfo().equals(hostInfo)) {
			return findLocal(hostInfo, id);
		}
		else {
			log.info("Request for {} is served from different host: {}", id, hostInfo);
			RestTemplate restTemplate = new RestTemplate();
			return restTemplate.getForObject(
					String.format("http://%s:%d/count/%s", hostInfo.host(),
							hostInfo.port(), id), CountBean.class);
		}
	}

	private CountBean findLocal(final HostInfo hostInfo, final String id) {
		log.info("Request for {} served from same host: {}", id, hostInfo);
		final ReadOnlyKeyValueStore<String, Long> store =
					interactiveQueryService.getQueryableStore(App.STORE_NAME, QueryableStoreTypes.<String, Long>keyValueStore());

		final Long count = store.get(id);
		if (count == null) {
			throw new NotFoundException("Not found??");
		}
		return new CountBean(id, count);
	}
}
