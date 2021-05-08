package guru.bonacci.kafka.reactive.interactive.query;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@AllArgsConstructor
public class CountBean {

	private String id;
	private Long count;
}
