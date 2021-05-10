package guru.bonacci.kafka.active.interactive.query;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class CountBean {

	private String id;
	private Long count;
}
