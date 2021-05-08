package kafka.streams.interactive.query;

public class CountBean {

	private Integer id;
	private Long count;

	public CountBean() {}

	public CountBean(final Integer id, final Long count) {
		this.id = id;
		this.count = count;
	}

	public Integer getId() {
		return id;
	}

	public Long getCount() {
		return count;
	}
}
