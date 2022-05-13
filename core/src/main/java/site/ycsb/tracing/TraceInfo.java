package site.ycsb.tracing;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TraceInfo {
	private final UUID id;

	private final Map<String, Integer> eventDurationMicros = new HashMap<>();

	public TraceInfo(UUID id) {
		this.id = id;
	}

	public UUID getId() {
		return id;
	}

	public Map<String, Integer> getEventDurationMicros() {
		return eventDurationMicros;
	}

	public void register(String event, int durationMicros) {
		eventDurationMicros.put(event, durationMicros);
	}
}
