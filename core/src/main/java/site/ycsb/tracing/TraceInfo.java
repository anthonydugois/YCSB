package site.ycsb.tracing;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TraceInfo {
	private final UUID id;

	private final Map<String, Event> events = new HashMap<>();

	public TraceInfo(UUID id) {
		this.id = id;
	}

	public UUID getId() {
		return id;
	}

	public Map<String, Event> getEvents() {
		return events;
	}

	public void registerEvent(String name, Event event) {
		events.put(name, event);
	}

	public static class Event {
		private final InetSocketAddress source;

		private final String thread;

		private final long timestamp;

		private final int durationMicros;

		public Event(InetSocketAddress source, String thread, long timestamp, int durationMicros) {
			this.source = source;
			this.thread = thread;
			this.timestamp = timestamp;
			this.durationMicros = durationMicros;
		}

		public InetSocketAddress getSource() {
			return source;
		}

		public String getThread() {
			return thread;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public int getDurationMicros() {
			return durationMicros;
		}
	}
}
