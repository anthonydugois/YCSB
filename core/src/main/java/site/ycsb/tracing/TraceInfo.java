package site.ycsb.tracing;

import site.ycsb.measures.Exporter;
import site.ycsb.measures.Exportable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TraceInfo {
	private final UUID id;

	private final Map<String, Event> events = new HashMap<>();

	private int responseSizeBytes;

	private int compressedResponseSizeBytes;

	public TraceInfo(UUID id) {
		this.id = id;
	}

	public UUID getId() {
		return id;
	}

	public Map<String, Event> getEvents() {
		return events;
	}

	public int getResponseSizeBytes() {
		return responseSizeBytes;
	}

	public void setResponseSizeBytes(int responseSizeBytes) {
		this.responseSizeBytes = responseSizeBytes;
	}

	public int getCompressedResponseSizeBytes() {
		return compressedResponseSizeBytes;
	}

	public void setCompressedResponseSizeBytes(int compressedResponseSizeBytes) {
		this.compressedResponseSizeBytes = compressedResponseSizeBytes;
	}

	public void registerEvent(Event event) {
		events.put(event.getName(), event);
	}

	public static class Event implements Exportable {
		private final TraceInfo traceInfo;

		private final String name;

		private final InetSocketAddress source;

		private final String thread;

		private final long timestamp;

		private final int durationMicros;

		public Event(TraceInfo traceInfo, String name, InetSocketAddress source, String thread, long timestamp, int durationMicros) {
			this.traceInfo = traceInfo;
			this.name = name;
			this.source = source;
			this.thread = thread;
			this.timestamp = timestamp;
			this.durationMicros = durationMicros;
		}

		public TraceInfo getTraceInfo() {
			return traceInfo;
		}

		public String getName() {
			return name;
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

		@Override
		public void export(Exporter exporter) throws IOException {
			exporter.write(
					traceInfo.getId() + "," +
							traceInfo.getResponseSizeBytes() + "," +
							name + "," +
							source + "," +
							thread + "," +
							durationMicros
			);
		}
	}
}
