package site.ycsb.measures;

import site.ycsb.tracing.LatencyHistogram;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public abstract class Measure {
	public enum Type {
		RAW,
		HISTOGRAM
	}

	protected final String name;

	protected final Type type;

	public Measure(String name, Type type) {
		this.name = name;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public Type getType() {
		return type;
	}

	public abstract void measure(Object value);

	public abstract void export(Exporter exporter);

	public static Measure create(String name, Measure.Type type) {
		switch (type) {
			case RAW:
				return new RawMeasure(name);
			case HISTOGRAM:
				return new HistogramMeasure(name);
		}

		return null;
	}

	public static class RawMeasure extends Measure {
		private final List<Exportable> values = new LinkedList<>();

		public RawMeasure(String name) {
			super(name, Type.RAW);
		}

		@Override
		public void measure(Object value) {
			values.add((Exportable) value);
		}

		@Override
		public void export(Exporter exporter) {
			try {
				exporter.write("");
				exporter.write(name);
				exporter.write("");

				for (Exportable value : values) {
					value.export(exporter);
				}
			} catch (IOException exception) {
				exception.printStackTrace();
			}
		}
	}

	public static class HistogramMeasure extends Measure {
		private final LatencyHistogram histogram = new LatencyHistogram();

		public HistogramMeasure(String name) {
			super(name, Type.HISTOGRAM);
		}

		@Override
		public void measure(Object value) {
			histogram.record((long) value);
		}

		@Override
		public void export(Exporter exporter) {
			histogram.save();

			try {
				exporter.write("");
				exporter.write(name);
				exporter.write("");

				histogram.export(exporter);
			} catch (IOException exception) {
				exception.printStackTrace();
			}
		}
	}
}
