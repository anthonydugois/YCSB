package site.ycsb.measures;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public abstract class Measure {
	public enum Type {
		RAW,
		HISTOGRAM
	}

	private final String name;

	private final Type type;

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
		private final List<Object> values = new LinkedList<>();

		public RawMeasure(String name) {
			super(name, Type.RAW);
		}

		@Override
		public void measure(Object value) {
			values.add(value);
		}

		@Override
		public void export(Exporter exporter) {
			try {
				for (Object value : values) {
					exporter.write(format("", value, " us"));
				}
			} catch (IOException exception) {
				exception.printStackTrace();
			}
		}

		private String format(String prefix, Object value, String suffix) {
			return "[" + getName() + "] " + prefix + value + suffix;
		}
	}

	public static class HistogramMeasure extends Measure {
		private static final int DIGITS = 3;

		private final Recorder recorder = new Recorder(DIGITS);

		private Histogram currentIntervalHistogram = null;
		private Histogram histogram = null;

		public HistogramMeasure(String name) {
			super(name, Type.HISTOGRAM);
		}

		@Override
		public void measure(Object value) {
			recorder.recordValue((long) value);
		}

		@Override
		public void export(Exporter exporter) {
			saveHistogram();

			try {
				exporter.write(format("", histogram.getTotalCount(), " operations"));
				exporter.write(format("avg : ", histogram.getMean(), " us"));
				exporter.write(format("min : ", histogram.getMinValue(), " us"));
				exporter.write(format("max : ", histogram.getMaxValue(), " us"));
				exporter.write(format("50p : ", histogram.getValueAtPercentile(50), " us"));
				exporter.write(format("75p : ", histogram.getValueAtPercentile(75), " us"));
				exporter.write(format("95p : ", histogram.getValueAtPercentile(95), " us"));
				exporter.write(format("99p : ", histogram.getValueAtPercentile(99), " us"));
				exporter.write(format("99.9p : ", histogram.getValueAtPercentile(99.9), " us"));
				exporter.write(format("99.99p : ", histogram.getValueAtPercentile(99.99), " us"));
			} catch (IOException exception) {
				exception.printStackTrace();
			}
		}

		private String format(String prefix, Object value, String suffix) {
			return "[" + getName() + "] " + prefix + value + suffix;
		}

		public void saveHistogram() {
			currentIntervalHistogram = recorder.getIntervalHistogram();

			if (histogram == null) {
				histogram = currentIntervalHistogram;
			} else {
				histogram.add(currentIntervalHistogram);
			}
		}

		public Histogram getIntervalHistogram() {
			return currentIntervalHistogram;
		}

		public Histogram getHistogram() {
			return histogram;
		}
	}
}
