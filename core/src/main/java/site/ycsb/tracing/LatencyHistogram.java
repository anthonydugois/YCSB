package site.ycsb.tracing;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import site.ycsb.measures.Exportable;
import site.ycsb.measures.Exporter;

import java.io.IOException;

public class LatencyHistogram implements Exportable {
	private static final int DIGITS = 3;

	private final Recorder recorder = new Recorder(DIGITS);

	private Histogram currentIntervalHistogram = null;
	private Histogram histogram = null;

	public void record(long value) {
		recorder.recordValue(value);
	}

	public void save() {
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

	@Override
	public void export(Exporter exporter) throws IOException {
		exporter.write("latency,count," + histogram.getTotalCount());
		exporter.write("latency,avg," + histogram.getMean());
		exporter.write("latency,min," + histogram.getMinValue());
		exporter.write("latency,max," + histogram.getMaxValue());
		exporter.write("latency,50p," + histogram.getValueAtPercentile(50));
		exporter.write("latency,75p," + histogram.getValueAtPercentile(75));
		exporter.write("latency,95p," + histogram.getValueAtPercentile(95));
		exporter.write("latency,99p," + histogram.getValueAtPercentile(99));
		exporter.write("latency,99.9p," + histogram.getValueAtPercentile(99.9));
		exporter.write("latency,99.99p," + histogram.getValueAtPercentile(99.99));
	}
}
