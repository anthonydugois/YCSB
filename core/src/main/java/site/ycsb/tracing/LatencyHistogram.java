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

		for (int p = 0; p < 90; p += 5) {
			exporter.write("latency,p" + p + "," + histogram.getValueAtPercentile(p));
		}

		for (int p = 0; p < 10; p++) {
			int perc = 90 + p;

			exporter.write("latency,p" + perc + "," + histogram.getValueAtPercentile(perc));
		}

		for (int p = 1; p < 10; p++) {
			double perc = Math.round((99.0 + p * 0.1) * 100.0) / 100.0;

			exporter.write("latency,p" + perc + "," + histogram.getValueAtPercentile(perc));
		}

		for (int p = 1; p < 10; p++) {
			double perc = Math.round((99.90 + p * 0.01) * 100.0) / 100.0;

			exporter.write("latency,p" + perc + "," + histogram.getValueAtPercentile(perc));
		}
	}
}
