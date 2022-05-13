/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.measurements;

import site.ycsb.measurements.exporter.MeasurementsExporter;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.HdrHistogram.HistogramLogWriter;
import org.HdrHistogram.Recorder;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Take measurements and maintain a HdrHistogram of a given metric, such as READ LATENCY.
 */
public class OneMeasurementHdrHistogram extends OneMeasurement {

	// We need one log per measurement histogram
	private final PrintStream log;
	private final HistogramLogWriter histogramLogWriter;

	private final Recorder histogram;
	private Histogram totalHistogram;

	/**
	 * The name of the property for deciding what percentile values to output.
	 */
	public static final String PERCENTILES_PROPERTY = "hdrhistogram.percentiles";
	public static final String PERCENTILES_PROPERTY_DEFAULT = "50,75,95,99,99.9,99.99";

	/**
	 * The name of the property for determining if we should print out the buckets.
	 */
	public static final String VERBOSE_PROPERTY = "measurement.histogram.verbose";

	/**
	 * Whether or not to emit the histogram buckets.
	 */
	private final boolean verbose;

	private final List<Double> percentiles;

	public OneMeasurementHdrHistogram(String name, Properties props) {
		super(name);

		this.percentiles = getPercentiles(props.getProperty(PERCENTILES_PROPERTY, PERCENTILES_PROPERTY_DEFAULT));
		this.verbose = Boolean.parseBoolean(props.getProperty(VERBOSE_PROPERTY, "false"));

		boolean shouldLog = Boolean.parseBoolean(props.getProperty("hdrhistogram.fileoutput", "false"));

		if (!shouldLog) {
			this.log = null;
			this.histogramLogWriter = null;
		} else {
			try {
				final String filename = props.getProperty("hdrhistogram.output.path", "") + name + ".hdr";

				this.log = new PrintStream(new FileOutputStream(filename), false);
			} catch (FileNotFoundException e) {
				throw new RuntimeException("Failed to open hdr histogram output file", e);
			}

			this.histogramLogWriter = new HistogramLogWriter(log);
			this.histogramLogWriter.outputComment("[Logging for: " + name + "]");
			this.histogramLogWriter.outputLogFormatVersion();

			long now = System.currentTimeMillis();

			this.histogramLogWriter.outputStartTime(now);
			this.histogramLogWriter.setBaseTime(now);
			this.histogramLogWriter.outputLegend();
		}

		this.histogram = new Recorder(3);
	}

	/**
	 * It appears latency is reported in micros.
	 * Using {@link Recorder} to support concurrent updates to histogram.
	 */
	public void measure(int latencyMicros) {
		histogram.recordValue(latencyMicros);
	}

	/**
	 * This is called from a main thread, on orderly termination.
	 */
	@Override
	public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
		// Accumulate the last interval which was not caught by status thread
		Histogram intervalHistogram = getIntervalHistogramAndAccumulate();

		if (histogramLogWriter != null) {
			histogramLogWriter.outputIntervalHistogram(intervalHistogram);

			// We can close now
			log.close();
		}

		exporter.write(getName(), "operation count", totalHistogram.getTotalCount());
		exporter.write(getName(), "avg latency (us)", totalHistogram.getMean());
		exporter.write(getName(), "min latency (us)", totalHistogram.getMinValue());
		exporter.write(getName(), "max latency (us)", totalHistogram.getMaxValue());

		for (Double percentile : percentiles) {
			exporter.write(getName(), percentile + "p latency (us)", totalHistogram.getValueAtPercentile(percentile));
		}

		exportStatusCounts(exporter);

		// Also export totalHistogram
		if (verbose) {
			for (HistogramIterationValue v : totalHistogram.recordedValues()) {
				int value;

				if (v.getValueIteratedTo() > (long) Integer.MAX_VALUE) {
					value = Integer.MAX_VALUE;
				} else {
					value = (int) v.getValueIteratedTo();
				}

				exporter.write(getName(), Integer.toString(value), (double) v.getCountAtValueIteratedTo());
			}
		}
	}

	/**
	 * This is called periodically from the StatusThread. There's a single
	 * StatusThread per Client process. We optionally serialize the interval to
	 * log on this opportunity.
	 *
	 * @see site.ycsb.measurements.OneMeasurement#getSummary()
	 */
	@Override
	public String getSummary() {
		Histogram intervalHistogram = getIntervalHistogramAndAccumulate();

		// We use the summary interval as the histogram file interval
		if (histogramLogWriter != null) {
			histogramLogWriter.outputIntervalHistogram(intervalHistogram);
		}

		DecimalFormat decimalFormat = new DecimalFormat("#.##");

		return "[" + getName() +
				": Count=" + intervalHistogram.getTotalCount() +
				", Max=" + intervalHistogram.getMaxValue() +
				", Min=" + intervalHistogram.getMinValue() +
				", Avg=" + decimalFormat.format(intervalHistogram.getMean()) +
				", 90=" + decimalFormat.format(intervalHistogram.getValueAtPercentile(90)) +
				", 99=" + decimalFormat.format(intervalHistogram.getValueAtPercentile(99)) +
				", 99.9=" + decimalFormat.format(intervalHistogram.getValueAtPercentile(99.9)) +
				", 99.99=" + decimalFormat.format(intervalHistogram.getValueAtPercentile(99.99)) +
				"]";
	}

	private Histogram getIntervalHistogramAndAccumulate() {
		Histogram intervalHistogram = histogram.getIntervalHistogram();

		// Add this to the total time histogram
		if (totalHistogram == null) {
			totalHistogram = intervalHistogram;
		} else {
			totalHistogram.add(intervalHistogram);
		}

		return intervalHistogram;
	}

	/**
	 * Helper method to parse the given percentile value string.
	 *
	 * @param str - comma delimited string of Integer values
	 * @return An Integer List of percentile values
	 */
	private List<Double> getPercentiles(String str) {
		List<Double> percentiles = new ArrayList<>();

		try {
			for (String percentile : str.split(",")) {
				percentiles.add(Double.parseDouble(percentile));
			}
		} catch (Exception e) {
			// If the given hdrhistogram.percentiles value is unreadable for whatever reason,
			// then calculate and return the default set.
			System.err.println("[WARN] Couldn't read " + PERCENTILES_PROPERTY + " value: '" + str +
					"', the default of '" + PERCENTILES_PROPERTY_DEFAULT + "' will be used.");
			e.printStackTrace();

			return getPercentiles(PERCENTILES_PROPERTY_DEFAULT);
		}

		return percentiles;
	}
}
