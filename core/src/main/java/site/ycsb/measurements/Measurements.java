/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2020 YCSB contributors All rights reserved.
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

import site.ycsb.Status;
import site.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects latency measurements, and reports them when requested.
 */
public class Measurements {
	/**
	 * All supported measurement types are defined in this enum.
	 */
	public enum MeasurementType {
		HISTOGRAM,
		HDRHISTOGRAM,
		HDRHISTOGRAM_AND_HISTOGRAM,
		HDRHISTOGRAM_AND_RAW,
		TIMESERIES,
		RAW
	}

	public static final String MEASUREMENT_TYPE_PROPERTY = "measurementtype";
	private static final String MEASUREMENT_TYPE_PROPERTY_DEFAULT = "hdrhistogram";

	public static final String MEASUREMENT_INTERVAL = "measurement.interval";
	private static final String MEASUREMENT_INTERVAL_DEFAULT = "op";

	public static final String MEASUREMENT_TRACK_JVM_PROPERTY = "measurement.trackjvm";
	public static final String MEASUREMENT_TRACK_JVM_PROPERTY_DEFAULT = "false";

	private static Measurements singleton = null;
	private static Properties measureProps = null;

	private final ConcurrentHashMap<String, OneMeasurement> measurements;
	private final ConcurrentHashMap<String, OneMeasurement> intendedMeasurements;
	private final MeasurementType measurementType;
	private final int measurementInterval;
	private final Properties props;

	/**
	 * Create a new object with the specified properties.
	 */
	public Measurements(Properties props) {
		this.measurements = new ConcurrentHashMap<>();
		this.intendedMeasurements = new ConcurrentHashMap<>();
		this.props = props;

		String type = this.props.getProperty(MEASUREMENT_TYPE_PROPERTY, MEASUREMENT_TYPE_PROPERTY_DEFAULT);

		switch (type) {
			case "histogram":
				this.measurementType = MeasurementType.HISTOGRAM;
				break;
			case "hdrhistogram":
				this.measurementType = MeasurementType.HDRHISTOGRAM;
				break;
			case "hdrhistogram+histogram":
				this.measurementType = MeasurementType.HDRHISTOGRAM_AND_HISTOGRAM;
				break;
			case "hdrhistogram+raw":
				this.measurementType = MeasurementType.HDRHISTOGRAM_AND_RAW;
				break;
			case "timeseries":
				this.measurementType = MeasurementType.TIMESERIES;
				break;
			case "raw":
				this.measurementType = MeasurementType.RAW;
				break;
			default:
				throw new IllegalArgumentException("unknown " + MEASUREMENT_TYPE_PROPERTY + "=" + type);
		}

		String interval = this.props.getProperty(MEASUREMENT_INTERVAL, MEASUREMENT_INTERVAL_DEFAULT);

		switch (interval) {
			case "op":
				this.measurementInterval = 0;
				break;
			case "intended":
				this.measurementInterval = 1;
				break;
			case "both":
				this.measurementInterval = 2;
				break;
			default:
				throw new IllegalArgumentException("unknown " + MEASUREMENT_INTERVAL + "=" + interval);
		}
	}

	public static synchronized Measurements getMeasurements() {
		if (singleton == null) {
			singleton = new Measurements(measureProps);
		}

		return singleton;
	}

	public static void setProperties(Properties props) {
		measureProps = props;
	}

	private OneMeasurement createMeasurement(String name) {
		switch (measurementType) {
			case HISTOGRAM:
				return new OneMeasurementHistogram(name, props);
			case HDRHISTOGRAM:
				return new OneMeasurementHdrHistogram(name, props);
			case HDRHISTOGRAM_AND_HISTOGRAM:
				return new TwoInOneMeasurement(name, new OneMeasurementHdrHistogram("Hdr" + name, props), new OneMeasurementHistogram("Bucket" + name, props));
			case HDRHISTOGRAM_AND_RAW:
				return new TwoInOneMeasurement(name, new OneMeasurementHdrHistogram("Hdr" + name, props), new OneMeasurementRaw("Raw" + name, props));
			case TIMESERIES:
				return new OneMeasurementTimeSeries(name, props);
			case RAW:
				return new OneMeasurementRaw(name, props);
			default:
				throw new AssertionError("Impossible to be here. Dead code reached. Bugs?");
		}
	}

	private static class StartTimeHolder {
		private long time;

		public void setTime(long time) {
			this.time = time;
		}

		public long startTime() {
			if (time == 0) {
				return System.nanoTime();
			}

			return time;
		}
	}

	private final ThreadLocal<StartTimeHolder> intendedStartTime = ThreadLocal.withInitial(StartTimeHolder::new);

	public void setIntendedStartTimeNanos(long time) {
		if (measurementInterval == 0) {
			return;
		}

		intendedStartTime.get().setTime(time);
	}

	public long getIntendedStartTimeNanos() {
		if (measurementInterval == 0) {
			return 0L;
		}

		return intendedStartTime.get().startTime();
	}

	/**
	 * Report a single value of a single metric.
	 */
	public void measure(String operation, int latency) {
		if (measurementInterval == 1) {
			return;
		}

		try {
			getMeasurement(operation).measure(latency);
		} catch (java.lang.ArrayIndexOutOfBoundsException e) {
			// This seems like a terribly hacky way to cover up for a bug in the measurement code
			System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");

			e.printStackTrace();
			e.printStackTrace(System.out);
		}
	}

	/**
	 * Report a single value of a single metric.
	 */
	public void measureIntended(String operation, int latency) {
		if (measurementInterval == 0) {
			return;
		}

		try {
			getIntendedMeasurement(operation).measure(latency);
		} catch (java.lang.ArrayIndexOutOfBoundsException e) {
			// This seems like a terribly hacky way to cover up for a bug in the measurement code
			System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");

			e.printStackTrace();
			e.printStackTrace(System.out);
		}
	}

	private OneMeasurement getMeasurement(String operation) {
		OneMeasurement measurement = measurements.get(operation);

		if (measurement == null) {
			measurement = createMeasurement(operation);

			OneMeasurement prevMeasurement = measurements.putIfAbsent(operation, measurement);

			if (prevMeasurement != null) {
				measurement = prevMeasurement;
			}
		}

		return measurement;
	}

	private OneMeasurement getIntendedMeasurement(String operation) {
		OneMeasurement measurement = intendedMeasurements.get(operation);

		if (measurement == null) {
			measurement = createMeasurement(measurementInterval == 1 ? operation : "Intended-" + operation);

			OneMeasurement prevMeasurement = intendedMeasurements.putIfAbsent(operation, measurement);

			if (prevMeasurement != null) {
				measurement = prevMeasurement;
			}
		}

		return measurement;
	}

	/**
	 * Report a return code for a single DB operation.
	 */
	public void reportStatus(final String operation, final Status status) {
		OneMeasurement measurement = measurementInterval == 1 ?
				getIntendedMeasurement(operation) :
				getMeasurement(operation);

		measurement.reportStatus(status);
	}

	/**
	 * Export the current measurements to a suitable format.
	 *
	 * @param exporter Exporter representing the type of format to write to.
	 * @throws IOException Thrown if the export failed.
	 */
	public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
		for (OneMeasurement measurement : measurements.values()) {
			measurement.exportMeasurements(exporter);
		}

		for (OneMeasurement measurement : intendedMeasurements.values()) {
			measurement.exportMeasurements(exporter);
		}
	}

	/**
	 * Return a one line summary of the measurements.
	 */
	public synchronized String getSummary() {
		StringBuilder builder = new StringBuilder();

		for (OneMeasurement measurement : measurements.values()) {
			builder.append(measurement.getSummary()).append(" ");
		}

		for (OneMeasurement measurement : intendedMeasurements.values()) {
			builder.append(measurement.getSummary()).append(" ");
		}

		return builder.toString();
	}
}
