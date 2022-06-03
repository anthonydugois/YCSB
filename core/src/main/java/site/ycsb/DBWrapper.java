/**
 * Copyright (c) 2010 Yahoo! Inc., 2016-2020 YCSB contributors. All rights reserved.
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

package site.ycsb;

import site.ycsb.measures.Measure;
import site.ycsb.measures.MeasureException;
import site.ycsb.measures.Measures;
import site.ycsb.tracing.TraceInfo;

import java.util.*;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 * Also reports latency separately between OK and failed operations.
 */
public class DBWrapper extends DB {
	private final DB db;
	private final Measures measures;

	public DBWrapper(DB db) {
		this.db = db;
		this.measures = Measures.instance;
	}

	@Override
	public void init() throws DBException {
		db.init();
	}

	@Override
	public void cleanup() throws DBException {
		db.cleanup();
	}

	@Override
	public Status read(String table, String key, Set<String> fields, Map<String, Object> options,
					   Map<String, ByteIterator> result) {
		long startTimeNanos = System.nanoTime();

		Status status = db.read(table, key, fields, options, result);

		long endTimeNanos = System.nanoTime();

		try {
			long latencyMicros = (endTimeNanos - startTimeNanos) / 1000;

			measures.getOrCreate("READ:LATENCY:HISTOGRAM", Measure.Type.HISTOGRAM).measure(latencyMicros);

			if (status.isOk()) {
				measures.getOrCreate("READ:SUCCESS:LATENCY:HISTOGRAM", Measure.Type.HISTOGRAM).measure(latencyMicros);
			} else {
				measures.getOrCreate("READ:ERROR:LATENCY:HISTOGRAM", Measure.Type.HISTOGRAM).measure(latencyMicros);
			}
		} catch (MeasureException exception) {
			exception.printStackTrace();
		}

		return status;
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
					   Vector<HashMap<String, ByteIterator>> result) {
		long startTimeNanos = System.nanoTime();

		Status res = db.scan(table, startkey, recordcount, fields, result);

		long endTimeNanos = System.nanoTime();

		try {
			long latencyMicros = (endTimeNanos - startTimeNanos) / 1000;

			measures.getOrCreate("SCAN:LATENCY:HISTOGRAM", Measure.Type.HISTOGRAM).measure(latencyMicros);
		} catch (MeasureException exception) {
			exception.printStackTrace();
		}

		return res;
	}

	@Override
	public Status update(String table, String key, Map<String, ByteIterator> values) {
		long startTimeNanos = System.nanoTime();

		Status res = db.update(table, key, values);

		long endTimeNanos = System.nanoTime();

		try {
			long latencyMicros = (endTimeNanos - startTimeNanos) / 1000;

			measures.getOrCreate("UPDATE:LATENCY:HISTOGRAM", Measure.Type.HISTOGRAM).measure(latencyMicros);
		} catch (MeasureException exception) {
			exception.printStackTrace();
		}

		return res;
	}

	@Override
	public Status insert(String table, String key, Map<String, ByteIterator> values, Map<String, Object> options) {
		long startTimeNanos = System.nanoTime();

		Status status = db.insert(table, key, values, options);

		long endTimeNanos = System.nanoTime();

		try {
			long latencyMicros = (endTimeNanos - startTimeNanos) / 1000;

			measures.getOrCreate("INSERT:LATENCY:HISTOGRAM", Measure.Type.HISTOGRAM).measure(latencyMicros);

			if (status.isOk()) {
				measures.getOrCreate("INSERT:SUCCESS:LATENCY:HISTOGRAM", Measure.Type.HISTOGRAM).measure(latencyMicros);
			} else {
				measures.getOrCreate("INSERT:ERROR:LATENCY:HISTOGRAM", Measure.Type.HISTOGRAM).measure(latencyMicros);
			}
		} catch (MeasureException exception) {
			exception.printStackTrace();
		}

		return status;
	}

	@Override
	public Status delete(String table, String key) {
		long startTimeNanos = System.nanoTime();

		Status res = db.delete(table, key);

		long endTimeNanos = System.nanoTime();

		try {
			long latencyMicros = (endTimeNanos - startTimeNanos) / 1000;

			measures.getOrCreate("DELETE:LATENCY:HISTOGRAM", Measure.Type.HISTOGRAM).measure(latencyMicros);
		} catch (MeasureException exception) {
			exception.printStackTrace();
		}

		return res;
	}

	@Override
	public Collection<TraceInfo> traces() {
		Collection<TraceInfo> traceInfos = db.traces();

		for (TraceInfo traceInfo : traceInfos) {
			for (TraceInfo.Event event : traceInfo.getEvents().values()) {
				try {
					measures.getOrCreate("TRACING:EVENTS:RAW", Measure.Type.RAW).measure(event);
				} catch (MeasureException exception) {
					exception.printStackTrace();
				}
			}
		}

		return traceInfos;
	}
}
