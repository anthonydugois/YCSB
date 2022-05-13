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

import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import site.ycsb.measurements.Measurements;
import site.ycsb.tracing.TraceInfo;

import java.util.*;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 * Also reports latency separately between OK and failed operations.
 */
public class DBWrapper extends DB {
	private final DB db;
	private final Measurements measurements;
	private final Tracer tracer;

	private final String scopeStringCleanup;
	private final String scopeStringDelete;
	private final String scopeStringInit;
	private final String scopeStringInsert;
	private final String scopeStringRead;
	private final String scopeStringScan;
	private final String scopeStringUpdate;

	public DBWrapper(final DB db, final Tracer tracer) {
		this.db = db;
		this.measurements = Measurements.getMeasurements();
		this.tracer = tracer;

		final String name = db.getClass().getSimpleName();

		this.scopeStringCleanup = name + "#cleanup";
		this.scopeStringDelete = name + "#delete";
		this.scopeStringInit = name + "#init";
		this.scopeStringInsert = name + "#insert";
		this.scopeStringRead = name + "#read";
		this.scopeStringScan = name + "#scan";
		this.scopeStringUpdate = name + "#update";
	}

	@Override
	public void setProperties(Properties props) {
		db.setProperties(props);
	}

	@Override
	public Properties getProperties() {
		return db.getProperties();
	}

	@Override
	public void init() throws DBException {
		try (final TraceScope span = tracer.newScope(scopeStringInit)) {
			db.init();
		}
	}

	@Override
	public void cleanup() throws DBException {
		try (final TraceScope span = tracer.newScope(scopeStringCleanup)) {
			/*long ist = measurements.getIntendedStartTimeNanos();
			long st = System.nanoTime();

			db.cleanup();

			long en = System.nanoTime();

			measure("CLEANUP", Status.OK, ist, st, en);*/
			db.cleanup();
		}
	}

	@Override
	public Status read(String table, String key, Set<String> fields, Map<String, Object> options,
					   Map<String, ByteIterator> result) {
		try (final TraceScope span = tracer.newScope(scopeStringRead)) {
			long ist = measurements.getIntendedStartTimeNanos();
			long st = System.nanoTime();

			Status status = db.read(table, key, fields, options, result);

			long en = System.nanoTime();

			measure("READ", status, ist, st, en);
			measurements.reportStatus("READ", status);

			return status;
		}
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields,
					   Vector<HashMap<String, ByteIterator>> result) {
		try (final TraceScope span = tracer.newScope(scopeStringScan)) {
			long ist = measurements.getIntendedStartTimeNanos();
			long st = System.nanoTime();

			Status res = db.scan(table, startkey, recordcount, fields, result);

			long en = System.nanoTime();

			measure("SCAN", res, ist, st, en);
			measurements.reportStatus("SCAN", res);

			return res;
		}
	}

	@Override
	public Status update(String table, String key, Map<String, ByteIterator> values) {
		try (final TraceScope span = tracer.newScope(scopeStringUpdate)) {
			long ist = measurements.getIntendedStartTimeNanos();
			long st = System.nanoTime();

			Status res = db.update(table, key, values);

			long en = System.nanoTime();

			measure("UPDATE", res, ist, st, en);
			measurements.reportStatus("UPDATE", res);

			return res;
		}
	}

	@Override
	public Status insert(String table, String key, Map<String, ByteIterator> values, Map<String, Object> options) {
		try (final TraceScope span = tracer.newScope(scopeStringInsert)) {
			long ist = measurements.getIntendedStartTimeNanos();
			long st = System.nanoTime();

			Status status = db.insert(table, key, values, options);

			long en = System.nanoTime();

			measure("INSERT", status, ist, st, en);
			measurements.reportStatus("INSERT", status);

			return status;
		}
	}

	@Override
	public Status delete(String table, String key) {
		try (final TraceScope span = tracer.newScope(scopeStringDelete)) {
			long ist = measurements.getIntendedStartTimeNanos();
			long st = System.nanoTime();

			Status res = db.delete(table, key);

			long en = System.nanoTime();

			measure("DELETE", res, ist, st, en);
			measurements.reportStatus("DELETE", res);

			return res;
		}
	}

	@Override
	public Collection<TraceInfo> traces() {
		Collection<TraceInfo> traceInfos = db.traces();

		for (TraceInfo traceInfo : traceInfos) {
			for (Map.Entry<String, Integer> entry : traceInfo.getEventDurationMicros().entrySet()) {
				measurements.measure(entry.getKey(), entry.getValue());
			}
		}

		return traceInfos;
	}

	private void measure(String op, Status result, long intendedStartTimeNanos, long startTimeNanos, long endTimeNanos) {
		String measurementName = op;

		if (result == null || !result.isOk()) {
			measurementName += "-FAILED";
		}

		measurements.measure(measurementName, (int) ((endTimeNanos - startTimeNanos) / 1000));
		measurements.measureIntended(measurementName, (int) ((endTimeNanos - intendedStartTimeNanos) / 1000));
	}
}
