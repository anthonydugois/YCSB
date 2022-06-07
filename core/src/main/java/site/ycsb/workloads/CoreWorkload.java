/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2020 YCSB contributors. All rights reserved.
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

package site.ycsb.workloads;

import site.ycsb.*;
import site.ycsb.generator.*;
import site.ycsb.generator.UniformLongGenerator;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The
 * relative proportion of different kinds of operations, and other properties of the workload,
 * are controlled by parameters specified at runtime.
 */
public class CoreWorkload extends Workload {
	private NumberGenerator rowLengthGenerator;

	private NumberGenerator loadIdGenerator;

	private NumberGenerator transactionIdGenerator;

	private AcknowledgedCounterGenerator transactionInsertIdGenerator;

	private DiscreteGenerator opGenerator;

	private long fieldCount;

	private List<String> fieldNames;

	private String table;

	private int zeroPadding;

	private double traceProportion;

	private NumberGenerator getRowLengthGenerator() throws WorkloadException {
		WorkloadDescriptor.Distribution rowLengthDistribution = WorkloadDescriptor.rowLengthDistribution();

		int rowLength = WorkloadDescriptor.rowLength();

		int minRowLength = WorkloadDescriptor.minRowLength();

		List<Long> values = new ArrayList<>();

		long cur = 1000;
		for (int i = 0; i < 12; ++i) { // from 1 KB to 1024 KB
			values.add(cur);
			cur *= 2;
		}

		NumberGenerator rowLengthGenerator;

		switch (rowLengthDistribution) {
			case CONSTANT:
				rowLengthGenerator = new ConstantIntegerGenerator(rowLength);
				break;
			case UNIFORM:
				rowLengthGenerator = new UniformLongGenerator(minRowLength, rowLength);
				break;
			case ZIPFIAN:
				rowLengthGenerator = new ListZipfianGenerator(values, 1.5);
				break;
			default:
				throw new WorkloadException();
		}

		return rowLengthGenerator;
	}

	private NumberGenerator getLoadIdGenerator() {
		long insertStart = WorkloadDescriptor.insertStart();

		return new CounterGenerator(insertStart);
	}

	private NumberGenerator getTransactionIdGenerator() throws WorkloadException {
		WorkloadDescriptor.Distribution requestDistribution = WorkloadDescriptor.requestDistribution();

		long recordCount = WorkloadDescriptor.recordCount();

		if (recordCount <= 0) {
			recordCount = Integer.MAX_VALUE;
		}

		long insertStart = WorkloadDescriptor.insertStart();
		long insertCount = WorkloadDescriptor.insertCount() > 0 ? WorkloadDescriptor.insertCount() : WorkloadDescriptor.recordCount() - insertStart;
		long insertEnd = insertStart + insertCount;

		if (recordCount < insertEnd) {
			throw new WorkloadException();
		}

		NumberGenerator readIdGenerator;

		switch (requestDistribution) {
			case UNIFORM:
				readIdGenerator = new UniformLongGenerator(insertStart, insertEnd - 1);
				break;
			case SEQUENTIAL:
				readIdGenerator = new SequentialGenerator(insertStart, insertEnd - 1);
				break;
			case ZIPFIAN:
				readIdGenerator = new ScrambledZipfianGenerator(insertStart, insertEnd);
				break;
			default:
				throw new WorkloadException();
		}

		/*if (requestDist.compareTo("uniform") == 0) {
			readIdGenerator = new UniformLongGenerator(insertStart, insertEnd - 1);
		} else if (requestDist.compareTo("exponential") == 0) {
			double percentile = Double.parseDouble(props.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY, ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
			double frac = Double.parseDouble(props.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY, ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));

			readIdGenerator = new ExponentialGenerator(percentile, recordCount * frac);
		} else if (requestDist.compareTo("sequential") == 0) {
			readIdGenerator = new SequentialGenerator(insertStart, insertEnd - 1);
		} else if (requestDist.compareTo("zipfian") == 0) {
			double insertProportion = Double.parseDouble(props.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
			int opCount = Integer.parseInt(props.getProperty(Application.OPERATION_COUNT_PROPERTY));
			int expectedNewKeys = (int) (opCount * insertProportion * 2.0); // 2 is fudge factor

			readIdGenerator = new ScrambledZipfianGenerator(insertStart, insertEnd + expectedNewKeys);
		} else {
			throw new WorkloadException();
		}*/

		return readIdGenerator;
	}

	private AcknowledgedCounterGenerator getTransactionInsertIdGenerator() {
		long recordCount = WorkloadDescriptor.recordCount();

		return new AcknowledgedCounterGenerator(recordCount);
	}

	private DiscreteGenerator getOpGenerator() {
		Map<String, Double> proportions = new HashMap<>();

		proportions.put("READ", WorkloadDescriptor.readProportion());
		proportions.put("INSERT", WorkloadDescriptor.insertProportion());

		DiscreteGenerator opGenerator = new DiscreteGenerator();

		for (Map.Entry<String, Double> entry : proportions.entrySet()) {
			String op = entry.getKey();
			double proportion = entry.getValue();

			if (proportion > 0) {
				opGenerator.addValue(proportion, op);
			}
		}

		return opGenerator;
	}

	private List<String> getFieldNames() {
		String fieldPrefix = WorkloadDescriptor.fieldPrefix();

		List<String> fieldNames = new ArrayList<>();

		for (int i = 0; i < fieldCount; i++) {
			fieldNames.add(fieldPrefix + i);
		}

		return fieldNames;
	}

	@Override
	public void init() throws WorkloadException {
		rowLengthGenerator = getRowLengthGenerator();

		loadIdGenerator = getLoadIdGenerator();

		transactionIdGenerator = getTransactionIdGenerator();

		transactionInsertIdGenerator = getTransactionInsertIdGenerator();

		opGenerator = getOpGenerator();

		fieldCount = WorkloadDescriptor.fieldCount();

		fieldNames = getFieldNames();

		table = WorkloadDescriptor.table();

		zeroPadding = WorkloadDescriptor.zeroPadding();

		traceProportion = WorkloadDescriptor.traceProportion();
	}

	private Map<String, ByteIterator> createRandomValues() {
		long rowLength = rowLengthGenerator.nextValue().longValue();
		long fieldLength = rowLength / fieldCount;

		HashMap<String, ByteIterator> values = new HashMap<>();

		for (String fieldName : fieldNames) {
			ByteIterator data = new RandomByteIterator(fieldLength);

			values.put(fieldName, data);
		}

		return values;
	}

	private String nextOperation() {
		return opGenerator.nextString();
	}

	private long nextLoadId() {
		return loadIdGenerator.nextValue().intValue();
	}

	private long nextTransactionId() {
		long id;

		if (transactionIdGenerator instanceof ExponentialGenerator) {
			do {
				id = transactionInsertIdGenerator.lastValue() - transactionIdGenerator.nextValue().intValue();
			} while (id < 0);
		} else {
			do {
				id = transactionIdGenerator.nextValue().intValue();
			} while (id > transactionInsertIdGenerator.lastValue());
		}

		return id;
	}

	private long nextTransactionInsertId() {
		return transactionInsertIdGenerator.nextValue();
	}

	private String buildKey(long id) {
		id = Utils.hash(id);

		String value = Long.toString(id);
		int fill = zeroPadding - value.length();

		StringBuilder builder = new StringBuilder("key");

		for (int i = 0; i < fill; i++) {
			builder.append('0');
		}

		builder.append(value);

		return builder.toString();
	}

	@Override
	public boolean doInsert(DB db) {
		long id = nextLoadId();

		String key = buildKey(id);
		Map<String, ByteIterator> values = createRandomValues();

		Map<String, Object> options = new HashMap<>();

		options.put("profile", "load");

		Status status;
		int retries = 0;

		do {
			status = db.insert(table, key, values, options);

			if (status != null && status.isOk()) {
				break;
			}

			if (++retries <= 5) {
				try {
					Thread.sleep((int) (1000 * 3 * (0.8 + 0.4 * Math.random())));
				} catch (InterruptedException exception) {
					break;
				}
			} else {
				break;
			}
		} while (true);

		return status != null && status.isOk();
	}

	@Override
	public boolean doTransaction(DB db) {
		String operation = nextOperation();

		if (operation == null) {
			return false;
		}

		switch (operation) {
			case "READ":
				doTransactionRead(db);
				break;
			case "UPDATE":
				doTransactionUpdate(db);
				break;
			case "INSERT":
				doTransactionInsert(db);
				break;
			case "SCAN":
				doTransactionScan(db);
				break;
		}

		return true;
	}

	public void doTransactionRead(DB db) {
		long id = nextTransactionId();

		String key = buildKey(id);

		Set<String> fields = new HashSet<>(fieldNames);

		Map<String, Object> options = new HashMap<>();

		options.put("profile", "read");
		options.put("tracing", traceProportion > 0 && ThreadLocalRandom.current().nextDouble() <= traceProportion);

		Map<String, ByteIterator> result = new HashMap<>();

		db.read(table, key, fields, options, result);
	}

	public void doTransactionScan(DB db) {
		// choose a random key
		/*long keynum = nextKey();

		String startkeyname = CoreWorkload.buildKeyName(keynum, zeroPadding, orderedInserts);

		// choose a random scan length
		int len = scanLength.nextValue().intValue();

		HashSet<String> fields = null;

		if (!readAllFields) {
			// read a random field
			String fieldname = fieldNames.get(fieldChooser.nextValue().intValue());

			fields = new HashSet<String>();
			fields.add(fieldname);
		}

		db.scan(table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>());*/
	}

	public void doTransactionUpdate(DB db) {
		// choose a random key
		/*long keynum = nextKey();

		String keyname = CoreWorkload.buildKeyName(keynum, zeroPadding, orderedInserts);

		HashMap<String, ByteIterator> values;

		if (writeAllFields) {
			// new data for all the fields
			values = buildValues(keyname);
		} else {
			// update a random field
			values = buildSingleValue(keyname);
		}

		db.update(table, keyname, values);*/
	}

	public void doTransactionInsert(DB db) {
		long id = nextTransactionInsertId();

		String key = buildKey(id);
		Map<String, ByteIterator> values = createRandomValues();

		//db.insert(table, key, values);

		transactionInsertIdGenerator.acknowledge(id);
	}
}
