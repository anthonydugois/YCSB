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
import site.ycsb.measurements.Measurements;

import java.io.IOException;
import java.util.*;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The
 * relative proportion of different kinds of operations, and other properties of the workload,
 * are controlled by parameters specified at runtime.
 * <p>
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>minfieldlength</b>: the minimum size of each field (default: 1)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just
 * one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record,
 * modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate
 * on - uniform, zipfian, hotspot, sequential, exponential or latest (default: uniform)
 * <LI><b>minscanlength</b>: for scans, what is the minimum number of records to scan (default: 1)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the
 * number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertstart</b>: for parallel loads and runs, defines the starting record for this
 * YCSB instance (default: 0)
 * <LI><b>insertcount</b>: for parallel loads and runs, defines the number of records for this
 * YCSB instance (default: recordcount)
 * <LI><b>zeropadding</b>: for generating a record sequence compatible with string sort order by
 * 0 padding the record number. Controls the number of 0s to use for padding. (default: 1)
 * For example for row 5, with zeropadding=1 you get 'user5' key and with zeropading=8 you get
 * 'user00000005' key. In order to see its impact, zeropadding needs to be bigger than number of
 * digits in the record number.
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed
 * order ("hashed") (default: hashed)
 * <LI><b>fieldnameprefix</b>: what should be a prefix for field names, the shorter may decrease the
 * required storage size (default: "field")
 * </ul>
 */
public class CoreWorkload extends Workload {
	/**
	 * The name of the database table to run queries against.
	 */
	public static final String TABLENAME_PROPERTY = "table";
	public static final String TABLENAME_PROPERTY_DEFAULT = "tbl";

	/**
	 * The name of the property for the number of fields in a record.
	 */
	public static final String FIELD_COUNT_PROPERTY = "fieldcount";
	public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

	/**
	 * The name of the property for the field length distribution. Options are "uniform", "zipfian"
	 * (favouring short records), "constant", and "histogram".
	 * <p>
	 * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the
	 * fieldlength property. If "histogram", then the histogram will be read from the filename
	 * specified in the "fieldlengthhistogram" property.
	 */
	public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";
	public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

	/**
	 * The name of the property for the length of a field in bytes.
	 */
	public static final String FIELD_LENGTH_PROPERTY = "fieldlength";
	public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

	/**
	 * The name of the property for the minimum length of a field in bytes.
	 */
	public static final String MIN_FIELD_LENGTH_PROPERTY = "minfieldlength";
	public static final String MIN_FIELD_LENGTH_PROPERTY_DEFAULT = "1";

	/**
	 * The name of a property that specifies the filename containing the field length histogram (only
	 * used if fieldlengthdistribution is "histogram").
	 */
	public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";
	public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

	/**
	 * The name of the property for deciding whether to read one field (false) or all fields (true) of
	 * a record.
	 */
	public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";
	public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

	/**
	 * The name of the property for determining how to read all the fields when readallfields is true.
	 * If set to true, all the field names will be passed into the underlying client. If set to false,
	 * null will be passed into the underlying client. When passed a null, some clients may retrieve
	 * the entire row with a wildcard, which may be slower than naming all the fields.
	 */
	public static final String READ_ALL_FIELDS_BY_NAME_PROPERTY = "readallfieldsbyname";
	public static final String READ_ALL_FIELDS_BY_NAME_PROPERTY_DEFAULT = "false";

	/**
	 * The name of the property for deciding whether to write one field (false) or all fields (true)
	 * of a record.
	 */
	public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
	public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

	/**
	 * The name of the property for deciding whether to check all returned
	 * data against the formation template to ensure data integrity.
	 */
	/*public static final String DATA_INTEGRITY_PROPERTY = "dataintegrity";
	public static final String DATA_INTEGRITY_PROPERTY_DEFAULT = "false";*/

	/**
	 * The name of the property for the proportion of transactions that are reads.
	 */
	public static final String READ_PROPORTION_PROPERTY = "readproportion";
	public static final String READ_PROPORTION_PROPERTY_DEFAULT = "1.0";

	/**
	 * The name of the property for the proportion of transactions that are updates.
	 */
	public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";
	public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.0";

	/**
	 * The name of the property for the proportion of transactions that are inserts.
	 */
	public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";
	public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

	/**
	 * The name of the property for the proportion of transactions that are scans.
	 */
	public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";
	public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

	/**
	 * The name of the property for the proportion of transactions that are read-modify-write.
	 */
	/*public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";
	public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";*/

	/**
	 * The name of the property for the the distribution of requests across the keyspace. Options are
	 * "uniform", "zipfian" and "latest"
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
	public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

	/**
	 * The name of the property for adding zero padding to record numbers in order to match
	 * string sort order. Controls the number of 0s to left pad with.
	 */
	public static final String ZERO_PADDING_PROPERTY = "zeropadding";
	public static final String ZERO_PADDING_PROPERTY_DEFAULT = "1";

	/**
	 * The name of the property for the min scan length (number of records).
	 */
	public static final String MIN_SCAN_LENGTH_PROPERTY = "minscanlength";
	public static final String MIN_SCAN_LENGTH_PROPERTY_DEFAULT = "1";

	/**
	 * The name of the property for the max scan length (number of records).
	 */
	public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
	public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";

	/**
	 * The name of the property for the scan length distribution. Options are "uniform" and "zipfian"
	 * (favoring short scans)
	 */
	public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";
	public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

	/**
	 * The name of the property for the order to insert records. Options are "ordered" or "hashed"
	 */
	public static final String INSERT_ORDER_PROPERTY = "insertorder";
	public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

	/**
	 * Percentage data items that constitute the hot set.
	 */
	public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";
	public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

	/**
	 * Percentage operations that access the hot set.
	 */
	public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";
	public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

	/**
	 * How many times to retry when insertion of a single item to a DB fails.
	 */
	/*public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
	public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";*/

	/**
	 * On average, how long to wait between the retries, in seconds.
	 */
	/*public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
	public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";*/

	/**
	 * Field name prefix.
	 */
	public static final String FIELD_NAME_PREFIX = "fieldnameprefix";
	public static final String FIELD_NAME_PREFIX_DEFAULT = "field";

	protected String table;

	private List<String> fieldNames;

	/**
	 * Generator object that produces field lengths.  The value of this depends on the properties that
	 * start with "FIELD_LENGTH_".
	 */
	protected NumberGenerator fieldLengthGenerator;

	protected boolean readAllFields;

	protected boolean readAllFieldsByName;

	protected boolean writeAllFields;

	/**
	 * Set to true if want to check correctness of reads. Must also be set to true during loading phase to function.
	 */
	//private boolean dataIntegrity;

	protected NumberGenerator idGenerator;
	protected DiscreteGenerator opGenerator;
	protected NumberGenerator idSelector;
	protected NumberGenerator fieldSelector;
	protected AcknowledgedCounterGenerator newIdGenerator;
	protected NumberGenerator scanLength;
	protected boolean orderedInserts;
	protected long fieldCount;
	protected long recordCount;
	protected int zeroPadding;
	protected int insertionRetryLimit;
	protected int insertionRetryInterval;

	private final Measurements measurements = Measurements.getMeasurements();

	public static String buildKey(long id, int zeroPadding, boolean orderedInserts) {
		if (!orderedInserts) {
			id = Utils.hash(id);
		}

		String value = Long.toString(id);
		int fill = zeroPadding - value.length();

		StringBuilder builder = new StringBuilder("key");

		for (int i = 0; i < fill; i++) {
			builder.append('0');
		}

		builder.append(value);

		return builder.toString();
	}

	protected static NumberGenerator getFieldLengthGenerator(Properties props) throws WorkloadException {
		NumberGenerator fieldLengthGenerator;

		String fieldLengthDist = props.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
		int fieldLength = Integer.parseInt(props.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
		int minFieldLength = Integer.parseInt(props.getProperty(MIN_FIELD_LENGTH_PROPERTY, MIN_FIELD_LENGTH_PROPERTY_DEFAULT));
		String fieldLengthHist = props.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);

		if (fieldLengthDist.compareTo("constant") == 0) {
			fieldLengthGenerator = new ConstantIntegerGenerator(fieldLength);
		} else if (fieldLengthDist.compareTo("uniform") == 0) {
			fieldLengthGenerator = new UniformLongGenerator(minFieldLength, fieldLength);
		} else if (fieldLengthDist.compareTo("zipfian") == 0) {
			fieldLengthGenerator = new ZipfianGenerator(minFieldLength, fieldLength);
		} else if (fieldLengthDist.compareTo("histogram") == 0) {
			try {
				fieldLengthGenerator = new HistogramGenerator(fieldLengthHist);
			} catch (IOException e) {
				throw new WorkloadException("Couldn't read field length histogram file: " + fieldLengthHist, e);
			}
		} else {
			throw new WorkloadException("Unknown field length distribution \"" + fieldLengthDist + "\"");
		}

		return fieldLengthGenerator;
	}

	/**
	 * Initialize the scenario.
	 * Called once, in the main client thread, before any operations are started.
	 */
	@Override
	public void init(Properties props) throws WorkloadException {
		table = props.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

		fieldCount = Long.parseLong(props.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));

		final String fieldPrefix = props.getProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);

		fieldNames = new ArrayList<>();

		for (int i = 0; i < fieldCount; i++) {
			fieldNames.add(fieldPrefix + i);
		}

		fieldLengthGenerator = getFieldLengthGenerator(props);

		recordCount = Long.parseLong(props.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));

		if (recordCount == 0) {
			recordCount = Integer.MAX_VALUE;
		}

		String requestDist = props.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);

		int minscanlength = Integer.parseInt(props.getProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_PROPERTY_DEFAULT));
		int maxscanlength = Integer.parseInt(props.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
		String scanlengthdistrib = props.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

		long insertStart = Long.parseLong(props.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
		long insertCount = Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordCount - insertStart)));

		// Confirm valid values for insertstart and insertcount in relation to recordcount
		if (recordCount < (insertStart + insertCount)) {
			System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
			System.err.println("recordcount must be bigger than insertstart + insertcount.");

			System.exit(-1);
		}

		zeroPadding = Integer.parseInt(props.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));

		readAllFields = Boolean.parseBoolean(props.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
		readAllFieldsByName = Boolean.parseBoolean(props.getProperty(READ_ALL_FIELDS_BY_NAME_PROPERTY, READ_ALL_FIELDS_BY_NAME_PROPERTY_DEFAULT));
		writeAllFields = Boolean.parseBoolean(props.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

		//dataIntegrity = Boolean.parseBoolean(props.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));

		// Confirm that fieldlengthgenerator returns a constant if data integrity check requested.
		/*if (dataIntegrity && !(props.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT)).equals("constant")) {
			System.err.println("Must have constant field size to check data integrity.");

			System.exit(-1);
		}

		if (dataIntegrity) {
			System.out.println("Data integrity is enabled.");
		}*/

		orderedInserts = props.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") != 0;

		idGenerator = new CounterGenerator(insertStart);

		newIdGenerator = new AcknowledgedCounterGenerator(recordCount);

		opGenerator = createOpGenerator(props);

		if (requestDist.compareTo("uniform") == 0) {
			idSelector = new UniformLongGenerator(insertStart, insertStart + insertCount - 1);
		} else if (requestDist.compareTo("exponential") == 0) {
			double percentile = Double.parseDouble(props.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY, ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
			double frac = Double.parseDouble(props.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY, ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));

			idSelector = new ExponentialGenerator(percentile, recordCount * frac);
		} else if (requestDist.compareTo("sequential") == 0) {
			idSelector = new SequentialGenerator(insertStart, insertStart + insertCount - 1);
		} else if (requestDist.compareTo("zipfian") == 0) {
			// It does this by generating a random "next key" in part by taking the modulus over the number of keys.
			// If the number of keys changes, this would shift the modulus, and we don't want that to
			// change which keys are popular so we'll actually construct the scrambled zipfian generator
			// with a keyspace that is larger than exists at the beginning of the test.
			// That is, we'll predict the number of inserts, and tell the scrambled zipfian generator the
			// number of existing keys plus the number of predicted keys as the total keyspace.
			// Then, if the generator picks a key that hasn't been inserted yet, will just ignore it and pick
			// another key.
			// This way, the size of the keyspace doesn't change from the perspective of the scrambled
			// zipfian generator.
			final double insertProportion = Double.parseDouble(props.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
			int opCount = Integer.parseInt(props.getProperty(Client.OPERATION_COUNT_PROPERTY));
			int expectedNewKeys = (int) (opCount * insertProportion * 2.0); // 2 is fudge factor

			idSelector = new ScrambledZipfianGenerator(insertStart, insertStart + insertCount + expectedNewKeys);
		} else if (requestDist.compareTo("latest") == 0) {
			idSelector = new SkewedLatestGenerator(newIdGenerator);
		} else if (requestDist.equals("hotspot")) {
			double hotsetfraction = Double.parseDouble(props.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
			double hotopnfraction = Double.parseDouble(props.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));

			idSelector = new HotspotIntegerGenerator(insertStart, insertStart + insertCount - 1, hotsetfraction, hotopnfraction);
		} else {
			throw new WorkloadException("Unknown request distribution \"" + requestDist + "\"");
		}

		fieldSelector = new UniformLongGenerator(0, fieldCount - 1);

		if (scanlengthdistrib.compareTo("uniform") == 0) {
			scanLength = new UniformLongGenerator(minscanlength, maxscanlength);
		} else if (scanlengthdistrib.compareTo("zipfian") == 0) {
			scanLength = new ZipfianGenerator(minscanlength, maxscanlength);
		} else {
			throw new WorkloadException("Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
		}

		//insertionRetryLimit = Integer.parseInt(props.getProperty(INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
		//insertionRetryInterval = Integer.parseInt(props.getProperty(INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));
	}

	/**
	 * Builds a value for a randomly chosen field.
	 */
	/*private HashMap<String, ByteIterator> buildSingleValue(String key) {
		HashMap<String, ByteIterator> value = new HashMap<>();
		String fieldKey = fieldNames.get(fieldChooser.nextValue().intValue());

		ByteIterator data;

		if (dataIntegrity) {
			data = new StringByteIterator(buildDeterministicValue(key, fieldKey));
		} else {
			data = new RandomByteIterator(fieldLengthGenerator.nextValue().longValue());
		}

		value.put(fieldKey, data);

		return value;
	}*/

	/**
	 * Builds values for all fields.
	 */
	private HashMap<String, ByteIterator> buildValues() {
		HashMap<String, ByteIterator> values = new HashMap<>();

		for (String fieldName : fieldNames) {
			ByteIterator data = new RandomByteIterator(fieldLengthGenerator.nextValue().longValue());

			/*if (dataIntegrity) {
				data = new StringByteIterator(buildDeterministicValue(key, fieldName));
			} else {
				data = new RandomByteIterator(fieldLengthGenerator.nextValue().longValue());
			}*/

			values.put(fieldName, data);
		}

		return values;
	}

	/**
	 * Build a deterministic value given the key information.
	 */
	/*private String buildDeterministicValue(String key, String fieldkey) {
		int size = fieldLengthGenerator.nextValue().intValue();
		StringBuilder builder = new StringBuilder(size);

		builder.append(key)
				.append(':')
				.append(fieldkey);

		while (builder.length() < size) {
			builder.append(':');
			builder.append(builder.toString().hashCode());
		}

		builder.setLength(size);

		return builder.toString();
	}*/

	/**
	 * Creates a weighted discrete values with database operations for a workload to perform.
	 * Weights/proportions are read from the properties list and defaults are used
	 * when values are not configured.
	 * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
	 *
	 * @param props The properties list to pull weights from.
	 * @return A generator that can be used to determine the next operation to perform.
	 * @throws IllegalArgumentException if the properties object was null.
	 */
	protected static DiscreteGenerator createOpGenerator(final Properties props) {
		if (props == null) {
			throw new IllegalArgumentException("Properties object cannot be null");
		}

		Map<String, Double> proportions = new HashMap<>();

		proportions.put("READ", Double.parseDouble(props.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT)));
		proportions.put("UPDATE", Double.parseDouble(props.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT)));
		proportions.put("INSERT", Double.parseDouble(props.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT)));
		proportions.put("SCAN", Double.parseDouble(props.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT)));
		//proportions.put("READMODIFYWRITE", Double.parseDouble(props.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT)));

		final DiscreteGenerator opGenerator = new DiscreteGenerator();

		for (Map.Entry<String, Double> entry : proportions.entrySet()) {
			String op = entry.getKey();
			double proportion = entry.getValue();

			if (proportion > 0) {
				opGenerator.addValue(proportion, op);
			}
		}

		return opGenerator;
	}

	/**
	 * Results are reported in the first three buckets of the histogram under
	 * the label "VERIFY".
	 * Bucket 0 means the expected data was returned.
	 * Bucket 1 means incorrect data was returned.
	 * Bucket 2 means null data was returned when some data was expected.
	 */
	/*protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
		Status verifyStatus = Status.OK;

		long startTime = System.nanoTime();

		if (!cells.isEmpty()) {
			for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
				if (!entry.getValue().toString().equals(buildDeterministicValue(key, entry.getKey()))) {
					verifyStatus = Status.UNEXPECTED_STATE;
					break;
				}
			}
		} else {
			// This assumes that null data is never valid
			verifyStatus = Status.ERROR;
		}

		long endTime = System.nanoTime();

		measurements.measure("VERIFY", (int) (endTime - startTime) / 1000);
		measurements.reportStatus("VERIFY", verifyStatus);
	}*/

	long nextId() {
		long id;

		if (idSelector instanceof ExponentialGenerator) {
			do {
				id = newIdGenerator.lastValue() - idSelector.nextValue().intValue();
			} while (id < 0);
		} else {
			do {
				id = idSelector.nextValue().intValue();
			} while (id > newIdGenerator.lastValue());
		}

		return id;
	}

	/**
	 * Do one insert operation. Because it will be called concurrently from multiple client threads,
	 * this function must be thread safe. However, avoid synchronized, or the threads will block waiting
	 * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
	 * have no side effects other than DB operations.
	 */
	@Override
	public boolean doInsert(DB db, Object threadState) {
		String key = buildKey(idGenerator.nextValue().intValue(), zeroPadding, orderedInserts);
		HashMap<String, ByteIterator> values = buildValues();

		Map<String, Object> options = new HashMap<>();

		options.put("profile", "load");

		Status status = db.insert(table, key, values, options);
		//int numOfRetries = 0;

		/*do {
			status = db.insert(table, key, values);

			if (null != status && status.isOk()) {
				break;
			}

			// Retry if configured. Without retrying, the load process will fail
			// even if one single insertion fails. User can optionally configure
			// an insertion retry limit (default is 0) to enable retry.
			if (++numOfRetries <= insertionRetryLimit) {
				System.err.println("Retrying insertion, retry count: " + numOfRetries);

				try {
					// Sleep for a random number between [0.8, 1.2) * insertionRetryInterval
					int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));

					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					break;
				}
			} else {
				System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
						"Insertion Retry Limit: " + insertionRetryLimit);

				break;
			}
		} while (true);*/

		return status != null && status.isOk();
	}

	/**
	 * Do one transaction operation. Because it will be called concurrently from multiple client
	 * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
	 * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
	 * have no side effects other than DB operations.
	 */
	@Override
	public boolean doTransaction(DB db, Object threadState) {
		String operation = opGenerator.nextString();

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
			/*default:
				doTransactionReadModifyWrite(db);*/
		}

		return true;
	}

	public void doTransactionRead(DB db) {
		// Choose a random key
		String key = buildKey(nextId(), zeroPadding, orderedInserts);

		Set<String> fields;

		if (readAllFields) {
			if (readAllFieldsByName) {
				fields = new HashSet<>(fieldNames);
			} else {
				fields = null;
			}
		} else {
			fields = new HashSet<>();

			// Read a random field
			// TODO: make possible to read a random number of fields
			String fieldName = fieldNames.get(fieldSelector.nextValue().intValue());

			fields.add(fieldName);
		}

		/*if (!readAllFields) {
			// Read a random field
			// TODO: make possible to read a random number of fields
			String fieldName = fieldNames.get(fieldChooser.nextValue().intValue());

			fields = new HashSet<>();
			fields.add(fieldName);
		} else if (dataIntegrity || readAllFieldsByName) {
			fields = new HashSet<>(fieldNames);
		} else {
			fields = null;
		}*/

		Map<String, Object> options = new HashMap<>();

		options.put("profile", "read");

		Map<String, ByteIterator> result = new HashMap<>();

		db.read(table, key, fields, options, result);

		/*if (dataIntegrity) {
			verifyRow(key, cells);
		}*/
	}

	public void doTransactionReadModifyWrite(DB db) {
		// choose a random key
		/*long keynum = nextKey();

		String keyname = CoreWorkload.buildKeyName(keynum, zeroPadding, orderedInserts);

		HashSet<String> fields = null;

		if (!readAllFields) {
			// read a random field
			String fieldname = fieldNames.get(fieldChooser.nextValue().intValue());

			fields = new HashSet<String>();
			fields.add(fieldname);
		}

		HashMap<String, ByteIterator> values;

		if (writeAllFields) {
			// new data for all the fields
			values = buildValues(keyname);
		} else {
			// update a random field
			values = buildSingleValue(keyname);
		}

		// do the transaction

		HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();


		long ist = measurements.getIntendedStartTimeNs();
		long st = System.nanoTime();
		db.read(table, keyname, fields, cells);

		db.update(table, keyname, values);

		long en = System.nanoTime();

		if (dataIntegrity) {
			verifyRow(keyname, cells);
		}

		measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
		measurements.measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));*/
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
		long id = newIdGenerator.nextValue();

		try {
			String key = buildKey(id, zeroPadding, orderedInserts);
			HashMap<String, ByteIterator> values = buildValues();

			//db.insert(table, key, values);
		} finally {
			newIdGenerator.acknowledge(id);
		}
	}
}
