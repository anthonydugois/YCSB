package site.ycsb;

import java.util.Properties;

public class WorkloadDescriptor {
	public enum Stage {
		LOAD,
		TRANSACTIONS
	}

	public enum Distribution {
		CONSTANT("constant"),
		SEQUENTIAL("sequential"),
		UNIFORM("uniform"),
		EXPONENTIAL("exponential"),
		ZIPFIAN("zipfian");

		private final String name;

		Distribution(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public static Distribution fromString(String name) {
			for (Distribution distribution : Distribution.values()) {
				if (distribution.getName().equals(name)) {
					return distribution;
				}
			}

			return null;
		}
	}

	public static final String RECORD_COUNT_PROPERTY = "recordcount";
	public static final String RECORD_COUNT_PROPERTY_DEFAULT = "0";

	public static long recordCount() {
		return Long.parseLong(props.getProperty(RECORD_COUNT_PROPERTY, RECORD_COUNT_PROPERTY_DEFAULT));
	}

	public static final String OPERATION_COUNT_PROPERTY = "operationcount";
	public static final String OPERATION_COUNT_PROPERTY_DEFAULT = "0";

	public static long operationCount() {
		return Long.parseLong(props.getProperty(OPERATION_COUNT_PROPERTY, OPERATION_COUNT_PROPERTY_DEFAULT));
	}

	public static final String WORKLOAD_PROPERTY = "workload";

	public static String workload() {
		return props.getProperty(WORKLOAD_PROPERTY);
	}

	public static final String DB_PROPERTY = "db";
	public static final String DB_PROPERTY_DEFAULT = "site.ycsb.db.CassandraClient";

	public static String db() {
		return props.getProperty(DB_PROPERTY, DB_PROPERTY_DEFAULT);
	}

	public static final String EXPORTER_PROPERTY = "exporter";
	public static final String EXPORTER_PROPERTY_DEFAULT = "site.ycsb.measures.Exporter.ConsoleExporter";

	public static String exporter() {
		return props.getProperty(EXPORTER_PROPERTY, EXPORTER_PROPERTY_DEFAULT);
	}

	public static final String EXPORT_FILE_PROPERTY = "exportfile";

	public static String exportFile() {
		return props.getProperty(EXPORT_FILE_PROPERTY);
	}

	public static final String THREAD_COUNT_PROPERTY = "threadcount";
	public static final String THREAD_COUNT_PROPERTY_DEFAULT = "1";

	public static int threadCount() {
		return Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY, THREAD_COUNT_PROPERTY_DEFAULT));
	}

	public static final String INSERT_START_PROPERTY = "insertstart";
	public static final String INSERT_START_PROPERTY_DEFAULT = "0";

	public static long insertStart() {
		return Long.parseLong(props.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
	}

	public static final String INSERT_COUNT_PROPERTY = "insertcount";
	public static final String INSERT_COUNT_PROPERTY_DEFAULT = "0";

	public static long insertCount() {
		return Long.parseLong(props.getProperty(INSERT_COUNT_PROPERTY, INSERT_COUNT_PROPERTY_DEFAULT));
	}

	public static final String TARGET_PROPERTY = "target";
	public static final String TARGET_PROPERTY_DEFAULT = "0";

	public static int target() {
		return Integer.parseInt(props.getProperty(TARGET_PROPERTY, TARGET_PROPERTY_DEFAULT));
	}

	public static final String MAX_EXECUTION_TIME = "maxexecutiontime";
	public static final String MAX_EXECUTION_TIME_DEFAULT = "0";

	public static long maxExecutionTime() {
		return Long.parseLong(props.getProperty(MAX_EXECUTION_TIME, MAX_EXECUTION_TIME_DEFAULT));
	}

	public static final String STAGE_PROPERTY = "stage";
	public static final String STAGE_PROPERTY_DEFAULT = "transactions";

	public static Stage stage() {
		if (props.getProperty(STAGE_PROPERTY, STAGE_PROPERTY_DEFAULT).equals("transactions")) {
			return Stage.TRANSACTIONS;
		}

		return Stage.LOAD;
	}

	public static final String TABLE_PROPERTY = "table";
	public static final String TABLE_PROPERTY_DEFAULT = "tbl";

	public static String table() {
		return props.getProperty(TABLE_PROPERTY, TABLE_PROPERTY_DEFAULT);
	}

	public static final String ROW_LENGTH_DISTRIBUTION_PROPERTY = "rowlengthdistribution";
	public static final String ROW_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

	public static Distribution rowLengthDistribution() {
		return Distribution.fromString(props.getProperty(ROW_LENGTH_DISTRIBUTION_PROPERTY, ROW_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT));
	}

	public static final String ROW_LENGTH_PROPERTY = "rowlength";
	public static final String ROW_LENGTH_PROPERTY_DEFAULT = "1000";

	public static int rowLength() {
		return Integer.parseInt(props.getProperty(ROW_LENGTH_PROPERTY, ROW_LENGTH_PROPERTY_DEFAULT));
	}

	public static final String MIN_ROW_LENGTH_PROPERTY = "minrowlength";
	public static final String MIN_ROW_LENGTH_PROPERTY_DEFAULT = "10";

	public static int minRowLength() {
		return Integer.parseInt(props.getProperty(MIN_ROW_LENGTH_PROPERTY, MIN_ROW_LENGTH_PROPERTY_DEFAULT));
	}

	public static final String FIELD_PREFIX = "fieldprefix";
	public static final String FIELD_PREFIX_DEFAULT = "field";

	public static String fieldPrefix() {
		return props.getProperty(FIELD_PREFIX, FIELD_PREFIX_DEFAULT);
	}

	public static final String FIELD_COUNT_PROPERTY = "fieldcount";
	public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

	public static long fieldCount() {
		return Long.parseLong(props.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
	}

	public static final String READ_PROPORTION_PROPERTY = "readproportion";
	public static final String READ_PROPORTION_PROPERTY_DEFAULT = "1.0";

	public static double readProportion() {
		return Double.parseDouble(props.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
	}

	public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";
	public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

	public static double insertProportion() {
		return Double.parseDouble(props.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
	}

	public static final String TRACE_PROPORTION_PROPERTY = "traceproportion";
	public static final String TRACE_PROPORTION_PROPERTY_DEFAULT = "0.01";

	public static double traceProportion() {
		return Double.parseDouble(props.getProperty(TRACE_PROPORTION_PROPERTY, TRACE_PROPORTION_PROPERTY_DEFAULT));
	}

	public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
	public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

	public static Distribution requestDistribution() {
		return Distribution.fromString(props.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT));
	}

	public static final String ZERO_PADDING_PROPERTY = "zeropadding";
	public static final String ZERO_PADDING_PROPERTY_DEFAULT = "1";

	public static int zeroPadding() {
		return Integer.parseInt(props.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));
	}

	public static final Properties props = new Properties();

	public static void setProperty(String name, String value) {
		props.setProperty(name, value);
	}
}
