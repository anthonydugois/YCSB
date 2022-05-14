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

package site.ycsb;

import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import site.ycsb.measures.Exporter;
import site.ycsb.measures.Measures;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Main class for executing YCSB.
 */
public final class Client {
	public static final String DEFAULT_RECORD_COUNT = "0";

	/**
	 * The target number of operations to perform.
	 */
	public static final String OPERATION_COUNT_PROPERTY = "operationcount";

	/**
	 * The number of records to load into the database initially.
	 */
	public static final String RECORD_COUNT_PROPERTY = "recordcount";

	/**
	 * The workload class to be loaded.
	 */
	public static final String WORKLOAD_PROPERTY = "workload";

	/**
	 * The database class to be used.
	 */
	public static final String DB_PROPERTY = "db";

	/**
	 * The exporter class to be used. The default is site.ycsb.measurements.exporter.TextMeasurementsExporter.
	 */
	public static final String EXPORTER_PROPERTY = "exporter";

	/**
	 * If set to the path of a file, YCSB will write all output to this file instead of STDOUT.
	 */
	public static final String EXPORT_FILE_PROPERTY = "exportfile";

	/**
	 * The number of YCSB client threads to run.
	 */
	public static final String THREAD_COUNT_PROPERTY = "threadcount";

	/**
	 * Indicates how many inserts to do if less than recordcount.
	 * Useful for partitioning the load among multiple servers if the client is the bottleneck.
	 * Additionally workloads should support the "insertstart" property which tells them which record to start at.
	 */
	public static final String INSERT_COUNT_PROPERTY = "insertcount";

	/**
	 * Target number of operations per second.
	 */
	public static final String TARGET_PROPERTY = "target";

	/**
	 * The maximum amount of time (in seconds) for which the benchmark will be run.
	 */
	public static final String MAX_EXECUTION_TIME = "maxexecutiontime";

	/**
	 * Whether or not this is the transaction phase (run) or not (load).
	 */
	public static final String DO_TRANSACTIONS_PROPERTY = "dotransactions";

	/**
	 * Whether or not to show status during run.
	 */
	public static final String STATUS_PROPERTY = "status";

	/**
	 * Use label for status (e.g., to label one experiment out of a whole batch).
	 */
	public static final String LABEL_PROPERTY = "label";

	/**
	 * An optional thread used to track progress and measure JVM stats.
	 */
	private static StatusThread statusThread = null;

	/**
	 * All keys for configuring the tracing system start with this prefix.
	 */
	private static final String HTRACE_KEY_PREFIX = "htrace.";

	private static final String CLIENT_WORKLOAD_INIT_SPAN = "Client#workload_init";
	private static final String CLIENT_INIT_SPAN = "Client#init";
	private static final String CLIENT_WORKLOAD_SPAN = "Client#workload";
	private static final String CLIENT_CLEANUP_SPAN = "Client#cleanup";
	private static final String CLIENT_EXPORT_MEASUREMENTS_SPAN = "Client#export_measurements";

	public static void usageMessage() {
		System.out.println("Usage: java site.ycsb.Client [options]");
		System.out.println("Options:");
		System.out.println("  -threads n: execute using n threads (default: 1) - can also be specified as the \n" +
				"        \"threadcount\" property using -p");
		System.out.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n" +
				"       be specified as the \"target\" property using -p");
		System.out.println("  -load:  run the loading phase of the workload");
		System.out.println("  -t:  run the transactions phase of the workload (default)");
		System.out.println("  -db dbname: specify the name of the DB to use (default: site.ycsb.BasicDB) - \n" +
				"        can also be specified as the \"db\" property using -p");
		System.out.println("  -P propertyfile: load properties from the given file. Multiple files can");
		System.out.println("           be specified, and will be processed in the order specified");
		System.out.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
		System.out.println("          multiple properties can be specified, and override any");
		System.out.println("          values in the propertyfile");
		System.out.println("  -s:  show status during run (default: no status)");
		System.out.println("  -l label:  use label for status (e.g. to label one experiment out of a whole batch)");
		System.out.println();
		System.out.println("Required properties:");
		System.out.println("  " + WORKLOAD_PROPERTY + ": the name of the workload class to use (e.g. " +
				"site.ycsb.workloads.CoreWorkload)");
		System.out.println();
		System.out.println("To run the transaction phase from multiple servers, start a separate client on each.");
		System.out.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
		System.out.println("use the \"insertcount\" and \"insertstart\" properties to divide up the records " +
				"to be inserted");
	}

	public static void main(String[] args) {
		Properties props = parseArguments(args);

		//boolean status = Boolean.parseBoolean(props.getProperty(STATUS_PROPERTY, "false"));
		//String label = props.getProperty(LABEL_PROPERTY, "");

		long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

		// Get number of threads, target and db
		int threadCount = Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY, "1"));
		String dbName = props.getProperty(DB_PROPERTY, "site.ycsb.BasicDB");
		int target = Integer.parseInt(props.getProperty(TARGET_PROPERTY, "0"));

		// Compute the target throughput
		double threadTargetPerMs = -1;

		if (target > 0) {
			double threadTarget = ((double) target) / ((double) threadCount);

			threadTargetPerMs = threadTarget / 1000.0;
		}

		Thread warningThread = setupWarningThread();

		warningThread.start();

		//Measurements.setProperties(props);

		Workload workload = getWorkload(props);
		final Tracer tracer = getTracer(props, workload);

		initWorkload(props, warningThread, workload, tracer);

		// System.err.println("Starting test.");

		final CountDownLatch completeLatch = new CountDownLatch(threadCount);
		final List<ClientThread> clients = createClients(dbName, props, threadCount, threadTargetPerMs, workload, tracer, completeLatch);

		/*if (status) {
			boolean stdStatus = props.getProperty(Measurements.MEASUREMENT_TYPE_PROPERTY, "").compareTo("timeseries") == 0;
			int statusIntervalSeconds = Integer.parseInt(props.getProperty("status.interval", "10"));
			boolean trackJVMStats = props.getProperty(Measurements.MEASUREMENT_TRACK_JVM_PROPERTY, Measurements.MEASUREMENT_TRACK_JVM_PROPERTY_DEFAULT).equals("true");

			statusThread = new StatusThread(completeLatch, clients, label, stdStatus, statusIntervalSeconds, trackJVMStats);
			statusThread.start();
		}*/

		Thread terminator = null;

		long startTimeMs;
		long endTimeMs;

		try (final TraceScope span = tracer.newScope(CLIENT_WORKLOAD_SPAN)) {
			List<Thread> threads = new ArrayList<>(threadCount);

			for (ClientThread client : clients) {
				client.init();

				threads.add(new Thread(tracer.wrap(client, "ClientThread")));
			}

			startTimeMs = System.currentTimeMillis();

			for (Thread thread : threads) {
				thread.start();
			}

			if (maxExecutionTime > 0) {
				terminator = new TerminatorThread(maxExecutionTime, threads, workload);
				terminator.start();
			}

			for (Thread thread : threads) {
				try {
					thread.join();
				} catch (InterruptedException ignored) {
					// Ignored
				}
			}

			endTimeMs = System.currentTimeMillis();

			for (ClientThread client : clients) {
				client.getTraces();
			}

			for (ClientThread client : clients) {
				client.cleanup();
			}
		}

		try {
			try (final TraceScope span = tracer.newScope(CLIENT_CLEANUP_SPAN)) {
				if (terminator != null && !terminator.isInterrupted()) {
					terminator.interrupt();
				}

				/*if (status) {
					// Wake up status thread if it's asleep
					statusThread.interrupt();

					// At this point we assume all the monitored threads are already gone as per above join loop
					try {
						statusThread.join();
					} catch (InterruptedException ignored) {
						// Ignored
					}
				}*/

				workload.cleanup();
			}
		} catch (WorkloadException e) {
			e.printStackTrace();
			e.printStackTrace(System.out);

			System.exit(0);
		}

		try {
			try (final TraceScope span = tracer.newScope(CLIENT_EXPORT_MEASUREMENTS_SPAN)) {
				export(props, clients, endTimeMs - startTimeMs);
			}
		} catch (IOException e) {
			System.err.println("Could not export measurements, error: " + e.getMessage());
			e.printStackTrace();

			System.exit(-1);
		}

		System.exit(0);
	}

	public static boolean checkRequiredProperties(Properties props) {
		if (props.getProperty(WORKLOAD_PROPERTY) == null) {
			System.out.println("Missing property: " + WORKLOAD_PROPERTY);

			return false;
		}

		return true;
	}

	/**
	 * Exports the measurements to either sysout or a file using the exporter loaded from conf.
	 *
	 * @throws IOException Either failed to write to output stream or failed to close it.
	 */
	private static void export(Properties props, Collection<ClientThread> clients, long runtime) throws IOException {
		Exporter exporter = null;

		try {
			OutputStream out;

			String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);

			if (exportFile == null) {
				out = System.out;
			} else {
				out = Files.newOutputStream(Paths.get(exportFile));
			}

			String exporterName = props.getProperty(EXPORTER_PROPERTY, "site.ycsb.measures.Exporter.ConsoleExporter");

			try {
				exporter = (Exporter) Class.forName(exporterName).getConstructor(OutputStream.class).newInstance(out);
			} catch (Exception exception) {
				exporter = new Exporter.ConsoleExporter(out);
			}

			int opCount = 0;
			for (ClientThread client : clients) {
				opCount += client.getOpsDone();
			}

			double throughput = 1000.0 * opCount / runtime;

			exporter.write("[TOTAL] " + opCount + " operations");
			exporter.write("[TOTAL] runtime : " + runtime + " ms");
			exporter.write("[TOTAL] throughput : " + throughput + " ops/s");

			for (ClientThread client : clients) {
				double clientThroughput = 1000.0 * client.getOpsDone() / client.getRuntime();

				exporter.write("[THREAD " + client.getThreadId() + "] " + client.getOpsDone() + " operations");
				exporter.write("[THREAD " + client.getThreadId() + "] " + client.getRuntime() + " ms");
				exporter.write("[THREAD " + client.getThreadId() + "] " + clientThroughput + " ops/s");
			}

			/*final Map<String, Long[]> gcs = Utils.getGCStatst();

			long totalGCCount = 0;
			long totalGCTime = 0;

			for (final Entry<String, Long[]> entry : gcs.entrySet()) {
				//exporter.write("TOTAL_GCS_" + entry.getKey(), "Count", entry.getValue()[0]);
				//exporter.write("TOTAL_GC_TIME_" + entry.getKey(), "Time(ms)", entry.getValue()[1]);
				//exporter.write("TOTAL_GC_TIME_%_" + entry.getKey(), "Time(%)", ((double) entry.getValue()[1] / runtime) * (double) 100);

				totalGCCount += entry.getValue()[0];
				totalGCTime += entry.getValue()[1];
			}

			exporter.write("GC", "count", totalGCCount);
			exporter.write("GC", "time (ms)", totalGCTime);
			exporter.write("GC", "time (%)", 100.0 * totalGCTime / runtime);

			if (statusThread != null && statusThread.trackJVMStats()) {
				exporter.write("MAX_MEM_USED", "MBs", statusThread.getMaxUsedMem());
				exporter.write("MIN_MEM_USED", "MBs", statusThread.getMinUsedMem());
				exporter.write("MAX_THREADS", "Count", statusThread.getMaxThreads());
				exporter.write("MIN_THREADS", "Count", statusThread.getMinThreads());
				exporter.write("MAX_SYS_LOAD_AVG", "Load", statusThread.getMaxLoadAvg());
				exporter.write("MIN_SYS_LOAD_AVG", "Load", statusThread.getMinLoadAvg());
			}*/

			Measures.instance.export(exporter);
		} finally {
			if (exporter != null) {
				exporter.close();
			}
		}
	}

	private static List<ClientThread> createClients(String dbName, Properties props, int threadCount,
													double threadTargetPerMs, Workload workload, Tracer tracer,
													CountDownLatch completeLatch) {
		boolean initFailed = false;
		boolean transactions = Boolean.parseBoolean(props.getProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(true)));

		final List<ClientThread> clients = new ArrayList<>(threadCount);

		try (final TraceScope span = tracer.newScope(CLIENT_INIT_SPAN)) {
			int opCount;

			if (transactions) {
				opCount = Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY, "0"));
			} else {
				if (props.containsKey(INSERT_COUNT_PROPERTY)) {
					opCount = Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY, "0"));
				} else {
					opCount = Integer.parseInt(props.getProperty(RECORD_COUNT_PROPERTY, DEFAULT_RECORD_COUNT));
				}
			}

			if (threadCount > opCount && opCount > 0) {
				// Limit thread count if there are not too many operations
				threadCount = opCount;
			}

			for (int threadId = 0; threadId < threadCount; ++threadId) {
				DB db;

				try {
					db = DBFactory.createDB(dbName, props, tracer);
				} catch (UnknownDBException e) {
					System.out.println("Unknown DB " + dbName);

					initFailed = true;

					break;
				}

				int threadOpCount = opCount / threadCount;

				// Ensure correct number of operations, in case opCount is not a multiple of threadCount
				if (threadId < opCount % threadCount) {
					++threadOpCount;
				}

				ClientThread client = new ClientThread(db, transactions, workload, props, threadOpCount, threadTargetPerMs, completeLatch);

				client.setThreadId(threadId);
				client.setThreadCount(threadCount);

				clients.add(client);
			}

			if (initFailed) {
				System.err.println("Error initializing datastore bindings.");
				System.exit(0);
			}
		}

		return clients;
	}

	private static Tracer getTracer(Properties props, Workload workload) {
		return new Tracer.Builder("YCSB " + workload.getClass().getSimpleName())
				.conf(getHTraceConfiguration(props))
				.build();
	}

	private static void initWorkload(Properties props, Thread warningThread, Workload workload,
									 Tracer tracer) {
		try {
			try (final TraceScope span = tracer.newScope(CLIENT_WORKLOAD_INIT_SPAN)) {
				workload.init(props);
				warningThread.interrupt();
			}
		} catch (WorkloadException e) {
			e.printStackTrace();
			e.printStackTrace(System.out);

			System.exit(0);
		}
	}

	private static HTraceConfiguration getHTraceConfiguration(Properties props) {
		final Map<String, String> filteredProperties = new HashMap<>();

		for (String key : props.stringPropertyNames()) {
			if (key.startsWith(HTRACE_KEY_PREFIX)) {
				filteredProperties.put(key.substring(HTRACE_KEY_PREFIX.length()), props.getProperty(key));
			}
		}

		return HTraceConfiguration.fromMap(filteredProperties);
	}

	private static Thread setupWarningThread() {
		// Show a warning message that creating the workload is taking a while but only do so if it is taking longer
		// than 2 seconds (showing the message right away if the setup wasn't taking very long was confusing people)
		return new Thread(() -> {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				return;
			}

			System.err.println(" (might take a few minutes for large data sets)");
		});
	}

	private static Workload getWorkload(Properties props) {
		ClassLoader classLoader = Client.class.getClassLoader();

		/*try {
			Properties projectProp = new Properties();

			projectProp.load(classLoader.getResourceAsStream("project.properties"));

			System.err.println("YCSB Client " + projectProp.getProperty("version"));
		} catch (IOException e) {
			System.err.println("Unable to retrieve client version.");
		}

		System.err.println();
		System.err.println("Loading workload...");*/

		try {
			Class<?> workloadClass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));

			return (Workload) workloadClass.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
			e.printStackTrace(System.out);

			System.exit(0);
		}

		return null;
	}

	private static Properties parseArguments(String[] args) {
		Properties props = new Properties();

		/*System.err.print("Command line:");

		for (String arg : args) {
			System.err.print(" " + arg);
		}

		System.err.println();*/

		if (args.length == 0) {
			usageMessage();

			System.out.println("At least one argument specifying a workload is required.");

			System.exit(0);
		}

		Properties fileProps = new Properties();

		int argindex = 0;

		while (args[argindex].startsWith("-")) {
			if (args[argindex].compareTo("-threads") == 0) {
				argindex++;

				if (argindex >= args.length) {
					usageMessage();

					System.out.println("Missing argument value for -threads.");

					System.exit(0);
				}

				int threadCount = Integer.parseInt(args[argindex]);

				props.setProperty(THREAD_COUNT_PROPERTY, String.valueOf(threadCount));

				argindex++;
			} else if (args[argindex].compareTo("-target") == 0) {
				argindex++;

				if (argindex >= args.length) {
					usageMessage();

					System.out.println("Missing argument value for -target.");

					System.exit(0);
				}

				int target = Integer.parseInt(args[argindex]);

				props.setProperty(TARGET_PROPERTY, String.valueOf(target));

				argindex++;
			} else if (args[argindex].compareTo("-load") == 0) {
				props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(false));
				argindex++;
			} else if (args[argindex].compareTo("-t") == 0) {
				props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(true));
				argindex++;
			} else if (args[argindex].compareTo("-s") == 0) {
				props.setProperty(STATUS_PROPERTY, String.valueOf(true));
				argindex++;
			} else if (args[argindex].compareTo("-db") == 0) {
				argindex++;

				if (argindex >= args.length) {
					usageMessage();

					System.out.println("Missing argument value for -db.");

					System.exit(0);
				}

				props.setProperty(DB_PROPERTY, args[argindex]);

				argindex++;
			} else if (args[argindex].compareTo("-l") == 0) {
				argindex++;

				if (argindex >= args.length) {
					usageMessage();

					System.out.println("Missing argument value for -l.");

					System.exit(0);
				}

				props.setProperty(LABEL_PROPERTY, args[argindex]);

				argindex++;
			} else if (args[argindex].compareTo("-P") == 0) {
				argindex++;

				if (argindex >= args.length) {
					usageMessage();

					System.out.println("Missing argument value for -P.");

					System.exit(0);
				}

				String fileProp = args[argindex];

				argindex++;

				Properties myFileProps = new Properties();

				try {
					myFileProps.load(Files.newInputStream(Paths.get(fileProp)));
				} catch (IOException e) {
					System.out.println("Unable to open the properties file " + fileProp);
					System.out.println(e.getMessage());

					System.exit(0);
				}

				//Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
				for (Enumeration<?> e = myFileProps.propertyNames(); e.hasMoreElements(); ) {
					String prop = (String) e.nextElement();

					fileProps.setProperty(prop, myFileProps.getProperty(prop));
				}
			} else if (args[argindex].compareTo("-p") == 0) {
				argindex++;

				if (argindex >= args.length) {
					usageMessage();

					System.out.println("Missing argument value for -p");

					System.exit(0);
				}

				int eq = args[argindex].indexOf('=');

				if (eq < 0) {
					usageMessage();

					System.out.println("Argument '-p' expected to be in key=value format (e.g., -p operationcount=99999)");

					System.exit(0);
				}

				String name = args[argindex].substring(0, eq);
				String value = args[argindex].substring(eq + 1);

				props.put(name, value);

				argindex++;
			} else {
				usageMessage();

				System.out.println("Unknown option " + args[argindex]);

				System.exit(0);
			}

			if (argindex >= args.length) {
				break;
			}
		}

		if (argindex != args.length) {
			usageMessage();

			if (argindex < args.length) {
				System.out.println("An argument value without corresponding argument specifier (e.g., -p, -s) was found. "
						+ "We expected an argument specifier and instead found " + args[argindex]);
			} else {
				System.out.println("An argument specifier without corresponding value was found at the end of the supplied " +
						"command line arguments.");
			}

			System.exit(0);
		}

		// Overwrite file properties with properties from the command line

		//Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
		for (Enumeration<?> e = props.propertyNames(); e.hasMoreElements(); ) {
			String prop = (String) e.nextElement();

			fileProps.setProperty(prop, props.getProperty(prop));
		}

		props = fileProps;

		if (!checkRequiredProperties(props)) {
			System.out.println("Failed check required properties.");

			System.exit(0);
		}

		return props;
	}
}
