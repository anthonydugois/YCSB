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

import site.ycsb.measures.Exporter;
import site.ycsb.measures.Measures;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Main class for executing YCSB.
 */
public final class Application {
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
		System.out.println("  " + WorkloadDescriptor.WORKLOAD_PROPERTY + ": the name of the workload class to use (e.g. " +
				"site.ycsb.workloads.CoreWorkload)");
		System.out.println();
		System.out.println("To run the transaction phase from multiple servers, start a separate client on each.");
		System.out.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
		System.out.println("use the \"insertcount\" and \"insertstart\" properties to divide up the records " +
				"to be inserted");
	}

	public static void main(String[] args) {
		parseArguments(args);

		int threadCount = WorkloadDescriptor.threadCount();

		int targetOpsPerSec = WorkloadDescriptor.target();

		double threadTargetOpsPerMs = -1;

		if (targetOpsPerSec > 0) {
			threadTargetOpsPerMs = ((double) targetOpsPerSec) / ((double) threadCount) / 1000.0;
		}

		String workloadName = WorkloadDescriptor.workload();

		Workload workload;

		try {
			workload = createWorkload(workloadName);
		} catch (Exception exception) {
			exception.printStackTrace();
			System.exit(1);
			return;
		}

		try {
			workload.init();
		} catch (WorkloadException exception) {
			exception.printStackTrace();
			System.exit(1);
			return;
		}

		List<Client> clients;

		try {
			clients = createClients(workload);
		} catch (Exception exception) {
			exception.printStackTrace();
			System.exit(1);
			return;
		}

		List<Thread> threads = new ArrayList<>(threadCount);

		// Creating client threads
		for (Client client : clients) {
			client.init(threadTargetOpsPerMs);

			Thread thread = new Thread(client);

			threads.add(thread);
		}

		long maxExecutionTimeSec = WorkloadDescriptor.maxExecutionTime();

		Thread terminator = null;

		if (maxExecutionTimeSec > 0) {
			terminator = new TerminatorThread(maxExecutionTimeSec, threads, workload);
		}

		long startTimeMs = System.currentTimeMillis();

		// Begin by starting clients
		for (Thread thread : threads) {
			thread.start();
		}

		// Then start terminator if needed
		if (terminator != null) {
			terminator.start();
		}

		// Wait for clients to complete
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException ignored) {
			}
		}

		long endTimeMs = System.currentTimeMillis();

		// Tracing and cleaning should be done in separate loops to avoid killing DB session too soon
		for (Client client : clients) {
			client.traces();
		}

		for (Client client : clients) {
			client.cleanup();
		}

		try {
			if (terminator != null && !terminator.isInterrupted()) {
				terminator.interrupt();
			}

			workload.cleanup();
		} catch (WorkloadException exception) {
			exception.printStackTrace();
			System.exit(1);
			return;
		}

		try {
			export(clients, endTimeMs - startTimeMs);
		} catch (IOException exception) {
			exception.printStackTrace();
			System.exit(1);
			return;
		}

		System.exit(0);
	}

	private static void export(Collection<Client> clients, long runtime) throws IOException {
		Exporter exporter = createExporter();

		int opCount = 0;

		for (Client client : clients) {
			opCount += client.getOpsDone();
		}

		double throughput = 1000.0 * opCount / runtime;

		exporter.write("TOTAL");
		exporter.write("");
		exporter.write("operations,count," + opCount);
		exporter.write("operations,runtime," + runtime);
		exporter.write("operations,throughput," + throughput);

		/* for (Client client : clients) {
			double clientThroughput = 1000.0 * client.getOpsDone() / client.getRuntime();

			exporter.write("[THREAD " + client.getId() + "] " + client.getOpsDone() + " operations");
			exporter.write("[THREAD " + client.getId() + "] " + client.getRuntime() + " ms");
			exporter.write("[THREAD " + client.getId() + "] " + clientThroughput + " ops/s");
		} */

		Measures.instance.export(exporter);

		exporter.close();
	}

	private static Workload createWorkload(String workloadName)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		ClassLoader classLoader = Application.class.getClassLoader();

		return (Workload) classLoader.loadClass(workloadName).newInstance();
	}

	private static List<Client> createClients(Workload workload)
			throws UnknownDBException {
		long opCount;

		if (WorkloadDescriptor.stage() == WorkloadDescriptor.Stage.LOAD) {
			opCount = WorkloadDescriptor.insertCount() > 0 ? WorkloadDescriptor.insertCount() : WorkloadDescriptor.recordCount();
		} else {
			opCount = WorkloadDescriptor.operationCount();
		}

		int threadCount = WorkloadDescriptor.threadCount();

		if (threadCount > opCount && opCount > 0) {
			// Limit thread count if there are too few operations
			threadCount = (int) opCount;
		}

		String dbName = WorkloadDescriptor.db();

		List<Client> clients = new ArrayList<>(threadCount);

		for (int threadId = 0; threadId < threadCount; ++threadId) {
			DB db;

			try {
				db = DBFactory.createDB(dbName);
			} catch (Exception exception) {
				throw new UnknownDBException();
			}

			int threadOpCount = ((int) opCount) / threadCount;

			// Ensure correct number of operations, in case opCount is not a multiple of threadCount
			if (threadId < opCount % threadCount) {
				++threadOpCount;
			}

			Client client = new Client(threadId, db, workload, threadOpCount);

			clients.add(client);
		}

		return clients;
	}

	private static Exporter createExporter() throws IOException {
		OutputStream out;

		String exportFile = WorkloadDescriptor.exportFile();

		if (exportFile == null) {
			out = System.out;
		} else {
			out = Files.newOutputStream(Paths.get(exportFile));
		}

		String exporterName = WorkloadDescriptor.exporter();

		Exporter exporter;

		try {
			exporter = (Exporter) Class.forName(exporterName).getConstructor(OutputStream.class).newInstance(out);
		} catch (Exception exception) {
			exporter = new Exporter.ConsoleExporter(out);
		}

		return exporter;
	}

	private static void parseArguments(String[] args) {
		if (args.length == 0) {
			usageMessage();

			System.out.println("At least one argument specifying a workload is required.");
			System.exit(1);
		}

		int argindex = 0;

		while (args[argindex].startsWith("-")) {
			if (args[argindex].compareTo("-threads") == 0) {
				argindex++;

				if (argindex >= args.length) {
					usageMessage();

					System.out.println("Missing argument value for -threads.");
					System.exit(1);
				}

				WorkloadDescriptor.setProperty(WorkloadDescriptor.THREAD_COUNT_PROPERTY, args[argindex]);

				argindex++;
			} else if (args[argindex].compareTo("-target") == 0) {
				argindex++;

				if (argindex >= args.length) {
					usageMessage();

					System.out.println("Missing argument value for -target.");
					System.exit(1);
				}

				WorkloadDescriptor.setProperty(WorkloadDescriptor.TARGET_PROPERTY, args[argindex]);

				argindex++;
			} else if (args[argindex].compareTo("-load") == 0) {
				WorkloadDescriptor.setProperty(WorkloadDescriptor.STAGE_PROPERTY, "insert");

				argindex++;
			} else if (args[argindex].compareTo("-t") == 0) {
				WorkloadDescriptor.setProperty(WorkloadDescriptor.STAGE_PROPERTY, "transactions");

				argindex++;
			} else if (args[argindex].compareTo("-s") == 0) {
				// props.setProperty(STATUS_PROPERTY, String.valueOf(true));

				argindex++;
			} else if (args[argindex].compareTo("-db") == 0) {
				argindex++;

				if (argindex >= args.length) {
					usageMessage();

					System.out.println("Missing argument value for -db.");
					System.exit(1);
				}

				WorkloadDescriptor.setProperty(WorkloadDescriptor.DB_PROPERTY, args[argindex]);

				argindex++;
			} else if (args[argindex].compareTo("-l") == 0) {
				argindex++;

				if (argindex >= args.length) {
					usageMessage();

					System.out.println("Missing argument value for -l.");
					System.exit(1);
				}

				// props.setProperty(LABEL_PROPERTY, args[argindex]);

				argindex++;
			} else if (args[argindex].compareTo("-P") == 0) {
				argindex++;

				if (argindex >= args.length) {
					usageMessage();

					System.out.println("Missing argument value for -P.");
					System.exit(1);
				}

				String filename = args[argindex];

				argindex++;

				Properties props = new Properties();

				try {
					props.load(Files.newInputStream(Paths.get(filename)));
				} catch (IOException exception) {
					exception.printStackTrace();
					System.exit(1);
				}

				Enumeration<?> enumeration = props.propertyNames();

				while (enumeration.hasMoreElements()) {
					String name = (String) enumeration.nextElement();
					WorkloadDescriptor.setProperty(name, props.getProperty(name));
				}
			} else if (args[argindex].compareTo("-p") == 0) {
				argindex++;

				if (argindex >= args.length) {
					usageMessage();

					System.out.println("Missing argument value for -p");
					System.exit(1);
				}

				int eq = args[argindex].indexOf('=');

				if (eq < 0) {
					usageMessage();

					System.out.println("Argument '-p' expected to be in key=value format (e.g., -p operationcount=99999)");
					System.exit(1);
				}

				String name = args[argindex].substring(0, eq);
				String value = args[argindex].substring(eq + 1);

				WorkloadDescriptor.setProperty(name, value);

				argindex++;
			} else {
				usageMessage();

				System.out.println("Unknown option " + args[argindex]);
				System.exit(1);
			}

			if (argindex >= args.length) {
				break;
			}
		}

		if (argindex != args.length) {
			usageMessage();

			if (argindex < args.length) {
				System.out.println("An argument value without corresponding argument specifier (e.g., -p, -s) was found. We expected an argument specifier and instead found " + args[argindex]);
			} else {
				System.out.println("An argument specifier without corresponding value was found at the end of the supplied command line arguments.");
			}

			System.exit(1);
		}

		if (!checkRequiredProperties()) {
			System.out.println("Failed check required properties.");
			System.exit(1);
		}
	}

	public static boolean checkRequiredProperties() {
		if (WorkloadDescriptor.workload() == null) {
			System.out.println("Missing property: " + WorkloadDescriptor.WORKLOAD_PROPERTY);

			return false;
		}

		return true;
	}
}
