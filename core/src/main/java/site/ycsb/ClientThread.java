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

import site.ycsb.tracing.TraceInfo;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

/**
 * A thread for executing transactions.
 */
public class ClientThread implements Runnable {
	private final DB db;
	private final boolean transactions;
	private final Workload workload;
	private final Properties props;
	private final int opCount;
	//private final CountDownLatch completeLatch;

	private double targetOpsPerMs;
	private long targetOpsTickNanos;
	private int opsDone;
	private long startTimeMs;
	private long endTimeMs;
	//private final Measurements measurements;
	private int threadId;
	private int threadCount;

	// should we keep this? probably not
	//private static boolean spinSleep;

	public ClientThread(DB db, boolean transactions, Workload workload, Properties props, int opCount,
						double threadTargetPerMs, CountDownLatch completeLatch) {
		this.db = db;
		this.transactions = transactions;
		this.workload = workload;
		this.props = props;
		this.opCount = opCount;
		//this.completeLatch = completeLatch;

		if (threadTargetPerMs > 0) {
			this.targetOpsPerMs = threadTargetPerMs;
			this.targetOpsTickNanos = (long) (1000000 / targetOpsPerMs);
		}

		this.opsDone = 0;
		//this.measurements = Measurements.getMeasurements();

		//spinSleep = Boolean.parseBoolean(this.props.getProperty("spin.sleep", "false"));
	}

	public int getThreadId() {
		return threadId;
	}

	public void setThreadId(final int threadId) {
		this.threadId = threadId;
	}

	public int getThreadCount() {
		return threadCount;
	}

	public void setThreadCount(final int threadCount) {
		this.threadCount = threadCount;
	}

	public int getOpsDone() {
		return opsDone;
	}

	public long getRuntime() {
		return endTimeMs - startTimeMs;
	}

	public void init() {
		try {
			db.init();
		} catch (DBException e) {
			e.printStackTrace();
			e.printStackTrace(System.out);
		}
	}

	public void cleanup() {
		try {
			//measurements.setIntendedStartTimeNanos(0);
			db.cleanup();
		} catch (DBException e) {
			e.printStackTrace();
			e.printStackTrace(System.out);
		}/* finally {
			//completeLatch.countDown();
		}*/
	}

	public Collection<TraceInfo> getTraces() {
		return db.traces();
	}

	@Override
	public void run() {
		Object state;

		try {
			state = workload.initThread(props, threadId, threadCount);
		} catch (WorkloadException e) {
			e.printStackTrace();
			e.printStackTrace(System.out);

			return;
		}

		startTimeMs = System.currentTimeMillis();

		// NOTE: Switching to using nanoTime and parkNanos for time management here such that the measurements
		// and the client thread have the same view on time.

		// Spread the thread operations out so they don't all hit the DB at the same time
		// GH issue 4 - throws exception if _target > 1 because random.nextInt argument must be > 0
		// and the sleep() doesn't make sense for granularities < 1 ms anyway
		if (targetOpsPerMs > 0 && targetOpsPerMs <= 1.0) {
			long randomMinorDelay = ThreadLocalRandom.current().nextInt((int) targetOpsTickNanos);

			sleepUntil(System.nanoTime() + randomMinorDelay);
		}

		try {
			long startTimeNanos = System.nanoTime();

			if (transactions) {
				while ((opCount == 0 || opsDone < opCount) && !workload.isStopRequested()) {
					if (!workload.doTransaction(db, state)) {
						break;
					}

					opsDone++;
					throttleNanos(startTimeNanos);
				}
			} else {
				while ((opCount == 0 || opsDone < opCount) && !workload.isStopRequested()) {
					if (!workload.doInsert(db, state)) {
						break;
					}

					opsDone++;
					throttleNanos(startTimeNanos);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			e.printStackTrace(System.out);

			System.exit(0);
		}

		endTimeMs = System.currentTimeMillis();
	}

	private static void sleepUntil(long deadline) {
		while (System.nanoTime() < deadline) {
			/*if (!spinSleep) {
				LockSupport.parkNanos(deadline - System.nanoTime());
			}*/
			LockSupport.parkNanos(deadline - System.nanoTime());
		}
	}

	private void throttleNanos(long startTimeNanos) {
		if (targetOpsPerMs > 0) {
			// Delay until next tick
			long deadline = startTimeNanos + opsDone * targetOpsTickNanos;

			sleepUntil(deadline);

			//measurements.setIntendedStartTimeNanos(deadline);
		}
	}

	/**
	 * The total amount of work this thread is still expected to do.
	 */
	public int remainingOps() {
		return Math.max(opCount - opsDone, 0);
	}
}
