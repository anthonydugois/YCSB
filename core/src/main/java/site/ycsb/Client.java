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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

/**
 * A thread for executing transactions.
 */
public class Client implements Runnable {
	private final int id;
	private final DB db;
	private final Workload workload;
	private final int opCount;

	private double targetOpsPerMs;
	private long targetOpsTickNanos;
	private int opsDone = 0;
	private long startTimeMs = 0;
	private long endTimeMs = 0;

	public Client(int id, DB db, Workload workload, int opCount) {
		this.id = id;
		this.db = db;
		this.workload = workload;
		this.opCount = opCount;
	}

	public int getId() {
		return id;
	}

	public int getOpsDone() {
		return opsDone;
	}

	public int getRemainingOps() {
		return Math.max(opCount - opsDone, 0);
	}

	public long getRuntime() {
		return endTimeMs - startTimeMs;
	}

	public void init(double targetOpsPerMs) {
		if (targetOpsPerMs > 0) {
			this.targetOpsPerMs = targetOpsPerMs;
			this.targetOpsTickNanos = (long) (1000000 / targetOpsPerMs);
		}

		try {
			db.init();
		} catch (DBException exception) {
			exception.printStackTrace();
		}
	}

	public void cleanup() {
		try {
			db.cleanup();
		} catch (DBException exception) {
			exception.printStackTrace();
		}
	}

	/**
	 * Retrieve all traces from the database.
	 * <p>
	 * Warning: this is expensive.
	 */
	public void traces() {
		db.traces();
	}

	@Override
	public void run() {
		// Spread the thread operations out so they don't all hit the DB at the same time
		if (targetOpsPerMs > 0 && targetOpsPerMs <= 1.0) {
			long delay = ThreadLocalRandom.current().nextInt((int) targetOpsTickNanos);

			sleepUntil(System.nanoTime() + delay);
		}

		WorkloadDescriptor.Stage stage = WorkloadDescriptor.stage();

		startTimeMs = System.currentTimeMillis();

		try {
			long startTimeNanos = System.nanoTime();

			while ((opCount == 0 || opsDone < opCount) && !workload.isStopRequested()) {
				if (!execute(stage)) {
					break;
				}

				opsDone++;
				throttleNanos(startTimeNanos);
			}
		} catch (Exception exception) {
			exception.printStackTrace();
			System.exit(1);
			return;
		}

		endTimeMs = System.currentTimeMillis();
	}

	private boolean execute(WorkloadDescriptor.Stage stage) {
		switch (stage) {
			case LOAD:
				return workload.doInsert(db);
			case TRANSACTIONS:
				return workload.doTransaction(db);
		}

		return false;
	}

	private static void sleepUntil(long timeNanos) {
		while (System.nanoTime() < timeNanos) {
			LockSupport.parkNanos(timeNanos - System.nanoTime());
		}
	}

	private void throttleNanos(long startTimeNanos) {
		if (targetOpsPerMs > 0) {
			// Delay until next tick
			sleepUntil(startTimeNanos + opsDone * targetOpsTickNanos);
		}
	}
}
