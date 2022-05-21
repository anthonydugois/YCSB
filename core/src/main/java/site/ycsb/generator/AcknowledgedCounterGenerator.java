/**
 * Copyright (c) 2015-2017 YCSB contributors. All rights reserved.
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
package site.ycsb.generator;

import java.util.concurrent.locks.ReentrantLock;

/**
 * A CounterGenerator that reports generated integers via lastInt()
 * only after they have been acknowledged.
 */
public class AcknowledgedCounterGenerator extends CounterGenerator {
	/**
	 * The size of the window of pending id ack's. 2^20 = {@value}
	 */
	static final int WINDOW_SIZE = Integer.rotateLeft(1, 20);

	/**
	 * The mask to use to turn an id into a slot in {@link #window}.
	 */
	private static final int WINDOW_MASK = WINDOW_SIZE - 1;

	private final ReentrantLock lock;
	private final boolean[] window;
	private volatile long limit;

	/**
	 * Create a counter that starts at countstart.
	 */
	public AcknowledgedCounterGenerator(long start) {
		super(start);

		lock = new ReentrantLock();
		window = new boolean[WINDOW_SIZE];
		limit = start - 1;
	}

	/**
	 * In this generator, the highest acknowledged counter value
	 * (as opposed to the highest generated counter value).
	 */
	@Override
	public Long lastValue() {
		return limit;
	}

	/**
	 * Make a generated counter value available via lastInt().
	 */
	public void acknowledge(long value) {
		int currentSlot = (int) (value & WINDOW_MASK);

		if (window[currentSlot]) {
			throw new RuntimeException("Too many unacknowledged insertion keys.");
		}

		window[currentSlot] = true;

		if (lock.tryLock()) {
			// move a contiguous sequence from the window
			// over to the "limit" variable
			try {
				// Only loop through the entire window at most once.
				long beforeFirstSlot = limit & WINDOW_MASK;
				long index = limit + 1;

				while (index != beforeFirstSlot) {
					int slot = (int) (index & WINDOW_MASK);

					if (!window[slot]) {
						break;
					}

					window[slot] = false;

					index++;
				}

				limit = index - 1;
			} finally {
				lock.unlock();
			}
		}
	}
}
