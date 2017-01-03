/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.eventloop;

import io.datakernel.async.AsyncCancellable;
import io.datakernel.util.ExposedLinkedList;
import io.datakernel.util.ExposedLinkedList.Node;

public final class ScheduledRunnable implements AsyncCancellable {
	private long timestamp;  // TODO(vmykhalko): is timestamp really needed here?
	private Runnable runnable;
	private boolean complete;
	private boolean cancelled;
	Node<ScheduledRunnable> node = new ExposedLinkedList.Node<>(this);
	private final ExposedLinkedList<ScheduledRunnable> bucket;

	// region builders

	/**
	 * Initializes a new instance of ScheduledRunnable
	 *
	 * @param timestamp timestamp after which this runnable will be executed
	 * @param runnable  runnable for executing
	 */
	public ScheduledRunnable(long timestamp, Runnable runnable, ExposedLinkedList<ScheduledRunnable> bucket) {
		this.timestamp = timestamp;
		this.runnable = runnable;
		this.bucket = bucket;
	}
	// endregion

	@Override
	public void cancel() {
		this.cancelled = true;
		this.runnable = null;
		if (node != null) {
			bucket.removeNode(node);
		}
	}

	public void complete() {
		this.complete = true;
		this.runnable = null;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Runnable getRunnable() {
		return runnable;
	}

	public boolean isCancelled() {
		return cancelled;
	}

	public boolean isComplete() {
		return complete;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName()
				+ "{"
				+ "timestamp=" + timestamp + ", "
				+ "cancelled=" + cancelled + ", "
				+ "complete=" + complete + ", "
				+ "runnable=" + runnable
				+ "}";
	}
}