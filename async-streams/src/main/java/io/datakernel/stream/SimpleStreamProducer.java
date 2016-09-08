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

package io.datakernel.stream;

import io.datakernel.eventloop.Eventloop;

public final class SimpleStreamProducer<T> extends AbstractStreamProducer<T> {
	private StatusListener statusListener;

	public interface StatusListener {
		void onResumed();

		void onSuspended();

		void onClosedWithError(Exception e);
	}

	// region creators
	private SimpleStreamProducer(Eventloop eventloop) {
		super(eventloop);
		this.statusListener = null;
	}

	public static <T> SimpleStreamProducer<T> create(Eventloop eventloop) {
		return new SimpleStreamProducer<>(eventloop);
	}

	public SimpleStreamProducer withStatusListener(StatusListener statusListener) {
		this.statusListener = statusListener;
		return this;
	}
	// endregion

	@Override
	protected void onDataReceiverChanged() {
	}

	@Override
	protected void onSuspended() {
		if (statusListener != null)
			statusListener.onSuspended();
	}

	@Override
	protected void onResumed() {
		if (statusListener != null)
			statusListener.onResumed();
	}

	@Override
	protected void onError(Exception e) {
		if (statusListener != null)
			statusListener.onClosedWithError(e);
	}

	public void send(T item) {
		super.send(item);
	}

	@Override
	public void sendEndOfStream() {
		super.sendEndOfStream();
	}
}
