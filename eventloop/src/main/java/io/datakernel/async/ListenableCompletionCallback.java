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

package io.datakernel.async;

import java.util.ArrayList;

/**
 * This callback contains collection of listeners {@link CompletionCallback},
 * on calling {@code onComplete} or {@code onException} of this callback it
 * calls listeners methods too. Each listener can react only on one action,
 * than it will be removed from this {@code ListenableCompletionCallback}.
 */
public class ListenableCompletionCallback extends CompletionCallback {
	private ArrayList<CompletionCallback> listeners = new ArrayList<>();
	private boolean completed = false;
	private Exception exception;

	// region builders
	private ListenableCompletionCallback() {}

	public static ListenableCompletionCallback create() {
		return new ListenableCompletionCallback();
	}
	// endregion

	public void addListener(CompletionCallback callback) {
		if (completed) {
			callback.setComplete();
			return;
		}

		if (exception != null) {
			callback.setException(exception);
			return;
		}

		listeners.add(callback);
	}

	@Override
	protected void onComplete() {
		assert exception == null;

		this.completed = true;

		for (CompletionCallback listener : listeners) {
			listener.setComplete();
		}

		listeners.clear();
	}

	@Override
	protected void onException(Exception exception) {
		assert !completed;
		assert exception != null;

		this.exception = exception;

		for (CompletionCallback listener : listeners) {
			listener.setException(exception);
		}

		listeners.clear();
	}
}
