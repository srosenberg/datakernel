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

import io.datakernel.eventloop.Eventloop;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

/**
 * Callback which calling after completing some action.
 */
public abstract class CompletionCallback extends ExceptionCallback {
	/**
	 * Called after completing some action. This method can handle some completed action.
	 */
	public final void setComplete() {
		CallbackRegistry.complete(this);
		onComplete();
	}

	public final void postComplete() {
		postComplete(getCurrentEventloop());
	}

	public final void postComplete(Eventloop eventloop) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				setComplete();
			}
		});
	}

	protected abstract void onComplete();
}
