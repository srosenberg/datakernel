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

package io.datakernel.stream.processor;

import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1_Stateless;
import io.datakernel.stream.StreamDataReceiver;

public class TransformerNoEnd extends AbstractStreamTransformer_1_1_Stateless<ByteBuf, ByteBuf> {

	private final CompletionCallback closeCallback = new CompletionCallback() {
		@Override
		public void onComplete() {
			upstreamProducer.close();
		}

		@Override
		public void onException(Exception e) {
			upstreamProducer.closeWithError(e);
		}
	};

	public TransformerNoEnd(Eventloop eventloop) {
		super(eventloop);
	}

	@Override
	public StreamDataReceiver<ByteBuf> getDataReceiver() {
		return downstreamDataReceiver;
	}

	@Override
	public void onClosed() {
		// ignored
	}

	public CompletionCallback getCloseCallback() {
		return closeCallback;
	}
}