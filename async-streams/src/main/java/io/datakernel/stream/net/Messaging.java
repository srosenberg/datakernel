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

package io.datakernel.stream.net;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

public interface Messaging<I, O> {
	interface MessagingProtocol<I, O> {
		void onStart(Messaging<I, O> messaging);
	}

	final class MessageOrEndOfStream<I> {
		@Nullable
		I message;

		MessageOrEndOfStream(I message) {
			this.message = message;
		}

		public I getMessage() {
			return message;
		}

		public boolean isEndOfStream() {
			return message == null;
		}
	}

	void read(ResultCallback<MessageOrEndOfStream<I>> callback);

	void write(O message, CompletionCallback callback);

	void writeEndOfStream(CompletionCallback callback);

	void streamFrom(StreamProducer<ByteBuf> streamProducer, CompletionCallback callback);

	void streamTo(StreamConsumer<ByteBuf> streamConsumer, CompletionCallback callback);

	void close();
}
