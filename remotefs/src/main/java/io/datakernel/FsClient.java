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

package io.datakernel;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.stream.StreamProducer;

import java.util.List;

public abstract class FsClient {
	public abstract void upload(String destinationFileName, StreamProducer<ByteBuf> producer, CompletionCallback callback);

	public abstract void download(String sourceFileName, long startPosition, ResultCallback<StreamTransformerWithCounter> callback);

	public void download(String sourceFileName, ResultCallback<StreamTransformerWithCounter> callback) {
		download(sourceFileName, 0, callback);
	}

	public abstract void list(ResultCallback<List<String>> callback);

	public abstract void delete(String fileName, CompletionCallback callback);
}