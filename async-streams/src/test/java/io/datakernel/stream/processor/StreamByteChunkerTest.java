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
import io.datakernel.bytebufnew.ByteBufN;
import io.datakernel.bytebufnew.ByteBufNPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.datakernel.bytebufnew.ByteBufNPool.*;
import static org.junit.Assert.*;

public class StreamByteChunkerTest {
	@Before
	public void before() {
		ByteBufNPool.clear();
		ByteBufNPool.setSizes(0, Integer.MAX_VALUE);
	}

	private static ByteBufN createRandomByteBuf(Random random) {
		int len = random.nextInt(100);
		ByteBufN result = ByteBufN.create(len);
		result.setReadPosition(0);
		result.setWritePosition(len);
		int lenUnique = 1 + random.nextInt(len + 1);
		for (int i = 0; i < len; i++) {
			result.array()[i] = (byte) (i % lenUnique);
		}
		return result;
	}

	private static byte[] byteBufsToByteArray(List<ByteBufN> byteBufs) {
		int size = 0;
		for (ByteBufN byteBuf : byteBufs) {
			size += byteBuf.remainingToRead();
		}
		byte[] result = new byte[size];
		int pos = 0;
		for (ByteBufN byteBuf : byteBufs) {
			System.arraycopy(byteBuf.array(), byteBuf.getReadPosition(), result, pos, byteBuf.remainingToRead());
			pos += byteBuf.remainingToRead();
		}
		return result;
	}

	@Test
	public void testResizer() throws Exception {
		final Eventloop eventloop = new Eventloop();

		List<ByteBufN> buffers = new ArrayList<>();
		Random random = new Random(123456);
		int buffersCount = 1000;
		int totalLen = 0;
		for (int i = 0; i < buffersCount; i++) {
			ByteBufN buffer = createRandomByteBuf(random);
			buffers.add(buffer);
			totalLen += buffer.remainingToRead();
		}
		byte[] expected = byteBufsToByteArray(buffers);

		int bufSize = 128;

		StreamProducer<ByteBufN> source = StreamProducers.ofIterable(eventloop, buffers);
		StreamByteChunker resizer = new StreamByteChunker(eventloop, bufSize / 2, bufSize);
		StreamFixedSizeConsumer streamFixedSizeConsumer = new StreamFixedSizeConsumer();

		source.streamTo(resizer.getInput());
		resizer.getOutput().streamTo(streamFixedSizeConsumer);

		eventloop.run();

		List<ByteBufN> receivedBuffers = streamFixedSizeConsumer.getBuffers();
		byte[] received = byteBufsToByteArray(receivedBuffers);
		assertArrayEquals(received, expected);

		int actualLen = 0;
		for (int i = 0; i < receivedBuffers.size() - 1; i++) {
			ByteBufN buf = receivedBuffers.get(i);
			actualLen += buf.remainingToRead();
			int receivedSize = buf.remainingToRead();
			assertTrue(receivedSize >= bufSize / 2 && receivedSize <= bufSize);
			buf.recycle();
		}
		actualLen += receivedBuffers.get(receivedBuffers.size() - 1).remainingToRead();
		receivedBuffers.get(receivedBuffers.size() - 1).recycle();

		assertEquals(totalLen, actualLen);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	private static class StreamFixedSizeConsumer implements StreamConsumer<ByteBufN>, StreamDataReceiver<ByteBufN> {

		private List<ByteBufN> buffers = new ArrayList<>();
		private List<CompletionCallback> callbacks = new ArrayList<>();

		@Override
		public StreamDataReceiver<ByteBufN> getDataReceiver() {
			return this;
		}

		@Override
		public void streamFrom(StreamProducer<ByteBufN> upstreamProducer) {

		}

		@Override
		public void onProducerEndOfStream() {
			for (CompletionCallback callback : callbacks) {
				callback.onComplete();
			}
		}

		@Override
		public void onProducerError(Exception e) {

		}

		@Override
		public StreamStatus getConsumerStatus() {
			return null;
		}

		@Override
		public Exception getConsumerException() {
			return null;
		}

		@Override
		public void onData(ByteBufN item) {
			buffers.add(item);
		}

		public List<ByteBufN> getBuffers() {
			return buffers;
		}
	}

}