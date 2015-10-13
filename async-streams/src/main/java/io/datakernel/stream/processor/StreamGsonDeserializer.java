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

import com.google.gson.Gson;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

import java.util.ArrayDeque;

public final class StreamGsonDeserializer<T> extends AbstractStreamTransformer_1_1<ByteBuf, T> implements StreamDeserializer<T>, StreamGsonDeserializerMBean {
	private final UpstreamConsumer upstreamConsumer;
	private final DownstreamProducer downstreamProducer;

	private final class UpstreamConsumer extends AbstractUpstreamConsumer {
		@Override
		protected void onUpstreamStarted() {
		}

		@Override
		protected void onUpstreamEndOfStream() {
			downstreamProducer.produce();
		}

		@Override
		public StreamDataReceiver<ByteBuf> getDataReceiver() {
			return downstreamProducer;
		}
	}

	private final class DownstreamProducer extends AbstractDownstreamProducer implements StreamDataReceiver<ByteBuf> {
		private static final int INITIAL_BUFFER_SIZE = 1;

		private final BufferReader bufferReader = new BufferReader();
		private final ArrayDeque<ByteBuf> byteBufs;
		private final int buffersPoolSize;

		private final Gson gson;
		private final Class<T> type;

		private ByteBuf buf;

		private int jmxItems;
		private int jmxBufs;
		private long jmxBytes;

		private DownstreamProducer(ArrayDeque<ByteBuf> byteBufs, int buffersPoolSize, Gson gson, Class<T> type) {
			this.byteBufs = byteBufs;
			this.buffersPoolSize = buffersPoolSize;
			this.gson = gson;
			this.type = type;
			this.buf = ByteBufPool.allocate(INITIAL_BUFFER_SIZE);
		}

		@Override
		protected void onDownstreamStarted() {

		}

		@Override
		protected void onDownstreamSuspended() {
			upstreamConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			upstreamConsumer.resume();
			resumeProduce();
		}

		@Override
		protected void doProduce() {
			while (isStatusReady() && !byteBufs.isEmpty()) {
				ByteBuf srcBuf = byteBufs.peek();

				while (isStatusReady() && srcBuf.hasRemaining()) {
					// read message body:
					int zeroPos = findZero(srcBuf.array(), srcBuf.position(), srcBuf.remaining());

					if (zeroPos == -1) {
						buf = ByteBufPool.append(buf, srcBuf);
						break;
					}

					int readLen = zeroPos - srcBuf.position();
					if (buf.position() != 0) {
						buf = ByteBufPool.append(buf, srcBuf, readLen);
						bufferReader.set(buf.array(), 0, buf.position());
						buf.position(0);
					} else {
						bufferReader.set(srcBuf.array(), srcBuf.position(), readLen);
						srcBuf.advance(readLen);
					}
					srcBuf.advance(1);

					T item = gson.fromJson(bufferReader, type);
					//noinspection AssertWithSideEffects
					assert jmxItems != ++jmxItems;
					downstreamDataReceiver.onData(item);
				}

				if (getProducerStatus().isClosed())
					return;

				if (!srcBuf.hasRemaining()) {
					byteBufs.poll();
					srcBuf.recycle();
				}
			}

			if (byteBufs.isEmpty()) {
				if (upstreamConsumer.getConsumerStatus().isClosed()) {
					sendEndOfStream();
					buf.recycle();
					buf = null;
					recycleBufs();
				} else {
					upstreamConsumer.resume();
				}
			}
		}

		@Override
		public void onData(ByteBuf buf) {
			jmxBufs++;
			jmxBytes += buf.remaining();
			byteBufs.offer(buf);
			downstreamProducer.produce();
			if (byteBufs.size() == buffersPoolSize) {
				upstreamConsumer.suspend();
			}
		}

		private void recycleBufs() {
			if (buf != null) {
				buf.recycle();
				buf = null;
			}
			for (ByteBuf byteBuf : byteBufs) {
				byteBuf.recycle();
			}
			byteBufs.clear();
		}
	}

	public StreamGsonDeserializer(Eventloop eventloop, Gson gson, Class<T> type, int buffersPoolSize) {
		super(eventloop);
		this.downstreamProducer = new DownstreamProducer(new ArrayDeque<ByteBuf>(buffersPoolSize), buffersPoolSize, gson, type);
		this.upstreamConsumer = new UpstreamConsumer();
	}

	private static int findZero(byte[] b, int off, int len) {
		for (int i = off; i < off + len; i++) {
			if (b[i] == 0) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public void drainBuffersTo(StreamDataReceiver<ByteBuf> dataReceiver) {
		for (ByteBuf byteBuf : downstreamProducer.byteBufs) {
			dataReceiver.onData(byteBuf);
		}
		downstreamProducer.byteBufs.clear();
		downstreamProducer.sendEndOfStream();
	}

	@Override
	public int getItems() {
		return downstreamProducer.jmxItems;
	}

	@Override
	public int getBufs() {
		return downstreamProducer.jmxBufs;
	}

	@Override
	public long getBytes() {
		return downstreamProducer.jmxBytes;
	}

	@SuppressWarnings({"AssertWithSideEffects", "ConstantConditions"})
	@Override
	public String toString() {
		boolean assertOn = false;
		assert assertOn = true;
		return '{' + super.toString()
				+ " items:" + (assertOn ? "" + downstreamProducer.jmxItems : "?")
				+ " bufs:" + downstreamProducer.jmxBufs
				+ " bytes:" + downstreamProducer.jmxBytes + '}';
	}
}