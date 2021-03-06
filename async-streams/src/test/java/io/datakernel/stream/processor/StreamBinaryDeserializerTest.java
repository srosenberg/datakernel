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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.processor.StreamBinarySerializer.MAX_SIZE;
import static java.lang.ClassLoader.getSystemClassLoader;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamBinaryDeserializerTest {
	private Eventloop eventloop;
	private StreamBinaryDeserializer<Data> deserializer;
	private StreamBinarySerializer<Data> serializer;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		deserializer = StreamBinaryDeserializer.create(eventloop, createSerializer(), MAX_SIZE);
		serializer = StreamBinarySerializer.create(eventloop, createSerializer(), 1, MAX_SIZE, 1000, false);
	}

	private BufferSerializer<Data> createSerializer() {
		return SerializerBuilder.create(getSystemClassLoader()).build(Data.class);
	}

	@Test
	public void deserializesSingleMessage() {
		Data data = new Data("a");
		StreamProducer<Data> producer = StreamProducers.ofValue(eventloop, data);
		StreamConsumers.ToList<Data> consumer = StreamConsumers.toList(eventloop);

		producer.streamTo(serializer.getInput());
		serializer.getOutput().streamTo(deserializer.getInput());
		deserializer.getOutput().streamTo(consumer);

		eventloop.run();

		assertEquals(Collections.singletonList(data), consumer.getList());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void deserializesMultipleMessages() {
		List<Data> inputData = asList(new Data("a"), new Data("b"), new Data("c"));
		StreamProducer<Data> producer = StreamProducers.ofIterable(eventloop, inputData);
		StreamConsumers.ToList<Data> consumer = StreamConsumers.toList(eventloop);

		producer.streamTo(serializer.getInput());
		serializer.getOutput().streamTo(deserializer.getInput());
		deserializer.getOutput().streamTo(consumer);

		eventloop.run();

		assertEquals(inputData, consumer.getList());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void deserializesMultipleMessages_SplittedIntoDifferentBytebufs() {
		List<Data> inputData = asList(new Data("a"), new Data("b"), new Data("c"));
		StreamProducer<Data> producer = StreamProducers.ofIterable(eventloop, inputData);
		StreamConsumers.ToList<Data> consumer = StreamConsumers.toList(eventloop);

		StreamByteChunker bufSplitter = StreamByteChunker.create(eventloop, 4, 4);

		producer.streamTo(serializer.getInput());
		serializer.getOutput().streamTo(bufSplitter.getInput());
		bufSplitter.getOutput().streamTo(deserializer.getInput());
		deserializer.getOutput().streamTo(consumer);

		eventloop.run();

		assertEquals(inputData, consumer.getList());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void deserializesMultipleMessages_SplittedIntoSingleByte_ByteBufs_withMaxHeaderSizeInMessage() {
		List<Data> inputData =
				asList(new Data("1"), new Data("8282"), new Data("80982"), new Data("3634921"), new Data("7162"));
		StreamProducer<Data> producer = StreamProducers.ofIterable(eventloop, inputData);
		StreamConsumers.ToList<Data> consumer = StreamConsumers.toList(eventloop);
		StreamByteChunker bufSplitter = StreamByteChunker.create(eventloop, 1, 1);

		producer.streamTo(serializer.getInput());
		serializer.getOutput().streamTo(bufSplitter.getInput());
		bufSplitter.getOutput().streamTo(deserializer.getInput());
		deserializer.getOutput().streamTo(consumer);

		eventloop.run();

		assertEquals(inputData, consumer.getList());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	public static class Data {
		private final String info;

		public Data(@Deserialize("info") String info) {
			this.info = info;
		}

		@Serialize(order = 1)
		public String getInfo() {
			return info;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			Data data = (Data) o;

			return !(info != null ? !info.equals(data.info) : data.info != null);

		}

		@Override
		public int hashCode() {
			return info != null ? info.hashCode() : 0;
		}

		@Override
		public String toString() {
			return "Data{" +
					"info='" + info + '\'' +
					'}';
		}
	}
}