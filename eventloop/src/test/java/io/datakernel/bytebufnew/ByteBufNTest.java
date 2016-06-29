package io.datakernel.bytebufnew;

import io.datakernel.util.ByteBufStrings;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.datakernel.bytebufnew.ByteBufNPool.*;
import static org.junit.Assert.*;

public class ByteBufNTest {
	private static final byte[] BYTES = new byte[]{'T', 'e', 's', 't', ' ', 'm', 'e', 's', 's', 'a', 'g', 'e'};

	@Before
	public void setUp() {
		ByteBufNPool.clear();
		ByteBufNPool.setSizes(32, 1 << 30);
	}

	@Test
	public void testSlice() {
		byte[] bytes = ByteBufStrings.encodeAscii("Hello, World");
		ByteBufN buf = ByteBufN.wrap(bytes);

		ByteBufN slice = buf.slice(7, 12);

		assertFalse(buf == slice);
		assertEquals("World", slice.toString());

		buf = ByteBufN.create(16);
		buf.put(bytes);

		slice = buf.slice();

		assertFalse(buf == slice);
		assertEquals("Hello, World", slice.toString());
	}

	@Test
	public void testEditing() {
		ByteBufN buf = ByteBufN.create(256);
		assertEquals(0, buf.readPosition());

		buf.put((byte) 'H');
		assertEquals(1, buf.writePosition());
		assertEquals('H', buf.at(0));

		buf.put(new byte[]{'e', 'l', 'l', 'o'});
		buf.put(new byte[]{';', ' ', ',', ' ', '.', ' ', '!', ' '}, 2, 4);
		assertEquals(7, buf.writePosition());

		ByteBufN worldBuf = ByteBufN.wrap(new byte[]{'W', 'o', 'r', 'l', 'd', '!'});
		buf.put(worldBuf);

		assertEquals(worldBuf.limit, worldBuf.readPosition());
		assertFalse(worldBuf.canWrite());
		assertEquals(13, buf.writePosition());

		ByteBufN slice = buf.slice();
		ByteBufN newBuf = ByteBufN.create(slice.limit);
		slice.drainTo(newBuf, 10);
		assertEquals(10, slice.readPosition());
		assertEquals(10, newBuf.writePosition());

		slice.drainTo(newBuf, 3);

		assertEquals("Hello, World!", newBuf.toString());
	}

	@Test
	public void testByteBuffer() {
		ByteBufN buf = ByteBufN.create(32);
		buf.put(BYTES);

		ByteBuffer buffer = buf.toByteBuffer();
		assertEquals(12, buffer.position());
		assertEquals(32, buffer.capacity());
		buffer.put("! Next test message!".getBytes());
		buffer.flip();
		buf.setByteBuffer(buffer);
		assertEquals(32, buf.writePosition());

		assertEquals("Test message! Next test message!", buf.slice().toString());
	}

	@Test
	public void testPoolAndRecycleMechanism() {
		int size = 500;
		ByteBufN buf = ByteBufNPool.allocateAtLeast(size);
		assertNotEquals(size, buf.limit); // {expected to create 2^N sized bufs only, 500 not in {a}|a == 2^N } => size != limit
		assertEquals(512, buf.limit);

		buf.put(BYTES);

		buf.recycle();

		try {
			buf.put((byte) 'a');
		} catch (AssertionError e) {
			assertEquals(AssertionError.class, e.getClass());
		}

		buf = ByteBufNPool.allocateAtLeast(300);
		buf.writePosition(BYTES.length);
		byte[] bytes = new byte[BYTES.length];
		buf.drainTo(bytes, 0, bytes.length);
		assertArrayEquals(bytes, BYTES);

		buf.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testSliceAndRecycleMechanism() {
		ByteBufN buf = ByteBufNPool.allocateAtLeast(5);
		ByteBufN slice0 = buf.slice(1, 4);
		ByteBufN slice01 = slice0.slice(2, 3);
		ByteBufN slice1 = buf.slice(4, 5);

		assertTrue(buf.canWrite());
		slice1.recycle();
		assertTrue(buf.canWrite());
		slice0.recycle();
		assertTrue(buf.canWrite());
		slice01.recycle();
	}

	@Test
	public void testViews() {
		// emulate engine that receives randomly sized bufs from `net` and sends them to some `consumer`

		class MockConsumer {
			private int i = 0;

			private void consume(ByteBufN buf) {
				assertEquals("Test message " + i++, buf.toString());
				buf.recycle();
			}
		}

		MockConsumer consumer = new MockConsumer();
		for (int i = 0; i < 100; i++) {
			ByteBufN buf = ByteBufNPool.allocateAtLeast(32);
			ByteBuffer buffer = buf.toByteBuffer();
			buffer.put(("Test message " + i).getBytes());
			buffer.flip();
			buf.setByteBuffer(buffer);
			consumer.consume(buf.slice());
			buf.recycle();
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testConcat() {
		ByteBufN buf = ByteBufNPool.allocateAtLeast(64);
		buf.put(BYTES);

		ByteBufN secondBuf = ByteBufNPool.allocateAtLeast(32);
		secondBuf.put(BYTES);

		buf = ByteBufNPool.concat(buf, secondBuf.slice());
		buf = ByteBufNPool.concat(buf, secondBuf.slice());
		buf = ByteBufNPool.concat(buf, secondBuf.slice());
		buf = ByteBufNPool.concat(buf, secondBuf.slice());
		buf = ByteBufNPool.concat(buf, secondBuf.slice());

		assertEquals(new String(BYTES)
				+ new String(BYTES)
				+ new String(BYTES)
				+ new String(BYTES)
				+ new String(BYTES)
				+ new String(BYTES), buf.toString());

		buf.recycle();
		secondBuf.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}