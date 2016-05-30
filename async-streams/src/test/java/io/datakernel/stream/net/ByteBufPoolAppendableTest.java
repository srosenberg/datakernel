package io.datakernel.stream.net;

import io.datakernel.async.ParseException;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.stream.net.MessagingSerializers.ByteBufPoolAppendable;
import io.datakernel.util.ByteBufStrings;
import org.junit.Test;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static org.junit.Assert.assertEquals;

public class ByteBufPoolAppendableTest {
	private static final String HELLO_WORLD = "Hello, World!";

	@Test
	public void testAppendSimple() {
		ByteBufPoolAppendable appendable = new ByteBufPoolAppendable();
		appendable.append(HELLO_WORLD);
		ByteBuf buf = appendable.get();
		buf.flip();
		assertEquals(0, buf.position());
		assertEquals(13, buf.limit());
		assertEquals(ByteBufStrings.decodeAscii(buf), HELLO_WORLD);
		buf.recycle();
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testAppendWithResizing() throws ParseException {
		ByteBufPoolAppendable appendable = new ByteBufPoolAppendable(8);
		appendable.append(HELLO_WORLD);
		ByteBuf buf = appendable.get();
		buf.flip();
		assertEquals(0, buf.position());
		assertEquals(13, buf.limit());
		assertEquals(ByteBufStrings.decodeAscii(buf), HELLO_WORLD);
		buf.recycle();
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}
}