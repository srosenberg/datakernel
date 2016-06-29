package io.datakernel.stream.net;

import io.datakernel.async.ParseException;
import io.datakernel.bytebufnew.ByteBufN;
import io.datakernel.stream.net.MessagingSerializers.ByteBufPoolAppendable;
import io.datakernel.util.ByteBufStrings;
import org.junit.Test;

import static io.datakernel.bytebufnew.ByteBufNPool.*;
import static org.junit.Assert.assertEquals;

public class ByteBufPoolAppendableTest {
	private static final String HELLO_WORLD = "Hello, World!";

	@Test
	public void testAppendSimple() {
		ByteBufPoolAppendable appendable = new ByteBufPoolAppendable();
		appendable.append(HELLO_WORLD);
		ByteBufN buf = appendable.get();
		assertEquals(0, buf.readPosition());
		assertEquals(13, buf.writePosition());
		assertEquals(ByteBufStrings.decodeAscii(buf), HELLO_WORLD);
		buf.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testAppendWithResizing() throws ParseException {
		ByteBufPoolAppendable appendable = new ByteBufPoolAppendable(8);
		appendable.append(HELLO_WORLD);
		ByteBufN buf = appendable.get();
		assertEquals(0, buf.readPosition());
		assertEquals(13, buf.writePosition());
		assertEquals(ByteBufStrings.decodeAscii(buf), HELLO_WORLD);
		buf.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}