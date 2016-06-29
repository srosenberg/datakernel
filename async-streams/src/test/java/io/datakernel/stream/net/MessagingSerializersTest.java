package io.datakernel.stream.net;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import io.datakernel.async.ParseException;
import io.datakernel.bytebufnew.ByteBufN;
import io.datakernel.util.ByteBufStrings;
import org.junit.Test;

import static io.datakernel.bytebufnew.ByteBufNPool.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings("ALL")
public class MessagingSerializersTest {
	private class Req {
		String text;
		int num;
		double val;

		public Req(String text, int num, double val) {
			this.text = text;
			this.num = num;
			this.val = val;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Req req = (Req) o;
			return num == req.num &&
					Double.compare(req.val, val) == 0 &&
					Objects.equal(text, req.text);
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(text, num, val);
		}

		@Override
		public String toString() {
			return "Req{" +
					"text='" + text + '\'' +
					", num=" + num +
					", val=" + val +
					'}';
		}
	}

	private class Res {
		boolean bool;

		public Res(boolean bool) {
			this.bool = bool;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Res res = (Res) o;
			return bool == res.bool;
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(bool);
		}

		@Override
		public String toString() {
			return "Res{" +
					"bool=" + bool +
					'}';
		}
	}

	private MessagingSerializer<Req, Res> serializer = MessagingSerializers.ofGson(new Gson(), Req.class, new Gson(), Res.class);
	private MessagingSerializer<Res, Req> deserializer = MessagingSerializers.ofGson(new Gson(), Res.class, new Gson(), Req.class);

	@Test
	public void simpleTestOfGson() throws ParseException {
		Req req = new Req("Hello", 1, 6.24);

		ByteBufN buf = deserializer.serialize(req);
		assertEquals("{\"text\":\"Hello\",\"num\":1,\"val\":6.24}\0", ByteBufStrings.decodeUTF8(buf));

		Req newReq = serializer.tryDeserialize(buf);
		assertEquals(req, newReq);
		buf.recycle();

		Res res = new Res(true);

		buf = serializer.serialize(res);
		assertEquals("{\"bool\":true}\0", ByteBufStrings.decodeUTF8(buf));

		Res newRes = deserializer.tryDeserialize(buf);
		assertEquals(res, newRes);
		buf.recycle();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testSerializeSeveralMessages() throws ParseException {
		ByteBufN readBuf = ByteBufStrings.wrapUTF8("{\"text\":\"Greetings\",\"num\":1,\"val\":3.12}\0" +
				"{\"text\":\"Hi\",\"num\":2,\"val\":6.24}\0" +
				"{\"text\":\"Good morning\",\"num\":3,\"val\":9.36}\0" +
				"{\"text\":\"Shalom\",\"n");

		Req req1 = serializer.tryDeserialize(readBuf);
		Req req2 = serializer.tryDeserialize(readBuf);
		Req req3 = serializer.tryDeserialize(readBuf);
		Req req4 = serializer.tryDeserialize(readBuf);

		assertEquals(req1, new Req("Greetings", 1, 3.12));
		assertEquals(req2, new Req("Hi", 2, 6.24));
		assertEquals(req3, new Req("Good morning", 3, 9.36));
		assertNull(req4);

		assertEquals(116, readBuf.readPosition());
		assertEquals(135, readBuf.writePosition());

		readBuf.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}