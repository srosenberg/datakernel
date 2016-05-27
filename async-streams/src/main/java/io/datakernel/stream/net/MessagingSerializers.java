package io.datakernel.stream.net;

import com.google.gson.Gson;
import io.datakernel.async.ParseException;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.util.ByteBufStrings;

import java.io.IOException;

public class MessagingSerializers {
	private MessagingSerializers() {
	}

	public static <I, O> MessagingSerializer ofGson(final Gson in, final Gson out) {
		return new MessagingSerializer<I, O>() {

			Class<I> iClass;
			Class<O> oClass;

			@Override
			public I tryDeserialize(ByteBuf buf) throws ParseException {
				return in.fromJson(ByteBufStrings.decodeUTF8(buf), iClass);
			}

			@Override
			public ByteBuf serialize(O item) {
				// специальный аппендебл, который будет работать с пулом байтбуфов, и ресайзится соответственно

				class ByteBufPoolAppendable implements Appendable {
					int size = 256;
					ByteBuf container = ByteBufPool.allocate(size);

					@Override
					public Appendable append(CharSequence csq) throws IOException {
						if (container.remaining() < csq.length() * 3) {
							container = ByteBufPool.resize(container, container.capacity() * 2);
						}
						for (int i = 0; i < csq.length(); i++) {
							writeUtfChar(container.array(), container.position(), csq.charAt(i));
						}
						return this;
					}

					private int writeUtfChar(byte[] buf, int pos, char c) {
						if (c <= 0x07FF) {
							buf[pos] = (byte) (0xC0 | c >> 6 & 0x1F);
							buf[pos + 1] = (byte) (0x80 | c & 0x3F);
							return pos + 2;
						} else {
							buf[pos] = (byte) (0xE0 | c >> 12 & 0x0F);
							buf[pos + 1] = (byte) (0x80 | c >> 6 & 0x3F);
							buf[pos + 2] = (byte) (0x80 | c & 0x3F);
							return pos + 3;
						}
					}

					@Override
					public Appendable append(CharSequence csq, int start, int end) throws IOException {
						return append(csq.subSequence(start, end));
					}

					@Override
					public Appendable append(char c) throws IOException {
						if (container.remaining() < 3) {
							container = ByteBufPool.resize(container, container.capacity() + 3);
						}

						writeUtfChar(container.array(), container.position(), c);

						return this;
					}
				}

				ByteBufPoolAppendable appendable = new ByteBufPoolAppendable();
				out.toJson(item, oClass, appendable);
				appendable.container.flip();
				return appendable.container;
			}
		};
	}
}