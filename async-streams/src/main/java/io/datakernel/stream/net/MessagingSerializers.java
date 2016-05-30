package io.datakernel.stream.net;

import com.google.gson.Gson;
import io.datakernel.async.ParseException;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.util.ByteBufStrings;

public class MessagingSerializers {

	private MessagingSerializers() {
	}

	public static <I, O> MessagingSerializer ofGson(final Gson in, final Class<I> inputClass,
	                                                final Gson out, final Class<O> outputClass) {
		return new MessagingSerializer<I, O>() {
			@Override
			public I tryDeserialize(ByteBuf buf) throws ParseException {
				int delim = -1;
				for (int i = buf.position(); i < buf.limit(); i++) {
					if (buf.array()[i] == '\0') {
						delim = i;
						break;
					}
				}
				if (delim != -1) {
					int len = delim - buf.position();
					I item = in.fromJson(ByteBufStrings.decodeUTF8(buf.array(), buf.position(), len), inputClass);
					buf.advance(len + 1); // skipping msg + delimiter
					return item;
				}
				return null;
			}

			@Override
			public ByteBuf serialize(O item) {
				ByteBufPoolAppendable appendable = new ByteBufPoolAppendable();
				out.toJson(item, outputClass, appendable);
				appendable.append("\0");
				ByteBuf result = appendable.get();
				result.flip();
				return result;
			}
		};
	}

	static class ByteBufPoolAppendable implements Appendable {
		static final int INITIAL_BUF_SIZE = 2 * 1024;
		ByteBuf container;

		ByteBufPoolAppendable() {this(INITIAL_BUF_SIZE);}

		ByteBufPoolAppendable(int size) {
			this.container = ByteBufPool.allocate(size);
		}

		@Override
		public Appendable append(CharSequence csq) {
			while (container.remaining() < csq.length() * 3) {
				container = ByteBufPool.resize(container, container.capacity() * 2);
			}
			int pos = container.position();
			for (int i = 0; i < csq.length(); i++) {
				pos = writeUtfChar(container.array(), pos, csq.charAt(i));
			}
			container.position(pos);
			return this;
		}

		@Override
		public Appendable append(CharSequence csq, int start, int end) {
			return append(csq.subSequence(start, end));
		}

		@Override
		public Appendable append(char c) {
			if (container.remaining() < 3) {
				container = ByteBufPool.resize(container, container.capacity() * 2);
			}
			int pos = writeUtfChar(container.array(), container.position(), c);
			container.position(pos);
			return this;
		}

		private int writeUtfChar(byte[] buf, int pos, char c) {
			if (c <= 0x007F) {
				buf[pos] = (byte) c;
				return pos + 1;
			} else if (c <= 0x07FF) {
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

		public ByteBuf get() {
			return container;
		}
	}
}