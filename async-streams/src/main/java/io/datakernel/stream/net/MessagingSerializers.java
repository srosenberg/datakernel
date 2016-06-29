package io.datakernel.stream.net;

import com.google.gson.Gson;
import io.datakernel.async.ParseException;
import io.datakernel.bytebufnew.ByteBufN;
import io.datakernel.bytebufnew.ByteBufNPool;
import io.datakernel.util.ByteBufStrings;

public class MessagingSerializers {

	private MessagingSerializers() {
	}

	public static <I, O> MessagingSerializer<I, O> ofGson(final Gson in, final Class<I> inputClass,
	                                                      final Gson out, final Class<O> outputClass) {
		return new MessagingSerializer<I, O>() {
			@Override
			public I tryDeserialize(ByteBufN buf) throws ParseException {
				int delim = -1;
				for (int i = buf.readPosition(); i < buf.writePosition(); i++) {
					if (buf.array()[i] == '\0') {
						delim = i;
						break;
					}
				}
				if (delim != -1) {
					int len = delim - buf.readPosition();
					I item = in.fromJson(ByteBufStrings.decodeUTF8(buf.array(), buf.readPosition(), len), inputClass);
					buf.readPosition(delim + 1); // skipping msg + delimiter
					return item;
				}
				return null;
			}

			@Override
			public ByteBufN serialize(O item) {
				ByteBufPoolAppendable appendable = new ByteBufPoolAppendable();
				out.toJson(item, outputClass, appendable);
				appendable.append("\0");
				return appendable.get();
			}
		};
	}

	static class ByteBufPoolAppendable implements Appendable {
		static final int INITIAL_BUF_SIZE = 2 * 1024;
		ByteBufN container;

		ByteBufPoolAppendable() {this(INITIAL_BUF_SIZE);}

		ByteBufPoolAppendable(int size) {
			this.container = ByteBufNPool.allocateAtLeast(size);
		}

		@Override
		public Appendable append(CharSequence csq) {
			while (container.remainingToWrite() < csq.length() * 3) {
				container = ByteBufNPool.reallocateAtLeast(container, container.limit * 2);
			}
			int pos = container.writePosition();
			for (int i = 0; i < csq.length(); i++) {
				pos = writeUtfChar(container.array(), pos, csq.charAt(i));
			}
			container.writePosition(pos);
			return this;
		}

		@Override
		public Appendable append(CharSequence csq, int start, int end) {
			return append(csq.subSequence(start, end));
		}

		@Override
		public Appendable append(char c) {
			if (container.remainingToWrite() < 3) {
				container = ByteBufNPool.reallocateAtLeast(container, container.limit * 2);
			}
			int pos = writeUtfChar(container.array(), container.writePosition(), c);
			container.writePosition(pos);
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

		public ByteBufN get() {
			return container;
		}
	}
}