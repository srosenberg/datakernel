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

package io.datakernel.serializer;

import java.io.UnsupportedEncodingException;

public class SerializationInputHelper {
//	TODO (vsavchuk) Ref в собі тримає і обєкт і логіку ensureCharArray, і просто Ref не Ref<T>, Ref створюється на топ левелі один раз

	public static int readByte(byte[] buf, int pos, Ref ref) {
		ref.set(buf[pos]);
		return pos + 1;
	}

	private static byte readByte(byte[] buf, int pos) {
		return buf[pos];
	}

	public static int read(byte[] buffer, Ref ref) {
		return read(buffer, 0, buffer.length, ref);
	}

	public static int read(byte[] buffer, int pos, int len, Ref ref) {
		byte[] bytes = new byte[len];
		System.arraycopy(buffer, pos, bytes, 0, len);
		ref.set(bytes);
		return pos + len;
	}

	public static int readBoolean(byte[] buf, int pos, Ref ref) {
		ref.set(buf[pos] != 0);
		return pos + 1;
	}

	public static int readChar(byte[] buf, int pos, Ref ref) {
		int ch1 = readByte(buf, pos);
		int ch2 = readByte(buf, pos + 1);
		int code = (ch1 << 8) + (ch2 & 0xFF);
		ref.set((char) code);
		return pos + 2;
	}

	public static int readDouble(byte[] buf, int pos, Ref ref) {
		pos = readLong(buf, pos, ref);
		ref.set(Double.longBitsToDouble(((Long) ref.get())));
		return pos;
	}

	public static int readFloat(byte[] buf, int pos, Ref ref) {
		pos = readInt(buf, pos, ref);
		ref.set(Float.intBitsToFloat(((Integer) ref.get())));
		return pos;
	}

	public static int readInt(byte[] buf, int pos, Ref ref) {
		int result = ((buf[pos] & 0xFF) << 24)
				| ((buf[pos + 1] & 0xFF) << 16)
				| ((buf[pos + 2] & 0xFF) << 8)
				| (buf[pos + 3] & 0xFF);
		ref.set(result);
		return pos + 4;
	}

	// TODO (vsavchuk) improve without Ref
	public static int readVarInt(byte[] buf, int pos, Ref ref) {
		int result;
		byte b = buf[pos];
		if (b >= 0) {
			ref.set(((int) b));
			return pos + 1;
		} else {
			result = b & 0x7f;
			if ((b = buf[pos + 1]) >= 0) {
				result |= b << 7;
				pos += 2;
			} else {
				result |= (b & 0x7f) << 7;
				if ((b = buf[pos + 2]) >= 0) {
					result |= b << 14;
					pos += 3;
				} else {
					result |= (b & 0x7f) << 14;
					if ((b = buf[pos + 3]) >= 0) {
						result |= b << 21;
						pos += 4;
					} else {
						result |= (b & 0x7f) << 21;
						if ((b = buf[pos + 4]) >= 0) {
							result |= b << 28;
							pos += 5;
						} else
							throw new IllegalArgumentException();
					}
				}
			}
		}
		ref.set(result);
		return pos;
	}

	public static int readLong(byte[] buf, int pos, Ref ref) {
		long result = ((long) buf[pos] << 56)
				| ((long) (buf[pos + 1] & 0xFF) << 48)
				| ((long) (buf[pos + 2] & 0xFF) << 40)
				| ((long) (buf[pos + 3] & 0xFF) << 32)
				| ((long) (buf[pos + 4] & 0xFF) << 24)
				| ((buf[pos + 5] & 0xFF) << 16)
				| ((buf[pos + 6] & 0xFF) << 8)
				| ((buf[pos + 7] & 0xFF));

		ref.set(result);
		return pos + 8;

	}

	public static int readShort(byte[] buf, int pos, Ref ref) {
		short result = (short) (((buf[pos] & 0xFF) << 8)
				| ((buf[pos + 1] & 0xFF)));

		ref.set(result);
		return pos + 2;
	}

	public static int readIso88591(byte[] buf, int pos, Ref ref) {
		Ref length = new Ref();
		int newPos = readVarInt(buf, pos, length);
		return doReadIso88591(buf, newPos, (Integer) length.get(), ref);
	}

	public static int readNullableIso88591(byte[] buf, int pos, Ref ref) {
		Ref length = new Ref();
		int newPos = readVarInt(buf, pos, length);
		if (length.get() == 0) {
			ref.set(null);
			return newPos;
		}
		return doReadIso88591(buf, newPos, ((Integer) length.get() - 1), ref);
	}

	public static int doReadIso88591(byte[] buf, int pos, int length, Ref ref) {
		if (length == 0) {
			ref.set("");
			return pos;
		}

		if (length > remaining(buf, pos))
			throw new IllegalArgumentException();

//      TODO (vsavchuk) check isRemoved in java 9?
//      public static String(byte[] ascii, int hibyte, int offset, int count) {

		char[] chars = ref.getCharArray(length);
		for (int i = 0; i < length; i++) {
			int c = readByte(buf, pos + i) & 0xff;
			chars[i] = (char) c;
		}

		// TODO (vsavchuk) replace by String(byte[] ascii, int hibyte, int offset, int count) with hibyte == 0
		ref.set(new String(chars, 0, length));

		return pos + length;
	}

	private static int remaining(byte[] bytes, int pos) {
		return bytes.length - pos;
	}

	public static int readUTF8(byte[] buf, int pos, Ref ref) {
		// TODO (vsavchuk) mb use ref from arg???
		Ref length = new Ref();
		pos = readVarInt(buf, pos, length);
		return doReadUTF8(buf, pos, (Integer) length.get(), ref);
	}

	public static int readNullableUTF8(byte[] buf, int pos, Ref ref) {
		Ref length = new Ref();
		pos = readVarInt(buf, pos, length);
		if (length.get() == 0) {
			ref.set(null);
			return pos;
		}
		return doReadUTF8(buf, pos, (Integer) length.get() - 1, ref);
	}

	private static int doReadUTF8(byte[] buf, int pos, int length, Ref ref) {
		if (length == 0) {
			ref.set("");
			return pos;
		}
		if (length > remaining(buf, pos))
			throw new IllegalArgumentException();
		char[] chars = ref.getCharArray(length);
		for (int i = 0; i < length; i++) {
			int c = readByte(buf, pos) & 0xff;
			pos += 1;
			if (c < 0x80) {
				chars[i] = (char) c;
			} else if (c < 0xE0) {
				chars[i] = (char) ((c & 0x1F) << 6 | readByte(buf, pos + 1) & 0x3F);
				pos += 2;
			} else {
				chars[i] = (char) ((c & 0x0F) << 12 | (readByte(buf, pos + 1) & 0x3F) << 6 | (readByte(buf, pos + 2) & 0x3F));
				pos += 3;
			}
		}
		ref.set(new String(chars, 0, length));
		return pos;
	}

	public static int readUTF16(byte[] buf, int pos, Ref ref) {
		Ref length = new Ref();
		pos = readVarInt(buf, pos, length);
		return doReadUTF16(buf, pos, ((Integer) length.get()), ref);
	}

	public static int readNullableUTF16(byte[] buf, int pos, Ref ref) {
		Ref length = new Ref();
		pos = readVarInt(buf, pos, length);
		if (length.get() == 0) {
			ref.set(null);
			return pos;
		}
		return doReadUTF16(buf, pos, ((Integer) length.get() - 1), ref);
	}

	private static int doReadUTF16(byte[] buf, int pos, int length, Ref ref) {
		if (length == 0) {
			ref.set(null);
			return pos;
		}
		if (length * 2 > remaining(buf, pos))
			throw new IllegalArgumentException();

		char[] chars = ref.getCharArray(length);
		for (int i = 0; i < length; i++) {
			chars[i] = (char) ((readByte(buf, pos) << 8) + (readByte(buf, pos + 1)));
			pos += 2;
		}
		ref.set(new String(chars, 0, length));
		return pos;
	}

	public static int readVarLong(byte[] buf, int pos, Ref ref) {
		long result = 0;
		for (int offset = 0; offset < 64; offset += 7) {
			byte b = readByte(buf, pos);
			pos += 1;
			result |= (long) (b & 0x7F) << offset;
			if ((b & 0x80) == 0) {
				ref.set(result);
				return pos;
			}
		}
		throw new IllegalArgumentException();
	}

	public static int readJavaUTF8(byte[] buf, int pos, Ref ref) {
		Ref length = new Ref();
		pos = readVarInt(buf, pos, length);
		return doReadJavaUTF8(buf, pos, (Integer) length.get(), ref);
	}

	public static int readNullableJavaUTF8(byte[] buf, int pos, Ref ref) {
		Ref length = new Ref();
		pos = readVarInt(buf, pos, length);
		if (length.get() == 0) {
			ref.set(null);
			return pos;
		}
		return doReadJavaUTF8(buf, pos, ((Integer) length.get() - 1), ref);
	}

	public static int doReadJavaUTF8(byte[] buf, int pos, int length, Ref ref) {
		if (length == 0) {
			ref.set("");
			return pos;
		}
		if (length > remaining(buf, pos))
			throw new IllegalArgumentException();
		pos += length;

		try {
			ref.set(new String(buf, pos - length, length, "UTF-8"));
			return pos;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException();
		}
	}
}

