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

	private static char[] charArray;

	public static char[] getCharArray(int length) {
		if (charArray == null || charArray.length < length) {
			charArray = new char[length + (length >>> 2)];
		}
		return charArray;
	}

	public static PairOffByte readByte(byte[] buf, int pos) {
		return new PairOffByte(pos + 1, buf[pos]);
	}

	private static byte readSimpleByte(byte[] buf, int pos) {
		return buf[pos];
	}

	public static PairOffByteArray read(byte[] buffer, int len) {
		return read(buffer, 0, len);
	}

	public static PairOffByteArray read(byte[] buffer, int pos, int len) {
		byte[] bytes = new byte[len];
		System.arraycopy(buffer, pos, bytes, 0, len);
		return new PairOffByteArray(pos + len, bytes);
	}

	public static PairOffBoolean readBoolean(final byte[] buf, final int pos) {
		return new PairOffBoolean(pos + 1, buf[pos] != 0);
	}

	public static PairOffChar readChar(byte[] buf, int pos) {
		int ch1 = readSimpleByte(buf, pos);
		int ch2 = readSimpleByte(buf, pos + 1);
		int code = (ch1 << 8) + (ch2 & 0xFF);
		return new PairOffChar(pos + 2, (char) code);
	}

	public static PairOffDouble readDouble(byte[] buf, int pos) {
		PairOffLong pairOffLong = readLong(buf, pos);
		return new PairOffDouble(pairOffLong.off, Double.longBitsToDouble(pairOffLong.instance));
	}

	public static PairOffFloat readFloat(byte[] buf, int pos) {
		PairOffInt pairOffInt = readInt(buf, pos);
		return new PairOffFloat(pairOffInt.off, Float.intBitsToFloat(pairOffInt.instance));
	}

	public static PairOffInt readInt(byte[] buf, int pos) {
		int result = ((buf[pos] & 0xFF) << 24)
				| ((buf[pos + 1] & 0xFF) << 16)
				| ((buf[pos + 2] & 0xFF) << 8)
				| (buf[pos + 3] & 0xFF);
		return new PairOffInt(pos + 4, result);
	}

	// TODO (vsavchuk) improve without Ref
	public static PairOffInt readVarInt(byte[] buf, int pos) {
		int result;
		byte b = buf[pos];
		if (b >= 0) {
			return new PairOffInt(pos + 1, b);
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
		return new PairOffInt(pos, result);
	}

	public static PairOffLong readLong(byte[] buf, int pos) {
		long result = ((long) buf[pos] << 56)
				| ((long) (buf[pos + 1] & 0xFF) << 48)
				| ((long) (buf[pos + 2] & 0xFF) << 40)
				| ((long) (buf[pos + 3] & 0xFF) << 32)
				| ((long) (buf[pos + 4] & 0xFF) << 24)
				| ((buf[pos + 5] & 0xFF) << 16)
				| ((buf[pos + 6] & 0xFF) << 8)
				| ((buf[pos + 7] & 0xFF));

		return new PairOffLong(pos + 8, result);

	}

	public static PairOffShort readShort(byte[] buf, int pos) {
		short result = (short) (((buf[pos] & 0xFF) << 8)
				| ((buf[pos + 1] & 0xFF)));

		return new PairOffShort(pos + 2, result);
	}

	public static PairOffString readIso88591(byte[] buf, int pos) {
		PairOffInt pairOffInt = readVarInt(buf, pos);
		return doReadIso88591(buf, pairOffInt.off, pairOffInt.instance);
	}

	public static PairOffString readNullableIso88591(byte[] buf, int pos) {
		PairOffInt pairOffInt = readVarInt(buf, pos);
		if (pairOffInt.instance == 0) {
			return new PairOffString(pairOffInt.off, null);
		}
		return doReadIso88591(buf, pairOffInt.off, pairOffInt.instance - 1);
	}

	public static PairOffString doReadIso88591(byte[] buf, int pos, int length) {
		if (length == 0) {
			return new PairOffString(pos, "");
		}

		if (length > remaining(buf, pos))
			throw new IllegalArgumentException();

//      TODO (vsavchuk) check isRemoved in java 9?
//      public static String(byte[] ascii, int hibyte, int offset, int count) {

		char[] chars = getCharArray(length);
		for (int i = 0; i < length; i++) {
			int c = readSimpleByte(buf, pos + i) & 0xff;
			chars[i] = (char) c;
		}

		// TODO (vsavchuk) replace by String(byte[] ascii, int hibyte, int offset, int count) with hibyte == 0
		return new PairOffString(pos + length, new String(chars, 0, length));
	}

	private static int remaining(byte[] bytes, int pos) {
		return bytes.length - pos;
	}

	public static PairOffString readUTF8(byte[] buf, int pos) {
		// TODO (vsavchuk) mb use ref from arg???
		PairOffInt pairOffInt = readVarInt(buf, pos);
		return doReadUTF8(buf, pairOffInt.off, pairOffInt.instance);
	}

	public static PairOffString readNullableUTF8(byte[] buf, int pos) {
		PairOffInt pairOffInt = readVarInt(buf, pos);
		if (pairOffInt.instance == 0) {
			return new PairOffString(pos, null);
		}
		return doReadUTF8(buf, pairOffInt.off, pairOffInt.instance - 1);
	}

	private static PairOffString doReadUTF8(byte[] buf, int pos, int length) {
		if (length == 0) {
			// TODO (vsavchuk) check all ifs
			return new PairOffString(pos, "");
		}
		if (length > remaining(buf, pos))
			throw new IllegalArgumentException();
		char[] chars = getCharArray(length);
		for (int i = 0; i < length; i++) {
			int c = readSimpleByte(buf, pos) & 0xff;
			pos += 1;
			if (c < 0x80) {
				chars[i] = (char) c;
			} else if (c < 0xE0) {
				chars[i] = (char) ((c & 0x1F) << 6 | readSimpleByte(buf, pos + 1) & 0x3F);
				pos += 2;
			} else {
				chars[i] = (char) ((c & 0x0F) << 12 | (readSimpleByte(buf, pos + 1) & 0x3F) << 6 | (readSimpleByte(buf, pos + 2) & 0x3F));
				pos += 3;
			}
		}
		return new PairOffString(pos, new String(chars, 0, length));
	}

	public static PairOffString readUTF16(byte[] buf, int pos) {
		PairOffInt pairOffInt = readVarInt(buf, pos);
		return doReadUTF16(buf, pairOffInt.off, pairOffInt.instance);
	}

	public static PairOffString readNullableUTF16(byte[] buf, int pos) {
		PairOffInt pairOffInt = readVarInt(buf, pos);
		if (pairOffInt.instance == 0) {
			return new PairOffString(pos, null);
		}
		return doReadUTF16(buf, pairOffInt.off, pairOffInt.instance - 1);
	}

	private static PairOffString doReadUTF16(byte[] buf, int pos, int length) {
		if (length == 0) {
			return new PairOffString(pos, null);
		}
		if (length * 2 > remaining(buf, pos))
			throw new IllegalArgumentException();

		char[] chars = getCharArray(length);
		for (int i = 0; i < length; i++) {
			chars[i] = (char) ((readSimpleByte(buf, pos) << 8) + (readSimpleByte(buf, pos + 1)));
			pos += 2;
		}
		return new PairOffString(pos, new String(chars, 0, length));
	}

	public static int readVarLong(byte[] buf, int pos, Ref ref) {
		long result = 0;
		for (int offset = 0; offset < 64; offset += 7) {
			byte b = readSimpleByte(buf, pos);
			pos += 1;
			result |= (long) (b & 0x7F) << offset;
			if ((b & 0x80) == 0) {
				ref.set(result);
				return pos;
			}
		}
		throw new IllegalArgumentException();
	}

	public static PairOffString readJavaUTF8(byte[] buf, int pos) {
		PairOffInt pairOffInt = readVarInt(buf, pos);
		return doReadJavaUTF8(buf, pairOffInt.off, pairOffInt.instance);
	}

	public static PairOffString readNullableJavaUTF8(byte[] buf, int pos) {
		PairOffInt pairOffInt = readVarInt(buf, pos);
		if (pairOffInt.instance == 0) {
			return new PairOffString(pos, null);
		}
		return doReadJavaUTF8(buf, pairOffInt.off, pairOffInt.instance - 1);
	}

	public static PairOffString doReadJavaUTF8(byte[] buf, int pos, int length) {
		if (length == 0) {
			return new PairOffString(pos, "");
		}
		if (length > remaining(buf, pos))
			throw new IllegalArgumentException();
		pos += length;

		try {
			return new PairOffString(pos, new String(buf, pos - length, length, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException();
		}
	}
}

