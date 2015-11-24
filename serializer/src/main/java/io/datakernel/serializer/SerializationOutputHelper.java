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

public class SerializationOutputHelper {
	private SerializationOutputHelper() {
	}

	protected static void ensureSize(int size) {
	}

	public static int write(byte[] bufferTo, int pos, byte[] bufferFrom) {
		return write(bufferFrom, 0, bufferTo, pos, bufferFrom.length);
	}

	public static int write(byte[] bufferFrom, int fromOff, byte[] bufferTo, int toOff, int len) {
		ensureSize(len);
		System.arraycopy(bufferFrom, fromOff, bufferTo, toOff, len);
		return toOff + len;
	}

	public static int writeBoolean(byte[] buf, int pos, boolean v) {
		return writeByte(buf, pos, v ? (byte) 1 : 0);
	}

	public static int writeByte(byte[] buf, int pos, byte v) {
		ensureSize(1);
		buf[pos] = v;
		return pos + 1;
	}

	public static int writeChar(byte[] buf, int pos, char v) {
		ensureSize(2);
		writeByte(buf, pos, (byte) (v >>> 8));
		writeByte(buf, pos + 1, (byte) (v));
		return pos + 2;
	}

	public static int writeDouble(byte[] buf, int pos, double v) {
		return writeLong(buf, pos, Double.doubleToLongBits(v));
	}

	public static int writeFloat(byte[] buf, int pos, float v) {
		return writeInt(buf, pos, Float.floatToIntBits(v));
	}

	public static int writeInt(byte[] buf, int pos, int v) {
		ensureSize(4);
		buf[pos] = (byte) (v >>> 24);
		buf[pos + 1] = (byte) (v >>> 16);
		buf[pos + 2] = (byte) (v >>> 8);
		buf[pos + 3] = (byte) (v);
		return pos + 4;
	}

	public static int writeLong(byte[] buf, int pos, long v) {
		ensureSize(8);
		int high = (int) (v >>> 32);
		int low = (int) v;
		buf[pos] = (byte) (high >>> 24);
		buf[pos + 1] = (byte) (high >>> 16);
		buf[pos + 2] = (byte) (high >>> 8);
		buf[pos + 3] = (byte) high;
		buf[pos + 4] = (byte) (low >>> 24);
		buf[pos + 5] = (byte) (low >>> 16);
		buf[pos + 6] = (byte) (low >>> 8);
		buf[pos + 7] = (byte) low;
		return pos + 8;
	}

	public static int writeShort(byte[] buf, int pos, short v) {
		ensureSize(2);
		buf[pos] = (byte) (v >>> 8);
		buf[pos + 1] = (byte) (v);
		return pos + 2;
	}

	public static int writeVarInt(byte[] buf, int pos, int v) {
		ensureSize(5);
		if ((v & ~0x7F) == 0) {
			buf[pos] = (byte) v;
			return pos + 1;
		}
		buf[pos] = (byte) ((v & 0x7F) | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			buf[pos + 1] = (byte) v;
			return pos + 2;
		}
		buf[pos + 1] = (byte) ((v & 0x7F) | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			buf[pos + 2] = (byte) v;
			return pos + 3;
		}
		buf[pos + 2] = (byte) ((v & 0x7F) | 0x80);
		v >>>= 7;
		if ((v & ~0x7F) == 0) {
			buf[pos + 3] = (byte) v;
			return pos + 4;
		}
		buf[pos + 3] = (byte) ((v & 0x7F) | 0x80);
		v >>>= 7;
		buf[pos + 4] = (byte) v;
		return pos + 5;
	}

	public static int writeVarLong(byte[] buf, int pos, long v) {
		ensureSize(9);
		if ((v & ~0x7F) == 0) {
			return writeByte(buf, pos, (byte) v);
		} else {
			pos = writeByte(buf, pos, (byte) ((v & 0x7F) | 0x80));
			v >>>= 7;
			if ((v & ~0x7F) == 0) {
				return writeByte(buf, pos, (byte) v);
			} else {
				pos = writeByte(buf, pos, (byte) ((v & 0x7F) | 0x80));
				v >>>= 7;
				if ((v & ~0x7F) == 0) {
					return writeByte(buf, pos, (byte) v);
				} else {
					pos = writeByte(buf, pos, (byte) ((v & 0x7F) | 0x80));
					v >>>= 7;
					if ((v & ~0x7F) == 0) {
						return writeByte(buf, pos, (byte) v);
					} else {
						pos = writeByte(buf, pos, (byte) ((v & 0x7F) | 0x80));
						v >>>= 7;
						if ((v & ~0x7F) == 0) {
							return writeByte(buf, pos, (byte) v);
						} else {
							pos = writeByte(buf, pos, (byte) ((v & 0x7F) | 0x80));
							v >>>= 7;
							if ((v & ~0x7F) == 0) {
								return writeByte(buf, pos, (byte) v);
							} else {
								pos = writeByte(buf, pos, (byte) ((v & 0x7F) | 0x80));
								v >>>= 7;

								for (; ; ) {
									if ((v & ~0x7FL) == 0) {
										pos = writeByte(buf, pos, (byte) v);
										return pos;
									} else {
										pos = writeByte(buf, pos, (byte) (((int) v & 0x7F) | 0x80));
										v >>>= 7;
									}
								}
							}
						}
					}
				}
			}
		}
	}

	public static int writeIso88591(byte[] buf, int pos, String s) {
		int length = s.length();
		pos = writeVarInt(buf, pos, length);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			buf[pos++] = (byte) c;
		}
		return pos;
	}

	public static int writeNullableIso88591(byte[] buf, int pos, String s) {
		if (s == null) {
			return writeByte(buf, pos, (byte) 0);
		}
		int length = s.length();
		pos = writeVarInt(buf, pos, length + 1);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			buf[pos++] = (byte) c;
		}
		return pos;
	}

	public static int writeJavaUTF8(byte[] buf, int pos, String s) {
		try {
			pos = writeWithLength(buf, pos, s.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException();
		}
		return pos;
	}

	public static int writeNullableJavaUTF8(byte[] buf, int pos, String s) {
		if (s == null) {
			return writeByte(buf, pos, (byte) 0);
		}
		try {
			pos = writeWithLengthNullable(buf, pos, s.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException();
		}
		return pos;
	}

	private static int writeWithLengthNullable(byte[] buf, int pos, byte[] buffer) {
		pos = writeVarInt(buf, pos, buffer.length + 1);
		return write(buf, pos, buffer);
	}

	public static int writeUTF8(byte[] buf, int pos, String s) {
		int length = s.length();
		pos = writeVarInt(buf, pos, length);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			if (c <= 0x007F) {
				buf[pos++] = (byte) c;
			} else {
				pos = writeUtfChar(buf, pos, c);
			}
		}
		return pos;
	}

	public static int writeNullableUTF8(byte[] buf, int pos, String s) {
		if (s == null) {
			return writeByte(buf, pos, (byte) 0);
		}
		int length = s.length();
		writeVarInt(buf, pos, length + 1);
		ensureSize(length * 3);
		for (int i = 0; i < length; i++) {
			int c = s.charAt(i);
			if (c <= 0x007F) {
				buf[pos++] = (byte) c;
			} else {
				pos = writeUtfChar(buf, pos, c);
			}
		}
		return pos;
	}

	private static int writeUtfChar(byte[] buf, int pos, int c) {
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

	public static int writeWithLength(byte[] buf, int pos, byte[] bytes) {
		pos = writeVarInt(buf, pos, bytes.length);
		return write(buf, pos, bytes);
	}

	public static int writeUTF16(byte[] buf, int pos, String s) {
		int length = s.length();
		pos = writeVarInt(buf, pos, length);
		ensureSize(length * 2);
		for (int i = 0; i < length; i++) {
			char v = s.charAt(i);
			pos = writeByte(buf, pos, (byte) (v >>> 8));
			pos = writeByte(buf, pos, (byte) (v));
		}
		return pos;
	}

	public static int writeNullableUTF16(byte[] buf, int pos, String s) {
		if (s == null) {
			return writeByte(buf, pos, (byte) 0);
		}
		int length = s.length();
		pos = writeVarInt(buf, pos, length + 1);
		ensureSize(length * 2);
		for (int i = 0; i < length; i++) {
			char v = s.charAt(i);
			pos = writeByte(buf, pos, (byte) (v >>> 8));
			pos = writeByte(buf, pos, (byte) (v));
		}
		return pos;
	}
}
