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

package io.datakernel.serializer.asm;

import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.Ref;
import io.datakernel.serializer.SerializationInputHelper;
import io.datakernel.serializer.SerializationOutputHelper;

public final class BufferSerializers {
	private BufferSerializers() {
	}

	private static final BufferSerializer<Byte> BYTE_SERIALIZER = new BufferSerializer<Byte>() {
		@Override
		public int serialize(byte[] byteArray, int pos, Byte item) {
			return SerializationOutputHelper.writeByte(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readByte(byteArray, pos, ref);
		}
	};

	private static final BufferSerializer<byte[]> BYTES_SERIALIZER = new BufferSerializer<byte[]>() {
		@Override
		public int serialize(byte[] byteArray, int pos, byte[] item) {
			pos = SerializationOutputHelper.writeVarInt(byteArray, pos, item.length);
			return SerializationOutputHelper.write(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			pos = SerializationInputHelper.readVarInt(byteArray, pos, ref);
			return SerializationInputHelper.read(byteArray, pos, (Integer)ref.get(), ref);
		}
	};

	private static final BufferSerializer<Short> SHORT_SERIALIZER = new BufferSerializer<Short>() {
		@Override
		public int serialize(byte[] byteArray, int pos, Short item) {
			return SerializationOutputHelper.writeShort(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readShort(byteArray, pos, ref);
		}
	};

	private static final BufferSerializer<Integer> INT_SERIALIZER = new BufferSerializer<Integer>() {
		@Override
		public int serialize(byte[] byteArray, int pos, Integer item) {
			return SerializationOutputHelper.writeInt(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readInt(byteArray, pos, ref);
		}
	};

	private static final BufferSerializer<Integer> VARINT_SERIALIZER = new BufferSerializer<Integer>() {
		@Override
		public int serialize(byte[] byteArray, int pos, Integer item) {
			return SerializationOutputHelper.writeVarInt(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readVarInt(byteArray, pos, ref);
		}
	};

	private static final BufferSerializer<Integer> VARINT_ZIGZAG_SERIALIZER = new BufferSerializer<Integer>() {
		@Override
		public int serialize(byte[] byteArray, int pos, Integer item) {
			return SerializationOutputHelper.writeVarInt(byteArray, pos, (item << 1) ^ (item >> 31));
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			pos = SerializationInputHelper.readVarInt(byteArray, pos, ref);
			int n = ((Integer) ref.get());
			ref.set((n >>> 1) ^ -(n & 1));
			return pos;
		}
	};

	private static final BufferSerializer<Long> LONG_SERIALIZER = new BufferSerializer<Long>() {
		@Override
		public int serialize(byte[] byteArray, int pos, Long item) {
			return SerializationOutputHelper.writeLong(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readLong(byteArray, pos, ref);
		}
	};

	private static final BufferSerializer<Long> VARLONG_SERIALIZER = new BufferSerializer<Long>() {
		@Override
		public int serialize(byte[] byteArray, int pos, Long item) {
			return SerializationOutputHelper.writeVarLong(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readVarLong(byteArray, pos, ref);
		}
	};

	private static final BufferSerializer<Long> VARLONG_ZIGZAG_SERIALIZER = new BufferSerializer<Long>() {
		@Override
		public int serialize(byte[] byteArray, int pos, Long item) {
			return SerializationOutputHelper.writeVarLong(byteArray, pos, (item << 1) ^ (item >> 63));
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			pos = SerializationInputHelper.readVarLong(byteArray, pos, ref);
			int n = ((Integer) ref.get());
			ref.set((n >>> 1) ^ -(n & 1));
			return pos;
		}
	};

	private static final BufferSerializer<Float> FLOAT_SERIALIZER = new BufferSerializer<Float>() {
		@Override
		public int serialize(byte[] byteArray, int pos, Float item) {
			return SerializationOutputHelper.writeFloat(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readFloat(byteArray, pos, ref);
		}
	};

	private static final BufferSerializer<Double> DOUBLE_SERIALIZER = new BufferSerializer<Double>() {
		@Override
		public int serialize(byte[] byteArray, int pos, Double item) {
			return SerializationOutputHelper.writeDouble(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readDouble(byteArray, pos, ref);
		}
	};

	private static final BufferSerializer<Character> CHAR_SERIALIZER = new BufferSerializer<Character>() {
		@Override
		public int serialize(byte[] byteArray, int pos, Character item) {
			return SerializationOutputHelper.writeChar(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readChar(byteArray, pos, ref);
		}
	};
	private static final BufferSerializer<String> UTF8_SERIALIZER = new BufferSerializer<String>() {
		@Override
		public int serialize(byte[] byteArray, int pos, String item) {
			return SerializationOutputHelper.writeUTF8(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readUTF8(byteArray, pos, ref);
		}
	};
	private static final BufferSerializer<String> UTF16_SERIALIZER = new BufferSerializer<String>() {
		@Override
		public int serialize(byte[] byteArray, int pos, String item) {
			return SerializationOutputHelper.writeUTF16(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readUTF16(byteArray, pos, ref);
		}
	};

	private static final BufferSerializer<Boolean> BOOLEAN_SERIALIZER = new BufferSerializer<Boolean>() {
		@Override
		public int serialize(byte[] byteArray, int pos, Boolean item) {
			return SerializationOutputHelper.writeBoolean(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readBoolean(byteArray, pos, ref);
		}
	};

	private static final BufferSerializer<String> ISO_8859_1_SERIALIZER = new BufferSerializer<String>() {
		@Override
		public int serialize(byte[] byteArray, int pos, String item) {
			return SerializationOutputHelper.writeIso88591(byteArray, pos, item);
		}

		@Override
		public int deserialize(byte[] byteArray, int pos, Ref ref) {
			return SerializationInputHelper.readIso88591(byteArray, pos, ref);
		}
	};

	public static BufferSerializer<Byte> byteSerializer() {
		return BYTE_SERIALIZER;
	}

	public static BufferSerializer<byte[]> bytesSerializer() {
		return BYTES_SERIALIZER;
	}

	public static BufferSerializer<Short> shortSerializer() {
		return SHORT_SERIALIZER;
	}

	public static BufferSerializer<Integer> intSerializer() {
		return INT_SERIALIZER;
	}

	public static BufferSerializer<Integer> varIntSerializer(boolean optimizePositive) {
		return optimizePositive ? VARINT_SERIALIZER : VARINT_ZIGZAG_SERIALIZER;
	}

	public static BufferSerializer<Long> longSerializer() {
		return LONG_SERIALIZER;
	}

	public static BufferSerializer<Long> varLongSerializer(boolean optimizePositive) {
		return optimizePositive ? VARLONG_SERIALIZER : VARLONG_ZIGZAG_SERIALIZER;
	}

	public static BufferSerializer<Float> floatSerializer() {
		return FLOAT_SERIALIZER;
	}

	public static BufferSerializer<Double> doubleSerializer() {
		return DOUBLE_SERIALIZER;
	}

	public static BufferSerializer<Character> charSerializer() {
		return CHAR_SERIALIZER;
	}

	public static BufferSerializer<String> stringSerializer() {
		return UTF8_SERIALIZER;
	}

	public static BufferSerializer<String> utf8Serializer() {
		return UTF8_SERIALIZER;
	}

	public static BufferSerializer<String> utf16Serializer() {
		return UTF16_SERIALIZER;
	}

	public static BufferSerializer<String> iso88591Serializer() { return ISO_8859_1_SERIALIZER; }
}
