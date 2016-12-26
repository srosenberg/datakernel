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

package io.datakernel.bytebuf;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BytesHexFormatterTest {
	@Test
	public void formatsByteBufToOffsetColumnHexColumnAndAsciiColumn() {
		// arrange
		BytesHexFormatter ft = BytesHexFormatter.create();
		byte[] bytes = new byte[]{
				0x00, 0x01, 0x02, 0x03, 0x20, 0x21, 0x23, 0x25,
				0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,

				0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
				0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70,

				0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F, -128, -127,
				-126, -125, -1
		};

		// act
		String formatted = ft.format(bytes);

		// assert
		String line_1 = "0000  00 01 02 03 20 21 23 25  31 32 33 34 35 36 37 38  .... !#%12345678";
		String line_2 = "0010  61 62 63 64 65 66 67 68  69 6A 6B 6C 6D 6E 6F 70  abcdefghijklmnop";
		String line_3 = "0020  7A 7B 7C 7D 7E 7F 80 81  82 83 FF                 z{|}~......     ";
		String expected = line_1 + "\n" + line_2 + "\n" + line_3;
		assertEquals(expected, formatted);
	}

	@Test
	public void formatsByteBufWithoutOffsetColumn() {
		// arrange
		BytesHexFormatter ft = BytesHexFormatter.create().withOffsetColumn(false);
		byte[] bytes = new byte[]{
				0x00, 0x01, 0x02, 0x03, 0x20, 0x21, 0x23, 0x25,
				0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,

				0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
				0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70,

				0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F, -128, -127,
				-126, -125, -1
		};

		// act
		String formatted = ft.format(bytes);

		// assert
		String line_1 = "00 01 02 03 20 21 23 25  31 32 33 34 35 36 37 38  .... !#%12345678";
		String line_2 = "61 62 63 64 65 66 67 68  69 6A 6B 6C 6D 6E 6F 70  abcdefghijklmnop";
		String line_3 = "7A 7B 7C 7D 7E 7F 80 81  82 83 FF                 z{|}~......     ";
		String expected = line_1 + "\n" + line_2 + "\n" + line_3;
		assertEquals(expected, formatted);
	}

	@Test
	public void formatsByteBufWithoutAsciiColumn() {
		// arrange
		BytesHexFormatter ft = BytesHexFormatter.create().withAsciiColumn(false);
		byte[] bytes = new byte[]{
				0x00, 0x01, 0x02, 0x03, 0x20, 0x21, 0x23, 0x25,
				0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,

				0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
				0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70,

				0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F, -128, -127,
				-126, -125, -1
		};

		// act
		String formatted = ft.format(bytes);

		// assert
		String line_1 = "0000  00 01 02 03 20 21 23 25  31 32 33 34 35 36 37 38";
		String line_2 = "0010  61 62 63 64 65 66 67 68  69 6A 6B 6C 6D 6E 6F 70";
		String line_3 = "0020  7A 7B 7C 7D 7E 7F 80 81  82 83 FF               ";
		String expected = line_1 + "\n" + line_2 + "\n" + line_3;
		assertEquals(expected, formatted);
	}

	@Test
	public void formatsByteBufWithoutWithoutOffsetColumnAndWithoutAsciiColumn() {
		// arrange
		BytesHexFormatter ft = BytesHexFormatter.create()
				.withOffsetColumn(false)
				.withAsciiColumn(false);
		byte[] bytes = new byte[]{
				0x00, 0x01, 0x02, 0x03, 0x20, 0x21, 0x23, 0x25,
				0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,

				0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
				0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70,

				0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F, -128, -127,
				-126, -125, -1
		};

		// act
		String formatted = ft.format(bytes);

		// assert
		String line_1 = "00 01 02 03 20 21 23 25  31 32 33 34 35 36 37 38";
		String line_2 = "61 62 63 64 65 66 67 68  69 6A 6B 6C 6D 6E 6F 70";
		String line_3 = "7A 7B 7C 7D 7E 7F 80 81  82 83 FF               ";
		String expected = line_1 + "\n" + line_2 + "\n" + line_3;
		assertEquals(expected, formatted);
	}

	@Test
	public void limitsMaxBytesPerColumn() {
		// arrange
		BytesHexFormatter ft = BytesHexFormatter.create().withMaxColumn(7);
		byte[] bytes = new byte[]{
				0x00, 0x01, 0x02, 0x03, 0x20, 0x21, 0x23, 0x25,
				0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,

				0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
				0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70,

				0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F, -128, -127,
				-126, -125, -1
		};

		// act
		String formatted = ft.format(bytes);

		// assert
		String line_1 = "0000  00 01 02 03 20 21 23  25 31 32 33 34 35 36  37 38  .... !#%12345678";
		String line_2 = "0010  61 62 63 64 65 66 67  68 69 6A 6B 6C 6D 6E  6F 70  abcdefghijklmnop";
		String line_3 = "0020  7A 7B 7C 7D 7E 7F 80  81 82 83 FF                  z{|}~......     ";
		String expected = line_1 + "\n" + line_2 + "\n" + line_3;
		assertEquals(expected, formatted);
	}

	@Test
	public void limitswMaxBytesPerLine() {
		// arrange
		BytesHexFormatter ft = BytesHexFormatter.create().withMaxLine(5);
		byte[] bytes = new byte[]{
				0x00, 0x01, 0x02, 0x03, 0x20, 0x21, 0x23, 0x25,
				0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38
		};

		// act
		String formatted = ft.format(bytes);

		// assert
		String line_1 = "0000  00 01 02 03 20  .... ";
		String line_2 = "0005  21 23 25 31 32  !#%12";
		String line_3 = "000A  33 34 35 36 37  34567";
		String line_4 = "000F  38              8    ";
		String expected = line_1 + "\n" + line_2 + "\n" + line_3 + "\n" + line_4;
		assertEquals(expected, formatted);
	}

	@Test
	public void limitsHeadAndTailBytes() {
		// arrange
		BytesHexFormatter ft = BytesHexFormatter.create()
				.withMaxHead(20)
				.withMaxTail(16);
		byte[] bytes = new byte[]{
				0x00, 0x01, 0x02, 0x03, 0x20, 0x21, 0x23, 0x25,
				0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,

				0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
				0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70,

				0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F, -128, -127,
				-126, -125, -1
		};

		// act
		String formatted = ft.format(bytes);

		// assert
		String line_1 = "0000  00 01 02 03 20 21 23 25  31 32 33 34 35 36 37 38  .... !#%12345678";
		String line_2 = "0010  61 62 63 64                                       abcd            ";
		String line_3 = "Skipped 7 bytes out of 43";
		String line_4 = "001B  6C 6D 6E 6F 70 7A 7B 7C  7D 7E 7F 80 81 82 83 FF  lmnopz{|}~......";
		String expected = line_1 + "\n" + line_2 + "\n" + line_3 + "\n" + line_4;
		assertEquals(expected, formatted);
	}

	@Test
	public void formatsOnlyHead() {
		// arrange
		BytesHexFormatter ft = BytesHexFormatter.create()
				.withMaxHead(16)
				.withMaxTail(0);
		byte[] bytes = new byte[]{
				0x00, 0x01, 0x02, 0x03, 0x20, 0x21, 0x23, 0x25,
				0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,

				0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
				0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70,

				0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F, -128, -127,
				-126, -125, -1
		};

		// act
		String formatted = ft.format(bytes);

		// assert
		String expected = "0000  00 01 02 03 20 21 23 25  31 32 33 34 35 36 37 38  .... !#%12345678";
		assertEquals(expected, formatted);
	}

	@Test
	public void formatsOnlyTail() {
		// arrange
		BytesHexFormatter ft = BytesHexFormatter.create()
				.withMaxHead(0)
				.withMaxTail(16);
		byte[] bytes = new byte[]{
				0x00, 0x01, 0x02, 0x03, 0x20, 0x21, 0x23, 0x25,
				0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,

				0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
				0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70,

				0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F, -128, -127,
				-126, -125, -1
		};

		// act
		String formatted = ft.format(bytes);

		// assert
		String expected = "001B  6C 6D 6E 6F 70 7A 7B 7C  7D 7E 7F 80 81 82 83 FF  lmnopz{|}~......";
		assertEquals(expected, formatted);
	}

	@Test
	public void formatsByteBufConsideringNonPrintable() {
		// arrange
		BytesHexFormatter ft = BytesHexFormatter.create().withNonPrintable('_');
		byte[] bytes = new byte[]{
				0x00, 0x01, 0x02, 0x03, 0x20, 0x21, 0x23, 0x25,
				0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
		};

		// act
		String formatted = ft.format(bytes);

		// assert
		String expected = "0000  00 01 02 03 20 21 23 25  31 32 33 34 35 36 37 38  ____ !#%12345678";
		assertEquals(expected, formatted);
	}

	@Test
	public void formatsByteBufConsideringColumnSeparator() {
		// arrange
		BytesHexFormatter ft = BytesHexFormatter.create().withColumnSeparator("    ");
		byte[] bytes = new byte[]{
				0x00, 0x01, 0x02, 0x03, 0x20, 0x21, 0x23, 0x25,
				0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
		};

		// act
		String formatted = ft.format(bytes);

		// assert
		String expected = "0000    00 01 02 03 20 21 23 25    31 32 33 34 35 36 37 38    .... !#%12345678";
		assertEquals(expected, formatted);
	}
}
