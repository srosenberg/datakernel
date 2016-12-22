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

import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.Integer.toHexString;

public final class ByteBufFormatter {
	private final boolean offsetColumn;
	private final boolean asciiColumn;
	private final int maxBytesPerLine;
	private final int maxBytesPerColumn;
	private final char nonPrintable;
	private final String columnSeparator;

	// region builders
	public ByteBufFormatter(boolean offsetColumn, boolean asciiColumn,
	                        int maxBytesPerLine, int maxBytesPerColumn,
	                        char nonPrintable, String columnSeparator) {
		this.offsetColumn = offsetColumn;
		this.asciiColumn = asciiColumn;
		this.maxBytesPerLine = maxBytesPerLine;
		this.maxBytesPerColumn = maxBytesPerColumn;
		this.nonPrintable = nonPrintable;
		this.columnSeparator = columnSeparator;
	}

	public static ByteBufFormatter create() {
		return new ByteBufFormatter(true, true, 16, 8, '.', "  ");
	}

	public ByteBufFormatter withOffsetColumn(boolean offsetColumn) {
		return new ByteBufFormatter(
				offsetColumn, asciiColumn, maxBytesPerLine, maxBytesPerColumn, nonPrintable, columnSeparator);
	}

	public ByteBufFormatter withAsciiColumn(boolean asciiColumn) {
		return new ByteBufFormatter(
				offsetColumn, asciiColumn, maxBytesPerLine, maxBytesPerColumn, nonPrintable, columnSeparator);
	}

	public ByteBufFormatter withMaxBytesPerLine(int maxBytesPerLine) {
		checkArgument(maxBytesPerLine > 0, "max bytes per line must be greater than zero");
		return new ByteBufFormatter(
				offsetColumn, asciiColumn, maxBytesPerLine, maxBytesPerColumn, nonPrintable, columnSeparator);
	}

	public ByteBufFormatter withMaxBytesPerColumn(int maxBytesPerColumn) {
		checkArgument(maxBytesPerColumn > 0, "max bytes per column must be greater than zero");
		return new ByteBufFormatter(
				offsetColumn, asciiColumn, maxBytesPerLine, maxBytesPerColumn, nonPrintable, columnSeparator);
	}

	public ByteBufFormatter withNonPrintable(char nonPrintable) {
		return new ByteBufFormatter(
				offsetColumn, asciiColumn, maxBytesPerLine, maxBytesPerColumn, nonPrintable, columnSeparator);
	}

	public ByteBufFormatter withColumnSeparator(String columnSeparator) {
		return new ByteBufFormatter(
				offsetColumn, asciiColumn, maxBytesPerLine, maxBytesPerColumn, nonPrintable, columnSeparator);
	}
	// endregion

	public String format(ByteBuf buf) {
		return format(buf.array(), buf.readPosition(), buf.readRemaining());
	}

	public String format(ByteBuf buf, int maxBytes) {
		return format(buf.array(), buf.readPosition(), Math.min(buf.readRemaining(), maxBytes));
	}

	public String format(byte[] bytes, int offset, int length) {
		int offsetColumnLength = Math.max(4, toHexString(length).length());
		StringBuilder allLines = new StringBuilder();

		int bytesInLine = 0;
		StringBuilder line = null;
		StringBuilder hex = null;
		StringBuilder ascii = null;
		for (int i = 0; i < length; i++) {
			if (bytesInLine == 0) {
				line = new StringBuilder();

				if (offsetColumn) {
					String hexOffset = toHexString(i).toUpperCase();
					line.append(leftPad(hexOffset, '0', offsetColumnLength));
					line.append(columnSeparator);
				}

				hex = new StringBuilder();
				ascii = asciiColumn ? new StringBuilder() : null;
			}

			byte b = bytes[offset + i];

			if (bytesInLine % maxBytesPerColumn != 0) { // if it's
				hex.append(' ');
			}
			hex.append(byteToHex(b));

			if (asciiColumn) {
				ascii.append(formatByte(b));
			}

			bytesInLine = (bytesInLine + 1) % maxBytesPerLine;

			if (bytesInLine == 0) {
				line.append(hex);
				if (asciiColumn) {
					line.append(columnSeparator);
					line.append(ascii);
				}

				allLines.append(line);
				allLines.append('\n');

				line = null;
				hex = null;
				ascii = null;
				continue;
			}

			if (bytesInLine % maxBytesPerColumn == 0) {
				hex.append(columnSeparator);
				if (asciiColumn) {
					ascii.append(columnSeparator);
				}
			}
		}

		if (line != null) {
			allLines.append(formatLastLine(line, hex, ascii));
		}

		return removeTrailingLineFeed(allLines);
	}

	private StringBuilder formatLastLine(StringBuilder line, StringBuilder hex, StringBuilder ascii) {
		int separatorsLen = columnSeparator.length() *
				(maxBytesPerLine / maxBytesPerColumn - (maxBytesPerLine % maxBytesPerColumn == 0 ? 1 : 0));
		int columns = maxBytesPerLine / maxBytesPerColumn + (maxBytesPerLine % maxBytesPerColumn != 0 ? 1 : 0);
		int hexLen = (maxBytesPerLine * 3 - columns) + separatorsLen;
		int asciiLen = maxBytesPerLine + separatorsLen;

		line.append(rightPad(hex.toString(), ' ', hexLen));
		if (asciiColumn) {
			line.append(columnSeparator);
			line.append(rightPad(ascii.toString(), ' ', asciiLen));
		}
		return line;
	}

	private String removeTrailingLineFeed(StringBuilder strBuilder) {
		String str = strBuilder.toString();
		return str.charAt(str.length() - 1) == '\n' ? str.substring(0, str.length() - 1) : str;
	}

	private String byteToHex(byte b) {
		return String.format("%02X", b);
	}

	private char formatByte(byte b) {
		return b > 32 && b < 127 ? (char) b : nonPrintable;
	}

	private static String leftPad(String str, char symbol, int targetSize) {
		if (str.length() >= targetSize) {
			return str;
		}

		StringBuilder result = new StringBuilder(targetSize);
		int neededPaddingSymbols = targetSize - str.length();
		for (int i = 0; i < neededPaddingSymbols; i++) {
			result.append(symbol);
		}
		result.append(str);

		return result.toString();
	}

	private static String rightPad(String str, char symbol, int targetSize) {
		if (str.length() >= targetSize) {
			return str;
		}

		StringBuilder result = new StringBuilder(targetSize);
		result.append(str);
		int neededPaddingSymbols = targetSize - str.length();
		for (int i = 0; i < neededPaddingSymbols; i++) {
			result.append(symbol);
		}

		return result.toString();
	}
}
