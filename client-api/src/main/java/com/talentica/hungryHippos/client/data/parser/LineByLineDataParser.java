package com.talentica.hungryHippos.client.data.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.InvalidRowExeption;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

/**
 * Data parser implementation for line by line reading of data file.
 */
public abstract class LineByLineDataParser implements DataParser {

	public static final char[] WINDOWS_LINE_SEPARATOR_CHARS = { 13, 10 };

	private byte[] dataBytes = new byte[65536];
	private ByteBuffer buf = ByteBuffer.wrap(dataBytes);
	private int readCount = -1;
	private MutableCharArrayString buffer;

	public LineByLineDataParser() {
		buf.clear();
	}

	@Override
	public Iterator<MutableCharArrayString[]> iterator(InputStream dataStream, DataDescription dataDescription)
			throws InvalidRowExeption {
		if (buffer == null) {
			buffer = new MutableCharArrayString(getMaximumSizeOfSingleBlockOfDataInBytes(dataDescription));
		}

		return new Iterator<MutableCharArrayString[]>() {

			@Override
			public boolean hasNext() {
				try {
					return dataStream.available() > 0 || readCount > 0;
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public MutableCharArrayString[] next() {
				try {
					return read();
				} catch (InvalidRowExeption | IOException e) {
					throw new RuntimeException(e);
				}
			}

			public MutableCharArrayString[] read() throws IOException, InvalidRowExeption {
				buffer.reset();
				while (true) {
					if (readCount <= 0) {
						buf.clear();
						readCount = dataStream.read(dataBytes);
						buf.limit(readCount);
						if (readCount < 0 && buffer.length() > 0) {
							return processLine(buffer, dataDescription);
						} else if (readCount < 0 && buffer.length() <= 0) {
							return null;
						}
					}
					byte nextChar = readNextChar();
					if (isNewLine(nextChar)) {
						break;
					}
					buffer.addCharacter((char) nextChar);
				}
				return processLine(buffer, dataDescription);
			}

			private byte readNextChar() {
				byte nextChar = buf.get();
				readCount--;
				return nextChar;
			}

			private boolean isNewLine(byte readByte) throws IOException {
				char[] windowsLineseparatorChars = WINDOWS_LINE_SEPARATOR_CHARS;
				if (windowsLineseparatorChars[1] == readByte) {
					return true;
				}
				boolean newLine = (windowsLineseparatorChars[0] == readByte);
				if (newLine) {
					for (int i = 1; i < windowsLineseparatorChars.length; i++) {
						if (readCount <= 0) {
							buf.clear();
							readCount = dataStream.read(dataBytes);
							if (readCount < 0) {
								return newLine;
							}
							buf.flip();
						}
						byte nextChar = readNextChar();
						newLine = newLine && (windowsLineseparatorChars[i] == nextChar);
					}
				}
				return newLine;
			}
		};
	}

	protected abstract MutableCharArrayString[] processLine(MutableCharArrayString line,
			DataDescription dataDescription);

	protected abstract int getMaximumSizeOfSingleBlockOfDataInBytes(DataDescription dataDescription);

}