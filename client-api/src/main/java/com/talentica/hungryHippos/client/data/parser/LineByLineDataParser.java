package com.talentica.hungryHippos.client.data.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.client.validator.DataParserValidator;

/**
 * Data parser implementation for line by line reading of data file.
 */
public abstract class LineByLineDataParser extends DataParser {

	public static final char[] WINDOWS_LINE_SEPARATOR_CHARS = { 13, 10 };

	private byte[] dataBytes = new byte[65536];
	private ByteBuffer buf = ByteBuffer.wrap(dataBytes);
	private int readCount = -1;
	private MutableCharArrayString buffer;
	private Iterator<DataTypes[]> iterator;
	protected DataParserValidator csvValidator;

	public LineByLineDataParser(DataDescription dataDescription) {
		super(dataDescription);
		buf.clear();
		csvValidator = createDataParserValidator();
	}

	@Override
	public Iterator<DataTypes[]> iterator(InputStream dataStream) {
		if (buffer == null) {
			buffer = new MutableCharArrayString(getDataDescription().getMaximumSizeOfSingleBlockOfData());
		}

		iterator = new Iterator<DataTypes[]>() {

			@Override
			public boolean hasNext() {
				try {
					return dataStream.available() > 0 || readCount > 0;
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public DataTypes[] next() {
				try {
					return read();
				} catch (InvalidRowException irex) {
					throw irex;
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			public DataTypes[] read() throws IOException, InvalidRowException {
				buffer.reset();
				while (true) {
					if (readCount <= 0) {
						buf.clear();
						readCount = dataStream.read(dataBytes);
						if (readCount != -1) {
							buf.limit(readCount);
						}
						if (readCount < 0 && buffer.length() > 0) {
							return processLine(buffer);
						} else if (readCount < 0 && buffer.length() <= 0) {
							return null;
						}
					}
					byte nextChar = readNextChar();
					if (isNewLine(nextChar)) {
						break;
					}
					buffer.addByte(nextChar);
				}
				return processLine(buffer);
			}

			private byte readNextChar() {
				byte nextChar = buf.get();
				readCount--;
				return nextChar;
			}

			private boolean isNewLine(byte readByte) throws IOException {
				char[] windowsLineseparatorChars = csvValidator.getLineSeparator();
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
		return iterator;
	}

	protected final Iterator<DataTypes[]> getIterator() {
		return iterator;
	}

	protected abstract DataTypes[] processLine(MutableCharArrayString line);

	protected abstract int getMaximumSizeOfSingleBlockOfDataInBytes(DataDescription dataDescription);

}
