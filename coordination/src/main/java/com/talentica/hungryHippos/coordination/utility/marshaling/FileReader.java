package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.talentica.hungryHippos.client.data.DataParser;
import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.utility.OsUtils;

/**
 * Created by debasishc on 22/6/15.
 */
public class FileReader implements Reader {

	private ByteBuffer buf = ByteBuffer.allocate(65536);
	private FileChannel channel;
	private int readCount = -1;
	private DataParser dataParser = null;
	private MutableCharArrayString buffer;

	@SuppressWarnings("resource")
	public FileReader(String filepath,DataParser parser) throws IOException {
		this.dataParser=parser;
		channel = new FileInputStream(filepath).getChannel();
		buf.clear();
		DataDescription dataDescription = dataParser.getDataDescription();
		buffer = new MutableCharArrayString(dataDescription.getMaximumSizeOfSingleBlockOfData());
	}

	public FileReader(File file,DataParser preProcessor) throws IOException {
		this(file.getAbsolutePath(),preProcessor);
	}

	@Override
	public String readLine() throws IOException {
		StringBuilder sb = new StringBuilder();
		while (true) {
			if (readCount <= 0) {
				buf.clear();
				readCount = channel.read(buf);
				if (readCount < 0) {
					break;
				}
				buf.flip();
			}
			byte nextChar = readNextChar();
			if (nextChar != '\n') {
				sb.append((char) nextChar);
			} else {
				break;
			}

		}
		return sb.toString();
	}

	@Override
	public MutableCharArrayString[] read() throws IOException {
		buffer.reset();
		while (true) {
			if (readCount <= 0) {
				buf.clear();
				readCount = channel.read(buf);
				if (readCount < 0 && buffer.length() > 0) {
						return dataParser.preprocess(buffer);
				} else if (readCount < 0 && buffer.length() <= 0) {
					return null;
				}
				buf.flip();
			}
			byte nextChar = readNextChar();
			if (isNewLine(nextChar)) {
				break;
			}
			buffer.addCharacter((char) nextChar);
		}
		return dataParser.preprocess(buffer);
	}

	private byte readNextChar() {
		byte nextChar = buf.get();
		readCount--;
		return nextChar;
	}

	private boolean isNewLine(byte readByte) throws IOException {
		char[] windowsLineseparatorChars = OsUtils.WINDOWS_LINE_SEPARATOR_CHARS;
		if (windowsLineseparatorChars[1] == readByte) {
			return true;
		}
		boolean newLine = (windowsLineseparatorChars[0] == readByte);
		if (newLine) {
			for (int i = 1; i < windowsLineseparatorChars.length; i++) {
				if (readCount <= 0) {
					buf.clear();
					readCount = channel.read(buf);
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

	@Override
	public void close() throws IOException {
		if (channel != null && channel.isOpen()) {
			channel.close();
		}
	}

	@Override
	public void reset() throws IOException {
		channel.position(0);
		buf.clear();
		readCount = -1;
	}

}