package com.talentica.hungryHippos.utility.marshaling;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

/**
 * Created by debasishc on 22/6/15.
 */
public class FileReader implements Reader {
	// 65536*8
	ByteBuffer buf = ByteBuffer.allocate(65536);
	FileChannel channel;
	int readCount = -1;

	@SuppressWarnings("resource")
	public FileReader(String filepath) throws IOException {
		channel = new FileInputStream(filepath).getChannel();
		buf.clear();
	}

	@SuppressWarnings("resource")
	public FileReader(File file) throws IOException {
		channel = new FileInputStream(file).getChannel();
		buf.clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.talentica.hungryHippos.utility.marshaling.Reader#readLine()
	 */
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
			byte nextChar = buf.get();
			readCount--;

			if (nextChar != '\n') {
				sb.append((char) nextChar);
			} else {
				break;
			}

		}
		return sb.toString();
	}

	private int numfields;
	private MutableCharArrayString[] buffer;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.talentica.hungryHippos.utility.marshaling.Reader#setNumFields(int)
	 */
	@Override
	public void setNumFields(int numFields) {
		this.numfields = numFields;
		buffer = new MutableCharArrayString[numFields];
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.talentica.hungryHippos.utility.marshaling.Reader#setMaxsize(int)
	 */
	@Override
	public void setMaxsize(int maxsize) {
		for (int i = 0; i < numfields; i++) {
			buffer[i] = new MutableCharArrayString(maxsize);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.talentica.hungryHippos.utility.marshaling.Reader#readCommaSeparated()
	 */
	@Override
	public MutableCharArrayString[] read() throws IOException {
		for (MutableCharArrayString s : buffer) {
			s.reset();
		}
		int fieldIndex = 0;
		while (true) {
			if (readCount <= 0) {
				buf.clear();
				readCount = channel.read(buf);
				if (readCount < 0) {
					if (fieldIndex == numfields - 1) {
						return buffer;
					}
					return null;
					}
				buf.flip();
				}
			byte nextChar = buf.get();
			readCount--;
			if (nextChar == ',') {
				fieldIndex++;
			} else if (nextChar == '\n') {
				// Ignore blank lines with no data.
				break;
			} else {
				buffer[fieldIndex].addCharacter((char) nextChar);
			}
		}
		return buffer;
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