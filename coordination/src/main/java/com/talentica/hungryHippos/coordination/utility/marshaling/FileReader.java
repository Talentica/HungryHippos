package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.DataLocator.DataType;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
import com.talentica.hungryHippos.coordination.utility.CommonUtil;

/**
 * Created by debasishc on 22/6/15.
 */
public class FileReader implements Reader {

	private ByteBuffer buf = ByteBuffer.allocate(65536);
	private FileChannel channel;
	private int readCount = -1;
	private int numfields;
	private MutableCharArrayString[] buffer;

	@SuppressWarnings("resource")
	public FileReader(String filepath) throws IOException {
		channel = new FileInputStream(filepath).getChannel();
		buf.clear();
		initializeMutableArrayStringBuffer();
	}

	@SuppressWarnings("resource")
	public FileReader(File file) throws IOException {
		channel = new FileInputStream(file).getChannel();
		buf.clear();
		initializeMutableArrayStringBuffer();
	}

	private void initializeMutableArrayStringBuffer() {
		DataDescription dataDescription = CommonUtil.getConfiguredDataDescription();
		numfields = dataDescription.getNumberOfDataFields();
		buffer = new MutableCharArrayString[numfields];
		for (int i = 0; i < numfields; i++) {
			DataLocator dataLocator = dataDescription.locateField(i);
			int numberOfCharsDataTypeTakes = dataLocator.getSize();
			// TODO: Need to fix hard coding later.
			if (dataLocator.getDataType() == DataType.DOUBLE || dataLocator.getDataType() == DataType.INT
					|| dataLocator.getDataType() == DataType.LONG || dataLocator.getDataType() == DataType.FLOAT) {
				numberOfCharsDataTypeTakes = 25;
			}
			buffer[i] = new MutableCharArrayString(numberOfCharsDataTypeTakes);
		}
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
			} else if (String.valueOf(nextChar).equalsIgnoreCase(System.getProperty("line.separator"))) {
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