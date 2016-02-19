package com.talentica.hungryHippos.client.domain;

import java.util.Arrays;

/**
 * This is the memory efficient implementation of byte buffer. It tries to
 * allocate minimal memory for the data to be stored in byte buffer. It doesn't
 * allocate memory of 4 bytes by default for characters as Java's default
 * implementation does. Instead it saves all information byte to byte in an
 * integer array in optimal manner.
 * 
 * @author nitink
 *
 */
public final class ByteBuffer {

	private int[] data;

	private DataDescription dataDescription;

	private final java.nio.ByteBuffer _INT_SIZE_BYTE_BUFFER = java.nio.ByteBuffer.allocate(Integer.BYTES);

	private MutableCharArrayString mutableCharArrayString = null;

	public ByteBuffer(DataDescription dataDescription) {
		this.dataDescription = dataDescription;
		int dataDescSize = dataDescription.getSize();
		int noOfIntegersNeededToStoreData = dataDescSize / Integer.BYTES;
		if (dataDescSize % Integer.BYTES != 0) {
			noOfIntegersNeededToStoreData++;
		}
		data = new int[noOfIntegersNeededToStoreData];
	}

	public ByteBuffer put(int offset, byte byteToPut) {
		if (offset > data.length * Integer.BYTES || offset < 0) {
			throw new IllegalArgumentException("The index to put byte into bytebuffer cannot exceed maximum index:"
					+ data.length * Integer.BYTES + " and it cannot be less than zero.");
		}
		int indexInDataArray = offset / Integer.BYTES;
		int indexToPutByteInDataAt = offset % Integer.BYTES;
		int oldValue = data[indexInDataArray];
		clearByteBuffer();
		_INT_SIZE_BYTE_BUFFER.putInt(oldValue);
		byte[] oldValueBytes = _INT_SIZE_BYTE_BUFFER.array();
		oldValueBytes[indexToPutByteInDataAt] = byteToPut;
		clearByteBuffer();
		_INT_SIZE_BYTE_BUFFER.put(oldValueBytes);
		_INT_SIZE_BYTE_BUFFER.position(0);
		data[indexInDataArray] = _INT_SIZE_BYTE_BUFFER.getInt();
		return this;
	}

	private void clearByteBuffer() {
		_INT_SIZE_BYTE_BUFFER.clear();
		_INT_SIZE_BYTE_BUFFER.position(0);
	}

	public ByteBuffer put(int offset, byte[] bytes) {
		putBytes(offset, bytes);
		return this;
	}

	private void putBytes(int offset, byte[] bytes) {
		if (bytes != null) {
			for (byte byteToPut : bytes) {
				put(offset, byteToPut);
				offset++;
			}
		}
	}

	public byte[] get(int index) {
		DataLocator dataLocator = dataDescription.locateField(index);
		int offset = dataLocator.getOffset();
		int size = dataLocator.getSize();
		byte[] value = new byte[size];
		int indexInDataArray = offset / Integer.BYTES;
		int integerToReadBytesFrom = data[indexInDataArray];
		for (int i = offset; i < offset + size; i++) {
			if (i != 0 && i % 4 == 0) {
				indexInDataArray++;
				integerToReadBytesFrom = data[indexInDataArray];
			}
			int numberOfBitsToShift = (Integer.BYTES - i % Integer.BYTES - 1) * Byte.SIZE;
			value[i - offset] = (byte) ((integerToReadBytesFrom >>> numberOfBitsToShift) & 0x000000FF);
		}
		return value;
	}

	public int getSize() {
		return data.length;
	}

	public ByteBuffer putInteger(int index, int value) {
		DataLocator dataLocator = dataDescription.locateField(index);
		int offset = dataLocator.getOffset();
		putBytes(offset, java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
		return this;
	}

	public int getInteger(int index) {
		java.nio.ByteBuffer byteBuffer = java.nio.ByteBuffer.allocate(Integer.BYTES);
		byteBuffer.put(get(index)).flip();
		return byteBuffer.getInt();
	}

	public ByteBuffer putLong(int index, long value) {
		DataLocator dataLocator = dataDescription.locateField(index);
		int offset = dataLocator.getOffset();
		putBytes(offset, java.nio.ByteBuffer.allocate(Long.BYTES).putLong(value).array());
		return this;
	}

	public long getLong(int index) {
		java.nio.ByteBuffer byteBuffer = java.nio.ByteBuffer.allocate(Long.BYTES);
		byteBuffer.put(get(index)).flip();
		return byteBuffer.getLong();
	}

	public ByteBuffer putString(int index, String value) {
		DataLocator dataLocator = dataDescription.locateField(index);
		int offset = dataLocator.getOffset();
		put(offset, value.getBytes());
		return this;
	}

	public MutableCharArrayString getString(int index) {
		byte[] stringInBytes = get(index);
		if (mutableCharArrayString == null) {

			return new MutableCharArrayString(removeNonFilledValuesInByteArray(stringInBytes));
		} else {
			mutableCharArrayString.reset();
			mutableCharArrayString.putBytes(stringInBytes);
		}
		return mutableCharArrayString;
	}

	private byte[] removeNonFilledValuesInByteArray(byte[] input) {
		int numberOfNonEmptyBytes = getNumberOfBytesFilledWithData(input);
		java.nio.ByteBuffer byteBuffer = java.nio.ByteBuffer.allocate(numberOfNonEmptyBytes);
		int index = 0;
		while (index < numberOfNonEmptyBytes) {
			byteBuffer.put(input[index]);
			index++;
		}
		return byteBuffer.array();
	}

	private int getNumberOfBytesFilledWithData(int index) {
		return getNumberOfBytesFilledWithData(get(index));
	}

	public ByteBuffer addCharacter(char nextChar, int index) {
		int offsetTillWhatCharactersAreFilled = getNumberOfBytesFilledWithData(index);
		put(offsetTillWhatCharactersAreFilled, (byte) nextChar);
		return this;
	}

	private int getNumberOfBytesFilledWithData(byte[] input) {
		int index = input.length - 1;
		int numberOfNonEmptyBytes = input.length;
		while (index >= 0) {
			if (input[index] == 0) {
				numberOfNonEmptyBytes--;
			} else {
				break;
			}
			index--;
		}
		return numberOfNonEmptyBytes;
	}

	public ByteBuffer putCharacter(int index, char value) {
		DataLocator dataLocator = dataDescription.locateField(index);
		int offset = dataLocator.getOffset();
		put(offset, (byte) value);
		return this;
	}

	public ByteBuffer putCharacters(int index, char[] values) {
		DataLocator dataLocator = dataDescription.locateField(index);
		int offset = dataLocator.getOffset();
		for (char value : values) {
			put(offset, (byte) value);
			offset++;
		}
		return this;
	}

	public char[] getCharacters(int index) {
		byte[] bytes = get(index);
		byte[] charBytes = removeNonFilledValuesInByteArray(bytes);
		char[] characters = new char[charBytes.length];
		for (int i = 0; i < charBytes.length; i++) {
			characters[i] = (char) charBytes[i];
		}
		return characters;
	}

	public ByteBuffer putDouble(int index, double value) {
		DataLocator dataLocator = dataDescription.locateField(index);
		int offset = dataLocator.getOffset();
		putBytes(offset, java.nio.ByteBuffer.allocate(Double.BYTES).putDouble(value).array());
		return this;
	}

	public double getDouble(int index) {
		java.nio.ByteBuffer byteBuffer = java.nio.ByteBuffer.allocate(Double.BYTES);
		byteBuffer.put(get(index)).flip();
		return byteBuffer.getDouble();
	}

	public ByteBuffer putFloat(int index, float value) {
		DataLocator dataLocator = dataDescription.locateField(index);
		int offset = dataLocator.getOffset();
		putBytes(offset, java.nio.ByteBuffer.allocate(Float.BYTES).putFloat(value).array());
		return this;
	}

	public float getFloat(int index) {
		java.nio.ByteBuffer byteBuffer = java.nio.ByteBuffer.allocate(Float.BYTES);
		byteBuffer.put(get(index)).flip();
		return byteBuffer.getFloat();
	}

	@Override
	public boolean equals(Object otherObject) {
		if (this == otherObject) {
			return true;
		}
		if (otherObject instanceof ByteBuffer) {
			ByteBuffer otherByteBuffer = (ByteBuffer) otherObject;
			if (otherByteBuffer.data != null && data != null) {
				return Arrays.equals(otherByteBuffer.data, data);
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		int hashCode = 0;
		if (data != null) {
			int off = 0;
			for (int i = 0; i < data.length; i++) {
				hashCode = 31 * hashCode + data[off++];
			}
			return hashCode;
		}
		return hashCode;
	}

	public int getSizeOfDataAtIndex(int index) {
		DataLocator dataLocator = dataDescription.locateField(index);
		return dataLocator.getSize();
	}

	public void reset() {
		if (data != null) {
			for (int i = 0; i < data.length; i++) {
				data[i] = 0;
			}
		}
	}
}
