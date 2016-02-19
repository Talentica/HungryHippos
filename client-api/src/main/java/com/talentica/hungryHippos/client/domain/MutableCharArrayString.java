package com.talentica.hungryHippos.client.domain;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by debasishc on 29/9/15.
 */
public class MutableCharArrayString implements CharSequence, Cloneable, Serializable {

	private static final long serialVersionUID = -6085804645372631875L;
	private int stringLength;
	private int[] data;
	private final java.nio.ByteBuffer _INT_SIZE_BYTE_BUFFER = java.nio.ByteBuffer.allocate(Integer.BYTES);

	public MutableCharArrayString(int length) {
		int noOfIntegersNeededToStoreData = length / Integer.BYTES;
		if (length % Integer.BYTES != 0) {
			noOfIntegersNeededToStoreData++;
		}
		data = new int[noOfIntegersNeededToStoreData];
		stringLength = 0;
	}

	public MutableCharArrayString(byte[] bytes) {
		this(bytes.length);
		putBytes(bytes);
	}

	@Override
	public int length() {
		return stringLength;
	}

	@Override
	public char charAt(int index) {
		return (char) getByteAt(index);
	}

	private byte getByteAt(int index) {
		int indexInDataArray = index / Integer.BYTES;
		int integerToReadBytesFrom = data[indexInDataArray];
		int numberOfBitsToShift = (Integer.BYTES - index % Integer.BYTES - 1) * Byte.SIZE;
		return (byte) ((integerToReadBytesFrom >>> numberOfBitsToShift) & 0x000000FF);
	}

	public byte[] getBytes() {
		byte[] value = new byte[stringLength];
		for (int i = 0; i < stringLength; i++) {
			value[i] = getByteAt(i);
		}
		return value;
	}

	@Override
	public MutableCharArrayString subSequence(int start, int end) {
		MutableCharArrayString newArray = new MutableCharArrayString(end - start);
		for (int i = start, j = 0; i < end; i++, j++) {
			newArray.put(j, (byte) charAt(i));
		}
		newArray.stringLength = end - start;
		return newArray;
	}

	@Override
	public String toString() {
		return new String(Arrays.copyOf(getBytes(), stringLength));
	}

	public void addCharacter(char ch) {
		putByte((byte) ch);
	}

	void putBytes(byte[] bytes) {
		for (byte byteToAdd : bytes) {
			putByte(byteToAdd);
		}
	}

	private void putByte(byte byteToPut) {
		put(stringLength, byteToPut);
		stringLength++;
	}

	private void put(int offset, byte byteToPut) {
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
	}

	private void clearByteBuffer() {
		_INT_SIZE_BYTE_BUFFER.clear();
		_INT_SIZE_BYTE_BUFFER.position(0);
	}

	public void reset() {
		stringLength = 0;
	}

	@Override
	public MutableCharArrayString clone() {
		return subSequence(0, stringLength);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || !(o instanceof MutableCharArrayString)) {
			return false;
		}
		MutableCharArrayString that = (MutableCharArrayString) o;
		if (stringLength == that.stringLength && data != null && that.data != null && data.length == that.data.length) {
			for (int i = 0; i < data.length; i++) {
				if (data[i] != that.data[i]) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	@Override
	public int hashCode() {
		int h = 0;
		int off = 0;
		int val[] = data;
		for (int i = 0; i < data.length; i++) {
			h = 31 * h + val[off++];
		}
		return h;
	}

	public static MutableCharArrayString from(String value) {
		MutableCharArrayString mutableCharArrayString = new MutableCharArrayString(value.length());
		for (char character : value.toCharArray()) {
			mutableCharArrayString.addCharacter(character);
		}
		return mutableCharArrayString;
	}
}
