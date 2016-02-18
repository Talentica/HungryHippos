package com.talentica.hungryHippos.utility;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.utility.marshaling.DataLocator.DataType;
import com.talentica.hungryHippos.utility.marshaling.FieldTypeArrayDataDescription;

public class ByteBufferTest {

	private ByteBuffer byteBuffer;

	@Before
	public void setup() {
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
		dataDescription.addFieldType(DataType.CHAR, 2);
		dataDescription.addFieldType(DataType.STRING, 8);
		dataDescription.addFieldType(DataType.INT, Integer.BYTES);
		dataDescription.addFieldType(DataType.DOUBLE, Double.BYTES);
		dataDescription.addFieldType(DataType.FLOAT, Float.BYTES);
		dataDescription.addFieldType(DataType.LONG, Long.BYTES);
		byteBuffer = new ByteBuffer(dataDescription);
	}

	@Test
	public void testGetSize() {
		Assert.assertEquals(9, byteBuffer.getSize());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPutIfIndexExceedsMaxIndexOfByteBuffer() {
		byteBuffer.put(100, new byte[] { (byte) 'a' });
	}

	@Test
	public void testPutAndGetForCharacters() {
		char[] charactersToStore = new char[] { 'a', 'b' };
		ByteBuffer byteBufferResult = byteBuffer.putCharacters(0, charactersToStore);
		char[] readCharacters = byteBufferResult.getCharacters(0);
		Assert.assertNotNull(readCharacters);
		Assert.assertTrue(Arrays.equals(charactersToStore, readCharacters));
	}

	@Test
	public void testPutAndGetForString() {
		String stringToPut = "Test";
		ByteBuffer byteBufferResult = byteBuffer.putString(1, stringToPut);
		String readString = byteBufferResult.getString(1);
		Assert.assertNotNull(readString);
		Assert.assertEquals(stringToPut, readString);
	}

	@Test
	public void testPutAndGetForPostiveInteger() {
		int integerToStore = 345345898;
		ByteBuffer byteBufferResult = byteBuffer.putInteger(2, integerToStore);
		int readInteger = byteBufferResult.getInteger(2);
		Assert.assertEquals(integerToStore, readInteger);
	}

	@Test
	public void testPutAndGetForNegativeInteger() {
		int integerToStore = -345345898;
		ByteBuffer byteBufferResult = byteBuffer.putInteger(2, integerToStore);
		int readInteger = byteBufferResult.getInteger(2);
		Assert.assertEquals(integerToStore, readInteger);
	}

	@Test
	public void testPutAndGetForDouble() {
		double doubleToStore = 1123123123123123123132.12121212d;
		ByteBuffer byteBufferResult = byteBuffer.putDouble(3, doubleToStore);
		double readDouble = byteBufferResult.getDouble(3);
		Assert.assertEquals(doubleToStore, readDouble, 1e20);
	}

	@Test
	public void testPutAndGetForFloat() {
		float floatToStore = 1123123123123123123132.12121212f;
		ByteBuffer byteBufferResult = byteBuffer.putFloat(4, floatToStore);
		float readFloat = byteBufferResult.getFloat(4);
		Assert.assertEquals(floatToStore, readFloat, 1e19);
	}

	@Test
	public void testPutAndGetForNegativeFloat() {
		float floatToStore = -1123123123123123123132.12121212f;
		ByteBuffer byteBufferResult = byteBuffer.putFloat(4, floatToStore);
		float readFloat = byteBufferResult.getFloat(4);
		Assert.assertEquals(floatToStore, readFloat, 1e19);
	}

	public static void main(String[] args) {
		System.out.println(1e-1);
	}

	@Test
	public void testPutAndGetForLong() {
		long longToStore = 12345213123123L;
		ByteBuffer byteBufferResult = byteBuffer.putLong(5, longToStore);
		long readLong = byteBufferResult.getLong(5);
		Assert.assertEquals(longToStore, readLong);
	}

}
