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
		byteBuffer = new ByteBuffer(dataDescription);
	}

	@Test
	public void testGetSize() {
		Assert.assertEquals(7, byteBuffer.getSize());
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
		String stringToPut = "Testabcd";
		ByteBuffer byteBufferResult = byteBuffer.putString(1, stringToPut);
		String readString = byteBufferResult.getString(1);
		Assert.assertNotNull(readString);
		Assert.assertEquals(stringToPut, readString);
	}

	@Test
	public void testPutAndGetForInteger() {
		int integerToStore = 12345;
		ByteBuffer byteBufferResult = byteBuffer.putInteger(2, integerToStore);
		int readInteger = byteBufferResult.getInteger(2);
		Assert.assertEquals(integerToStore, readInteger);
	}

}
