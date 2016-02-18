package com.talentica.hungryHippos.client.domain;

import java.io.Serializable;
import java.util.Arrays;

import com.talentica.hungryHippos.client.domain.DataLocator.DataType;

/**
 * Created by debasishc on 29/9/15.
 */
public class MutableCharArrayString implements CharSequence, Cloneable, Serializable {

	private static final long serialVersionUID = -6085804645372631875L;
	private ByteBuffer byteBuffer;
	private int byteArrayDataIndexOfString = 0;

	public MutableCharArrayString(ByteBuffer byteBuffer) {
		this.byteBuffer = byteBuffer;
    }

	public MutableCharArrayString(ByteBuffer byteBuffer, int bytebufferStringIndex) {
		this.byteBuffer = byteBuffer;
		this.byteArrayDataIndexOfString = bytebufferStringIndex;
	}

    @Override
    public int length() {
		return byteBuffer.getString(byteArrayDataIndexOfString).length();
    }

    @Override
    public char charAt(int index) {
		return byteBuffer.getCharacters(byteArrayDataIndexOfString)[index];
    }

	private char[] getUnderlyingArray() {
		return byteBuffer.getCharacters(byteArrayDataIndexOfString);
    }

    @Override
    public MutableCharArrayString subSequence(int start, int end) {
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
		dataDescription.addFieldType(DataType.STRING, end - start);
		ByteBuffer byteBuffer = new ByteBuffer(dataDescription);
		MutableCharArrayString newArray = new MutableCharArrayString(byteBuffer);
		char[] characters = getUnderlyingArray();
		for (int i = 0; i < end - start; i++) {
			byteBuffer.addCharacter(characters[i], byteArrayDataIndexOfString);
        }
        return newArray;
    }

    @Override
    public String toString() {
		char[] underlyingArray = getUnderlyingArray();
		return new String(Arrays.copyOf(underlyingArray, underlyingArray.length));
    }

    @Override
	public MutableCharArrayString clone(){
		return subSequence(0, getUnderlyingArray().length);
    }

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || !(o instanceof MutableCharArrayString)) {
			return false;
		}
		MutableCharArrayString that = (MutableCharArrayString) o;
		if(that.byteBuffer!=null && byteBuffer!=null){
			return that.byteBuffer.equals(byteBuffer);
		}
		return false;
	}

    @Override
    public int hashCode() {
    	if(byteBuffer!=null){
    		return byteBuffer.hashCode();
    	}
    	return super.hashCode();
    }

	public static MutableCharArrayString from(String value) {
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription();
		dataDescription.addFieldType(com.talentica.hungryHippos.client.domain.DataLocator.DataType.STRING,
				value.length());
		int index = 0;
		ByteBuffer byteBuffer = new ByteBuffer(dataDescription);
		for (char character : value.toCharArray()) {
			byteBuffer.putCharacter(index, character);
			index++;
		}
		MutableCharArrayString mutableCharArrayString = new MutableCharArrayString(byteBuffer);
		return mutableCharArrayString;
	}

	public void addCharacter(char charToAdd) {
		byteBuffer.addCharacter(charToAdd, byteArrayDataIndexOfString);
	}
}
