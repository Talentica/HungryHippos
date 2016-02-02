package com.talentica.hungryHippos.client.domain;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by debasishc on 29/9/15.
 */
public class MutableCharArrayString implements CharSequence, Cloneable, Serializable {

	private static final long serialVersionUID = -6085804645372631875L;
	private char[] array;
	private int stringLength;

    public MutableCharArrayString(int length){
        array = new char[length];
        stringLength = 0;
    }
    @Override
    public int length() {
		return stringLength;
    }

    @Override
    public char charAt(int index) {
        return array[index];
    }

    public char [] getUnderlyingArray(){
        return array;
    }

    @Override
    public MutableCharArrayString subSequence(int start, int end) {
        MutableCharArrayString newArray = new MutableCharArrayString(end -start);
        for(int i=start,j=0;i<end;i++,j++){
            newArray.array[j] = array[i];
        }
        newArray.stringLength = end - start;
        return newArray;
    }

    @Override
    public String toString() {
		return new String(Arrays.copyOf(array, stringLength));
    }

    public void addCharacter(char ch){
        array[stringLength]=ch;
        stringLength++;
    }

    public void reset(){
        stringLength=0;
    }


    @Override
	public MutableCharArrayString clone(){
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
		if (stringLength == that.stringLength) {
			for (int i = 0; i < stringLength; i++) {
				if (array[i] != that.array[i]) {
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
        char val[] = array;
        int len = stringLength;
        for (int i = 0; i < len; i++) {
            h = 31*h + val[off++];
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
