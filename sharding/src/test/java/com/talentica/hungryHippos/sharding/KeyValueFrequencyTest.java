package com.talentica.hungryHippos.sharding;

import org.junit.Assert;
import org.junit.Test;

import com.talentica.hungryHippos.utility.marshaling.MutableCharArrayString;

public class KeyValueFrequencyTest{
	
	@Test
	public void testEquals() {
		MutableCharArrayString mutableCharArrayStringL1 = new MutableCharArrayString(1);
		mutableCharArrayStringL1.addCharacter('l');
		KeyValueFrequency keyValue1frequency1 = new KeyValueFrequency(mutableCharArrayStringL1, 10);
		MutableCharArrayString mutableCharArrayStringL2 = new MutableCharArrayString(1);
		mutableCharArrayStringL2.addCharacter('l');
		KeyValueFrequency keyValue1frequency2 = new KeyValueFrequency(mutableCharArrayStringL2, 10);
		Assert.assertEquals(keyValue1frequency1.hashCode(), keyValue1frequency2.hashCode());
		Assert.assertTrue(keyValue1frequency1.equals(keyValue1frequency2));
		Assert.assertTrue(keyValue1frequency2.equals(keyValue1frequency1));
	}
    
}
