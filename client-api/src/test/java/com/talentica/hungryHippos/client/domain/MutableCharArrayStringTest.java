/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.client.domain;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataLocator.DataType;

public class MutableCharArrayStringTest {

	private static final MutableCharArrayStringCache MUTABLE_CHAR_ARRAY_STRING_CACHE = MutableCharArrayStringCache
			.newInstance();

	private MutableCharArrayString stringL1;

	private MutableCharArrayString stringL2;

	@Before
	public void setUp() throws InvalidRowException {
		FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription(50);
		dataDescription.addFieldType(DataType.STRING, 1);
		stringL1 = new MutableCharArrayString(1);
		stringL1.addByte((byte)'l');
		stringL2 = new MutableCharArrayString(1);
		stringL2.addByte((byte)'l');
	}

	@Test
	public void testHashCode() throws InvalidRowException {
		Assert.assertEquals(stringL1.hashCode(), stringL2.hashCode());
		MutableCharArrayString stringForHashCode = new MutableCharArrayString(3);
		stringForHashCode.addByte((byte)'t');
		stringForHashCode.addByte((byte)'e');
		stringForHashCode.addByte((byte)'s');
		int hashcode = stringForHashCode.hashCode();
		Assert.assertTrue(hashcode >= 0);
	}

	@Test
	public void testEquals() {
		Assert.assertTrue(stringL1.equals(stringL1));
		Assert.assertTrue(stringL2.equals(stringL2));
		Assert.assertTrue(stringL1.equals(stringL2));
		Assert.assertTrue(stringL2.equals(stringL1));
	}

	@Test
	public void testClone() throws InvalidRowException {
		MutableCharArrayString testString = createTestString();
		MutableCharArrayString clonedString1 = testString.clone();
		Assert.assertNotNull(clonedString1);
		MutableCharArrayString expectedString1 = new MutableCharArrayString(2);
		expectedString1.addByte((byte)'l');
		expectedString1.addByte((byte)'c');
		Assert.assertEquals(expectedString1, clonedString1);
	}

	@Test
	public void testPutOfMutableCharArrayStringInMapAsKey() throws InvalidRowException {
		MutableCharArrayString testString = createTestString();
		Map<MutableCharArrayString, Integer> mapOfKeyValueFrequencies = new HashMap<>();
		mapOfKeyValueFrequencies.put(testString, 0);
		Integer count = mapOfKeyValueFrequencies.get(testString);
		Assert.assertNotNull(count);
		Assert.assertEquals(0, count.intValue());
	}

	private MutableCharArrayString createTestString() throws InvalidRowException {
		MutableCharArrayString testString = MUTABLE_CHAR_ARRAY_STRING_CACHE.getMutableStringFromCacheOfSize(2);
		testString.addByte((byte)'l');
		testString.addByte((byte)'c');
		return testString;
	}

	@Test
	public void testRetrievalOfMutableCharArrayStringKeyFromMap() throws InvalidRowException {
		MutableCharArrayString testString = createTestString();
		Map<MutableCharArrayString, Integer> mapOfKeyValueFrequencies = new HashMap<>();
		mapOfKeyValueFrequencies.put(testString, 0);
		MutableCharArrayString testStringNew = createTestString();
		mapOfKeyValueFrequencies.put(testStringNew, 5);
		Integer count = mapOfKeyValueFrequencies.get(testString);
		Assert.assertNotNull(count);
		Assert.assertEquals(5, count.intValue());
	}

	@Test
	public void testCompareToOfNonEqualSize() throws InvalidRowException {
		MutableCharArrayString string1 = new MutableCharArrayString(1);
		string1.addByte((byte)'a');
		MutableCharArrayString string2 = new MutableCharArrayString(2);
		string2.addByte((byte)'a');
		string2.addByte((byte)'b');
		Assert.assertTrue(string1.compareTo(string2) < 0);
		Assert.assertTrue(string2.compareTo(string1) > 0);
	}

	@Test
	public void testCompareToOfEqualSizeStrings() throws InvalidRowException {
		MutableCharArrayString string1 = new MutableCharArrayString(2);
		string1.addByte((byte)'a');
		string1.addByte((byte)'c');
		MutableCharArrayString string2 = new MutableCharArrayString(2);
		string2.addByte((byte)'a');
		string2.addByte((byte)'d');
		Assert.assertTrue(string1.compareTo(string2) < 0);
		Assert.assertTrue(string2.compareTo(string1) > 0);
	}

	@Test
	public void testCompareToOfEqualStrings() throws InvalidRowException {
		MutableCharArrayString string1 = new MutableCharArrayString(2);
		string1.addByte((byte)'a');
		string1.addByte((byte)'d');
		MutableCharArrayString string2 = new MutableCharArrayString(2);
		string2.addByte((byte)'a');
		string2.addByte((byte)'d');
		Assert.assertTrue(string1.compareTo(string2) == 0);
		Assert.assertTrue(string2.compareTo(string1) == 0);
	}

}
