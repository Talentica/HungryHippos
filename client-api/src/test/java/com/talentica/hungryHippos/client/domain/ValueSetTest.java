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
import org.junit.Test;


public class ValueSetTest {

	@Test
	public void testToString() {
		ValueSet valueSet = new ValueSet(new int[] { 0, 1 }, new String[] { "India", "Sony" });
		String actual = valueSet.toString();
		Assert.assertEquals("India-Sony", actual);
	}
	
	@Test
	public void testValueSetCompare(){
	  MutableCharArrayString string1 = new MutableCharArrayString(1);
	  string1.addByte((byte)'a');
	  MutableCharArrayString string2 = new MutableCharArrayString(1);
	  string2.addByte((byte)'a');
	  MutableCharArrayString string3 = new MutableCharArrayString(1);
	  string3.addByte((byte)3);
	  ValueSet valueSet1 = new ValueSet(new int[] { 0, 1, 3 }, new MutableCharArrayString[] { string1, string2 , string3});
	  MutableCharArrayString string4 = new MutableCharArrayString(1);
      string4.addByte((byte)'a');
      MutableCharArrayString string5 = new MutableCharArrayString(1);
      string5.addByte((byte)'a');
      MutableCharArrayString string6 = new MutableCharArrayString(1);
      string6.addByte((byte)5);
	  ValueSet valueSet2 = new ValueSet(new int[] { 0, 1, 3 }, new MutableCharArrayString[] { string4, string5,string6 });
	  Map<ValueSet,String> valueSetMap = new HashMap<>();
	  valueSetMap.put(valueSet1, "testString1");
	  valueSetMap.put(valueSet2, "testString2");
	  Assert.assertEquals("testString1", valueSetMap.get(valueSet1));
	  Assert.assertEquals("testString2", valueSetMap.get(valueSet2));
	}

	@Test
	public void testEquals() {
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new String[] { "India", "Sony" });
		ValueSet valueSet2 = new ValueSet(new int[] { 0, 1 }, new String[] { "India", "Sony" });
		Assert.assertEquals(valueSet1, valueSet2);
	}

	@Test
	public void testEqualsForDifferentIndexes() {
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new String[] { "India", "Sony" });
		ValueSet valueSet2 = new ValueSet(new int[] { 1, 0 }, new String[] { "India", "Sony" });
		Assert.assertNotEquals(valueSet1, valueSet2);
	}

	@Test
	public void testCompareToOfNotSameLengthValueSets() {
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new String[] { "India", "Sony" });
		ValueSet valueSet2 = new ValueSet(new int[] { 1 }, new String[] { "India" });
		Assert.assertTrue(valueSet1.compareTo(valueSet2) > 0);
		Assert.assertTrue(valueSet2.compareTo(valueSet1) < 0);
	}

	@Test
	public void testCompareToOfSameValueSets() {
		MutableCharArrayString string1 = new MutableCharArrayString(2);
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new MutableCharArrayString[] { string1, string1 });
		ValueSet valueSet2 = new ValueSet(new int[] { 0, 1 }, new MutableCharArrayString[] { string1, string1 });
		Assert.assertTrue(valueSet1.compareTo(valueSet2) == 0);
		Assert.assertTrue(valueSet2.compareTo(valueSet1) == 0);
	}

	@Test
	public void testCompareToOfValueSetsOfSameSize() throws InvalidRowException {
		MutableCharArrayString country1 = new MutableCharArrayString(3);
		country1.addByte((byte)'I').addByte((byte)'n').addByte((byte)'d');
		MutableCharArrayString device1 = new MutableCharArrayString(3);
		device1.addByte((byte)'S').addByte((byte)'a').addByte((byte)'m');
		ValueSet valueSet1 = new ValueSet(new int[] { 0, 1 }, new MutableCharArrayString[] { country1, device1 });

		MutableCharArrayString country2 = new MutableCharArrayString(3);
		country2.addByte((byte)'I').addByte((byte)'n').addByte((byte)'d');
		MutableCharArrayString device2 = new MutableCharArrayString(3);
		device2.addByte((byte)'S').addByte((byte)'o').addByte((byte)'n');
		ValueSet valueSet2 = new ValueSet(new int[] { 0, 1 }, new MutableCharArrayString[] { country2, device2 });

		Assert.assertTrue(valueSet1.compareTo(valueSet2) < 0);
		Assert.assertTrue(valueSet2.compareTo(valueSet1) > 0);
	}

}
