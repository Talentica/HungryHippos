// package com.talentica.hungryHippos.sharding;
//
// import org.junit.Assert;
// import org.junit.Test;
//
// import com.talentica.hungryHippos.client.domain.ByteBuffer;
// import
// com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
// import com.talentica.hungryHippos.client.domain.MutableCharArrayString;
// import com.talentica.hungryHippos.utility.Property;
//
// public class KeyValueFrequencyTest {
//
// @Test
// public void testEquals() {
// FieldTypeArrayDataDescription dataDescription = FieldTypeArrayDataDescription
// .createDataDescription(Property.getDataTypeConfiguration());
// dataDescription.setKeyOrder(Property.getKeyOrder());
// ByteBuffer byteBuffer1 = new ByteBuffer(dataDescription);
// byteBuffer1.put(0, (byte) 'l');
// MutableCharArrayString mutableCharArrayStringL1 = new
// MutableCharArrayString(byteBuffer1, 0);
// KeyValueFrequency keyValue1frequency1 = new
// KeyValueFrequency(mutableCharArrayStringL1, 10);
// ByteBuffer byteBuffer2 = new ByteBuffer(dataDescription);
// MutableCharArrayString mutableCharArrayStringL2 = new
// MutableCharArrayString(byteBuffer2, 0);
// byteBuffer2.put(0, (byte) 'l');
// KeyValueFrequency keyValue1frequency2 = new
// KeyValueFrequency(mutableCharArrayStringL2, 10);
// Assert.assertEquals(keyValue1frequency1.hashCode(),
// keyValue1frequency2.hashCode());
// Assert.assertTrue(keyValue1frequency1.equals(keyValue1frequency2));
// Assert.assertTrue(keyValue1frequency2.equals(keyValue1frequency1));
// }
//
// }
