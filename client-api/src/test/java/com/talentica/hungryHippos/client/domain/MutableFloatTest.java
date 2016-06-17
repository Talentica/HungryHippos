package com.talentica.hungryHippos.client.domain;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataLocator.DataType;

public class MutableFloatTest {

  private MutableFloat floatL1;

  private MutableFloat floatL2;
  
  @Before
  public void setUp() throws Exception {
    FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription(50);
    dataDescription.addFieldType(DataType.DOUBLE, 1);
    floatL1 = new MutableFloat(8);
    floatL1.addByte((byte) 1);
    floatL1.addByte((byte) '.');
    floatL1.addByte((byte) 0);
    floatL1.addByte((byte) 3);
    floatL1.addByte((byte) 4);
    floatL1.addByte((byte) 5);
    floatL2 = new MutableFloat(6);
    floatL2.addByte((byte) 1);
    floatL2.addByte((byte) '.');
    floatL2.addByte((byte) 0);
    floatL2.addByte((byte) 3);
    floatL2.addByte((byte) 4);
    floatL2.addByte((byte) 5);
  }

  @After
  public void tearDown() throws Exception {

    floatL1 = null;
    floatL2 = null;
  }


  @Test
  
  public void testMutableFloat() {
    MutableFloat localFloat = new MutableFloat(6);
    localFloat.addByte((byte) 1);
    localFloat.addByte((byte) '.');
    localFloat.addByte((byte) 0);
    localFloat.addByte((byte) 3);
    localFloat.addByte((byte) 4);
    localFloat.addByte((byte) 5);
    assertNotNull(localFloat);
    assertEquals(6, localFloat.getLength());
  }

  @Test
  public void testHashCode() {
    int hash1 = floatL1.hashCode();
    int hash2 = floatL2.hashCode();
    assertEquals(hash1, hash2);
  }
  
  @Test
  public void testGetLength() {
    assertEquals(6, floatL1.getLength());
    assertEquals(6, floatL2.getLength());
  }

  @Test
  public void testByteAt() {
    byte digit =  floatL1.byteAt(0);
    assertEquals(digit, 1);
  }

  @Test
  public void testGetUnderlyingArray() {
   byte[] byteArray = floatL1.getUnderlyingArray();
   assertNotNull(byteArray);
  }

  @Test
  public void testToString() {
    String s = floatL1.toString();
   System.out.println(s); 
   assertNotNull(s);
  }

  @Test
  public void testAddByte() {
    int sizeBeforeAdding = floatL1.getLength();
    MutableFloat localFloat = floatL1.addByte((byte)'9');
    int sizeAfterAddingByte = localFloat.getLength();
    assertEquals(sizeBeforeAdding+1, sizeAfterAddingByte);
  }

  @Test
  public void testReset() {
    int length = floatL1.getLength();
    floatL1.reset();
    int length1 = floatL1.getLength();
    assertNotEquals(length, length1);
  }

  @Test
  public void testClone() {
    MutableFloat localFloat = floatL1.clone();
    assertNotNull(localFloat);
  }

  @Test
  public void testEqualsObject() {
    assertTrue(floatL1.equals(floatL2));
  }

  @Test
  public void testCompareTo() {
    int a = floatL1.compareTo(floatL2);
    assertEquals(0,a);
  }

}
