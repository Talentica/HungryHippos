package com.talentica.hungryHippos.client.domain;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataLocator.DataType;

public class MutableIntegerTest {

  private MutableInteger integerL1;

  private MutableInteger integerL2;
  @Before
  public void setUp() throws Exception {
    FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription(50);
    dataDescription.addFieldType(DataType.DOUBLE, 1);
    integerL1 = new MutableInteger(8);
    integerL1.addByte((byte) 1);
    integerL1.addByte((byte) '.');
    integerL1.addByte((byte) 0);
    integerL1.addByte((byte) 3);
    integerL1.addByte((byte) 4);
    integerL1.addByte((byte) 5);
    integerL2 = new MutableInteger(6);
    integerL2.addByte((byte) 1);
    integerL2.addByte((byte) '.');
    integerL2.addByte((byte) 0);
    integerL2.addByte((byte) 3);
    integerL2.addByte((byte) 4);
    integerL2.addByte((byte) 5);
  }

  @After
  public void tearDown() throws Exception {

    integerL1 = null;
    integerL2 = null;
  }


  @Test
  public void testHashCode() {
    int hash1 = integerL1.hashCode();
    int hash2 = integerL2.hashCode();
    assertEquals(hash1, hash2);
  }

  @Test
  public void testMutableInteger() {
    MutableInteger localInteger = new MutableInteger(6);
    localInteger.addByte((byte) 1);
    localInteger.addByte((byte) '.');
    localInteger.addByte((byte) 0);
    localInteger.addByte((byte) 3);
    localInteger.addByte((byte) 4);
    localInteger.addByte((byte) 5);
    assertNotNull(localInteger);
    assertEquals(6, localInteger.getLength());
  }

  @Test
  public void testGetLength() {
    assertEquals(6, integerL1.getLength());
    assertEquals(6, integerL2.getLength());
  }

  @Test
  public void testByteAt() {
    byte digit =  integerL1.byteAt(0);
    assertEquals(digit, 1);
  }

  @Test
  public void testGetUnderlyingArray() {
   byte[] byteArray = integerL1.getUnderlyingArray();
   assertNotNull(byteArray);
  }

  @Test
  public void testToString() {
    String s = integerL1.toString();
   System.out.println(s); 
   assertNotNull(s);
  }

  @Test
  public void testAddByte() {
    int sizeBeforeAdding = integerL1.getLength();
    MutableInteger localInteger = integerL1.addByte((byte)'9');
    int sizeAfterAddingByte = localInteger.getLength();
    assertEquals(sizeBeforeAdding+1, sizeAfterAddingByte);
  }

  @Test
  public void testReset() {
    int length = integerL1.getLength();
    integerL1.reset();
    int length1 = integerL1.getLength();
    assertNotEquals(length, length1);
  }

  @Test
  public void testClone() {
    MutableInteger localInteger = integerL1.clone();
    assertNotNull(localInteger);
  }

  @Test
  public void testEqualsObject() {
    assertTrue(integerL1.equals(integerL2));
  }

  @Test
  public void testCompareTo() {
    int a = integerL1.compareTo(integerL2);
    assertEquals(0,a);
  }

}
