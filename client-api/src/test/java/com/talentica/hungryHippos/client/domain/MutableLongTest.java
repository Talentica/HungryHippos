package com.talentica.hungryHippos.client.domain;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataLocator.DataType;


public class MutableLongTest {

  private MutableLong longL1;

  private MutableLong longL2;
  @Before
  public void setUp() throws Exception {
    FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription(50);
    dataDescription.addFieldType(DataType.DOUBLE, 1);
    longL1 = new MutableLong(8);
    longL1.addByte((byte) 1);
    longL1.addByte((byte) '.');
    longL1.addByte((byte) 0);
    longL1.addByte((byte) 3);
    longL1.addByte((byte) 4);
    longL1.addByte((byte) 5);
    longL2 = new MutableLong(6);
    longL2.addByte((byte) 1);
    longL2.addByte((byte) '.');
    longL2.addByte((byte) 0);
    longL2.addByte((byte) 3);
    longL2.addByte((byte) 4);
    longL2.addByte((byte) 5);
  }

  @After
  public void tearDown() throws Exception {
    longL1 = null;
    longL2 = null;
  }


  @Test
  public void testMutableLong() {
    MutableLong localLong = new MutableLong(6);
    localLong.addByte((byte) 1);
    localLong.addByte((byte) '.');
    localLong.addByte((byte) 0);
    localLong.addByte((byte) 3);
    localLong.addByte((byte) 4);
    localLong.addByte((byte) 5);
    assertNotNull(localLong);
    assertEquals(6, localLong.getLength());
  }

  @Test
  public void testHashCode() {
    int hash1 = longL1.hashCode();
    int hash2 = longL2.hashCode();
    assertEquals(hash1, hash2);
  }
  
  @Test
  public void testGetLength() {
    assertEquals(6, longL1.getLength());
    assertEquals(6, longL2.getLength());
  }

  @Test
  public void testByteAt() {
    byte digit =  longL1.byteAt(0);
    assertEquals(digit, 1);
  }

  @Test
  public void testGetUnderlyingArray() {
   byte[] byteArray = longL1.getUnderlyingArray();
   assertNotNull(byteArray);
  }

  @Test
  public void testToString() {
    String s = longL1.toString();
   System.out.println(s); 
   assertNotNull(s);
  }

  @Test
  public void testAddByte() {
    int sizeBeforeAdding = longL1.getLength();
    MutableLong localLong = longL1.addByte((byte)'9');
    int sizeAfterAddingByte = localLong.getLength();
    assertEquals(sizeBeforeAdding+1, sizeAfterAddingByte);
  }

  @Test
  public void testReset() {
    int length = longL1.getLength();
    longL1.reset();
    int length1 = longL1.getLength();
    assertNotEquals(length, length1);
  }

  @Test
  public void testClone() {
    MutableLong localLong = longL1.clone();
    assertNotNull(localLong);
  }

  @Test
  public void testEqualsObject() {
    assertTrue(longL1.equals(longL2));
  }

  @Test
  public void testCompareTo() {
    int a = longL1.compareTo(longL2);
    assertEquals(0,a);
  }

}
