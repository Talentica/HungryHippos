package com.talentica.hungryHippos.client.domain;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataLocator.DataType;

public class MutableDoubleTest {

  private MutableDouble doubleL1;

  private MutableDouble doubleL2;

  @Before
  public void setUp() throws Exception {
    FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription(50);
    dataDescription.addFieldType(DataType.DOUBLE, 1);
    doubleL1 = new MutableDouble();
    doubleL1.addByte((byte) 1);
    doubleL1.addByte((byte) '.');
    doubleL1.addByte((byte) 0);
    doubleL1.addByte((byte) 3);
    doubleL1.addByte((byte) 4);
    doubleL1.addByte((byte) 5);
    doubleL2 = new MutableDouble();
    doubleL2.addByte((byte) 1);
    doubleL2.addByte((byte) '.');
    doubleL2.addByte((byte) 0);
    doubleL2.addByte((byte) 3);
    doubleL2.addByte((byte) 4);
    doubleL2.addByte((byte) 5);
  }

  @After
  public void tearDown() throws Exception {

    doubleL1 = null;
    doubleL2 = null;
  }

  @Test
  public void testHashCode() {
    int hash1 = doubleL1.hashCode();
    int hash2 = doubleL2.hashCode();
    assertEquals(hash1, hash2);
  }

  @Test
  public void testMutableDouble() {
    MutableDouble localDouble = new MutableDouble();
    localDouble.addByte((byte) 1);
    localDouble.addByte((byte) '.');
    localDouble.addByte((byte) 0);
    localDouble.addByte((byte) 3);
    localDouble.addByte((byte) 4);
    localDouble.addByte((byte) 5);
    assertNotNull(localDouble);
    assertEquals(6, localDouble.getLength());
  }

  @Test
  public void testGetLength() {
    assertEquals(6, doubleL1.getLength());
    assertEquals(6, doubleL2.getLength());
  }

  @Test
  public void testByteAt() {
    byte digit =  doubleL1.byteAt(0);
    assertEquals(digit, 1);
  }

  @Test
  public void testGetUnderlyingArray() {
   byte[] byteArray = doubleL1.getUnderlyingArray();
   assertNotNull(byteArray);
  }

  @Test
  public void testToString() {
    String s = doubleL1.toString();
   System.out.println(s); 
   assertNotNull(s);
  }

  @Test
  public void testAddByte() {
    int sizeBeforeAdding = doubleL1.getLength();
    MutableDouble localDouble = doubleL1.addByte((byte)'9');
    int sizeAfterAddingByte = localDouble.getLength();
    assertEquals(sizeBeforeAdding+1, sizeAfterAddingByte);
  }

  @Test
  public void testReset() {
    int length = doubleL1.getLength();
    doubleL1.reset();
    int length1 = doubleL1.getLength();
    assertNotEquals(length, length1);
  }

  @Test
  public void testClone() {
    MutableDouble localDouble = doubleL1.clone();
    assertNotNull(localDouble);
  }

  @Test
  public void testEqualsObject() {
    assertTrue(doubleL1.equals(doubleL2));
  }

  @Test
  public void testCompareTo() {
    int a = doubleL1.compareTo(doubleL2);
    assertEquals(0,a);
  }

}
