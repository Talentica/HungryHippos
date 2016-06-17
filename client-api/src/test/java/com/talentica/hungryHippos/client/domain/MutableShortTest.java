package com.talentica.hungryHippos.client.domain;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.talentica.hungryHippos.client.domain.DataLocator.DataType;

public class MutableShortTest {

  private MutableShort shortL1;

  private MutableShort shortL2;

  @Before
  public void setUp() throws Exception {
    FieldTypeArrayDataDescription dataDescription = new FieldTypeArrayDataDescription(50);
    dataDescription.addFieldType(DataType.DOUBLE, 1);
    shortL1 = new MutableShort(8);
    shortL1.addByte((byte) 1);
    shortL1.addByte((byte) '.');
    shortL1.addByte((byte) 0);
    shortL1.addByte((byte) 3);
    shortL1.addByte((byte) 4);
    shortL1.addByte((byte) 5);
    shortL2 = new MutableShort(6);
    shortL2.addByte((byte) 1);
    shortL2.addByte((byte) '.');
    shortL2.addByte((byte) 0);
    shortL2.addByte((byte) 3);
    shortL2.addByte((byte) 4);
    shortL2.addByte((byte) 5);
  }

  @After
  public void tearDown() throws Exception {
    shortL1 = null;
    shortL2 = null;
  }


  @Test
  public void testMutableShort() {
    MutableShort localShort = new MutableShort(6);
    localShort.addByte((byte) 1);
    localShort.addByte((byte) '.');
    localShort.addByte((byte) 0);
    localShort.addByte((byte) 3);
    localShort.addByte((byte) 4);
    localShort.addByte((byte) 5);
    assertNotNull(localShort);
    assertEquals(6, localShort.getLength());
  }

  @Test
  public void testHashCode() {
    int hash1 = shortL1.hashCode();
    int hash2 = shortL2.hashCode();
    assertEquals(hash1, hash2);
  }

  @Test
  public void testGetLength() {
    assertEquals(6, shortL1.getLength());
    assertEquals(6, shortL2.getLength());
  }

  @Test
  public void testByteAt() {
    byte digit = shortL1.byteAt(0);
    assertEquals(digit, 1);
  }

  @Test
  public void testGetUnderlyingArray() {
    byte[] byteArray = shortL1.getUnderlyingArray();
    assertNotNull(byteArray);
  }

  @Test
  public void testToString() {
    String s = shortL1.toString();
    System.out.println(s);
    assertNotNull(s);
  }

  @Test
  public void testAddByte() {
    int sizeBeforeAdding = shortL1.getLength();
    MutableShort localShort = shortL1.addByte((byte) '9');
    int sizeAfterAddingByte = localShort.getLength();
    assertEquals(sizeBeforeAdding + 1, sizeAfterAddingByte);
  }

  @Test
  public void testReset() {
    int length = shortL1.getLength();
    shortL1.reset();
    int length1 = shortL1.getLength();
    assertNotEquals(length, length1);
  }

  @Test
  public void testClone() {
    MutableShort localShort = shortL1.clone();
    assertNotNull(localShort);
  }

  @Test
  public void testEqualsObject() {
    assertTrue(shortL1.equals(shortL2));
  }

  @Test
  public void testCompareTo() {
    int a = shortL1.compareTo(shortL2);
    assertEquals(0, a);
  }
}
