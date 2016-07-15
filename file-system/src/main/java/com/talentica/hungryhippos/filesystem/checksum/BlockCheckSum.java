package com.talentica.hungryhippos.filesystem.checksum;

import java.util.zip.Checksum;

public class BlockCheckSum implements Checksum {

  private int checkSum = 0;

  public BlockCheckSum() {

  }

  @Override
  public void update(int byteValue) {
    checkSum = checkSum + byteValue & 0xff;
    checkSum = ((checkSum ^ 0xFF) + 1) & 0xFF;
  }

  @Override
  public void update(byte[] block, int start, int length) {

    if (block == null) {
      throw new NullPointerException("byte array can't be null");
    }

    if ((start > length)) {
      throw new IllegalAccessError("off");
    }

    for (int i = start; i < length; i++) {
      checkSum = checkSum + block[i] & 0xff;
    }
    checkSum = ((checkSum ^ 0xFF) + 1) & 0xFF;
  }

  @Override
  public long getValue() {

    return checkSum;
  }

  @Override
  public void reset() {
    this.checkSum = 0;

  }

  public void update(byte[] block) {
    update(block, 0, block.length);
  }

  @Override
  public String toString() {
    return String.valueOf(checkSum);
  }
}
