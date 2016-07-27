package com.talentica.hungryhippos.filesystem.checksum;

public class Block {
  private int blockId;
  private String fileName;
  private String path;
  private int blockSize; // in MB
  private int numberOfLines;
  private String[] shardedKey;
  private String[] BucketName;
  private long checkSum;

  public int getBlockId() {
    return blockId;
  }

  public void setBlockId(int blockId) {
    this.blockId = blockId;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public void setBlockSize(int blockSize) {
    this.blockSize = blockSize;
  }

  public int getNumberOfLines() {
    return numberOfLines;
  }

  public void setNumberOfLines(int numberOfLines) {
    this.numberOfLines = numberOfLines;
  }

  public String[] getShardedKey() {
    return shardedKey;
  }

  public void setShardedKey(String[] shardedKey) {
    this.shardedKey = shardedKey;
  }

  public String[] getBucketName() {
    return BucketName;
  }

  public void setBucketName(String[] bucketName) {
    BucketName = bucketName;
  }

  public long getCheckSum() {
    return this.checkSum;
  }

  public void setCheckSum(long checkSum) {
    this.checkSum = checkSum;
  }


}
