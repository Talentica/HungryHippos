package com.talentica.hungryHippos.utility;

public class Chunk {

  private String parentFilePath;
  private String parentFileName;
  private String name;
  private int id;
  private long start;
  private long end;
  private long idealSizeOfChunk;
  private String fileType = null;
  private HHFStream hhfsStream;

  public Chunk(String parentFilePath, String fileName, int id, long start, long end,
      long idealSizeOfChunk) {
    super();
    this.parentFilePath = parentFilePath;
    this.id = id;
    this.setDetails(fileName);
    this.start = start;
    this.end = end;
    this.idealSizeOfChunk = idealSizeOfChunk;

  }

  public String getFileType() {
    return this.fileType;
  }

  public String getParentFilePath() {
    return parentFilePath;
  }



  public String getFileName() {
    return name;
  }

  public int getId() {
    return id;
  }


  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }



  public long getIdealSizeOfChunk() {
    return idealSizeOfChunk;
  }


  public long getActualSizeOfChunk() {
    return end - start;
  }

  public HHFStream getHHFStream() {
    if (this.hhfsStream != null) {
      return hhfsStream;
    }
    this.hhfsStream = new HHFStream(this);
    return this.hhfsStream;
  }

  public String getParentFileName() {
    return this.parentFileName;
  }

  private void setDetails(String name) {
    this.parentFileName = name;
    String[] str = name.split("\\.");
    this.name = str[0] + "-" + this.id;
    this.fileType = str[1];

  }

  @Override
  public String toString() {

    StringBuilder sb = new StringBuilder();
    sb.append("ParentFilePath : " + parentFilePath);
    sb.append("\n");
    sb.append("ParentFileName : " + parentFileName);
    sb.append("\n");
    sb.append("name : " + name);
    sb.append("\n");
    sb.append("id : " + id);
    sb.append("\n");
    sb.append("start : " + start);
    sb.append("\n");
    sb.append("end : " + end);
    sb.append("\n");
    sb.append("idealSizeOfChunk : " + idealSizeOfChunk + "in bytes");
    sb.append("\n");
    sb.append("actualSizeOfChunk : " + getActualSizeOfChunk() + "in bytes");
    sb.append("\n");

    return sb.toString();


  }



}
