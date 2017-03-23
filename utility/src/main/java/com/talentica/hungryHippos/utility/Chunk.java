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
package com.talentica.hungryHippos.utility;

public class Chunk {

  private String parentFilePath;
  private String parentFileName;
  private String name;
  private int id;
  private long start;
  private long end;
  private long idealSizeOfChunk;
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
    this.name = name + "-" + this.id;
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
