package com.talentica.hungryhippos.filesystem;
import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
public class HHFileAttributeView implements BasicFileAttributeView {

  @Override
  public String name() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BasicFileAttributes readAttributes() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime)
      throws IOException {
    // TODO Auto-generated method stub
    
  }

}
