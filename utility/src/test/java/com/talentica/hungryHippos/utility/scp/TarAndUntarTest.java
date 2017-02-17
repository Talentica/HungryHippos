package com.talentica.hungryHippos.utility.scp;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TarAndUntarTest {

  private final String sourceFolder = "/home/sohanc/D_drive/dataSet";
  private final String destination = "/home/sohanc/D_drive/dataSet/result.tar";
  private final String tarFile = "/home/sohanc/D_drive/dataSet/result.tar";
  private String destinationFolder = "/home/sohanc/D_drive/untarFolder";
  private Set<String> fileNames;
  
  @Before
  public void setUp(){
    fileNames = new HashSet<String>();
    fileNames.add("e.txt");
    fileNames.add("p.txt");
  }
  
  @Test
  @Ignore
  public void createTarTest() throws IOException{
    TarAndUntar.createTar(sourceFolder, fileNames, destination);
  }
  
  @Test
  public void untarTest() throws IOException{
    TarAndUntar.untar(tarFile, destinationFolder);
  }
}
