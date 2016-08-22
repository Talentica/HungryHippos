package com.talentica.hungryHippos.utility.scp;

import java.io.File;
import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

public class TarAndGZipTest {

  private static final File file = new File("/home/sohanc/ScpAndGzip/sharding-files");
  
  private static final String sourceFilePath = "/home/sohanc/ScpAndGzip/sharding-files.tar.gz";
  
  @Test
  @Ignore
  public void testFolder() throws IOException{
    TarAndGzip.folder(file);
  }
  
  @Test
  @Ignore
  public void testuntarTGzFile() throws IOException{
    TarAndGzip.untarTGzFile(sourceFilePath);
  }
}
