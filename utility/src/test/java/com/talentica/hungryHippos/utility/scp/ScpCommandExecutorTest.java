package com.talentica.hungryHippos.utility.scp;

import org.junit.Ignore;
import org.junit.Test;

public class ScpCommandExecutorTest {

  private static final String userName = "root";
  private static final String host = "139.59.28.135";
  private static final String remoteDir = "test/sharding-files.tar.gz";
  private static final String localDir = "/home/sohanc/ScpAndGzip";
  
  @Test
  @Ignore
  public void testDownload(){
    ScpCommandExecutor.download(userName, host, remoteDir, localDir);
  }
  
  @Test
  @Ignore
  public void testUpload(){
    ScpCommandExecutor.upload(userName, host, remoteDir, localDir);
  }
}
