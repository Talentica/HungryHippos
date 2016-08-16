package com.talentica.hungryHippos.utility.scp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ScpCommandExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScpCommandExecutor.class);
  
  public static void download(String userName, String host, String remoteDir, String localDir) {
    ProcessBuilder builder =
        new ProcessBuilder("scp", userName + "@" + host + ":" + remoteDir, localDir);
    execute(builder);
  }

  public static void upload(String userName, String host, String remoteDir, String localDir) {
    ProcessBuilder builder =
        new ProcessBuilder("scp", localDir, userName + "@" + host + ":" + remoteDir);
    execute(builder);
  }
  
  private static void execute(ProcessBuilder builder){
    Process process;
    try {
      process = builder.start();
      int processStatus = process.waitFor();
      BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      String line = null;
      StringBuilder sb = new StringBuilder();
      while ((line = br.readLine()) != null) {
        sb.append(line).append("\n");
      }
      System.out.println(sb.toString());
      LOGGER.error("Console OUTPUT : \n" + sb.toString());
      br = new BufferedReader(new InputStreamReader(process.getInputStream()));
      sb = new StringBuilder();
      while ((line = br.readLine()) != null) {
        sb.append(line).append("\n");
      }
      LOGGER.info("Console OUTPUT : \n" + sb.toString());
      if(processStatus!=0){
        throw new RuntimeException("File Transfer failed");
      }
    } catch (IOException | InterruptedException e1) {
      e1.printStackTrace();
      throw new RuntimeException(e1);
    }
  }


}
