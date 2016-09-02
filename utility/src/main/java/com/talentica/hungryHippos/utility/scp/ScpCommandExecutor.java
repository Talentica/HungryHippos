package com.talentica.hungryHippos.utility.scp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;


public class ScpCommandExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScpCommandExecutor.class);

  public static void download(String userName, String host, String remoteDir, String localDir) {
    ProcessBuilder builder = new ProcessBuilder("scp", "-o StrictHostKeyChecking=no",
        userName + "@" + host + ":" + remoteDir, localDir);
    execute(builder);
  }

  public static void upload(String userName, String host, String remoteDir, String localDir) {
    createRemoteDirs(userName, host, remoteDir);
    ProcessBuilder builder = new ProcessBuilder("scp", "-r", "-o StrictHostKeyChecking=no",
        localDir, userName + "@" + host + ":" + remoteDir);
    execute(builder);
  }

  public static void createRemoteDirs(String userName, String host, String remoteDir) {
    ProcessBuilder builder = new ProcessBuilder("ssh", "-o StrictHostKeyChecking=no",
        userName + "@" + host, "mkdir", "-p", remoteDir);
    execute(builder);
  }

  public static void removeDir(String userName, String host, String remoteDir) {
    ProcessBuilder builder = new ProcessBuilder("ssh", "-o StrictHostKeyChecking=no",
        userName + "@" + host, "rm", "-r", remoteDir);
    execute(builder);
  }


  private static void execute(ProcessBuilder builder) {
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

      br = new BufferedReader(new InputStreamReader(process.getInputStream()));
      sb = new StringBuilder();
      while ((line = br.readLine()) != null) {
        sb.append(line).append("\n");
        System.out.println(line);
      }

      if (processStatus != 0) {
        throw new RuntimeException("Operation " + builder.command() + " failed");
      }
    } catch (IOException | InterruptedException e1) {
      e1.printStackTrace();
      throw new RuntimeException(e1);
    }
  }

  public static void cat(String userName, String host, String fileName) {
    ProcessBuilder builder = new ProcessBuilder("ssh", "-o StrictHostKeyChecking=no",
        userName + "@" + host, "cat", fileName);
    execute(builder);
  }

}
