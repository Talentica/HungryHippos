package com.talentica.hungryhippos.filesystem.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Process;

/**
 * {@code JavaProcessBuilder} used for build unix related commands.
 *
 */
public class JavaProcessBuilder {

  public static ProcessBuilder constructProcessBuilder(boolean isLocal, String userName,
      String host, String command) {
    ProcessBuilder pbd = null;
    if (isLocal) {
      pbd = new ProcessBuilder("/bin/bash", "-c", command);

    } else {
      pbd =
          new ProcessBuilder("ssh", "-o StrictHostKeyChecking=no", userName + "@" + host, command);
    }
    return pbd;
  }

  public static int execute(ProcessBuilder builder) {
    Process process;
    int processStatus = -1;
    try {
      process = builder.start();
      processStatus = process.waitFor();
      BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      String line = null;
      StringBuilder sb = new StringBuilder();
      while ((line = br.readLine()) != null) {
        System.out.println("Displaying Error Message");
        sb.append(line).append("\n");
      }
      System.out.println(sb.toString());
      br = new BufferedReader(new InputStreamReader(process.getInputStream()));
      sb = new StringBuilder();
      while ((line = br.readLine()) != null) {
        System.out.println("Displaying Output Message");

        sb.append(line).append("\n");
      }
      System.out.println(sb.toString());

    } catch (IOException | InterruptedException e1) {
      e1.printStackTrace();
      throw new RuntimeException(e1);
    }
    return processStatus;
  }

}
