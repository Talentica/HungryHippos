/**
 * 
 */
package com.talentica.hungryHippos.utility;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author PooshanS
 *
 */
public class ExecuteShellCommand {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteShellCommand.class);

  public static void main(String[] args) {
    try {
      if (args.length < 1) {
        LOGGER.info("Please provide argument for shell script");
        return;
      }
      Runtime rt = Runtime.getRuntime();
      Process pr = rt.exec(new String[] {"/bin/sh", args[0]});

      BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));
      String line = "";
      while ((line = input.readLine()) != null) {
        LOGGER.info(line);
      }
    } catch (Exception e) {
      LOGGER.info("Execption {}", e);
    }
  }

  public static int executeScript(boolean showInConsole, String... args) {
    int errorLines = 0;
    try {
      if (args.length < 1) {
        throw new IllegalArgumentException("Please provide argument for shell script");

      }

      Runtime rt = Runtime.getRuntime();

      Process pr = rt.exec(args);

      BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));
      String line = "";
      while ((line = input.readLine()) != null) {
        LOGGER.info(line);
        if (showInConsole) {
          System.out.println(line);
        }
      }
      input.close();
      BufferedReader errorStream = new BufferedReader(new InputStreamReader(pr.getErrorStream()));
      line = "";

      while ((line = errorStream.readLine()) != null) {
        errorLines++;
        LOGGER.error(line);
        if (showInConsole) {
          System.out.println(line);
        }
      }
      errorStream.close();
    } catch (Exception e) {
      LOGGER.info("Execption {}", e);
    }
    return errorLines;
  }
}
