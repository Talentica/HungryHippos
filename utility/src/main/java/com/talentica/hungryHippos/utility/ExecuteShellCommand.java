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
