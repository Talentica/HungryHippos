/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
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
package com.talentica.hungryhippos.filesystem.command;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
/**
 * {@code CatCommand} 
 * @author sudarshans
 * @deprecated
 */
public class CatCommand {


  private static Options options = new Options();

  static {
    options.addOption("r", "remote location", false,
        "download file of particular dimension from nodes");
    options.addOption("h", "help", false, "The cat command will be removed");

  }

  public static void execute(CommandLineParser parser, String... args) {
    try {
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("h")) {
        usage();
        return;
      }
      if (line.getArgList() == null || line.getArgList().size() < 2) {
        System.out.println("Argument can't be empty");
        return;
      }

      String fileName = line.getArgList().get(1);

      if (line.hasOption("r")) {

        // ScpCommandExecutor.cat(TestLauncher.getUserName(), host, fileName);
      } else {

        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
          stream.forEach(System.out::println);
        } catch (IOException e) {
          e.printStackTrace();
        }


      }

    } catch (ParseException e) {
      usage();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("get", options);
  }

}
