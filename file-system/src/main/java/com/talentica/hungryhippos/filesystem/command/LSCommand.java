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
package com.talentica.hungryhippos.filesystem.command;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.talentica.hungryhippos.filesystem.main.HungryHipposFileSystemMain;

/**
 * {@code LSCommand} used for listing the files and its metadata. options allowed are "-l" => shows
 * entire detail of a file. "-h" => prints the help message.
 * 
 * @author sudarshans
 *
 */
public class LSCommand {

  private static Options options = new Options();

  static {
    options.addOption("l", "list", false, "show entire details of the file");
    options.addOption("h", "help", false, "");
  }

  /**
   * executes the command.
   * 
   * @param parser
   * @param args
   */
  public static void execute(CommandLineParser parser, String... args) {
    try {
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("h")) {
        usage();
        return;
      }

      validateLine(line);

      String fileName = line.getArgList().get(1);

      if (line.hasOption("l")) {
        HungryHipposFileSystemMain.getCommandDetails("show", fileName);
      } else {
        HungryHipposFileSystemMain.getCommandDetails("ls", fileName);
      }

    } catch (ParseException e) {
      usage();
    }
  }

  public static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("ls", options);
  }

  private static void validateLine(CommandLine line) {
    List<String> argList = line.getArgList();
    if (argList == null || argList.isEmpty()) {
      System.out.println("Argument can't be empty or null");
      return;
    } else if (argList.size() < 2) {
      System.out.println("FileName / Folder name is needed , format is :- ");
      usage();
    }
  }

}
