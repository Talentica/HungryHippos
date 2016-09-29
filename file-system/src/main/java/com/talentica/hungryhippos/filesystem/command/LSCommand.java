package com.talentica.hungryhippos.filesystem.command;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.talentica.hungryhippos.filesystem.main.HungryHipposFileSystemMain;

public class LSCommand {

  private static Options options = new Options();

  static {
    options.addOption("l", "list", false, "show entire details of the file");
    options.addOption("h", "help", false, "");
  }

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
