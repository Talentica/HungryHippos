package com.talentica.hungryhippos.filesystem.main;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class LSCommand {

  private static Options options = new Options();

  static {
    options.addOption("l", "list", false, "show entire details of the file");
  }

  public static void execute(CommandLineParser parser, String... args) {
    try {
      CommandLine line = parser.parse(options, args);
      String fileName = line.getArgList().get(1);
      if (line.hasOption("l")) {
        HungryHipposFileSystemMain.getCommandDetails("show", fileName);
      } else {
        HungryHipposFileSystemMain.getCommandDetails("ls", fileName);
      }

    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("ls", options);
  }


}
