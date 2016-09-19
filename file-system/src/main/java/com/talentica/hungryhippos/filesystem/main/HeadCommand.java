package com.talentica.hungryhippos.filesystem.main;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class HeadCommand {

  private static Options option = new Options();
  static {
    option.addOption("n", "number", false, "number of lines to be read");
  }

  public static void execute(CommandLineParser parser, String... args) {
    try {
      CommandLine line = parser.parse(option, args);
      int numberOfLines = 10;
      if (line.hasOption("n")) {
        numberOfLines = Integer.valueOf(line.getArgList().get(2));
      } else {

      }

    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

}
