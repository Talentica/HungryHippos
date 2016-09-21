package com.talentica.hungryhippos.filesystem.main;

import java.io.File;
import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.talentica.hungryHippos.utility.FileSystemConstants;

public class HeadCommand {

  private static Options options = new Options();
  static {
    options.addOption("n", "number", true, "number of lines to be read");
    options.addOption("h", "help", false, "");
  }

  public static void execute(CommandLineParser parser, String... args) {
    try {
      CommandLine line = parser.parse(options, args);
      int numberOfLines = 10;
      if (line.hasOption("h")) {
        usage();
        return;
      }
      if (line.getArgList() == null || line.getArgList().size() < 3) {
        System.out.println("Argument can't be empty");
        return;
      }
      String filePath = line.getArgList().get(1);
      String[] dir = filePath.split(FileSystemConstants.ZK_PATH_SEPARATOR);
      String fileName = dir[dir.length - 1];
      String fileToRead = System.getProperty("user.home") + File.separatorChar + fileName
          + File.separatorChar + line.getArgList().get(2);

      String shardingClientConfigLoc =
          System.getProperty("user.home") + File.separatorChar + fileName + File.separatorChar
              + "sharding-table" + File.separatorChar + "sharding-client-config.xml";

      try {
        if (line.hasOption("n")) {
          numberOfLines = Integer.valueOf(line.getOptionValue("n"));
          System.out.println(numberOfLines);
          ShardedFile.read(fileToRead, shardingClientConfigLoc, numberOfLines);

        } else {
          ShardedFile.read(fileToRead, shardingClientConfigLoc, numberOfLines);
        }
      } catch (Exception e) {

      }

    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  public static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("head", options);
  }


}
