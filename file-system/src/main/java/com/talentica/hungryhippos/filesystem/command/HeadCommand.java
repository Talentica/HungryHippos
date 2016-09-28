package com.talentica.hungryhippos.filesystem.command;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.filesystem.client.DataRetrieverClient;
import com.talentica.hungryhippos.filesystem.helper.ShardedFile;

public class HeadCommand {

  private static Options options = new Options();
  static {
    options.addOption("n", "number", true, "number of lines to be read");
    options.addOption("s", "sharded", false, "if the file that has to be read is sharded");
    options.addOption("r", "remote location", true, "the remote ip from where you want to read");
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
        }

        if (line.hasOption("s") && line.hasOption("r")) {
          String nodeIp = line.getOptionValue("r");

          DataRetrieverClient.retrieveDataBlocks_test(nodeIp, fileToRead, 1024, 33, 6989, 5, 5,
              shardingClientConfigLoc);
        } else if (line.hasOption("s")) {
          ShardedFile.read(fileToRead, shardingClientConfigLoc, numberOfLines);
        } else {
          try (Stream<String> stream = Files.lines(Paths.get(fileName))) {

            stream.limit(numberOfLines).forEach(System.out::println);

          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
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
