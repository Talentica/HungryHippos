package com.talentica.hungryhippos.filesystem.main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.talentica.hungryHippos.utility.scp.ScpCommandExecutor;

public class CatCommand {


  private static Options options = new Options();

  static {
    options.addOption("r", "remote location", false,
        "download file of particular dimension from nodes");

  }

  public static void execute(CommandLineParser parser, String... args) {
    try {
      CommandLine line = parser.parse(options, args);
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
