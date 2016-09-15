package com.talentica.hungryhippos.filesystem.main;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.scp.ScpCommandExecutor;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.client.DataRetrieverClient;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

public class GetCommand {


  private static Options options = new Options();

  static {
    options.addOption("d", "dimension", false,
        "download file of particular dimension from nodes. i.e get -d zookeeperPath 2");
    options.addOption("s", "sharded File", false, "download sharded file associated with the file");
  }

  public static void execute(CommandLineParser parser, String... args) {
    try {
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("s") && line.hasOption("d")) {
        System.out.println("choose either -d or -s. clubbing two options is not supported");
        return;
      }
      String hungryHippoFilePath = line.getArgList().get(1);
      HungryHipposFileSystem hhfs = HungryHipposCommandLauncher.getHHFSInstance();
      String data = hhfs.getData(hungryHippoFilePath);
      if (!data.equals(FileSystemConstants.IS_A_FILE)) {
        System.out.println("The location " + hungryHippoFilePath
            + " is not a file. Please provide a valid file path");
        return;
      }

      String outputDirName = System.getProperty("user.dir");
      int dimension = 0;
      if (line.hasOption("d")) {
        dimension = Integer.valueOf(line.getArgList().get(2));
        DataRetrieverClient.getHungryHippoData(hungryHippoFilePath, outputDirName, dimension);
      } else if (line.hasOption("s")) {
        String userName = HungryHipposCommandLauncher.getUserName();
        String host = HungryHipposCommandLauncher.getNodesInCluster().get(0).getIp();
        List<String> children = hhfs.getChildZnodes(hungryHippoFilePath);
        for (String child : children) {
          if (!(child.equals(FileSystemConstants.SHARDED))) {
            System.out.println("The File mentioned is not sharded, dont use -s option for this "
                + hungryHippoFilePath);
            return;
          }
        }
        String remoteDir = hhfs.getData(hungryHippoFilePath + FileSystemConstants.ZK_PATH_SEPARATOR
            + FileSystemConstants.SHARDED);
        ScpCommandExecutor.download(userName, host, remoteDir, outputDirName);
      } else {
        DataRetrieverClient.getHungryHippoData(hungryHippoFilePath, outputDirName);
      }
      System.out.println("File is saved " + outputDirName);
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
