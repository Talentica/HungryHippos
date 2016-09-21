package com.talentica.hungryhippos.filesystem.main;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.scp.ScpCommandExecutor;
import com.talentica.hungryHippos.utility.scp.TarAndGzip;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.NodeFileSystem;
import com.talentica.hungryhippos.filesystem.client.DataRetrieverClient;
import com.talentica.hungryhippos.filesystem.context.FileSystemContext;

import javafx.scene.shape.Path;

public class GetCommand {


  private static Options options = new Options();

  static {
    options.addOption("d", "dimension", false,
        "download file of particular dimension from nodes. i.e get -d zookeeperPath 2");
    options.addOption("s", "sharded File", false, "download sharded file associated with the file");
    options.addOption("h", "help", false, "");
  }

  public static void execute(CommandLineParser parser, String... args) {
    try {
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("h")) {
        usage();
        return;
      }

      if (line.hasOption("s") && line.hasOption("d")) {
        System.out.println("choose either -d or -s. clubbing two options is not supported");
        return;
      }
      if (line.getArgList() == null || line.getArgList().size() < 2) {
        System.out.println("Argument can't be empty");
        return;
      }
      String hungryHippoFilePath = line.getArgList().get(1);
      hungryHippoFilePath = hungryHippoFilePath.endsWith(FileSystemConstants.ZK_PATH_SEPARATOR)
          ? hungryHippoFilePath.substring(0, hungryHippoFilePath.length() - 1)
          : hungryHippoFilePath;
      HungryHipposFileSystem hhfs = HungryHipposCommandLauncher.getHHFSInstance();
      String nodeHHFSDir = hhfs.getHHFSNodeRoot();
      nodeHHFSDir = nodeHHFSDir.endsWith(FileSystemConstants.ZK_PATH_SEPARATOR)
          ? nodeHHFSDir.substring(0, nodeHHFSDir.length() - 1) : nodeHHFSDir;

      String data = hhfs.getData(hungryHippoFilePath);
      if (!data.contains(FileSystemConstants.IS_A_FILE)) {
        System.out.println("The location " + hungryHippoFilePath
            + " is not a file. Please provide a valid file path");
        return;
      }

      String outputDirName =
          System.getProperty("user.home") + File.separatorChar + fileName(hungryHippoFilePath);
      if (!Files.exists(Paths.get(outputDirName))) {
        Files.createDirectory(Paths.get(outputDirName));
      }
      int dimension = 0;
      if (line.hasOption("d")) {
        dimension = Integer.valueOf(line.getArgList().get(2));
        DataRetrieverClient.getHungryHippoData(hungryHippoFilePath, outputDirName, dimension);
      } else if (line.hasOption("s")) {
        String userName = HungryHipposCommandLauncher.getUserName();
        String host = HungryHipposCommandLauncher.getNodesInCluster().get(0).getIp();
        List<String> children = hhfs.getChildZnodes(hungryHippoFilePath);
        boolean isSharded = false;
        for (String child : children) {
          if ((child.equals(FileSystemConstants.SHARDED))) {
            isSharded = true;
            break;
          }
        }
        if (isSharded) {
          String remoteDir =
              nodeHHFSDir + hungryHippoFilePath + File.separatorChar + "sharding-table.tar.gz";
          ScpCommandExecutor.download(userName, host, remoteDir, outputDirName);
          TarAndGzip.untarTGzFile(outputDirName + File.separatorChar + "sharding-table.tar.gz");
        } else {
          System.out.println("File is not sharded , don't use -s option");
        }
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

  private static String fileName(String name) {
    String[] dir = name.split("/");
    return dir[dir.length - 1];
  }


}
