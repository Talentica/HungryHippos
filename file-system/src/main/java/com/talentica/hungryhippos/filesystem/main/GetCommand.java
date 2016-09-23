package com.talentica.hungryhippos.filesystem.main;

import java.io.File;
import java.io.IOException;
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
import com.talentica.hungryhippos.filesystem.client.DataRetrieverClient;

public class GetCommand {


  private static Options options = new Options();
  private static HungryHipposFileSystem hhfs = null;
  private static String nodeHHFSDir = null;
  private static String hungryHippoFilePath = null;
  private static String outputDirName = System.getProperty("user.home");

  static {
    options.addOption("d", "dimension", true,
        "download file of particular dimension from nodes. i.e get -d zookeeperPath 2");
    options.addOption("s", "sharded File", false, "download sharded file associated with the file");
    options.addOption("n", "node", true, "from which node you want to see the data.");
    options.addOption("h", "help", false, "");
    options.addOption("o", "output folder", true, "The folder where output file has to be stored");
  }

  public static void execute(CommandLineParser parser, String... args) {
    try {
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("h")) {
        usage();
        return;
      }

      if (line.hasOption("s") && line.hasOption("d")) {
        System.out.println("choose either -d or -s. clubbing this two options is not supported");
        return;
      }
      if (line.getArgList() == null || line.getArgList().size() < 2) {
        System.out.println("Argument can't be empty");
        return;
      }

      hungryHippoFilePath = line.getArgList().get(1);
      hungryHippoFilePath = removeLeftSlashAtEnd(hungryHippoFilePath);

      hhfs = HungryHipposCommandLauncher.getHHFSInstance();

      nodeHHFSDir = hhfs.getHHFSNodeRoot();
      nodeHHFSDir = removeLeftSlashAtEnd(nodeHHFSDir);

      String data = hhfs.getData(hungryHippoFilePath);
      if (!data.contains(FileSystemConstants.IS_A_FILE)) {
        System.out.println("The location " + hungryHippoFilePath
            + " is not a file. Please provide a valid file path");
        return;
      }

      if (line.hasOption("o")) {
        outputDirName = line.getOptionValue("o");
      } else {
        outputDirName += File.separatorChar + fileName(hungryHippoFilePath);
      }

      if (!Files.exists(Paths.get(outputDirName))) {
        Files.createDirectory(Paths.get(outputDirName));
      }
      int dimension = 0;
      if (line.hasOption("d")) {
        dimension = Integer.valueOf(line.getOptionValue("d"));
        DataRetrieverClient.getHungryHippoData(hungryHippoFilePath, outputDirName, dimension);
      } else if (line.hasOption("s")) {
        getShardedFile();
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

  private static void getShardedFile() throws IOException {
    List<String> children = hhfs.getChildZnodes(hungryHippoFilePath);
    boolean isSharded = validateFileIsSharded(children);
    if (isSharded) {
      runScpAndUntarFile();
    } else {
      System.out.println("File is not sharded , don't use -s option");
    }
  }

  private static boolean validateFileIsSharded(List<String> children) {
    boolean isSharded = false;
    for (String child : children) {
      if ((child.contains(FileSystemConstants.SHARDED))) {
        isSharded = true;
        break;
      }
    }
    return isSharded;
  }

  private static void runScpAndUntarFile() throws IOException {
    String userName = HungryHipposCommandLauncher.getUserName();
    String host = HungryHipposCommandLauncher.getNodesInCluster().get(0).getIp();
    String remoteDir =
        nodeHHFSDir + hungryHippoFilePath + File.separatorChar + "sharding-table.tar.gz";
    ScpCommandExecutor.download(userName, host, remoteDir, outputDirName);
    TarAndGzip.untarTGzFile(outputDirName + File.separatorChar + "sharding-table.tar.gz");
  }

  private static String removeLeftSlashAtEnd(String path) {

    return path.endsWith(FileSystemConstants.ZK_PATH_SEPARATOR)
        ? path.substring(0, path.length() - 1) : path;
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
