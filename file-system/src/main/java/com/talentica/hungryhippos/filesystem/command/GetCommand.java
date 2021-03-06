/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
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

import java.io.File;
import java.io.IOException;
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
import com.talentica.hungryhippos.filesystem.main.HungryHipposCommandLauncher;

/**
 * {@code GetCommand} used for downloading variety of files from the system. different files are
 * downloaded is controlled by the options you set. the set of options that are available are "-d"
 * -> gives the dimension applies to sharded files. "-s" -> downloads the sharding table. "-n" ->
 * represents a node in a cluster. "-h" -> for printing help. "-o" -> otuput folder where you want
 * to download the file.
 * 
 * @author sudarshans
 *
 */
public class GetCommand {


  private static Options options = new Options();
  private static HungryHipposFileSystem hhfs = null;
  private static String nodeHHFSDir = null;
  private static String hungryHippoFilePath = null;
  private static String outputDirName = System.getProperty("user.home");
  static {
    options.addOption("d", "dimension", true,
        "download file of particular dimension from nodes. i.e get -d zookeeperPath 2");
    options.addOption("s", "sharding table", false,
        "download sharded file associated with the file");
    options.addOption("n", "node", true, "from which node you want to see the data.");
    options.addOption("h", "help", false, "");
    options.addOption("o", "output folder", true, "The folder where output file has to be stored");
  }

  /**
   * used for parsing the arguments and executing a particular scenario based on the arguments.
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

      String data = hhfs.getNodeData(hungryHippoFilePath);
      if (data == null || !data.contains(FileSystemConstants.IS_A_FILE)) {
        System.out.println("The location " + hungryHippoFilePath
            + " is not a file. Please provide a valid file path");
        return;
      }

      if (line.hasOption("o")) {
        outputDirName = line.getOptionValue("o");
      } else {
        outputDirName += File.separatorChar + fileName(hungryHippoFilePath);
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

    }
  }

  private static void getShardedFile() throws IOException {
    List<String> children = hhfs.getChildZnodes(hungryHippoFilePath);
    boolean isSharded = validateFileIsSharded(children);
    String userName = null;
    String node = null;
    if (isSharded) {
      runScpAndUntarFile(userName, node);
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

  private static void runScpAndUntarFile(String userName, String host) throws IOException {
    String remoteDir =
        nodeHHFSDir + hungryHippoFilePath + File.separatorChar + "sharding-table.tar.gz";
    ScpCommandExecutor.download(userName, host, remoteDir, outputDirName);
    TarAndGzip.untarTGzFile(outputDirName + File.separatorChar + "sharding-table.tar.gz");
  }

  private static String removeLeftSlashAtEnd(String path) {

    return path.endsWith(FileSystemConstants.ZK_PATH_SEPARATOR)
        ? path.substring(0, path.length() - 1) : path;
  }

  /**
   * prints the usage.
   */
  public static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("get", options);
  }

  private static String fileName(String name) {
    String[] dir = name.split("/");
    return dir[dir.length - 1];
  }


}
