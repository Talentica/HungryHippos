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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.utility.ExecuteShellCommand;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.main.HungryHipposCommandLauncher;
import com.talentica.hungryhippos.filesystem.main.HungryHipposFileSystemMain;

/**
 * {@code RMCommand} used for removing files associated with the system from Zookeeper as well as
 * node. options allowed are "-r" => delete all the subfolders and files of a dir. including the
 * dir.
 * 
 * @author sudarshans
 *
 */
public class RMCommand {

  private static Options options = new Options();
  private static final String SCRIPT_LOC =
      "/home/sudarshans/RD/HH_NEW/HungryHippos/utility/scripts/file-system-commands.sh";
  static {
    options.addOption("r", "deleteall", false, "deletes the folder and subfolder if present");
    options.addOption("n", "to remove it from all the nodes in the cluster", false,
        "deletes the folder and subfolder if present");
    options.addOption("h", "help", false, "");
  }

  /**
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
      if (line.getArgList() == null || line.getArgList().size() < 2) {
        System.out.println("Argument can't be empty");
        return;
      }
      String path = line.getArgList().get(1);
      if (line.hasOption("r")) {
        HungryHipposFileSystemMain.getCommandDetails("deleteall", path);
        runOnAllNodes("rm", path);
      } else {
        HungryHipposFileSystemMain.getCommandDetails("delete", path);
        runOnAllNodes("rm", path);
      }
      if (line.hasOption("n")) {
        String nodeIp = line.getOptionValue("n");
        runOnAllNodes("rm", path, nodeIp);
      }
    } catch (ParseException e) {

      e.printStackTrace();
    }
  }

  public static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("rm", options);
  }



  private static int runOnAllNodes(String... args) {

    String operation = args[0];
    String fname = args[1];
    ClusterConfig clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache();
    List<Node> nodesInCluster = clusterConfig.getNode();

    List<String> argumentsTobePassed = new ArrayList<>();
    argumentsTobePassed.add("/bin/sh");
    argumentsTobePassed.add(SCRIPT_LOC);
    argumentsTobePassed.add("");
    String[] scriptArgs = null;
    argumentsTobePassed.add(operation);
    argumentsTobePassed.add(fname);
    int errorCount = 0;
    if (args.length < 3) {
      for (Node node : nodesInCluster) { // don't execute ls on node
        argumentsTobePassed.add(node.getIp());
        scriptArgs = argumentsTobePassed.stream().toArray(String[]::new);
        errorCount = ExecuteShellCommand.executeScript(false, scriptArgs);
        argumentsTobePassed.remove(node.getIp());
      }
    } else {
      argumentsTobePassed.add(args[2]);
      scriptArgs = argumentsTobePassed.stream().toArray(String[]::new);
      errorCount = ExecuteShellCommand.executeScript(false, scriptArgs);
      argumentsTobePassed.remove(args[2]);
    }

    argumentsTobePassed.remove(operation);
    argumentsTobePassed.remove(fname);
    return errorCount;
  }

}
