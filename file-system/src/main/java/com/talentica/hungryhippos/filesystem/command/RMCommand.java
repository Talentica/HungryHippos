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
    argumentsTobePassed.add(new HungryHipposCommandLauncher().getUserName());
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
