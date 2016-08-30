package com.talentica.hungryhippos.filesystem.main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.utility.ExecuteShellCommand;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.client.Output;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;

public class CreateFileSystemInCluster {

  private static final String SCRIPT_LOC =
      "/home/sudarshans/RD/HH_NEW/HungryHippos/utility/scripts/file-system-commands.sh";
  private static String userName = null;
  private static String key = null;
  private static String clientConfig = null;
  private static String clusterConfig = null;
  private static String operation = "";
  private static String fname = "";
  private static List<Node> nodesInCluster = null;
  private static List<String> argumentsTobePassed = new ArrayList<String>();
  private static String currentDir = null;
  private static String rootDir = null;
  private static HungryHipposFileSystem hhfs = null;

  public static void main(String[] args) throws FileNotFoundException, JAXBException {
    validateArguments(args);

    clientConfig = args[0];
    setClientConfig(clientConfig);
    clusterConfig = args[1];
    CoordinationConfigUtil.setLocalClusterConfigPath(clusterConfig);
    ClusterConfig configuration = CoordinationConfigUtil.getLocalClusterConfig();
    nodesInCluster = configuration.getNode();
    argumentsTobePassed.add("/bin/sh");
    argumentsTobePassed.add(SCRIPT_LOC);
    argumentsTobePassed.add(userName);
    intializeNodeManager();
    setHHFSInstanceInMain();
    rootDir = hhfs.getHHFSZROOT();
    currentDir = rootDir;
    // argumentsTobePassed.add(operation);
    // argumentsTobePassed.add(fname);
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String line = "";
    while (true) {
      try {
        line = br.readLine();
        operationAllowed(line);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }
  }

  private static void operationAllowed(String line) {
    String[] cmds = line.split(" ");
    operation = cmds[0];
    if (cmds.length > 1) {
      fname = cmds[1];
    }

    switch (cmds[0]) {
      case "ls":
        if (cmds.length == 1) {
          runHungryHipposFileSystemMain(cmds[0], currentDir);
        } else {
          runHungryHipposFileSystemMain(operation, fname);
        }
        break;
      case "mkdir":

        int errorCount = runOnAllNodes();
        if (errorCount == 0) {
          runHungryHipposFileSystemMain(operation, fname);
          updateFsBlockMetaData();
        }

        break;
      case "rm":
        runHungryHipposFileSystemMain(operation, fname);
        runOnAllNodes();
        break;
      case "show":
        runHungryHipposFileSystemMain(operation, fname);
        break;
      case "less":
        printOnScreen(operation, fname);
        break;
      case "tail":
        printOnScreen(operation, fname);
        break;
      case "cd":
        if (cmds.length == 1) {
          currentDir = rootDir;
        } else {
          currentDir = hhfs.checkNameContainsFileSystemRoot(fname);
        }
      default:
        System.out.println("command not found");
    }
  }

  private static void printOnScreen(String op, String name) {
    List<String> args = new ArrayList<>();
    args.add("/bin/sh");
    args.add("/home/sudarshans/RD/HH_NEW/HungryHippos/utility/scripts/less.sh");
    if (op.equals("tail")) {
      op = "tail -f";
    }
    args.add(op);
    args.add(name);
    String[] scriptArgs = args.stream().toArray(String[]::new);
    ExecuteShellCommand.executeScript(true, scriptArgs);
  }

  private static int runOnAllNodes() {
    String[] scriptArgs = null;
    argumentsTobePassed.add(operation);
    argumentsTobePassed.add(fname);
    int errorCount = 0;
    for (Node node : nodesInCluster) { // don't execute ls on node
      argumentsTobePassed.add(node.getIp());
      scriptArgs = argumentsTobePassed.stream().toArray(String[]::new);
      errorCount = ExecuteShellCommand.executeScript(false, scriptArgs);
      argumentsTobePassed.remove(node.getIp());
    }
    argumentsTobePassed.remove(operation);
    argumentsTobePassed.remove(fname);
    return errorCount;
  }

  private static void updateFsBlockMetaData() {
    {

      for (Node node : nodesInCluster) {
        hhfs.updateFSBlockMetaData(fname, String.valueOf(node.getIdentifier()), 0l);
      }

    }
  }

  private static void validateArguments(String... args) {
    if (args.length < 2) {
      throw new IllegalArgumentException("Need client , Cluster Configuration details");
    }
  }

  private static void runHungryHipposFileSystemMain(String operation, String fname) {
    HungryHipposFileSystemMain.getCommandDetails(operation, fname);

  }

  private static void intializeNodeManager() throws FileNotFoundException, JAXBException {
    NodesManagerContext.getNodesManagerInstance(clientConfig);

  }

  private static void setHHFSInstanceInMain() throws FileNotFoundException, JAXBException {
    hhfs = HungryHipposFileSystemMain.getHHFSInstance();
  }

  private static void setClientConfig(String cliConfig) {
    ClientConfig clientConfig = null;
    try {
      clientConfig = JaxbUtil.unmarshalFromFile(cliConfig, ClientConfig.class);
    } catch (FileNotFoundException | JAXBException e) {
      throw new RuntimeException(e.getMessage());
    }
    Output output = clientConfig.getOutput();
    userName = output.getNodeSshUsername();
    key = output.getNodeSshPrivateKeyFilePath();
  }

}
