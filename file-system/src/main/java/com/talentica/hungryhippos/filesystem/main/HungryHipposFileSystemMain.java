package com.talentica.hungryhippos.filesystem.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.data.Stat;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.utility.ExecuteShellCommand;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.client.DataRetrieverClient;

/**
 * {@code HungryHipposFileSystemMain} used for file related operations.
 * 
 * @author sudarshans
 *
 */
public class HungryHipposFileSystemMain {

  private static String userName = null;


  private static final String SCRIPT_LOC =
      new File(".").getAbsolutePath() + File.separatorChar + "src" + File.separatorChar + "main"
          + File.separatorChar + "resources" + File.separatorChar + "file-system-commands.sh";


  enum Operations {
    LS(0), TOUCH(1), MKDIR(2), FIND(3), DELETE(4), DELETEALL(5), EXIT(6), SHOW(7), DU(8), DOWNLOAD(
        9);

    private int option = 0;

    Operations(int option) {
      this.option = option;
    }

    public int getOption() {
      return this.option;
    }

    public static Operations getOpertionsFromOption(int option) {
      return Operations.values()[option];
    }

  }


  private static List<String> fileSystemConstants = new ArrayList<>();
  static {
    fileSystemConstants.add(FileSystemConstants.DATA_READY);
    fileSystemConstants.add(FileSystemConstants.DATA_SERVER_AVAILABLE);
    fileSystemConstants.add(FileSystemConstants.DATA_SERVER_BUSY);
    fileSystemConstants.add(FileSystemConstants.DATA_TRANSFER_COMPLETED);
    fileSystemConstants.add(FileSystemConstants.DFS_NODE);
    fileSystemConstants.add(FileSystemConstants.DOWNLOAD_FILE_PREFIX);
    fileSystemConstants.add(FileSystemConstants.IS_A_FILE);
    fileSystemConstants.add(FileSystemConstants.PUBLISH_FAILED);
    fileSystemConstants.add(FileSystemConstants.SHARDED);
  }

  private static String[] commands =
      {"ls", "touch", "mkdir", "find", "delete", "deleteall", "exit", "show", "du", "download"};
  private static HungryHipposFileSystem hhfs = null;

  public static void main(String[] args) throws FileNotFoundException, JAXBException {
    String clientXmlFile = args[0];
    ClientConfig clientConfig = JaxbUtil.unmarshalFromFile(clientXmlFile, ClientConfig.class);
    userName = clientConfig.getOutput().getNodeSshUsername();
    String connectString = clientConfig.getCoordinationServers().getServers();
    int sessionTimeOut = Integer.parseInt(clientConfig.getSessionTimout());
    HungryHippoCurator.getInstance(connectString, sessionTimeOut);
    hhfs = HungryHipposFileSystem.getInstance();

    getCommandDetails(args);

  }

  public static void getCommandDetails(String... args) {
    String operation = args[1];
    String[] fromSecondArg = new String[args.length - 2];
    for (int i = 0; i < fromSecondArg.length; i++) {
      fromSecondArg[i] = args[i + 2];
    }

    if (hhfs == null) {
      throw new RuntimeException(
          "HungryHipposFileSystem is not created, please create it calling getHHFSInstance() method");
    }

    if (fromSecondArg[0] == null) {
      fromSecondArg[0] = "HungryHipposFs";
    }
    for (int i = 0; i < commands.length; i++) {
      if (operation.equalsIgnoreCase(commands[i])) {
        Operations op = Operations.getOpertionsFromOption(i);
        runOperation(op, fromSecondArg);
        break;
      }
    }

  }


  private static void printOnScreen(List<String> list) {

    if (list == null) {
      System.out.println("No Files are present");
      return;
    }

    boolean isFileConstant = false;
    for (String fName : list) {
      for (String constants : fileSystemConstants) {
        if (fName.equals(constants)) {
          isFileConstant = true;
          break;
        }
      }
      if (!isFileConstant) {
        System.out.println(fName);
      }
    }
  }

  private static void showMetaData(List<String> list, String name) {

    name = name.endsWith(FileSystemConstants.ZK_PATH_SEPARATOR)
        ? name.substring(0, name.length() - 1) : name;
    if (list == null) {
      return;
    } else if (list.contains(FileSystemConstants.SHARDED)) {
      System.out.println("File Name:- " + fileName(name));
      System.out.println("sharded:- true");
    } else {
      System.out.println("Folder Name:- " + fileName(name));
      printOnScreen(hhfs.getChildZnodes(name));
    }

    if (list.contains(FileSystemConstants.DFS_NODE)) {
      List<String> nodeDetails = hhfs.getChildZnodes(
          name + FileSystemConstants.ZK_PATH_SEPARATOR + FileSystemConstants.DFS_NODE);

      System.out.print("File is Distributed on Following nodes:- ");
      for (String node : nodeDetails) {
        System.out.print("   " + node);
      }
      System.out.println();
      String dimension = null;


      final String name1 = name;
      long size = 0l;
      for (String node : nodeDetails) {
        List<String> childs = hhfs.getChildZnodes(name + FileSystemConstants.ZK_PATH_SEPARATOR
            + FileSystemConstants.DFS_NODE + FileSystemConstants.ZK_PATH_SEPARATOR + node);
        if (dimension == null) {
          dimension = String.valueOf((int) (Math.log(childs.size()) / Math.log(2)));
        }


        try {
          for (String child : childs) {

            List<String> childsChild =
                hhfs.getChildZnodes(name + FileSystemConstants.ZK_PATH_SEPARATOR
                    + FileSystemConstants.DFS_NODE + FileSystemConstants.ZK_PATH_SEPARATOR + node
                    + FileSystemConstants.ZK_PATH_SEPARATOR + child);


            for (String leaf : childsChild) {

              long length = (long) hhfs.getObjectData(name1 + FileSystemConstants.ZK_PATH_SEPARATOR
                  + FileSystemConstants.DFS_NODE + FileSystemConstants.ZK_PATH_SEPARATOR + node
                  + FileSystemConstants.ZK_PATH_SEPARATOR + child
                  + FileSystemConstants.ZK_PATH_SEPARATOR + leaf);
              size += length;
            }


          }

        } catch (NumberFormatException e) {
          System.out.println("size is stored in serialized form");
        }
      }
      System.out.println("Dimension:- " + dimension);
      System.out.println("size of the file Combined :- " + size + " bytes");
      Stat stat = hhfs.getZnodeStat(name);
      System.out.println("Created On:-" + getDateString(stat.getCtime()));
      System.out.println("Modified On:-" + getDateString(stat.getMtime()));
      System.out.println();
    }
  }

  public static long size(String path) {

    return hhfs.size(path);
  }

  public static void download(String path, String outputDirName) {
    try {
      DataRetrieverClient.getHungryHippoData(path, outputDirName);
      System.out.println("download:success");
    } catch (Exception e) {
      System.out.println("download:failed");

    }
  }

  private static String getDateString(long value) {
    Date date = new Date(value);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
    return sdf.format(date);
  }

  private static String fileName(String name) {
    String[] dir = name.split(FileSystemConstants.ZK_PATH_SEPARATOR);
    return dir[dir.length - 1];
  }

  private static int runOnAllNodes(String... args) {
    String fileSystemRoot = args[0];
    String operation = args[1];
    String fname = args[2];
    ArrayList<String> argumentsTobePassed = new ArrayList<>();
    argumentsTobePassed.add("/bin/sh");
    argumentsTobePassed.add(SCRIPT_LOC);
    argumentsTobePassed.add(userName);
    String[] scriptArgs = null;
    argumentsTobePassed.add(fileSystemRoot);
    argumentsTobePassed.add(operation);
    argumentsTobePassed.add(fname);
    int errorCount = 0;
    ClusterConfig clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache();
    List<Node> nodesInCluster = clusterConfig.getNode();
    for (Node node : nodesInCluster) { // don't execute ls on node
      argumentsTobePassed.add(node.getIp());
      scriptArgs = argumentsTobePassed.stream().toArray(String[]::new);
      errorCount = ExecuteShellCommand.executeScript(false, scriptArgs);
      argumentsTobePassed.remove(node.getIp());
    }

    return errorCount;
  }

  private static void runOperation(Operations op, String... args) {
    String name = args[0];
    switch (op) {
      case LS:
        String data = hhfs.getNodeData(name);
        if (data != null && data.contains(FileSystemConstants.IS_A_FILE)) {
          System.out.println(fileName(name) + " " + FileSystemConstants.IS_A_FILE);
        } else {
          printOnScreen(hhfs.getChildZnodes(name));
        }
        break;

      case MKDIR:
        hhfs.createZnode(name);
        break;
      case DELETE:
        hhfs.deleteNode(name);
        runOnAllNodes(op.name().toLowerCase(), name);
        break;
      case DELETEALL:
        hhfs.deleteNodeRecursive(name);
        runOnAllNodes(hhfs.getHHFSNodeRoot(), op.name().toLowerCase(), name);
        break;
      case SHOW:
        List<String> childNodes = hhfs.getChildZnodes(name);
        if (childNodes == null) {
          System.out.println("Folder Name:- " + fileName(name) + " doesn't exist.");
          return;
        }
        showMetaData(childNodes, name);
        break;
      case DU:
        long size = size(name);
        System.out.println("fileSize is :" + size);
        break;
      case DOWNLOAD:
        if (args.length < 2) {
          throw new IllegalArgumentException("Output folder path is mandatory");
        }
        download(name, args[1]);
        break;

      default:
    }

  }

}
