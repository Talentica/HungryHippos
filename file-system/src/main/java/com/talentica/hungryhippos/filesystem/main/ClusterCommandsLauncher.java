package com.talentica.hungryhippos.filesystem.main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.utility.ExecuteShellCommand;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryHippos.utility.scp.ScpCommandExecutor;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.client.Output;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;

/**
 * This class is written for doing basic operation on cluster enviroment (initial setup). Copying
 * required jar from one node to another node in a cluster. Creating/ destroying directories /
 * files. If a command fails in a node in a cluster that node will be displayed. This class is
 * temporary . until wget url works.
 * 
 * @author sudarshans
 *
 */
public class ClusterCommandsLauncher {
  private static String userName = null;
  private static String key = null;
  private static List<Node> nodes = null;
  private static boolean isJavaCmd = false;
  private static String configurationFolder;
  private static String remoteDir;


  public static void main(String[] args) {

    validateArgs(args);
    setClusterConfig(args[0]);
    setClientConfig(args[1]);
    printWelcome();
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    String line = null;
    while (true) {
      try {
        line = br.readLine();
        if (isJavaCmd) {
          if (configurationFolder == null) {
            setUpConfigurationFile(line);
          } else {
            runJavaCommands(line);
          }
        } else {
          runCommandOnAllNodes(line);
        }

      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }

  }

  private static void runMkdir(String[] commands) {
    for (Node node : nodes) {
      String host = node.getIp();
      ScpCommandExecutor.createRemoteDirsWithKey(userName, key, host, commands[1]);
    }
  }

  private static void runScp(String[] commands) {
    String localFile = commands[1];
    String remoteDir = commands[2];
    for (Node node : nodes) {
      String host = node.getIp();
      // sshpass -p "password"
      ScpCommandExecutor.uploadWithKey(userName, key, host, remoteDir, localFile);
    }
    
  }

  private static void runDownload(String[] commands) {
    for (Node node : nodes) {
      String host = node.getIp();
      ScpCommandExecutor.download(userName, host, commands[1], commands[2]);
    }
  }


  private static void runUpload(String[] commands) {
    for (Node node : nodes) {
      String host = node.getIp();
      ScpCommandExecutor.download(userName, host, commands[1], commands[2]);
    }
  }


  private static void runRemoveDirs(String[] commands) {
    for (Node node : nodes) {
      String host = node.getIp();
      ScpCommandExecutor.removeDir(userName, host, commands[1]);
    }
  }

  public static void runCoordinationStarter(String userName, String host, String remoteDir) {
    ProcessBuilder builder = new ProcessBuilder("ssh", userName + "@" + host, "cd",
        remoteDir + ";" + "java" + "com.talentica.hungryHippos.coordination.CoordinationStarter"
            + "../config/client-config.xml" + "../config/coordination-config.xml"
            + "../config/cluster-config.xml" + "../config/datapublisher-config.xml"
            + "../config/filesystem-config.xml" + "../config/job-runner-config.xml" + ">"
            + "../logs/coordination.out" + "2" + ">" + "../logs/coordination.err" + "&");

    // execute(builder);
  }


  public static void runDataSynchronizerStarter(String userName, String host, String remoteDir,
      String zkIp) {
    ProcessBuilder builder = new ProcessBuilder("ssh", userName + "@" + host, "cd",
        remoteDir + ";" + "java" + "com.talentica.torrent.DataSynchronizerStarter" + zkIp + host
            + ">" + "../logs/data-sync.out" + "2" + ">" + "../logs/data-sync.err" + "&");

    // execute(builder);
  }


  public static void runTorrentTrackerStarter(String userName, String host, String remoteDir,
      String zkIp, String torrentIp, int port) {
    ProcessBuilder builder = new ProcessBuilder("ssh", userName + "@" + host, "cd",
        remoteDir + ";" + "java" + "com.talentica.torrent.TorrentTrackerStarter" + zkIp + torrentIp
            + port + ">" + "../logs/data-trac.out" + "2" + ">" + "../logs/data-trac.err" + "&");

    // execute(builder);
  }


  public static void runShardingStarter(String userName, String host, String remoteDir) {
    ProcessBuilder builder = new ProcessBuilder("ssh", userName + "@" + host, "cd",
        remoteDir + ";" + "java" + "-cp" + "sharding.jar"
            + "com.talentica.hungryHippos.sharding.main.ShardingStarter"
            + "../config/client-config.xml" + "../config/" + ">" + "../logs/sharding.out" + "2"
            + ">" + "../logs/sharding.er" + "&");

    // execute(builder);
  }


  public static void runDataReceiverStarter(String userName, String host, String remoteDir) {
    ProcessBuilder builder = new ProcessBuilder("ssh", userName + "@" + host, "cd",
        remoteDir + ";" + "java" + "com.talentica.hungryHippos.node.DataReceiver"
            + "../config/client-config.xml" + ">" + "../logs/datareciever.out" + "2" + ">"
            + "../logs/datareciever.err" + "&");

    // execute(builder);
  }


  public static void runDataPublisherStarter(String userName, String host, String remoteDir) {
    ProcessBuilder builder = new ProcessBuilder("ssh", userName + "@" + host, "cd", remoteDir + ";"
        + "java" + "-cp" + "data-publisher.jar"
        + "com.talentica.hungryHippos.master.DataPublisherStarter" + "../config/client-config.xml"
        + "/home/sohanc/D_drive/HungryHippos_project/HungryHippos/sampledata.csv" + "/dir/input1"
        + ">" + "../logs/datapub.outt" + "2" + ">" + "../logs/datapub.err" + "&");

    // execute(builder);
  }

  public static void runJobOrchestrator(String userName, String host, String remoteDir) {
    ProcessBuilder builder = new ProcessBuilder("ssh", userName + "@" + host, "cd",
        remoteDir + ";" + "java" + "-cp" + "job-manager.jar"
            + "com.talentica.hungryHippos.job.main.JobOrchestrator" + "../config/client-config.xml"
            + "/home/sohanc/D_drive/HungryHippos_project/HungryHippos/test-hungry-hippos/build/libs/test-jobs.jar"
            + " com.talentica.hungryHippos.test.sum.SumJobMatrixImpl" + "/dir/input" + "/dir/output"
            + ">" + "../logs/job-man.out" + "2" + ">" + "../logs/job-man.err" + "&");



    // execute(builder);
  }



  public static void runJobExecutorProcessBuilder(String userName, String host, String remoteDir) {
    ProcessBuilder builder = new ProcessBuilder("ssh", userName + "@" + host, "cd",
        remoteDir + ";" + "java" + "com.talentica.hungryHippos.node.JobExecutorProcessBuilder"
            + "../config/client-config.xml" + ">" + "../logs/job.out" + "2" + ">"
            + "../logs/job.err" + "&");

    // execute(builder);
  }

  public static void runCoordinationStarter1(String userName, String host, String remoteDir,
      String ClassName, String outFile, String errFile) {
    ProcessBuilder builder = new ProcessBuilder("ssh", userName + "@" + host, "cd",
        remoteDir + ";" + "java" + ClassName + "../config/client-config.xml"
            + "../config/coordination-config.xml" + "../config/cluster-config.xml"
            + "../config/datapublisher-config.xml" + "../config/filesystem-config.xml"
            + "../config/job-runner-config.xml" + ">" + outFile + "2" + ">" + errFile + "&");

    // execute(builder);
  }

  private static void setUpConfigurationFile(String folderLoc) {
    configurationFolder = folderLoc;
  }

  private static void printHungryHipposJavaCommands() {
    System.out.println("1.Choose option \"1\" to run Coordination Starter.");
    System.out.println("2.Choose option \"2\" to run Sharding Starter.");
    System.out.println("3.Choose option \"3\" to run DataReceiver Starter.");
    System.out.println("4.Choose option \"4\" to run DataPublisher Starter.");
    System.out.println("5.Choose option \"5\" to run JobManager Starter.");
    System.out.println("6.Choose option \"6\" to run JobExecutor Starter.");
    System.out.println("7. exit");
  }

  private static void runJavaCommands(String line) {

    switch (line) {
      case "1":
        System.out.println("Coordination Starter is running");
        break;
      case "2":
        System.out.println("Sharding Starter is running");
        break;
      case "3":
        System.out.println("DataReceiver Starter is running");
        break;
      case "4":
        System.out.println("DataPublisher Starter is running");
        break;
      case "5":
        System.out.println("JobManager Starter is running");
        break;
      case "6":
        System.out.println("JobExecutor Starter is running");
        break;
      case "7":
        isJavaCmd = false;
        break;
      default:
        System.out
            .println("improper command used. to run normal commands. please exit by typing \"7\"");
    }
  }

  private static void runCommandOnAllNodes(String line) {
    String[] commands = line.split(" ");

    switch (commands[0]) {
      case "mkdir":
        runMkdir(commands);
        System.out.println("mkdir command successfully executed");
        break;
      case "scp":
        runScp(commands);
        System.out.println("Executed scp operation successfully!!!");
        break;
      case "rm":
        runRemoveDirs(commands);        
        System.out.println("Executed rm operation successfully!!!");
        break;
      case "download":
        runDownload(commands);
        break;
      case "upload":
        runUpload(commands);
        break;
      case "javahelp":
        printHungryHipposJavaCommands();
        break;
      case "runjava":
        printHungryHipposJavaCommands();
        isJavaCmd = true;
        break;
      case "exit":
        System.out.println("Bye!!! , Have a nice day");
        System.exit(1);
      case "help":
        printUsage();
        break;
      default:
        System.out.println(
            "Commands allowed are :- \"scp\", \"mkdir\", \"rm\", \"download\", \"upload\", \"exit\"");
        System.out.println(
            "Miscellanous Activities allowed are running java commands on HungryHippos Jar: use \"javahelp\" ");
        System.out.println("to actual run the commands use \"runjava\".");

    }
  }

  private static void validateArgs(String... args) {
    if (args.length != 2) {
      throw new IllegalArgumentException("cluster-config.xml / client-config.xml is missing. "
          + "usage:- java -cp filesystem.jar com.talentica.hungryhippos.filesystem.main.ClusterCommandsLauncher "
          + "{path to Cluster-Config file.} {path to Client-Config file.} ");

    }

  }

  private static void printUsage() {
    System.out.println("Commands that can be used in this Enviroment are:-");
    System.out.println("");
    System.out.println(
        " 1. mkdir {folderTobeCreated}.            \"Result of this execution will create all parentFolder in all the nodes under the user\"");
    System.out.println("2. rm {folder}.                   \"For removing empty Folder \" ");
    System.out.println(
        "3. scp {locDir} {remoteDir}.   \"Copies the file specified (locDir) to the cluster (remoteDir).\"");
    System.out.println(
        "4. download {remoteDir} {locDir}. \" Copies remote file to the specified locDir in current machine");
    System.out.println("exit. \"To exit the application. \"");
  }

  private static void setClusterConfig(String clusConfig) {
    ClusterConfig clusterConfig = null;
    try {
      clusterConfig = JaxbUtil.unmarshalFromFile(clusConfig, ClusterConfig.class);
    } catch (FileNotFoundException | JAXBException e) {
      throw new RuntimeException(e.getMessage());
    }
    nodes = clusterConfig.getNode();
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

  public static void printWelcome() {
    System.out.println("Welcome to ClusterCommand Env");
    System.out.println("");


  }

}
