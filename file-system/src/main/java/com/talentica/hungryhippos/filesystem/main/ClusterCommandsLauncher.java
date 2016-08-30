package com.talentica.hungryhippos.filesystem.main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

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
  private static String JAVA_SCRIPT_LOC =
      "/home/sudarshans/RD/HH_NEW/HungryHippos/utility/scripts/run-java-class-on-all-clusters.sh";
  private static final String CLIENT_CONFIG = "client-config.xml";
  private static final String COORDINATION_CONFIG = "coordination-config.xml";
  private static final String CLUSTER_CONFIG = "cluster-config.xml";
  private static final String DATA_PUBLISHER_CONFIG = "datapublisher-config.xml";
  private static final String File_SYSTEM_CONFIG = "filesystem-config.xml";
  private static final String JOB_RUNNER_CONFIG = "job-runner-config.xml";
  private static String errorFile = null;
  private static String outPutFile = null;


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
            printHungryHipposJavaCommands();
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
    List<String> args = new ArrayList<String>();
    List<String> shellArgs = new ArrayList<String>();
    shellArgs.add("/bin/sh");
    shellArgs.add(JAVA_SCRIPT_LOC);
    shellArgs.add(userName);
    switch (line) {
      case "1":
        System.out.println("Preparing Coordination Starter");

        if (!configurationFolder.endsWith("/")) {
          configurationFolder = configurationFolder + "/";
        }
        args.add(configurationFolder + CLIENT_CONFIG);
        args.add(configurationFolder + COORDINATION_CONFIG);
        args.add(configurationFolder + CLUSTER_CONFIG);
        args.add(configurationFolder + DATA_PUBLISHER_CONFIG);
        args.add(configurationFolder + File_SYSTEM_CONFIG);
        args.add(configurationFolder + JOB_RUNNER_CONFIG);
        readInput();

        for (Node node : nodes) {
        
          shellArgs.add(node.getIp());
          shellArgs.add(
              buildJavaCmd("java", "com.talentica.hungryHippos.coordination.CoordinationStarter",
                  args, outPutFile, errorFile));
          String[] scriptArgs = shellArgs.stream().toArray(String[]::new);
          ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                              // node.getIp(), configurationFolder);
        }

        break;
      case "2":
        System.out.println("Sharding Starter is running");

        if (!configurationFolder.endsWith("/")) {
          configurationFolder = configurationFolder + "/";
        }
        args.add(configurationFolder + CLIENT_CONFIG);
        args.add(configurationFolder);
        readInput();

        for (Node node : nodes) {
          
          shellArgs.add(node.getIp());
          shellArgs.add(buildJavaCmd("java -cp sharding.jar",
              "com.talentica.hungryHippos.sharding.main.ShardingStarter", args, outPutFile,
              errorFile));
          String[] scriptArgs = shellArgs.stream().toArray(String[]::new);
          ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                              // node.getIp(), configurationFolder);
        }

        break;
      case "3":
        System.out.println("DataReceiver Starter is running");
        if (!configurationFolder.endsWith("/")) {
          configurationFolder = configurationFolder + "/";
        }
        args.add(configurationFolder + CLIENT_CONFIG);
        readInput();
        for (Node node : nodes) {
         
          shellArgs.add(node.getIp());
          shellArgs.add(
              buildJavaCmd("java", "com.talentica.hungryHippos.coordination.CoordinationStarter",
                  args, outPutFile, errorFile));
          String[] scriptArgs = shellArgs.stream().toArray(String[]::new);
          ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                              // node.getIp(), configurationFolder);
        }
        break;
      case "4":
        System.out.println("DataPublisher Starter is running");
        if (!configurationFolder.endsWith("/")) {
          configurationFolder = configurationFolder + "/";
        }
        shellArgs.add(configurationFolder + CLIENT_CONFIG);
        getInputDataForDataPublisher(args);
        readInput();

        for (Node node : nodes) {
         
          shellArgs.add(node.getIp());
          shellArgs.add(buildJavaCmd("java -cp data-publisher.jar",
              "com.talentica.hungryHippos.master.DataPublisherStarter", args, "../logs/datapub.out",
              "../logs/datapub.err"));
          String[] scriptArgs = shellArgs.stream().toArray(String[]::new);
          ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                              // node.getIp(), configurationFolder);
        }
        break;
      case "5":
        System.out.println("JobManager Starter is running");
        if (!configurationFolder.endsWith("/")) {
          configurationFolder = configurationFolder + "/";
        }
        args.add(configurationFolder + CLIENT_CONFIG);
        getInputDataForJobManager(args);
        readInput();
        for (Node node : nodes) {
         
          shellArgs.add(node.getIp());
          shellArgs.add(buildJavaCmd("java -cp job-manager.jar",
              "com.talentica.hungryHippos.job.main.JobOrchestrator", args, "../logs/job-man.out",
              "../logs/job-man.err"));
          String[] scriptArgs = args.stream().toArray(String[]::new);
          ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                              // node.getIp(), configurationFolder);
        }
        break;
      case "6":
        System.out.println("JobExecutor Starter is running");
        if (!configurationFolder.endsWith("/")) {
          configurationFolder = configurationFolder + "/";
        }
        args.add(configurationFolder + CLIENT_CONFIG);

        for (Node node : nodes) {
        
          shellArgs.add(node.getIp());
          shellArgs.add(buildJavaCmd("java", "com.talentica.hungryHippos.node.JobExecutorProcessBuilder",
              args, "../logs/job.out", "../logs/job.err"));
          String[] scriptArgs = args.stream().toArray(String[]::new);
          ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                              // node.getIp(), configurationFolder);
        }
        break;
      case "7":
        isJavaCmd = false;
        break;
      default:
        System.out
            .println("improper command used. to run normal commands. please exit by typing \"7\"");
    }
  }

  private static void getInputDataForJobManager(List<String> args) {

    Scanner sc = new Scanner(System.in);
    String line = null;
    System.out.println("Enter Jar Path");
    line = sc.nextLine();
    args.add(line);
    System.out.println(" Enter Class Name");
    line = sc.nextLine();
    args.add(line);
    System.out.println("Input Dir");
    line = sc.nextLine();
    args.add(line);
    System.out.println("set outPut dir ");
    line = sc.nextLine();
    args.add(line);
    sc.close();
  }

  private static void getInputDataForDataPublisher(List<String> args) {

    Scanner sc = new Scanner(System.in);
    System.out.println("Enter FilePath");
    String fileLoc = sc.nextLine();
    System.out.println(" Set Relative Distributed Dir Location");
    String dirLoc = sc.nextLine();
    sc.close();
    args.add(fileLoc);
    args.add(dirLoc);
  }

  private static void readInput() {
    Scanner sc = new Scanner(System.in);
    System.out.print("set outputFile:-");
    outPutFile = sc.nextLine();
    System.out.println();
    System.out.print("set error File:-");
    errorFile = sc.nextLine();
    sc.close();
  }

  private static String buildJavaCmd(String java, String clazz, List<String> configFiles,
      String out, String err) {
    List<String> command = new ArrayList<>();
    command.add(java);
    command.add(clazz);
    for (String configFile : configFiles) {
      command.add(configFile);
    }
    command.add(">");
    command.add(out);
    command.add("2");
    command.add(">");
    command.add(err);

    StringBuilder sb = new StringBuilder();

    for (String arg : command) {
      sb.append(arg);
      sb.append(" ");
    }
    return sb.toString();
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
        System.out.println("Please type remote location of configuration folder");
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
