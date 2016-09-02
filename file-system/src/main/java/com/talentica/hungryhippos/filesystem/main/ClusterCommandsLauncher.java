package com.talentica.hungryhippos.filesystem.main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.utility.ExecuteShellCommand;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryHippos.utility.scp.ScpCommandExecutor;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.client.CoordinationServers;
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
  private static String configurationFolder = null;
  private static String JAVA_SCRIPT_LOC =
      "/home/sudarshans/RD/HH_NEW/HungryHippos/utility/scripts/run-java-class-on-all-clusters.sh";
  private static String LOCAL_JAVA_SCRIPT_LOC =
      "/home/sudarshans/RD/HH_NEW/HungryHippos/utility/scripts/run-java-on-local-machine.sh";
  private static final String CLIENT_CONFIG = "client-config.xml";
  private static final String COORDINATION_CONFIG = "coordination-config.xml";
  private static final String CLUSTER_CONFIG = "cluster-config.xml";
  private static final String DATA_PUBLISHER_CONFIG = "datapublisher-config.xml";
  private static final String File_SYSTEM_CONFIG = "filesystem-config.xml";
  private static final String JOB_RUNNER_CONFIG = "job-runner-config.xml";
  private static String errorFile = null;
  private static String outPutFile = null;
  private static String zkIp = null;
  private static String localDir = null;
  private static String clusterDir = null;
  private static BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
  private static String classpath = null;
  private static String clusterClassPath = null;
  private static String clusterConfigPath = null;
  private static ExecutorService executorService = Executors.newFixedThreadPool(3);

  public static void main(String[] args) {

    validateArgs(args);
    setClusterConfig(args[0]);
    setClientConfig(args[1]);
    printWelcome();
    inputListener();
  }

  private static void validateArgs(String... args) {
    if (args.length != 2) {
      throw new IllegalArgumentException("cluster-config.xml / client-config.xml is missing. "
          + "usage:- java -cp filesystem.jar com.talentica.hungryhippos.filesystem.main.ClusterCommandsLauncher "
          + "{path to Cluster-Config file.} {path to Client-Config file.} ");

    }
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
    CoordinationServers server = clientConfig.getCoordinationServers();
    server.getServers();
    zkIp = server.getServers();
    userName = output.getNodeSshUsername();
    key = output.getNodeSshPrivateKeyFilePath();
  }

  public static void printWelcome() {
    System.out.println("Welcome to HungryHippos ClusterCommand Env");
    System.out.println("");
    printUsage();
  }


  private static void printUsage() {
    System.out.println("Commands that can be used in this Enviroment are:-");
    System.out.println("");
    System.out.println(
        "1. mkdir {folderTobeCreated}.            \"Result of this execution will create all parentFolder in all the nodes under the user\"");
    System.out.println("2. rm {folder}.               \"For removing empty Folder \" ");
    System.out.println(
        "3. scp {locDir} {remoteDir}.   \"Copies the file specified (locDir) to the cluster (remoteDir).\"");
    System.out.println(
        "4. download {remote host ip} {remoteDir} {locDir}. \" Copies remote file to the specified locDir in current machine");
    System.out.println("4. javahelp \"print java commands that can be executed\"");
    System.out.println("5. help \"To print the message again\"");
    System.out.println("6. runjava \"To enter Running java classes\"");
    // System.out.println("5. upload {localDir} {remoteDir}" );
    System.out.println("7. exit. \"To exit the application. \"");

  }

  private static void inputListener() {
    String line = null;
    while (true) {
      try {
        line = br.readLine();
        if (isJavaCmd) {
          runJavaCommands(line);
          if (line.equals("9")) {
            printHungryHipposJavaCommands();
          }
        } else {
          runCommandOnAllNodes(line);
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
  }

  private static void runJavaCommands(String line) {
    List<String> args = new ArrayList<String>();
    List<String> shellArgs = new ArrayList<String>();
    shellArgs.add("/bin/sh");

    switch (line) {
      case "0":
        initConfig();
        System.out.println("Classpath is set successfully");
        break;
      case "1":
        System.out.println("Preparing enviroment to run Coordination Starter");
        // client
        args.add(configurationFolder + CLIENT_CONFIG);
        args.add(configurationFolder + COORDINATION_CONFIG);
        args.add(configurationFolder + CLUSTER_CONFIG);
        args.add(configurationFolder + DATA_PUBLISHER_CONFIG);
        args.add(configurationFolder + File_SYSTEM_CONFIG);
        args.add(configurationFolder + JOB_RUNNER_CONFIG);
        // readInput();
        outPutFile = "../logs/coordination.out";
        errorFile = "../logs/coordination.err";
        shellArgs.add(LOCAL_JAVA_SCRIPT_LOC);
        shellArgs.add(localDir);
        // shellArgs.add();
        shellArgs.add(buildJavaCmd("java -cp " + classpath + "node.jar",
            "com.talentica.hungryHippos.coordination.CoordinationStarter", args, outPutFile,
            errorFile));
        String[] scriptArgs = shellArgs.stream().toArray(String[]::new);
        ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                            // node.getIp(), configurationFolder);
        System.out.println("successfully executed coordination starter");
        break;
      case "2":
        // client
        System.out.println("Preparing enviroment to run sharding Starter");
        args.add(configurationFolder + CLIENT_CONFIG);
        args.add(configurationFolder);
        // readInput();
        outPutFile = "../logs/sharding.out";
        errorFile = "../logs/sharding.err";
        shellArgs.add(LOCAL_JAVA_SCRIPT_LOC);
        shellArgs.add(localDir);
        shellArgs.add(buildJavaCmd("java -cp " + classpath + "sharding.jar",
            "com.talentica.hungryHippos.sharding.main.ShardingStarter", args, outPutFile,
            errorFile));
        scriptArgs = shellArgs.stream().toArray(String[]::new);
        ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                            // node.getIp(), configurationFolder);

        break;
      case "3":
        System.out.println("Preparing enviroment to run DataReceiver Starter");
        args.add(clusterConfigPath + CLIENT_CONFIG);
        // readInput();
        outPutFile = "../logs/datareciever.out";
        errorFile = "../logs/datareciever.err";
        shellArgs.add(JAVA_SCRIPT_LOC);
        shellArgs.add(userName);
        for (Node node : nodes) {
          shellArgs.add(node.getIp());
          shellArgs.add(clusterDir);
          String javacmd = buildJavaCmd("java -cp " + clusterClassPath + "node.jar",
              "com.talentica.hungryHippos.node.DataReceiver", args, outPutFile, errorFile);
          shellArgs.add(javacmd);
          scriptArgs = shellArgs.stream().toArray(String[]::new);
          ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                              // node.getIp(), configurationFolder);
          shellArgs.remove(node.getIp());
          shellArgs.remove(clusterDir);
          shellArgs.remove(javacmd);
        }
        break;
      case "4":
        // client
        System.out.println("Preparing enviroment to run DataPublisher Starter");
        args.add(configurationFolder + CLIENT_CONFIG);
        // readInput();
        outPutFile = "../logs/datapub.out";
        errorFile = "../logs/datapub.err";
        shellArgs.add(LOCAL_JAVA_SCRIPT_LOC);
        shellArgs.add(localDir);
        try {
          System.out.println("Enter FilePath");
          String fileLoc = br.readLine();
          System.out.println(" Set Relative Distributed Dir Location");
          String dirLoc = br.readLine();

          args.add(fileLoc);
          args.add(dirLoc);
        } catch (IOException e) {

        }
        shellArgs.add(buildJavaCmd("java -cp " + classpath + "data-publisher.jar",
            "com.talentica.hungryHippos.master.DataPublisherStarter", args, outPutFile, errorFile));
        scriptArgs = shellArgs.stream().toArray(String[]::new);
        ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                            // node.getIp(), configurationFolder);

        break;
      case "5":
        System.out.println("Preparing enviroment to run JobManager Starter");
        args.add(configurationFolder + CLIENT_CONFIG);
        getInputDataForJobManager(args);
        // readInput();
        outPutFile = "../logs/job-man.out";
        errorFile = "../logs/job-man.err";
        shellArgs.add(LOCAL_JAVA_SCRIPT_LOC);
        shellArgs.add(localDir);
        shellArgs.add(buildJavaCmd("java -cp " + classpath + "job-manager.jar",
            "com.talentica.hungryHippos.job.main.JobOrchestrator", args, outPutFile, errorFile));
        scriptArgs = shellArgs.stream().toArray(String[]::new);
        ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                            // node.getIp(), configurationFolder);

        break;
      case "6":
        System.out.println("Preparing enviroment to run JobExecutor Starter");
        args.add(clusterConfigPath + CLIENT_CONFIG);
        // readInput();
        outPutFile = "../logs/job.out";
        errorFile = "../logs/job.err";
        shellArgs.add(JAVA_SCRIPT_LOC);
        shellArgs.add(userName);
        for (Node node : nodes) {
          shellArgs.add(node.getIp());
          shellArgs.add(clusterDir);
          String javacmd = buildJavaCmd("java -cp " + clusterClassPath + "node.jar",
              "com.talentica.hungryHippos.node.JobExecutorProcessBuilder", args, outPutFile,
              errorFile);
          shellArgs.add(javacmd);
          scriptArgs = shellArgs.stream().toArray(String[]::new);
          ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
          shellArgs.remove(clusterDir); // node.getIp(), configurationFolder);
          shellArgs.remove(node.getIp());
          shellArgs.remove(javacmd);
        }
        break;

      case "7":
        System.out.println("Preparing enviroment to run DataSynchronization");
        // readInput();
        outPutFile = "../logs/data-sync.out";
        errorFile = "../logs/data-sync.err";
        args.add(zkIp);
        shellArgs.add(JAVA_SCRIPT_LOC);
        shellArgs.add(userName);
        for (Node node : nodes) {
          args.add(node.getIp());
          shellArgs.add(node.getIp());
          shellArgs.add(clusterDir);
          String javacmd = buildJavaCmd("java -cp " + clusterClassPath + "node.jar",
              "com.talentica.torrent.DataSynchronizerStarter", args, outPutFile, errorFile);
          shellArgs.add(javacmd);
          scriptArgs = shellArgs.stream().toArray(String[]::new);
          ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                              // node.getIp(), configurationFolder);
          args.remove(node.getIp());
          shellArgs.remove(node.getIp());
          shellArgs.remove(clusterDir);
          shellArgs.remove(javacmd);

        }

        break;
      case "8":
        System.out.println("Preparing enviroment to run TorrentTracker");
        // readInput();
        outPutFile = "../logs/torrent-track.out";
        errorFile = "../logs/torrent-track.err";
        shellArgs.add(JAVA_SCRIPT_LOC);
        shellArgs.add(userName);
        args.add(zkIp);
        try {
          System.out.print("Ip of the node where you want to start TorrentTracker:-");
          String ip = br.readLine();
          System.out.println("Port number on which you want to start");
          String port = br.readLine();
          shellArgs.add(ip);
          shellArgs.add(clusterDir);
          args.add(ip);
          args.add(port);
        } catch (IOException e) {
          System.out.println(e.getMessage());
        }
        shellArgs.add(buildJavaCmd("java -cp " + clusterClassPath + "node.jar",
            "com.talentica.torrent.TorrentTrackerStarter", args, outPutFile, errorFile));
        scriptArgs = shellArgs.stream().toArray(String[]::new);
        ExecuteShellCommand.executeScript(true, scriptArgs);// runCoordinationStarter(userName,
                                                            // node.getIp(), configurationFolder);
        break;
      case "9":
        isJavaCmd = false;
        break;
      default:
        System.out
            .println("improper command used. to run normal commands. please exit by typing \"9\"");
    }
  }


  private static void initConfig() {
    try {
      System.out.println("input local configuration folder where all xml files are present.");
      configurationFolder = br.readLine();
      System.out.println("input cluster configuration folder where all xml files are present.");
      clusterConfigPath = br.readLine();
      System.out.println("input Directory from where you want to run java command in local");
      localDir = br.readLine();
      System.out.println("input Directory from where you want to run java command in cluster");
      clusterDir = br.readLine();
      System.out.println(
          "Enter the classpath location you want to set for local where all jar files are present");
      classpath = br.readLine();
      System.out.println(
          "Enter the classpath location you want to set for cluster  where all jar files are present");
      clusterClassPath = br.readLine();
      validateDir();


    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  private static void validateDir() {
    if (!configurationFolder.endsWith("/")) {
      configurationFolder = configurationFolder + "/";
    }

    if (!clusterConfigPath.endsWith("/")) {
      clusterConfigPath = clusterConfigPath + "/";
    }

    if (!localDir.endsWith("/")) {
      localDir = localDir + "/";
    }

    if (!clusterDir.endsWith("/")) {
      clusterDir = clusterDir + "/";
    }

    if (!classpath.endsWith("/")) {
      classpath = classpath + "/";
    }

    if (!clusterClassPath.endsWith("/")) {
      clusterClassPath = clusterClassPath + "/";
    }
  }

  private static void printHungryHipposJavaCommands() {
    System.out.println(
        "0.Choose option \"0\" to set CLASSPATH on all node in the cluster !. Mandatory to run this at first.");
    System.out.println("1.Choose option \"1\" to run Coordination Starter.");
    System.out.println("2.Choose option \"2\" to run Sharding Starter.");
    System.out.println("3.Choose option \"3\" to run DataReceiver Starter.");
    System.out.println("4.Choose option \"4\" to run DataPublisher Starter.");
    System.out.println("5.Choose option \"5\" to run JobManager Starter.");
    System.out.println("6.Choose option \"6\" to run JobExecutor Starter.");
    System.out.println("7.Choose option \"7\" to run DataSynchrozation Starter");
    System.out.println("8.Choose option \"8\" to run TorrentTracker Starter");
    System.out.println("9. exit");
  }


  private static void getInputDataForJobManager(List<String> args) {

    try {
      String line = null;
      System.out.println("Enter Jar Path");
      line = br.readLine();
      args.add(line);
      System.out.println("Enter Class Name");
      line = br.readLine();
      args.add(line);
      System.out.println("Input Dir");
      line = br.readLine();
      args.add(line);
      System.out.println("set outPut dir ");
      line = br.readLine();
      args.add(line);
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }

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
    command.add("&");


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
        break;
      case "scp":
        runScp(commands);
        break;
      case "rm":
        runRemoveDirs(commands);
        break;
      case "download":
        runDownload(commands);
        System.out.println("completed downloading of file");
        break;
      case "javahelp":
        printHungryHipposJavaCommands();
        break;
      case "runjava":
        printHungryHipposJavaCommands();
        isJavaCmd = true;
        break;
      case "cat":
        runCat(commands);
        System.out.println();
        System.out.println("Finished Reading File");
        break;
      case "exit":
        System.out.println("Bye!!! , Have a nice day");
        System.exit(1);
      case "help":
        printUsage();
        break;
      default:
        System.out
            .println("Commands allowed are :- \"scp\", \"mkdir\", \"rm\", \"download\", \"exit\"");
        System.out.println(
            "Miscellanous Activities allowed are running java commands on HungryHippos Jar: use \"javahelp\" ");
        System.out.println("to actual run the commands use \"runjava\".");

    }
  }



  private static void runMkdir(String[] commands) {
    Map<String, Future<?>> futureDetails = new HashMap<>();
    for (Node node : nodes) {
      String host = node.getIp();
      Future<?> future = executorService.submit(new Runnable() {
        @Override
        public void run() {
          ScpCommandExecutor.createRemoteDirs(userName, host, commands[1]);

        }
      });
      futureDetails.put(host, future);
    }
    int count = 0;
    int length = futureDetails.size();
    while (true) {
      Iterator<Entry<String, Future<?>>> iterator = futureDetails.entrySet().iterator();
      try {
        while (iterator.hasNext()) {
          Map.Entry<String, Future<?>> pair = (Entry<String, Future<?>>) iterator.next();
          String host = pair.getKey();
          Future<?> future = pair.getValue();
          if (future.get() == null) {
            iterator.remove();
            System.out.println("Directory " + commands[1] + " is created on" + host);
          }
        }
        {
          if (count == length) {
            break;
          }
          Thread.sleep(1000);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void runScp(String[] commands) {
    String localFile = commands[1];
    String remoteDir = commands[2];
    Map<String, Future<?>> futureDetails = new HashMap<>();
    for (Node node : nodes) {
      String host = node.getIp();
      Future<?> future = executorService.submit(new Runnable() {
        @Override
        public void run() {
          System.out.println("File Transfer started on " + host);
          ScpCommandExecutor.upload(userName, host, remoteDir, localFile);

        }
      });
      futureDetails.put(host, future);

    }
    int count = 0;
    int length = futureDetails.size();
    Iterator<Entry<String, Future<?>>> iterator = futureDetails.entrySet().iterator();
    while (true) {

      try {
        while (iterator.hasNext()) {
          Map.Entry<String, Future<?>> pair = (Entry<String, Future<?>>) iterator.next();
          String host = pair.getKey();
          Future<?> future = pair.getValue();
          if (future.get() == null) {
            iterator.remove();
            count++;
            System.out.println("File Transfer finished on " + host);
          } else {
            System.out.println("File is still transfering on " + host);
          }
        }
        {
          if (count == length) {
            break;
          }
          Thread.sleep(1000);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }



  }

  private static void runDownload(String[] commands) {

    // String host = node.getIp();
    ScpCommandExecutor.download(userName, commands[1], commands[2], commands[3]);

  }

  private static void runCat(String[] commands) {
    String host = commands[1];
    String fileName = commands[2];
    ScpCommandExecutor.cat(userName, host, fileName);
  }



  private static void runRemoveDirs(String[] commands) {
    Map<String, Future<?>> futureDetails = new HashMap<>();
    for (Node node : nodes) {
      String host = node.getIp();
      Future<?> future = executorService.submit(new Runnable() {
        @Override
        public void run() {
          System.out.println("Remvoing Files started on " + host);
          ScpCommandExecutor.removeDir(userName, host, commands[1]);

        }
      });
      futureDetails.put(host, future);

    }

    int count = 0;
    int length = futureDetails.size();

    while (true) {
      Iterator<Entry<String, Future<?>>> iterator = futureDetails.entrySet().iterator();
      try {
        while (iterator.hasNext()) {
          Map.Entry<String, Future<?>> pair = (Entry<String, Future<?>>) iterator.next();
          String host = pair.getKey();
          Future<?> future = pair.getValue();
          if (future.get() == null) {
            iterator.remove();
            System.out.println("Remvoing Files finished on " + host);
          } else {
            System.out.println("File is still transfering on " + host);
          }
        }
        {
          if (count == length) {
            break;
          }
          Thread.sleep(1000);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void readInput() {
    try {
      System.out.println("Select Directory from where you want to run java command");
      localDir = br.readLine();
      System.out.print("set outputFile:-");
      outPutFile = br.readLine();
      System.out.println();
      System.out.print("set error File:-");
      errorFile = br.readLine();

    } catch (IOException e) {
      System.out.println(e.getMessage());
    }

  }
}
