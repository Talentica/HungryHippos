package com.talentica.hungryhippos.filesystem.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.coordination.domain.NodesManagerContext;
import com.talentica.hungryHippos.utility.ExecuteShellCommand;
import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryHippos.utility.scp.ScpCommandExecutor;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.client.Output;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.client.DataRetrieverClient;

public class HungryHipposCommandLauncher {

  private static Map<Integer, String> history = new HashMap<>();
  private static int commandCount = 0;
  private static HungryHipposFileSystem hhfs = null;
  protected static final Map<String, String> commandMap = new HashMap<String, String>();
  protected static final List<?> HHFScommand = new ArrayList<String>();
  private static final String SCRIPT_LOC =
      "/home/sudarshans/RD/HH_NEW/HungryHippos/utility/scripts/file-system-commands.sh";
  private static String userName = null;
  private static String key = null;
  private static String clientConfig = null;
  private static String clusterConfig = null;
  private static String clientConfigPath = null;
  private static String operation = "";
  private static String fname = "";
  private static List<Node> nodesInCluster = null;
  private static String currentDir = null;
  private static String rootDir = null;

  static {
    commandMap.put("ls", "[fileName]");
    commandMap.put("rm", "[fileName]");
    // commandMap.put("cd", "[file path]");
    // commandMap.put("pwd", "");
    commandMap.put("cat", "[fileName/host]");
    commandMap.put("download", "fileName/host");
    commandMap.put("exit", "");
  }


  public static List<Node> getNodesInCluster() {
    return nodesInCluster;
  }

  public static String getUserName() {
    return userName;
  }


  public static void main(String[] args) throws FileNotFoundException, JAXBException {
    validateArguments(args);
    clientConfig = args[0];

    boolean isXML = checkXMLProvidedInArg(clientConfig);
    if (isXML) {
      setClientConfig(clientConfig);
      NodesManagerContext.getNodesManagerInstance(clientConfig);
      ClusterConfig clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache();
      nodesInCluster = clusterConfig.getNode();

    }
    hhfs = HungryHipposFileSystemMain.getHHFSInstance();
    try {
      Class<?> consoleC = Class.forName("jline.ConsoleReader");
      Object console = consoleC.getConstructor().newInstance();
      Class<?> completorC =
          Class.forName("com.talentica.hungryhippos.filesystem.main.TestCompletor");
      Object completor = completorC.getConstructor().newInstance();
      Method addCompletor = consoleC.getMethod("addCompletor", Class.forName("jline.Completor"));
      addCompletor.invoke(console, completor);

      String line;
      Method readLine = consoleC.getMethod("readLine", String.class);
      while ((line = (String) readLine.invoke(console, getPrompt())) != null) {
        executeLine(line);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void validateArguments(String... args) {
    if (args.length < 1) {
      throw new IllegalArgumentException(
          "Need zookeeper id details. Either provide client-config.xml or type \"host:port\""
              + " where host is the ip of the zookeeper client and port number where its running. i.e:- localhost:2181");
    }
  }

  private static boolean checkXMLProvidedInArg(String arg) {
    boolean flag = false;
    if (arg.endsWith(".xml")) {
      flag = true;
    }

    return flag;

  }

  public static List<String> getCommands() {
    return new LinkedList<String>(commandMap.keySet());
  }



  public static HungryHipposFileSystem getHHFSInstance()
      throws FileNotFoundException, JAXBException {
    hhfs = HungryHipposFileSystem.getInstance();
    return hhfs;
  }

  protected static String getPrompt() {
    return "[HHFS: " + " " + commandCount + "] ";
  }

  public static void executeLine(String line) throws InterruptedException, IOException {
    if (!line.equals("")) {
      String[] cmd = parseCommand(line);
      addToHistory(commandCount, line);
      processCmd(cmd);
      commandCount++;
    }
  }

  public static String[] parseCommand(String line) {
    return line.split(" ");

  }

  public static void processCmd(String[] str) {
    CommandLineParser parser = new DefaultParser();
    switch (str[0]) {
      case "ls":
        LSCommand.execute(parser, str);
        break;
      case "rm":
        RMCommand.execute(parser, str);
        break;
      case "get":
        GetCommand.execute(parser, str);
        break;
      case "cat":
        CatCommand.execute(parser, str);
        break;
      case "exit":
        System.exit(1);
      default:
        System.out.println("Not yet implemented");
    }
  }


  protected static void addToHistory(int i, String cmd) {
    history.put(i, cmd);
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
