package com.talentica.hungryhippos.filesystem.main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import javax.xml.bind.JAXBException;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

import com.talentica.hungryHippos.coordination.HungryHippoCurator;
import com.talentica.hungryHippos.coordination.context.CoordinationConfigUtil;
import com.talentica.hungryHippos.utility.jaxb.JaxbUtil;
import com.talentica.hungryhippos.config.client.ClientConfig;
import com.talentica.hungryhippos.config.client.Output;
import com.talentica.hungryhippos.config.cluster.ClusterConfig;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;
import com.talentica.hungryhippos.filesystem.command.CatCommand;
import com.talentica.hungryhippos.filesystem.command.GetCommand;
import com.talentica.hungryhippos.filesystem.command.HeadCommand;
import com.talentica.hungryhippos.filesystem.command.LSCommand;
import com.talentica.hungryhippos.filesystem.command.RMCommand;
import com.talentica.hungryhippos.filesystem.helper.SignalHandlerImpl;

/**
 * {@code HungryHipposCommandLauncher } creates the hhfs console and also calls other classes to
 * provide funcitonality.
 * 
 * @author sudarshans
 *
 */
public class HungryHipposCommandLauncher implements Observer {

  private Map<Integer, String> history = new HashMap<>();
  private int commandCount = 0;
  private static HungryHipposFileSystem hhfs = null;
  protected static final Map<String, String> commandMap = new HashMap<String, String>();
  protected final List<?> HHFScommand = new ArrayList<String>();
  private String userName = null;
  private String key = null;
  private String connectString = null;
  private int sessionTimeOut;
  private List<Node> nodesInCluster = null;

  static {
    commandMap.put("ls", "[fileName]");
    commandMap.put("rm", "[fileName]");
    commandMap.put("cat", "[fileName/host]");
    commandMap.put("get", "fileName/host");
    commandMap.put("head", "HHFileLocation/FileToBeRead");
    commandMap.put("exit", "exit the application");
  }

  public void setNodesInCluster() {
    ClusterConfig clusterConfig = CoordinationConfigUtil.getZkClusterConfigCache();
    this.nodesInCluster = clusterConfig.getNode();
  }


  public static void main(String[] args) throws FileNotFoundException, JAXBException {
    validateArguments(args);
    HungryHipposCommandLauncher hhcl = new HungryHipposCommandLauncher();
    hhcl.setClientConfig(args[0]);
    HungryHippoCurator.getInstance(hhcl.connectString, hhcl.sessionTimeOut);
    hhcl.setNodesInCluster();
    // hhfs = HungryHipposFileSystemMain.getHHFSInstance();

    try {
      hhcl.consoleReader();
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


  private void consoleReader() throws ClassNotFoundException, InstantiationException,
      IllegalAccessException, IllegalArgumentException, InvocationTargetException,
      NoSuchMethodException, SecurityException, InterruptedException, IOException {

    Class<?> consoleC = Class.forName("jline.ConsoleReader");
    Object console = consoleC.getConstructor().newInstance();
    Class<?> completorC =
        Class.forName("com.talentica.hungryhippos.filesystem.helper.TabCompletor");
    Object completor = completorC.getConstructor().newInstance();
    Method addCompletor = consoleC.getMethod("addCompletor", Class.forName("jline.Completor"));
    addCompletor.invoke(console, completor);

    String line;
    Method readLine = consoleC.getMethod("readLine", String.class);
    while ((line = (String) readLine.invoke(console, this.getPrompt())) != null) {
      this.handleSignal();
      this.executeLine(line);
    }
  }



  public static HungryHipposFileSystem getHHFSInstance()
      throws FileNotFoundException, JAXBException {
    hhfs = HungryHipposFileSystem.getInstance();
    return hhfs;
  }

  protected String getPrompt() {
    return "[HHFS: " + " " + commandCount + "] ";
  }

  public void executeLine(String line) throws InterruptedException, IOException {
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
      case "head":
        HeadCommand.execute(parser, str);
        break;
      case "exit":
        System.exit(1);

      default:
        System.out.println("Not yet implemented");
    }
  }


  protected void addToHistory(int i, String cmd) {
    history.put(i, cmd);
  }

  private void setClientConfig(String cliConfig) {
    ClientConfig clientConfig = null;
    try {
      clientConfig = JaxbUtil.unmarshalFromFile(cliConfig, ClientConfig.class);
    } catch (FileNotFoundException | JAXBException e) {
      throw new RuntimeException(e.getMessage());
    }
    Output output = clientConfig.getOutput();
    userName = output.getNodeSshUsername();
    key = output.getNodeSshPrivateKeyFilePath();
    connectString = clientConfig.getCoordinationServers().getServers();
    sessionTimeOut = Integer.valueOf(clientConfig.getSessionTimout());

  }

  @Override
  public void update(Observable o, Object arg) {
    System.out.println("INT signal received");
  }

  private void handleSignal() {
    final SignalHandlerImpl sh = new SignalHandlerImpl();
    sh.addObserver(this);
    sh.handleSignal("TERM", Thread.currentThread());
    sh.handleSignal("INT", Thread.currentThread());
  }

  public List<Node> getNodesInCluster() {
    return Collections.unmodifiableList(nodesInCluster);
  }

  public String getUserName() {
    return userName;
  }

  public static List<String> getCommands() {
    return Collections.unmodifiableList(new LinkedList<String>(commandMap.keySet()));
  }

}
