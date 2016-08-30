package com.talentica.hungryhippos.filesystem.main;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Scanner;

import javax.xml.bind.JAXBException;

import com.talentica.hungryHippos.utility.FileSystemConstants;
import com.talentica.hungryhippos.config.cluster.Node;
import com.talentica.hungryhippos.filesystem.HungryHipposFileSystem;

public class HungryHipposFileSystemMain {

  enum Operations {
    LS(0), TOUCH(1), MKDIR(2), FIND(3), DELETE(4), DELETEALL(5), EXIT(6), SHOW(7);

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

  private static String[] commands =
      {"ls", "touch", "mkdir", "find", "delete", "deleteall", "exit", "show"};
  private static HungryHipposFileSystem hhfs = null;

  public static HungryHipposFileSystem getHHFSInstance()
      throws FileNotFoundException, JAXBException {
    hhfs = HungryHipposFileSystem.getInstance();
    return hhfs;
  }

  public static void main(String[] args) {

    if (args == null || args.length == 0) {
      usage();
      Scanner sc = new Scanner(System.in);
      if (sc.hasNext()) {
        String s = sc.nextLine();
        if (s != null) {
          String[] operationFileName = s.split(" ");
          getCommandDetails(operationFileName[0], operationFileName[1]);
        }
      }
      sc.close();
    } else {
      if (args.length == 2) {
        getCommandDetails(args[0], args[1]);
      } else {
        getCommandDetails(args[0], null);
      }
    }

  }

  public static void getCommandDetails(String operation, String name) {

    if (hhfs == null) {
      throw new RuntimeException(
          "HungryHipposFileSystem is not created, please create it calling getHHFSInstance() method");
    }

    if (name == null) {
      name = "HungryHipposFs";
    }
    for (int i = 0; i < commands.length; i++) {
      if (operation.equalsIgnoreCase(commands[i])) {
        Operations op = Operations.getOpertionsFromOption(i);
        runOperation(op, name);
        break;
      }
    }

  }

  private static void usage() {
    System.out.println("Please choose what operation you want to do");
    System.out.println("ls \"fileName\"");
    System.out.println("touch \"fileName\"");
    System.out.println("mkdir \"dirName\"");
    System.out.println("find \"fileName\" ");
    System.out.println("delete \"fileName\"");
    System.out.println("deleteall \"fileName\"");
    System.out.println("show \"fileName\"");
    System.out.println("exit");
  }

  private static void printOnScreen(List<String> list) {

    if (list == null) {
      System.out.println("No Files are present");
      return;
    }

    for (String fName : list) {
      System.out.println(fName);
    }
  }

  private static void showMetaData(List<String> list, String name) {
    System.out.println("fileName:- " + name);
    if (list.isEmpty()) {
      System.out.println("MetaData is not set");
    }
    if (list.contains(FileSystemConstants.SHARDED)) {
      System.out.println("sharded:- true");
    }
    int size = 0;
    if (list.contains(FileSystemConstants.DFS_NODE)) {
      List<String> nodeDetails = hhfs.getChildZnodes(name + "/" + FileSystemConstants.DFS_NODE);
      System.out.print("File is Distributed on Following nodes:- ");
      for (String node : nodeDetails) {
        System.out.print("   " + node);
      }
      for (String node : nodeDetails) {
       List<String> childs = hhfs.getChildZnodes(name + "/" + FileSystemConstants.DFS_NODE + "/" + node);
       for (String child : childs) {
        size += Integer.valueOf(hhfs.getData(name + "/" + FileSystemConstants.DFS_NODE + "/" + node + "/" + child));
      }
    }
      System.out.println("size of the file Combined :- " + size/1000 + " kb");
    }
  }


  private static void runOperation(Operations op, String name) {
    switch (op) {
      case LS:
        printOnScreen(hhfs.getChildZnodes(name));
        break;
      case TOUCH:
        hhfs.createZnode(name);
        break;
      case MKDIR:
        hhfs.createZnode(name);
        break;
      case FIND:
        hhfs.findZnodePath(name);
        break;
      case DELETE:
        hhfs.deleteNode(name);
        break;
      case DELETEALL:
        hhfs.deleteNodeRecursive(name);
        break;
      case SHOW:
        showMetaData(hhfs.getChildZnodes(name), name);
      default:
    }

  }

}
