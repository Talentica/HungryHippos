package com.talentica.hungryhippos.filesystem.main;

import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.data.Stat;

import com.talentica.hungryHippos.utility.FileSystemConstants;
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

    name = name.endsWith("/") ? name.substring(0, name.length() - 1) : name;
    if (list == null) {

    } else if (list.contains(FileSystemConstants.SHARDED)) {
      System.out.println("File Name:- " + fileName(name));
      System.out.println("sharded:- true");
    } else {
      System.out.println("Folder Name:- " + fileName(name));
      printOnScreen(hhfs.getChildZnodes(name));
    }

    if (list.contains(FileSystemConstants.DFS_NODE)) {
      List<String> nodeDetails = hhfs.getChildZnodes(name + "/" + FileSystemConstants.DFS_NODE);

      System.out.print("File is Distributed on Following nodes:- ");
      for (String node : nodeDetails) {
        System.out.print("   " + node);
      }
      System.out.println();
      String dimension = null;


      final String name1 = name;
      HungryHipposFileSystemMain.Size size = new HungryHipposFileSystemMain().new Size();
      for (String node : nodeDetails) {
        List<String> childs =
            hhfs.getChildZnodes(name + "/" + FileSystemConstants.DFS_NODE + "/" + node);
        if (dimension == null) {
          dimension = String.valueOf((int) (Math.log(childs.size()) / Math.log(2)));
        }


        try {
          for (String child : childs) {

            List<String> childsChild = hhfs.getChildZnodes(
                name + "/" + FileSystemConstants.DFS_NODE + "/" + node + "/" + child);


            for (String leaf : childsChild) {

              long length = Long.valueOf(hhfs.getData(name1 + "/" + FileSystemConstants.DFS_NODE
                  + "/" + node + "/" + child + "/" + leaf));
              size.addSize(length);
            }


          }

        } catch (NumberFormatException e) {
          System.out.println("size is stored in serialized form");
        }
      }
      System.out.println("Dimension:- " + dimension);
      System.out.println("size of the file Combined :- " + size.getSize() + " bytes");
      Stat stat = hhfs.getZnodeStat(name);
      System.out.println("Created On:-" + getDateString(stat.getCtime()));
      System.out.println("Modified On:-" + getDateString(stat.getMtime()));
      System.out.println();
    }
  }

  private static String getDateString(long value) {
    Date date = new Date(value);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
    return sdf.format(date);
  }

  private static String fileName(String name) {
    String[] dir = name.split("/");
    return dir[dir.length - 1];
  }


  private static void runOperation(Operations op, String name) {
    switch (op) {
      case LS:
        String data = hhfs.getData(name);
        if (data != null && data.contains(FileSystemConstants.IS_A_FILE)) {
          System.out.println(fileName(name) + " " + FileSystemConstants.IS_A_FILE);
        } else {
          printOnScreen(hhfs.getChildZnodes(name));
        }
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
        List<String> childNodes = hhfs.getChildZnodes(name);
        if (childNodes != null) {
          showMetaData(childNodes, name);
        } else {

        }
      default:
    }

  }


  private class Size {
    long size = 0l;

    synchronized void addSize(long l) {
      size += l;
    }

    long getSize() {
      return size;
    }
  }

}
