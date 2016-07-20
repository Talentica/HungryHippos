package com.talentica.hungryhippos.filesystem;

import static java.nio.file.FileVisitResult.CONTINUE;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeFileSystem {

  private static final Logger logger = LoggerFactory.getLogger(NodeFileSystem.class);
  private static final String HHDIR = "HungryHipposFs";
  private static final String USER_DIR = System.getProperty("user.home");
  private static final String HungryHipposRootDir = USER_DIR + File.separatorChar + HHDIR;

  /**
   * helper method to append the rootdirectory
   * 
   * @param loc
   * @return
   */
  private static String checkNameContainsFileSystemRoot(String loc) {
    if (null == loc) {
      return HungryHipposRootDir;
    } else if (!(loc.contains(HungryHipposRootDir))) {
      if (loc.startsWith("/")) {
        loc = HungryHipposRootDir + loc;
      } else {
        loc = HungryHipposRootDir + File.separatorChar + loc;
      }
    }
    return loc;
  }

  /**
   * Returns whether a file exists in the HungryHipposFileSystem with specified name.
   * 
   * @param loc
   * @return
   */
  public static boolean checkFileExistsInNode(String loc) {
    loc = checkNameContainsFileSystemRoot(loc);
    return Files.exists(Paths.get(loc));
  }

  /**
   * return all the regular files in the folder provided as an argument as well as regular files in
   * its subfolder.
   * 
   * i.e; /{user.dir}/HungryHippos/data/data.txt -> regular file will get be added to the list.
   * /{user.dir}/HungryHippos/data -> is a directory and will not get added.
   * 
   * 
   * @param loc
   * @return
   */
  public static List<String> getAllRgularFilesPath(String loc) {
    loc = checkNameContainsFileSystemRoot(loc);
    List<String> filesPresentInRootFolder = new ArrayList<>();
    try {
      Files.walk(Paths.get(loc)).forEach(filePath -> {
        if (Files.isRegularFile(filePath)) {
          filesPresentInRootFolder.add(filePath.toString());
        }
      });
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
    return filesPresentInRootFolder;
  }

  /**
   * return all the directory in the folder provided as an argument as well as in its subfolder.
   * 
   * i.e; /{user.dir}/HungryHippos/data/data.txt -> regular file will get be added to the list.
   * /{user.dir}/HungryHippos/data -> is a directory and will not get added.
   * 
   * 
   * @param loc
   * @return
   */

  public static List<String> getAllDirectoryPath(String loc) {
    loc = checkNameContainsFileSystemRoot(loc);
    List<String> filesPresentInRootFolder = new ArrayList<>();
    try {
      Files.walk(Paths.get(loc)).forEach(filePath -> {
        if (Files.isDirectory(filePath)) {
          filesPresentInRootFolder.add(filePath.toString());
        }
      });
    } catch (IOException e) {
      logger.error(e.getMessage());

    }
    return filesPresentInRootFolder;
  }

  /**
   * Delete a file. fails when the folder has children
   * 
   * @param loc
   */

  public static void deleteFile(String loc) {

    loc = checkNameContainsFileSystemRoot(loc);

    if (loc.toString().equals(HungryHipposRootDir)) {
      return;
    }
    try {
      Files.delete(Paths.get(loc));
    } catch (IOException e) {
      logger.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Deletes all the items inside a folder including that folder.
   * 
   * @param loc
   */

  public static void deleteAllFilesInsideAFolder(String loc) {
    loc = checkNameContainsFileSystemRoot(loc);
    List<String> regularFilesInFolder = getAllRgularFilesPath(loc);
    List<String> directoryInsideFolder = getAllDirectoryPath(loc);

    regularFilesInFolder.stream().forEach(NodeFileSystem::deleteFile);
    // reversing the list so first subfolders of a folder is deleted first.
    directoryInsideFolder.stream().sorted().collect(Collectors.toCollection(ArrayDeque::new))
        .descendingIterator().forEachRemaining(NodeFileSystem::deleteFile);

  }

  public static String createDir(String loc) {
    loc = checkNameContainsFileSystemRoot(loc);
    Path path = null;

    try {
      path = Files.createDirectories(Paths.get(loc));
    } catch (IOException e) {

      e.printStackTrace();
    }
    return path.toString();
  }

  public static String createFile(String loc) {
    loc = checkNameContainsFileSystemRoot(loc);
    Path path = null;
    try {
      path = Files.createFile(Paths.get(loc));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return path.toString();
  }

  public static List<String> findFilesWithPattern(String... args) {
    String loc = null;
    String pattern = null;
    if (args.length == 3) {
      loc = checkNameContainsFileSystemRoot(args[0]);
      pattern = args[2];
    } else {
      loc = checkNameContainsFileSystemRoot(loc);
      pattern = args[1];
    }

    Finder finder = new Finder(pattern);
    try {
      Files.walkFileTree(Paths.get(loc), finder);
    } catch (IOException e) {
      throw new RuntimeException("system corrupted");
    }
   
    return finder.done();
  }

  public static void createDirAndFile(String loc) {
    String fileName = getFileNameFromPath(loc);
    if (null == fileName) {
      createDir(loc);
    } else {
      String parentFolder = loc.substring(0, (loc.length() - fileName.length()));
      if (!checkFileExistsInNode(parentFolder)) {
        createDir(parentFolder);
      }
      createFile(loc);
    }

  }

  private static String getFileNameFromPath(String loc) {
    String[] fileNames = loc.split(String.valueOf(File.separatorChar));
    String fileName = null;
    int arrayIdOfLastElement = fileNames.length - 1;
    if (fileNames[arrayIdOfLastElement].contains(".")) {
      fileName = fileNames[arrayIdOfLastElement];
    }
    return fileName;
  }

  public static class Finder extends SimpleFileVisitor<Path> {

    private final PathMatcher matcher;
    private int numMatches = 0;
    private List<String> files = new ArrayList<>();

    Finder(String pattern) {
      matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
    }

    // Compares the glob pattern against
    // the file or directory name.
    void find(Path file) {
      Path name = file.getFileName();
      if (name != null && matcher.matches(name)) {
        numMatches++;
        files.add(name.toString());
      }
    }

    // return the matched files.
    List<String> done() {
      return this.files;
    }

    // Invoke the pattern matching
    // method on each file.
    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      find(file);
      return CONTINUE;
    }

    // Invoke the pattern matching
    // method on each directory.
    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
      find(dir);
      return CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) {
      System.err.println(exc);
      return CONTINUE;
    }
  }

  static void usage() {
    System.err.println("java Find <path>" + " -name \"<glob_pattern>\"");
    System.exit(-1);
  }
}
