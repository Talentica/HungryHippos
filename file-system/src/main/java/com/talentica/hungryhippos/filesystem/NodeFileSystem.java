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

/**
 * 
 * {@code NodeFileSystem} used for doing Node related fileSystem operations.
 * 
 * @author sudarshans
 *
 */
public class NodeFileSystem {

  private static final Logger logger = LoggerFactory.getLogger(NodeFileSystem.class);
  private final String HungryHipposRootDir;

  /**
   * creates an instance of the NodeFileSystem.
   * 
   * @param rootDir
   */
  public NodeFileSystem(String rootDir) {
    this.HungryHipposRootDir = rootDir;
    logger.info("File System Root Dir is set to {},rootDir");
  }

  /**
   * helper method to append the rootdirectory
   * 
   * @param loc
   * @return
   */
  private String checkNameContainsFileSystemRoot(String loc) {
    if (null == loc) {
      return HungryHipposRootDir;
    } else if (!(loc.contains(HungryHipposRootDir))) {
      if (loc.startsWith(String.valueOf(File.separatorChar))) {
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
  public boolean checkFileExistsInNode(String loc) {
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
  public List<String> getAllRgularFilesPath(String loc) {
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

  public List<String> getAllDirectoryPath(String loc) {
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

  public void deleteFile(String loc) {

    loc = checkNameContainsFileSystemRoot(loc);

    if (loc.toString().equals(HungryHipposRootDir)) {
      return;
    }
    try {
      Files.delete(Paths.get(loc));
      logger.info("deleted file : {} ", loc);
    } catch (IOException e) {
      logger.error(e.getMessage() + "is not present");
    }
  }

  /**
   * Deletes all the items inside a folder including that folder.
   * 
   * @param loc
   */

  public void deleteAllFilesInsideAFolder(String loc) {
    loc = checkNameContainsFileSystemRoot(loc);
    List<String> regularFilesInFolder = getAllRgularFilesPath(loc);
    List<String> directoryInsideFolder = getAllDirectoryPath(loc);

    regularFilesInFolder.stream().forEach(this::deleteFile);
    // reversing the list so first subfolders of a folder is deleted first.
    directoryInsideFolder.stream().sorted().collect(Collectors.toCollection(ArrayDeque::new))
        .descendingIterator().forEachRemaining(this::deleteFile);
    logger.info("deleted all the files including : {} ", loc);

  }

  /**
   * creates directory.
   * 
   * @param loc
   * @return
   */
  public String createDir(String loc) {
    loc = checkNameContainsFileSystemRoot(loc);
    Path path = null;

    try {
      path = Files.createDirectories(Paths.get(loc));
      logger.info("created dir : {}", loc);
    } catch (IOException e) {

      logger.error(e.getMessage());
    }
    if (path != null) {
      return path.toString();
    }
    return null;
  }

  /**
   * creates file.
   * 
   * @param loc
   * @return String, representing the path.
   */
  public String createFile(String loc) {
    loc = checkNameContainsFileSystemRoot(loc);
    Path path = null;
    try {
      path = Files.createFile(Paths.get(loc));
      logger.info("created dir : {}", loc);
    } catch (IOException e) {
      logger.error(e.getMessage() + "is not present");
    }
    if (path != null) {
      return path.toString();
    }
    return null;
  }

  /**
   * retrieves file with similar pattern.
   * 
   * @param args
   * @return a List containing all the files with similar pattern else a null value is provided.
   */
  public List<String> findFilesWithPattern(String... args) {
    String loc = null;
    String pattern = null;
    if (args.length == 3) {
      loc = checkNameContainsFileSystemRoot(args[0]);
      pattern = args[2];
    } else {
      throw new IllegalArgumentException("java Find <path>" + " -name \"<glob_pattern>\"");
    }

    Finder finder = new Finder(pattern);
    try {
      Files.walkFileTree(Paths.get(loc), finder);
    } catch (IOException e) {
      throw new RuntimeException("system corrupted");
    }

    return finder.done();
  }

  /**
   * used for creating directory and file.
   * 
   * @param loc
   */
  public void createDirAndFile(String loc) {
    String fileName = getFileNameFromPath(loc);
    if (null == fileName) {
      createDir(loc);
      logger.info("created dir : {}", loc);
    } else {
      String parentFolder = loc.substring(0, (loc.length() - fileName.length()));
      if (!checkFileExistsInNode(parentFolder)) {
        createDir(parentFolder);
        logger.info("created parent dir : {}", parentFolder);
      }
      createFile(loc);
      logger.info("created file : {}", loc);
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

  /**
   * {@code Finder} used for searching file in a system.
   * 
   * @author sudarshans
   *
   */
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
