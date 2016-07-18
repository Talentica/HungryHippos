package com.talentica.hungryhippos.filesystem;

import static java.nio.file.FileVisitResult.CONTINUE;

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

public class NodeFileSystems {

  private static final Logger logger = LoggerFactory.getLogger(CleanFileSystem.class);
  private static final String ROOT_DIR = "HungryHipposFs";
  private static final String USER_DIR = System.getProperty("user.dir");

  public boolean checkFileExistsInNode(Path path) {

    return Files.exists(path);
  }

  /**
   * return all the regular files in the folder provided as an argument as well as regular files in
   * its subfolder.
   * 
   * i.e; /{user.dir}/HungryHippos/data/data.txt -> regular file will get be added to the list.
   * /{user.dir}/HungryHippos/data -> is a directory and will not get added.
   * 
   * 
   * @param path
   * @return
   */
  public static List<Path> getAllRgularFilesPath(String path) {
    List<Path> filesPresentInRootFolder = new ArrayList<>();
    try {
      Files.walk(Paths.get(path)).forEach(filePath -> {
        if (Files.isRegularFile(filePath)) {
          filesPresentInRootFolder.add(filePath);
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
   * @param path
   * @return
   */

  public static List<Path> getAllDirectoryPath(String path) {
    List<Path> filesPresentInRootFolder = new ArrayList<>();
    try {
      Files.walk(Paths.get(path)).forEach(filePath -> {
        if (Files.isDirectory(filePath)) {
          filesPresentInRootFolder.add(filePath);
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
   * @param path
   */

  public static void deleteFile(String path) {
    if (path.toString().equals(USER_DIR + ROOT_DIR)) {
      return;
    }
    try {
      Files.delete(Paths.get(path));
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Deletes all the items inside a folder including that folder.
   * 
   * @param path
   */

  public static void deleteAllFilesInsideAFolder(String path) {
    List<Path> regularFilesInFolder = getAllRgularFilesPath(path);
    List<Path> directoryInsideFolder = getAllDirectoryPath(path);

    regularFilesInFolder.stream().forEach(CleanFileSystem::deleteFile);
    // reversing the list so first subfolders of a folder is deleted first.
    directoryInsideFolder.stream().sorted().collect(Collectors.toCollection(ArrayDeque::new))
        .descendingIterator().forEachRemaining(CleanFileSystem::deleteFile);

  }

  public static Path createDir(String loc) {
    Path path = null;

    try {
      path = Files.createDirectories(path);
    } catch (IOException e) {

      e.printStackTrace();
    }
    return path;
  }

  public static Path createFile(String loc) {
    Path path = null;
    try {
      path = Files.createFile(Paths.get(loc));
    } catch (IOException e) {

    }
    return path;
  }


  public static class Finder extends SimpleFileVisitor<Path> {

    private final PathMatcher matcher;
    private int numMatches = 0;

    Finder(String pattern) {
      matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
    }

    // Compares the glob pattern against
    // the file or directory name.
    void find(Path file) {
      Path name = file.getFileName();
      if (name != null && matcher.matches(name)) {
        numMatches++;
        System.out.println(file);
      }
    }

    // Prints the total number of
    // matches to standard out.
    void done() {
      System.out.println("Matched: " + numMatches);
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
