package com.talentica.hungryhippos.filesystem.main;

import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FileOperations {
  /**
   * args[0] contains the
   * 
   * @param args
   */
  private static FileSystem fs = FileSystems.getDefault();
  private static Path rootDir = null;
  private static String userHome = null;
  private static String userDir = null;
  private static final String HUNGRY_HIPPOS_FOLDER = "/HungryHippos/";
  private static String hhroot = null;
  private static Logger logger = LoggerFactory.getLogger(FileOperations.class);

  /*
   * public static void main(String[] args) { System.out.println(fs); Iterable<Path> it =
   * fs.getRootDirectories(); Path actualRoot = null; for (Path path : it) { actualRoot =
   * path.getRoot(); } String userDir = System.getProperty("user.home");
   * System.out.println(userDir); Path path = fs.getPath(actualRoot.toString(), userDir,
   * "/HungryHippos/" + args[0]); System.out.println(path);
   * 
   * try { Path path1 = Files.createDirectories(path, attrs); if (path1 != null) {
   * System.out.println("created Directory sucessfully"); } } catch (IOException e) {
   * 
   * e.printStackTrace(); }
   * 
   * }
   */

  /**
   * This method returns the defaultFileSystem of the Operating System.
   * 
   * @return FileSystem
   */
  public static FileSystem getDefaultFileSystem() {
    logger.info("Default fileSystem is " + fs);
    return fs;
  }

  /**
   * This method returns the path of the root directory of the FileSystem.
   * 
   * @return String
   * 
   *         Note:- To convert the string to root. You can use createPath(String arg).
   */
  public static String getRoot() {
    Iterable<Path> it = fs.getRootDirectories();
    rootDir = null;
    for (Path path : it) {
      rootDir = path.getRoot();
    }
    return rootDir.toString();
  }

  /**
   * This method retrieves the HungryHippos Root folder. all the files related to HungryHippos is
   * stored here whether its
   * 
   * @return
   * 
   *         Note:- To convert the string to root. You can use createPath(String arg)
   */
  public static String getHungryHipposRoot() {

    hhroot = rootDir.toString() + userDir + HUNGRY_HIPPOS_FOLDER;
    return hhroot;
  }

  /**
   * This method retrieves the current user home directory
   * 
   * @return String
   * 
   *         Note:- To convert the string to root. You can use createPath(String arg) *
   */
  public static String getUserHome() {
    userHome = System.getProperty("user.home");
    return userHome;
  }

  /**
   * This method retrieves the present location where code is running.
   * 
   * @return String
   * 
   *         Note:- To convert the string to root. You can use createPath(String arg) *
   */
  public static String getUserDir() {
    userDir = System.getProperty("user.dir");
    return userDir;
  }

  /**
   * Method to use for converting a string to a path.
   * 
   * @param dirName
   * @return
   */
  public static Path createPath(String dirName) {
    // fs.getPath(rootDir.toString(), userDir, "/HungryHippos/" + args[0]);
    Path path = fs.getPath(hhroot, dirName);
    return path;
  }

  /**
   * This method is used for setting FileAttributes.
   * 
   * @param attributes is a string of this format "rwxr-x--x"
   * @return
   */
  public static FileAttribute<Set<PosixFilePermission>> setAttributes(String attributes) {
    Set<PosixFilePermission> perms = setPermission(attributes);
    FileAttribute<Set<PosixFilePermission>> attrs = PosixFilePermissions.asFileAttribute(perms);
    return attrs;
  }

  /**
   * This method is used for setting FilePermission.
   * 
   * @param attributes is a string of this format "rwxr-x--x"
   * @return
   */
  public static Set<PosixFilePermission> setPermission(String attributes) {
    return PosixFilePermissions.fromString(attributes);
  }

  /**
   * This method is used for creating Directory, if parent directory is not present it will create
   * parent directory first.
   * 
   * @param path
   * @param attrs
   * @return boolean
   */
  public static boolean createDirectory(Path path, FileAttribute<Set<PosixFilePermission>> attrs) {
    boolean created = false;
    try {
      path = Files.createDirectories(path, attrs);
      if (path != null) {
        created = true;
      }
    } catch (IOException e) {

      System.out.println(e.getMessage());
    }

    return created;
  }

  /**
   * This method is used for deleting a single file. It fails when the file is a directory which is
   * not empty.
   * 
   * @param String
   * @return boolean
   */
  public boolean deleteFile(String file) {
    Path path = createPath(file);
    return deleteFile(path);
  }

  /**
   * This method is used for deleting a single file. It fails when the file is a directory which is
   * not empty.
   * 
   * @param Path
   * @return boolean
   */
  public boolean deleteFile(Path path) {
    boolean flag = false;
    try {
      Files.deleteIfExists(path);
      flag = true;
    } catch (NoSuchFileException x) {

      System.err.format("%s: no such" + " file or directory%n", path);
    } catch (DirectoryNotEmptyException x) {
      System.err.format("%s not empty%n", path);
    } catch (IOException x) {
      // File permission problems are caught here.
      System.err.println(x);
    }
    return flag;
  }

  /**
   * This method is used for deleting everything inside the folder. Even its subfolder and contents
   * will be deleted.
   * 
   * @param path
   * @return
   */
  public boolean deleteEverything(Path path) {
    // LinkOption[] link = null;
    boolean flag = false;
    boolean isDir = isDirectory(path);
    if (isDir) {
      List<Path> files = listFiles(path);
      for (Path file : files) {
        deleteEverything(file);
      }

    }
    try {
      Files.deleteIfExists(path);
      flag = true;
    } catch (NoSuchFileException x) {

      System.err.format("%s: no such" + " file or directory%n", path);
    } catch (DirectoryNotEmptyException x) {
      System.err.format("%s not empty%n", path);
    } catch (IOException x) {
      // File permission problems are caught here.
      System.err.println(x);
    }
    return flag;
  }

  /**
   * This method is used to list all the files inside Hungry Hippos root folder.
   */
  public void listFilesInsideHHRoot() {
    listFiles(hhroot);
  }


  /**
   * This method is used to list all the files inside the subFolder of HungryHippos root folder.
   */
  public List<Path> listFiles(String file) {
    Path path = createPath(file);
    return listFiles(path);
  }

  public List<Path> listFiles(Path path) {
    List<Path> files = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
      for (Path file : stream) {
        files.add(file);
      }
    } catch (IOException | DirectoryIteratorException x) {
      // IOException can never be thrown by the iteration.
      // In this snippet, it can only be thrown by newDirectoryStream.
      System.err.println(x);
    }
    return files;
  }

  /**
   * This method used for creating a file with attributes.
   * 
   * @param fileName
   * @param attrs
   */
  public void createFile(String fileName, FileAttribute<Set<PosixFilePermission>> attrs) {
    Path path = createPath(fileName);
    createFile(path, attrs);
  }


  /**
   * This method used for creating a file with attributes.
   * 
   * @param fileName
   * @param attrs
   */
  public void createFile(Path path, FileAttribute<Set<PosixFilePermission>> attrs) {
    LinkOption[] link = null;

    try {
      if (!checkFileExist(path, link)) {
        Files.createFile(path, attrs);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * This method is used for checking whether the files already created.
   * 
   * @param String
   * @param link
   * @return
   */
  public boolean checkFileExist(String fileName, LinkOption[] link) {
    Path path = createPath(fileName);
    return checkFileExist(path, link);
  }

  /**
   * This method is used for checking whether the files already created.
   * 
   * @param Path
   * @param link
   * @return
   */
  public boolean checkFileExist(Path path, LinkOption[] link) {
    return Files.exists(path, link);
  }

  /**
   * This method checks what all permission all given to the user on the file.
   * 
   * @param fileName
   * @return
   */
  public boolean isRegularExecutableFile(String fileName) {
    return isReadable(fileName) & isWritable(fileName) & isExecutable(fileName);
  }

  /**
   * This method checks whether file is readable.
   * 
   * @param String
   * @return
   */
  public boolean isReadable(String fileName) {
    Path path = createPath(fileName);
    return isReadable(path);
  }

  /**
   * This method checks whether file is writable.
   * 
   * @param String
   * @return
   */
  public boolean isWritable(String fileName) {
    Path path = createPath(fileName);
    return isWritable(path);

  }

  /**
   * This method checks whether file is Executable.
   * 
   * @param String
   * @return
   */
  public boolean isExecutable(String fileName) {
    Path path = createPath(fileName);
    return isExecutable(path);
  }

  /**
   * This method checks whether file is readable.
   * 
   * @param Path
   * @return
   */
  public boolean isReadable(Path path) {
    return Files.isReadable(path);
  }

  /**
   * This method checks whether file is writable.
   * 
   * @param Path
   * @return
   */
  public boolean isWritable(Path path) {
    return Files.isWritable(path);

  }

  /**
   * This method checks whether file is Executable.
   * 
   * @param Path
   * @return
   */
  public boolean isExecutable(Path path) {
    return Files.isExecutable(path);
  }

  /**
   * This method returns the size of the file.
   * 
   * @param path
   * @return
   */
  public long size(Path path) {
    long size = 0L;
    try {
      size = Files.size(path);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return size;
  }

  /**
   * This method checks whether the file is a directory or not.
   * 
   * @param path
   * @return
   */
  public boolean isDirectory(Path path) {
    return Files.isDirectory(path);
  }

  /**
   * This method checks whether the file is a regular file or not.
   * 
   * @param path
   * @return
   */
  public boolean isRegularFile(Path path) {
    return Files.isRegularFile(path);
  }

  /**
   * This method checks whether the Path is a Symbolic link.
   * 
   * @param path
   * @return
   */
  public boolean isSymbolicLink(Path path) {
    return Files.isSymbolicLink(path);
  }

  /**
   * This method checks whether the file is hidden.
   * 
   * @param path
   * @return
   */
  public boolean isHidden(Path path) {
    boolean flag = false;
    try {
      flag = Files.isHidden(path);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return flag;
  }

  /**
   * This method gets the lastModifiedTime
   * 
   * @param path
   * @return
   */
  public FileTime getLastModifiedTime(Path path) {
    FileTime fileTime = null;
    try {
      fileTime = Files.getLastModifiedTime(path);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return fileTime;
  }

  /**
   * This method sets the lastModifiedTime
   * 
   * @param path
   * @return
   */
  public void setLastModifiedTime(Path path, FileTime time) {
    try {
      Files.setLastModifiedTime(path, time);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * This method retrieves owner of the file.
   * 
   * @param path
   * @return
   */
  public UserPrincipal getOwner(Path path) {
    UserPrincipal user = null;
    try {
      user = Files.getOwner(path);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return user;
  }

  /**
   * This method sets owner of the file.
   * 
   * @param path
   * @return
   */
  public void setOwner(Path path, UserPrincipal user) {
    try {
      Files.setOwner(path, user);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * This method reads all the meta-data details of a file.
   * 
   * @param path
   */
  public void getAttribute(Path path) {
    try {
      BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
      FileStore store = Files.getFileStore(path);
      PosixFileAttributes posAttr = Files.readAttributes(path, PosixFileAttributes.class);

      long total = store.getTotalSpace() / 1024;
      long used = (store.getTotalSpace() - store.getUnallocatedSpace()) / 1024;
      long avail = store.getUsableSpace() / 1024;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
