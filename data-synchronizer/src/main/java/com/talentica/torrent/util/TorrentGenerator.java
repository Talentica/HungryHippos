package com.talentica.torrent.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.io.FilenameUtils;

import com.turn.ttorrent.common.Torrent;

public class TorrentGenerator {

  /**
   * Generates and returns torrent for the source file.
   * 
   * @param sourceFilePath
   * @param trackerUri
   * @param createdBy
   */
  public static Torrent generate(File sourceFile, URI trackerUri, String createdBy) {
    Torrent torrent;
    try {
      torrent = Torrent.create(sourceFile, trackerUri, createdBy);
      return torrent;
    } catch (NoSuchAlgorithmException | InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generates a new torrent file at specified path.
   * 
   * @param sourceFilePath
   * @param trackerUri
   * @param createdBy
   * @param torrentFilePath - path to generate torrent file at. Please specify full path along with
   *        name of the torrent file and extension.
   */
  public static void generateTorrentFile(String sourceFilePath, URI trackerUri, String createdBy,
      String torrentFilePath) {
    try {
      if (!FilenameUtils.isExtension(torrentFilePath, "torrent")) {
        throw new RuntimeException("Invalid torrent file name.");
      }
      String directory = FilenameUtils.getFullPath(torrentFilePath);
      new File(directory).mkdirs();
      FileOutputStream output = new FileOutputStream(torrentFilePath);
      Torrent torrent = generate(new File(sourceFilePath), trackerUri, createdBy);
      torrent.save(output);
      output.flush();
      output.close();
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  public static void main(String[] args) throws FileNotFoundException {
    validateArguments(args);
    String sourceFilePath = args[0];
    URI trackerUri = URI.create(args[1]);
    String createdBy = args[2];
    String outputDirectory = args[3];
    generateTorrentFile(sourceFilePath, trackerUri, createdBy, outputDirectory);
  }

  private static void validateArguments(String[] args) {
    if (args.length < 4) {
      System.out.println(
          "Missing required arguments of source file path, tracker URI, created by and torrent file path. Please provide them and try again.");
      System.exit(1);
    }
  }

}
