package com.talentica.torrent.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.torrent.TorrentTrackerStarter;
import com.turn.ttorrent.common.Torrent;

/**
 *{@code TorrentGenerator}  Used for generating torrent file.
 *
 */
public class TorrentGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(TorrentGenerator.class);

  /**
   * creates a list of path and announce it.
   * @param client
   * @param seedFile
   * @return
   * @throws Exception
   * @throws IOException
   */
  public static File generateTorrentFile(CuratorFramework client, File seedFile)
      throws Exception, IOException {
    List<URI> announceList = new ArrayList<>(1);
    client.getChildren().forPath(TorrentTrackerStarter.TRACKERS_NODE_PATH).forEach(childPath -> {
      try {
        announceList.add(URI.create(new String(
            client.getData().forPath(TorrentTrackerStarter.TRACKERS_NODE_PATH + "/" + childPath))));
      } catch (Exception exception) {
        LOGGER.error("Error occurred while creating trackers announce list for path: {}", childPath,
            exception);
      }
    });
    File torrentFile = File.createTempFile(seedFile.getName(), ".torrent");
    generateTorrentFile(seedFile.getAbsolutePath(), announceList, "SYSTEM",
        torrentFile.getAbsolutePath());
    return torrentFile;
  }


  /**
   * Generates and returns torrent for the source file.
   * 
   * @param sourceFilePath
   * @param trackerUri
   * @param createdBy
   */
  public static Torrent generate(File sourceFile, List<URI> trackerUri, String createdBy) {
    Torrent torrent;
    try {
      List<List<URI>> announceList = new ArrayList<>();
      announceList.add(new ArrayList<>(trackerUri));
      torrent = Torrent.create(sourceFile, Torrent.DEFAULT_PIECE_LENGTH, announceList, createdBy);
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
  private static void generateTorrentFile(String sourceFilePath, List<URI> announceList,
      String createdBy, String torrentFilePath) {
    try {
      if (!FilenameUtils.isExtension(torrentFilePath, "torrent")) {
        throw new RuntimeException("Invalid torrent file name.");
      }
      String directory = FilenameUtils.getFullPath(torrentFilePath);
      new File(directory).mkdirs();
      FileOutputStream output = new FileOutputStream(torrentFilePath);
      Torrent torrent = generate(new File(sourceFilePath), announceList, createdBy);
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
    List<URI> trackers = new ArrayList<>();
    trackers.add(trackerUri);
    generateTorrentFile(sourceFilePath, trackers, createdBy, outputDirectory);
  }

  private static void validateArguments(String[] args) {
    if (args.length < 4) {
      System.out.println(
          "Missing required arguments of source file path, tracker URI, created by and torrent file path. Please provide them and try again.");
      System.exit(1);
    }
  }

}
