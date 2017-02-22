/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import scala.collection.AbstractIterator;

/**
 * It is an abstract class which need to be extended by the sub class to facilitate the iteration
 * over the partition.
 * 
 * @author pooshans
 * @param <T>
 *
 */
public abstract class HHRDDIterator<T> extends AbstractIterator<T> {
  private static Logger logger = LoggerFactory.getLogger(HHRDDIterator.class);
  protected Set<String> trackRemoteFiles;
  protected String currentFile;
  protected String currentFilePath;
  protected String filePath;
  protected byte[] byteBufferBytes;
  protected Iterator<Tuple2<String, int[]>> fileIterator;
  protected int recordLength;
  private File blacklistIpFile;
  private final String BLACKLIST_IP_FILE = "blacklist_ip";

  public HHRDDIterator(String filePath, List<Tuple2<String, int[]>> files,
      Map<Integer, SerializedNode> nodeInfo, String dataDirectory) throws IOException {
    downloadRemoteFilesIfNotExists(filePath, files, nodeInfo, dataDirectory);
  }

  public HHRDDIterator(String filePath, int rowSize, List<Tuple2<String, int[]>> files,
      Map<Integer, SerializedNode> nodeInfo, String dataDirectory) throws IOException {
    downloadRemoteFilesIfNotExists(filePath, files, nodeInfo, dataDirectory);
    // this.hhRDDRowReader = new HHRDDRowReader(dataDescription);
    this.byteBufferBytes = new byte[rowSize];
    // this.byteBuffer = ByteBuffer.wrap(byteBufferBytes);
    this.recordLength = rowSize;
    // this.hhRDDRowReader.setByteBuffer(byteBuffer);
  }


  protected abstract void readyFileProcess() throws IOException;

  protected abstract boolean downloadFile(String filePath, String ip, int port);

  protected abstract void closeStream() throws IOException;

  private void downloadRemoteFilesIfNotExists(String filePath, List<Tuple2<String, int[]>> files,
      Map<Integer, SerializedNode> nodeInfo, String dataDirectory) throws IOException {
    String tmpFileDirectoryLocation =
        dataDirectory + File.separator + "_tmp" + File.separator + BLACKLIST_IP_FILE;
    this.filePath = filePath + File.separator;
    trackRemoteFiles = new HashSet<>();
    boolean isNodeFail = false;
    for (Tuple2<String, int[]> tuple2 : files) {
      File file = new File(this.filePath + tuple2._1);
      if (!file.exists()) {
        logger.info("Downloading file {}/{} from nodes {} ", filePath, tuple2._1, tuple2._2);
        boolean isFileDownloaded = false;
        for (int hostIndex = 0; hostIndex < tuple2._2.length; hostIndex++) {
          int index = tuple2._2[hostIndex];
          String ip = nodeInfo.get(index).getIp();
          int port = nodeInfo.get(index).getPort();
          if (isNodeFail) {
            FileReader fileReader = new FileReader(blacklistIpFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            boolean isBlacklistIpFound = false;
            while ((line = bufferedReader.readLine()) != null) {
              if (ip.equals(line)) {
                isBlacklistIpFound = true;
                break;
              }
            }
            bufferedReader.close();
            fileReader.close();
            if (isBlacklistIpFound) {
              continue;
            }
          }
          int maxRetry = 3;
          while (!isFileDownloaded && (maxRetry--) > 0) {
            isFileDownloaded = downloadFile(this.filePath + tuple2._1, ip, port);
       
          }
          logger.info("File downloaded success status {} from ip {}", isFileDownloaded, ip);
          if (isFileDownloaded) {
            break;
          } else {
            blacklistIpFile = new File(tmpFileDirectoryLocation);
            if (!blacklistIpFile.exists()) {
              blacklistIpFile.getParentFile().mkdirs();
              blacklistIpFile.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(blacklistIpFile, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(ip);
            bufferedWriter.newLine();
            fileWriter.close();
            bufferedWriter.close();
            isNodeFail = true;
          }
        }
        if (!isFileDownloaded) {
          logger.warn("Nodes {} are dead", Arrays.toString(tuple2._2));
          throw new RuntimeException("Files are not downloaded becasue replica nodes are dead.");
        }
        trackRemoteFiles.add(tuple2._1);
      }

    }
    fileIterator = files.iterator();
    readyFileProcess();
  }

  protected boolean hasNextFile() {
    return fileIterator.hasNext();
  }

  protected Tuple2<String, int[]> nextFile() {
    return fileIterator.next();
  }
}
