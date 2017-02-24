/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.File;
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



  public HHRDDIterator(String filePath, List<Tuple2<String, int[]>> files,
      Map<Integer, SerializedNode> nodeInfo, String dataDirectory) throws IOException {
    try {
      downloadRemoteFilesIfNotExists(filePath, files, nodeInfo, dataDirectory);
    } catch (IOException ex) {
      ex.printStackTrace();
      System.out.println(ex.getMessage());
    }
  }

  public HHRDDIterator(String filePath, int rowSize, List<Tuple2<String, int[]>> files,
      Map<Integer, SerializedNode> nodeInfo, String dataDirectory) throws IOException {
    try {
      downloadRemoteFilesIfNotExists(filePath, files, nodeInfo, dataDirectory);
    } catch (IOException ex) {
      ex.printStackTrace();
      System.out.println(ex.getMessage());
    }
    // this.hhRDDRowReader = new HHRDDRowReader(dataDescription);
    this.byteBufferBytes = new byte[rowSize];
    // this.byteBuffer = ByteBuffer.wrap(byteBufferBytes);
    this.recordLength = rowSize;
    // this.hhRDDRowReader.setByteBuffer(byteBuffer);
  }


  protected abstract void readyFileProcess() throws IOException;

  protected abstract boolean downloadFile(String filePath, String ip, int port);

  protected abstract void closeStream() throws IOException;

  private void downloadRemoteFilesIfNotExists(String filePath,
      final List<Tuple2<String, int[]>> files, Map<Integer, SerializedNode> nodeInfo,
      String dataDirectory) throws IOException {
    String tmpFileDirectoryLocation = dataDirectory + File.separator + "_tmp";
    File tmpDir = new File(tmpFileDirectoryLocation);
    if (!tmpDir.exists()) {
      tmpDir.mkdir();
    }
    this.filePath = filePath + File.separator;
    trackRemoteFiles = new HashSet<>();

    for (Tuple2<String, int[]> tuple2 : files) {
      File file = new File(filePath + File.separator + tuple2._1);
      if (!file.exists()) {
        logger.info("Downloading file {}/{} from nodes {} ", filePath, tuple2._1, tuple2._2);
        boolean isFileDownloaded = false;
        for (int hostIndex = 0; hostIndex < tuple2._2.length; hostIndex++) {
          int index = tuple2._2[hostIndex];
          String ip = nodeInfo.get(index).getIp();
          int port = nodeInfo.get(index).getPort();
          File blacklistIPFile = new File(tmpFileDirectoryLocation + File.separator + ip);
          if (blacklistIPFile.exists()) {
            continue;
          }

          int maxRetry = 5;
          while (!isFileDownloaded && (maxRetry--) > 0) {
            isFileDownloaded = downloadFile(this.filePath + tuple2._1, ip, port);
          }
          if (isFileDownloaded) {
            logger.info("File downloaded success status {} from ip {}", isFileDownloaded, ip);
            break;
          } else {
            logger.info(" Node {} is dead", ip);
            if (!blacklistIPFile.exists()) {
              blacklistIPFile.createNewFile();
            }
          }
        }
        if (!isFileDownloaded) {
          File[] tempIpfiles = tmpDir.listFiles();
          String[] ip = new String[tempIpfiles.length];
          for (int index = 0; index < ip.length; index++) {
            ip[index] = tempIpfiles[index].getName();
          }
          throw new RuntimeException(
              "Application cannot run as nodes :: " + Arrays.toString(ip) + " are not listening");
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
