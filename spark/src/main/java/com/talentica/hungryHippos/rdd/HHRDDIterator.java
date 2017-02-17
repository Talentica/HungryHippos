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
      Map<Integer, SerializedNode> nodeInfo) throws IOException {
    downloadRemoteFilesIfNotExists(filePath, files, nodeInfo);
  }

  public HHRDDIterator(String filePath, int rowSize, List<Tuple2<String, int[]>> files,
      Map<Integer, SerializedNode> nodeInfo) throws IOException {
    downloadRemoteFilesIfNotExists(filePath, files, nodeInfo);
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
      Map<Integer, SerializedNode> nodeInfo) throws IOException {
    this.filePath = filePath + File.separator;
    trackRemoteFiles = new HashSet<>();
    int selectIndexForHost = 0;
    for (Tuple2<String, int[]> tuple2 : files) {
      File file = new File(filePath + File.separator + tuple2._1);
      if (!file.exists()) {
        logger.info("Downloading file {}/{} from nodes {} ", filePath, tuple2._1, tuple2._2);
        boolean isFileDownloaded = false;
        while (!isFileDownloaded) {
          for (int id = 0; id < tuple2._2.length; id++) {
            if (selectIndexForHost != id) {
              continue;
            }
            int index = tuple2._2[id];
            String ip = nodeInfo.get(index).getIp();
            int port = nodeInfo.get(index).getPort();
            isFileDownloaded = downloadFile(this.filePath + tuple2._1, ip, port);
            logger.info("File downloaded success status {} from ip {}", isFileDownloaded, ip);
            if (isFileDownloaded) {
              break;
            } else {
              if (selectIndexForHost < tuple2._2.length - 1) {
                selectIndexForHost++;
              } else {
                throw new RuntimeException("File :: " + file + " is not available on any hosts :: "
                    + Arrays.toString(tuple2._2));
              }
            }
          }
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
