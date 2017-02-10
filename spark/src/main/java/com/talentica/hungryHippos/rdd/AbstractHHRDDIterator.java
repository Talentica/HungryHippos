/**
 * 
 */
package com.talentica.hungryHippos.rdd;

import java.io.File;
import java.io.IOException;
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
 * @author pooshans
 * @param <T>
 *
 */
public abstract class AbstractHHRDDIterator<T> extends AbstractIterator<T> {
  private static Logger logger = LoggerFactory.getLogger(AbstractHHRDDIterator.class);
  protected Set<String> remoteFiles;
  protected String currentFile;
  protected String currentFilePath;
  protected String filePath;
  protected byte[] byteBufferBytes;
  protected Iterator<Tuple2<String, int[]>> fileIterator;
  protected int recordLength;

  public AbstractHHRDDIterator(String filePath, List<Tuple2<String, int[]>> files,
      Map<Integer, String> nodIdToIp) throws IOException {
    downloadRemoteFilesIfNotExists(filePath, files, nodIdToIp);
  }

  public AbstractHHRDDIterator(String filePath, int rowSize, List<Tuple2<String, int[]>> files,
      Map<Integer, String> nodIdToIp) throws IOException {
    downloadRemoteFilesIfNotExists(filePath, files, nodIdToIp);
    // this.hhRDDRowReader = new HHRDDRowReader(dataDescription);
    this.byteBufferBytes = new byte[rowSize];
    // this.byteBuffer = ByteBuffer.wrap(byteBufferBytes);
    this.recordLength = rowSize;
    // this.hhRDDRowReader.setByteBuffer(byteBuffer);
  }


  protected abstract void iterateOnFiles() throws IOException;

  protected abstract boolean downloadFile(String filePath, String ip);

  protected abstract void closeStream() throws IOException;

  private void downloadRemoteFilesIfNotExists(String filePath, List<Tuple2<String, int[]>> files,
      Map<Integer, String> nodIdToIp) throws IOException {
    this.filePath = filePath + File.separator;
    remoteFiles = new HashSet<>();
    for (Tuple2<String, int[]> tuple2 : files) {
      File file = new File(filePath + File.separator + tuple2._1);
      if (!file.exists()) {
        logger.info("Downloading file {}/{} from nodes {} ", filePath, tuple2._1, tuple2._2);
        boolean isFileDownloaded = false;
        while (!isFileDownloaded) {
          for (int id : tuple2._2) {
            String ip = nodIdToIp.get(id);
            isFileDownloaded = downloadFile(this.filePath + tuple2._1, ip);
            logger.info("File downloaded success status {} from ip {}", isFileDownloaded, ip);
            if (isFileDownloaded) {
              break;
            }
          }
        }
        remoteFiles.add(tuple2._1);
      }

    }
    fileIterator = files.iterator();
    iterateOnFiles();
  }
}
