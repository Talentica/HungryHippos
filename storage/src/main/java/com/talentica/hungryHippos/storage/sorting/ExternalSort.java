package com.talentica.hungryHippos.storage.sorting;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;


public class ExternalSort {

  private static int defaultCharBufferSize = 8192;
  public static long TEMPFILES;
  public static final Logger LOGGER = LoggerFactory.getLogger(ExternalSort.class);
  private DimensionComparator comparator;
  private static ShardingApplicationContext context;
  private static FieldTypeArrayDataDescription dataDescription;
  private DynamicMarshal dynamicMarshal;

  public ExternalSort() throws ClassNotFoundException, FileNotFoundException, KeeperException,
      InterruptedException, IOException, JAXBException {
    dynamicMarshal = getDynamicMarshal();
    comparator = new DimensionComparator(dynamicMarshal);
    comparator.setDimenstion(new int[] {0, 1, 2});
  }

  public static void main(String[] args) throws IOException, InsufficientMemoryException,
      ClassNotFoundException, KeeperException, InterruptedException, JAXBException {
    long startTIme = System.currentTimeMillis();
    validateArguments(args);
    context = new ShardingApplicationContext(args[2]);
    ExternalSort externalSort = new ExternalSort();
    String tmpDirPath = (args.length == 3) ? args[1] : null;
    int index = 0;
    String dataDir = args[0];
    dataDir = validateDirectory(dataDir);
    File tmpDir = (tmpDirPath == null ? null : new File(tmpDirPath));
    while (true) {
      File inputFlatFile = new File(dataDir + "data_" + (index));
      if (!inputFlatFile.exists()) {
        break;
      }
      DataInputStream in = new DataInputStream(new FileInputStream(inputFlatFile));
      long dataSize = inputFlatFile.length();
      if (dataSize <= 0) {
        continue;
      }
      File f2 = File.createTempFile("hh_", "_sorted_flat_file_" + (index++), tmpDir);
      externalSort.mergeSortedFiles(
          externalSort.sortInBatch(in, dataSize, tmpDir, Charset.defaultCharset()), f2,
          Charset.defaultCharset(), true);
      LOGGER.info("Total time taken in sec {} ", ((System.currentTimeMillis() - startTIme) / 1000));
    }
  }

  private List<File> sortInBatch(DataInputStream file, final long datalength, File tmpDirectory,
      Charset charset) throws IOException, InsufficientMemoryException, ClassNotFoundException,
      KeeperException, InterruptedException, JAXBException {
    return sortInBatch(file, datalength, availableMemory(), charset, tmpDirectory);
  }

  private List<File> sortInBatch(final DataInputStream dataInputStream, final long datalength,
      long maxFreeMemory, final Charset cs, final File tmpdirectory)
      throws IOException, InsufficientMemoryException, ClassNotFoundException, KeeperException,
      InterruptedException, JAXBException {
    long startTIme = System.currentTimeMillis();
    int noOfBytesInOneDataSet = dataDescription.getSize();
    List<File> files = new ArrayList<>();
    long blocksize = getSizeOfBlocks(datalength, maxFreeMemory);
    try {
      List<byte[]> tmplist = new ArrayList<byte[]>();
      try {
        long dataFileSize = datalength;
        long currentblocksize = 0;
        byte[] bytes;
        while (dataFileSize > 0) {
          bytes = new byte[noOfBytesInOneDataSet];
          if (currentblocksize < blocksize) {
            dataInputStream.readFully(bytes);
            dataFileSize = dataFileSize - bytes.length;
            tmplist.add(bytes);
            currentblocksize +=  StringSize.estimatedSizeOfLine(noOfBytesInOneDataSet);
            bytes = null;
          } else {
            files.add(sortAndSave(tmplist, cs, tmpdirectory));
            tmplist.clear();
            currentblocksize = 0;
          }
        }
        if (tmplist.size() > 0) {
          files.add(sortAndSave(tmplist, cs, tmpdirectory));
          tmplist.clear();
        }
      } catch (EOFException oef) {
        if (tmplist.size() > 0) {
          files.add(sortAndSave(tmplist, cs, tmpdirectory));
          tmplist.clear();
        }
      }
    } finally {
      dataInputStream.close();
    }
    LOGGER.info("Total sorting time taken in sec {} ",
        ((System.currentTimeMillis() - startTIme) / 1000));
    return files;
  }

  private int batchId = 0;

  private File sortAndSave(List<byte[]> tmpList, Charset cs, File tmpDirectory) throws IOException {
    LOGGER.info("Batch id {} is getting sorted and saved", (batchId++));
    Collections.sort(tmpList, comparator.getDefaultComparator());
    File newtmpfile = File.createTempFile("batch", "sorted_flat_file", tmpDirectory);
    LOGGER.info("Temporary directory {}", newtmpfile.getAbsolutePath());
    newtmpfile.deleteOnExit();
    try (OutputStream out = new FileOutputStream(newtmpfile)) {
      Iterator<byte[]> i = tmpList.iterator();
      if (i.hasNext()) {
        while (i.hasNext()) {
          byte[] r = i.next();
          out.write(r);
        }
      }
    }
    return newtmpfile;
  }

  private int mergeSortedFiles(List<File> files, File outputfile, Charset cs, boolean append)
      throws IOException {
    long startTIme = System.currentTimeMillis();
    LOGGER.info("Now merging sorted files...");
    ArrayList<BinaryFileBuffer> bfbs = new ArrayList<>();
    for (File f : files) {
      FileInputStream fis = new FileInputStream(f);
      DataInputStream dis = new DataInputStream(fis);
      BinaryFileBuffer bfb = new BinaryFileBuffer(dis, dataDescription.getSize());
      bfbs.add(bfb);
    }
    OutputStream os = new FileOutputStream(outputfile, append);
    int rowcounter = mergeSortedFiles(os, bfbs);

    for (File f : files) {
      f.delete();
    }
    LOGGER.info("Sorted files are merged successfully.");
    LOGGER.info("Total merging time taken in sec {} ",
        ((System.currentTimeMillis() - startTIme) / 1000));
    return rowcounter;
  }

  private int mergeSortedFiles(OutputStream os, List<BinaryFileBuffer> buffers) throws IOException {
    int rowcounter = 0;
    for (BinaryFileBuffer bfb : buffers) {
      if (!bfb.empty()) {
        pq.add(bfb);
      }
    }
    try {
      while (pq.size() > 0) {
        BinaryFileBuffer bfb = pq.poll();
        ByteBuffer r = bfb.pop();
        os.write(r.array());
        ++rowcounter;
        if (bfb.empty()) {
          bfb.getReader().close();
        } else {
          pq.add(bfb);
        }
      }
    } catch (EOFException eof) {
      LOGGER.info("End of file");
    } finally {
      os.close();
      for (BinaryFileBuffer bfb : pq) {
        bfb.close();
      }
    }
    return rowcounter;
  }

  private static String validateDirectory(String dataDir) {
    if (dataDir.charAt(dataDir.length() - 1) != File.separatorChar) {
      dataDir = dataDir.concat("" + File.separatorChar);
    }
    return dataDir;
  }

  private PriorityQueue<BinaryFileBuffer> pq =
      new PriorityQueue<>(11, new Comparator<BinaryFileBuffer>() {
        @Override
        public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
          return comparator.getDefaultComparator().compare(i.peek().array(), j.peek().array());
        }
      });

  private long getSizeOfBlocks(final long fileSize, final long maxFreeMemory)
      throws InsufficientMemoryException {
    LOGGER.info("Input file size {} and maximum memory available {}", fileSize, maxFreeMemory);
    long allocateMemory = maxFreeMemory / 2;
    if (allocateMemory < (defaultCharBufferSize * 2)) {
      throw new InsufficientMemoryException(
          "Inssufficient memory available. Require {" + (defaultCharBufferSize * 2)
              + "} bytes for buffer writer and available memory is {" + allocateMemory + "}");
    }
    TEMPFILES = (fileSize < allocateMemory) ? 1
        : (fileSize / allocateMemory + (fileSize % allocateMemory == 0 ? 0 : 1));
    long blocksize = allocateMemory;
    return blocksize;
  }

  public long availableMemory() {
    System.gc();
    Runtime runtime = Runtime.getRuntime();
    long allocatedMemory = runtime.totalMemory() - runtime.freeMemory();
    long currentFreeMemory = runtime.maxMemory() - allocatedMemory;
    LOGGER.info("Current free memory {}", currentFreeMemory);
    return currentFreeMemory;
  }

  private static DynamicMarshal getDynamicMarshal() throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    dataDescription = context.getConfiguredDataDescription();
    dataDescription.setKeyOrder(context.getShardingDimensions());
    DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
    return dynamicMarshal;
  }

  private static void validateArguments(String[] args) {
    if (args != null && args.length < 3) {
      throw new RuntimeException(
          "Invalid argument. Please provide 1st argument as input file and 2nd argument tmp directory and 3rd argument sharding folder path.");
    }
  }
}
