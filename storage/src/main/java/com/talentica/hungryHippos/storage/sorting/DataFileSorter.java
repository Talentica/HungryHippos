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
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;


public class DataFileSorter {

  public static final int DEFAULTMAXTEMPFILES = 1024;
  public static final Logger LOGGER = LoggerFactory.getLogger(DataFileSorter.class);
  private DataFileComparator comparator;
  private static ShardingApplicationContext context;
  private static FieldTypeArrayDataDescription dataDescription;
  private static DynamicMarshal dynamicMarshal;
  private final static String DATAFILEPRIFIX = "data_";
  private final static String prefix = "sorted_";
  private final static String suffix = "_data_";

  public DataFileSorter() throws ClassNotFoundException, FileNotFoundException, KeeperException,
      InterruptedException, IOException, JAXBException {
    dynamicMarshal = getDynamicMarshal();
    comparator = new DataFileComparator(dynamicMarshal,dataDescription.getSize());
    comparator.setDimenstion(new int[] {0, 1, 2});
  }

  public static void main(String[] args) throws IOException, InsufficientMemoryException,
      ClassNotFoundException, KeeperException, InterruptedException, JAXBException {
    long startTIme = System.currentTimeMillis();
    validateArguments(args);
    String dataDir = args[0];
    String ouputDirPath = args[1];
    String shardingDir = args[2];
    context = new ShardingApplicationContext(shardingDir);
    DataFileSorter dataFileSorted = new DataFileSorter();
    int index = 0;
    dataDir = validateDirectory(dataDir);
    File tmpDir = (ouputDirPath == null ? null : new File(ouputDirPath));
    DataInputStream in;
    File outputfile;
    File inputFlatFile;
    while (true) {
      inputFlatFile = new File(dataDir + DATAFILEPRIFIX + (index));
      if (!inputFlatFile.exists()) {
        break;
      }
      LOGGER.info("Sorting for file [{}] is started...",inputFlatFile.getName());
      outputfile = File.createTempFile(prefix, suffix + (index), tmpDir);
      in = new DataInputStream(new FileInputStream(inputFlatFile));
      long dataSize = inputFlatFile.length();
      if (dataSize <= 0) {
        continue;
      }
      dataFileSorted.mergeSortedFiles(
          dataFileSorted.sortInBatch(in, dataSize, tmpDir, Charset.defaultCharset()), outputfile,
          Charset.defaultCharset(), true);
      index++;
      break;
    }
    LOGGER.info("Completed file sorting and total time taken in sec {} ", ((System.currentTimeMillis() - startTIme) / 1000));
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
    Queue<byte[]> tmplist = new PriorityQueue<byte[]>(comparator);
    LOGGER.info("Sorting in batch started...");
    try {
      long dataFileSize = datalength;
      long currentBatchsize = 0l;
      byte[] bytes;
      while (dataFileSize > 0) {
        if ((blocksize - currentBatchsize) > 0) {
          bytes = new byte[noOfBytesInOneDataSet];
          dataInputStream.readFully(bytes);
          dataFileSize = dataFileSize - bytes.length;
          tmplist.offer(bytes);
          currentBatchsize += DataSizeCalculator.estimatedSizeOfRow(noOfBytesInOneDataSet);
          bytes = null;
        } else {
          files.add(sortAndSave(tmplist, cs, tmpdirectory));
          tmplist.clear();
          currentBatchsize = 0;
          availableMemory();
        }
      }
      if (tmplist.size() > 0) {
        files.add(sortAndSave(tmplist, cs, tmpdirectory));
        tmplist.clear();
        availableMemory();
      }
    } catch (EOFException oef) {
      if (tmplist.size() > 0) {
        files.add(sortAndSave(tmplist, cs, tmpdirectory));
        tmplist.clear();
        availableMemory();
      }
    } finally {
      if (tmplist.size() > 0) {
        files.add(sortAndSave(tmplist, cs, tmpdirectory));
        tmplist.clear();
      }
      dataInputStream.close();
      availableMemory();
    }
    LOGGER.info("Total sorting time taken in sec {} ",
        ((System.currentTimeMillis() - startTIme) / 1000));
    return files;
  }

  private int batchId = 0;

  private File sortAndSave( Queue<byte[]> tmplist, Charset cs, File tmpDirectory) throws IOException {
    LOGGER.info("Batch id {} is getting sorted and saved", (batchId++));
    File newtmpfile = File.createTempFile("tmp_", "_sorted_file", tmpDirectory);
    LOGGER.info("Temporary directory {}", newtmpfile.getAbsolutePath());
    newtmpfile.deleteOnExit();
    try (OutputStream out = new FileOutputStream(newtmpfile)) {
      while(!tmplist.isEmpty()){
        out.write(tmplist.poll());
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
    int rowcounter = mergeSortedFiles(outputfile, bfbs, append);
    for (File f : files) {
      f.delete();
    }
    LOGGER.info("Sorted files are merged successfully.");
    LOGGER.info("Total merging time taken in sec {} ",
        ((System.currentTimeMillis() - startTIme) / 1000));
    return rowcounter;
  }

  private int mergeSortedFiles(File outputfile, List<BinaryFileBuffer> buffers, boolean append)
      throws IOException {
    int rowcounter = 0;
    OutputStream os = new FileOutputStream(outputfile, append);
    for (BinaryFileBuffer bfb : buffers) {
      if (!bfb.empty()) {
        pq.add(bfb);
      }
    }
    try {
      while (pq.size() > 0) {
        BinaryFileBuffer bfb = pq.poll();
        ByteBuffer row = bfb.pop();
        os.write(row.array());
        ++rowcounter;
        if (bfb.empty()) {
          bfb.getReader().close();
        } else {
          pq.add(bfb);
        }
      }
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
          return comparator.compare(i.peek().array(), j.peek().array());
        }
      });

  private long getSizeOfBlocks(final long fileSize, final long maxFreeMemory)
      throws InsufficientMemoryException {
    LOGGER.info("Input file size {} and maximum memory available {}", fileSize, maxFreeMemory);
    long blocksize = fileSize / DEFAULTMAXTEMPFILES + (fileSize % DEFAULTMAXTEMPFILES == 0 ? 0 : 1);
    if (blocksize < maxFreeMemory) {
      blocksize = maxFreeMemory;
    }
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
