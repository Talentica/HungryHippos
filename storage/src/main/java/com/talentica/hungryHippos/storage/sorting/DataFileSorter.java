package com.talentica.hungryHippos.storage.sorting;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;


/**
 * @author pooshans
 *
 */
public class DataFileSorter {

  public static final int DEFAULTMAXTEMPFILES = 1024;
  public static final Logger LOGGER = LoggerFactory.getLogger(DataFileSorter.class);
  private DataFileComparator comparator;
  private static ShardingApplicationContext context;
  private static FieldTypeArrayDataDescription dataDescription;
  private static DynamicMarshal dynamicMarshal;
  private final static String INPUT_DATAFILE_PRIFIX = "data_";
  private DataFileHeapSort dataFileHeapSort;

  public DataFileSorter() throws ClassNotFoundException, FileNotFoundException, KeeperException,
      InterruptedException, IOException, JAXBException {
    dynamicMarshal = getDynamicMarshal();
    comparator = new DataFileComparator(dynamicMarshal, dataDescription.getSize());
    comparator.setDimenstion(new int[] {0, 1, 2});
    dataFileHeapSort = new DataFileHeapSort(dataDescription.getSize(), dynamicMarshal, comparator);
  }

  public static void main(String[] args) throws IOException, InsufficientMemoryException,
      ClassNotFoundException, KeeperException, InterruptedException, JAXBException {
    long startTIme = System.currentTimeMillis();
    DataInputStream in;
    File inputFile;
    validateArguments(args);
    String dataDir = args[0];
    String ouputDirPath = args[1];
    String shardingDir = args[2];
    context = new ShardingApplicationContext(shardingDir);
    DataFileSorter dataFileSorted = new DataFileSorter();
    int index = 0;
    dataDir = validateDirectory(dataDir);
    File outputDir = new File(ouputDirPath);
    while (true) {
      inputFile = new File(dataDir + INPUT_DATAFILE_PRIFIX + (index));
      if (!inputFile.exists()) {
        break;
      }
      LOGGER.info("Sorting for file [{}] is started...", inputFile.getName());
      in = new DataInputStream(new FileInputStream(inputFile));
      long dataSize = inputFile.length();
      if (dataSize <= 0) {
        continue;
      }
      List<File> files =
          dataFileSorted.sortInBatch(in, dataSize, outputDir, Charset.defaultCharset(), inputFile);

      if (files.size() > 1) { // merge should happen for at least two files
        dataFileSorted.mergeSortedFiles(files, inputFile, Charset.defaultCharset(), true);
      }
      index++;
    }
    LOGGER.info("Completed file sorting and total time taken in sec {} ",
        ((System.currentTimeMillis() - startTIme) / 1000));
  }

  private List<File> sortInBatch(DataInputStream file, final long datalength, File outputDirectory,
      Charset charset, final File outputFile) throws IOException, InsufficientMemoryException,
      ClassNotFoundException, KeeperException, InterruptedException, JAXBException {
    return sortInBatch(file, datalength, availableMemory(), charset, outputDirectory, outputFile);
  }

  private List<File> sortInBatch(final DataInputStream dataInputStream, final long datalength,
      long maxFreeMemory, final Charset cs, final File outputdirectory, final File outputFile)
      throws IOException, InsufficientMemoryException, ClassNotFoundException, KeeperException,
      InterruptedException, JAXBException {
    long startTIme = System.currentTimeMillis();
    int noOfBytesInOneDataSet = dataDescription.getSize();
    List<File> files = new ArrayList<>();
    long blocksize = getSizeOfBlocks(datalength, maxFreeMemory);
    long objectOverhead = DataSizeCalculator.getObjectOverhead();
    int effectiveBlockSizeBytes =
        ((int) ((blocksize / 2) / (noOfBytesInOneDataSet + objectOverhead)))
            * noOfBytesInOneDataSet;
    LOGGER.info("Sorting in batch started...");
    int batchId = 0;
    try {
      long dataFileSize = datalength;
      byte[] chunk;
      while (dataFileSize > 0) {
        if (dataFileSize > effectiveBlockSizeBytes) {
          chunk = new byte[effectiveBlockSizeBytes];
        } else { // remaining chunk
          chunk = new byte[(int) dataFileSize];
        }
        dataInputStream.readFully(chunk);
        dataFileSize = dataFileSize - chunk.length;
        if (dataFileSize == 0 && batchId == 0) {
          files.add(sortAndSave(chunk, cs, outputFile, batchId, true));
        } else {
          files.add(sortAndSave(chunk, cs, outputdirectory, batchId, false));
        }
        availableMemory();
        batchId++;
      }
    } catch (Exception e) {
      LOGGER.error("Unable to process due to {}", e.getMessage());
      throw e;
    } finally {
      dataInputStream.close();
      availableMemory();
    }
    LOGGER.info("Total sorting time taken in sec {} ",
        ((System.currentTimeMillis() - startTIme) / 1000));
    return files;
  }

  private File sortAndSave(byte[] chunk, Charset cs, File output, int batchId,
      boolean isSingalBatch) throws IOException {
    LOGGER.info("Batch id {} is getting sorted and saved", (batchId));
    LOGGER.info("Sorting started for chunk size {}...",chunk.length);
    dataFileHeapSort.setChunk(chunk);
    dataFileHeapSort.heapSort();
    LOGGER.info("Sorting completed.");
    File file;
    RandomAccessFile raf = null;
    long position = 0;
    if (isSingalBatch) {
      try {
        raf = new RandomAccessFile(output, "rw");
        raf.seek(position);
        raf.write(chunk);
        position = raf.getFilePointer();
      } catch (IOException e) {
        LOGGER.info("Unable to write into file {}", e.getMessage());
        throw e;
      } finally {
        if (raf != null)
          raf.close();
      }
      file = output;
    } else {
      file = File.createTempFile("tmp_", "_sorted_file", output);
      LOGGER.info("Temporary directory {}", file.getAbsolutePath());
      file.deleteOnExit();
      try (OutputStream out = new FileOutputStream(file)) {
        out.write(chunk);
      }
    }
    availableMemory();
    return file;
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
    RandomAccessFile raf = new RandomAccessFile(outputfile, "rw");
    long position = 0l;
    try {
      while (pq.size() > 0) {
        BinaryFileBuffer bfb = pq.poll();
        ByteBuffer row = bfb.pop();
        raf.seek(position);
        raf.write(row.array());
        // position = position + row.array().length - 1;
        position = raf.getFilePointer();
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
