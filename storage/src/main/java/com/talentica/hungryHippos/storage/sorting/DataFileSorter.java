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
import com.talentica.hungryHippos.client.job.Job;
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
  private ShardingApplicationContext context;
  private FieldTypeArrayDataDescription dataDescription;
  private DynamicMarshal dynamicMarshal;
  private final static String INPUT_DATAFILE_PRIFIX = "data_";
  private DataFileHeapSort dataFileHeapSort;
  private int[] shardDims;
  private int[] sortDims;
  private String dataDir;
  private String shardingDir;
  private DataFileSorter dataFileSorted;
  private int numFiles;

  public DataFileSorter(String dataDir, String shardingDir) throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    context = new ShardingApplicationContext(shardingDir);
    shardDims = context.getShardingIndexes();
    dynamicMarshal = getDynamicMarshal();
    comparator = new DataFileComparator(dynamicMarshal, dataDescription.getSize());
    dataFileHeapSort = new DataFileHeapSort(dataDescription.getSize(), dynamicMarshal, comparator);
    sortDims = new int[shardDims.length];
    this.dataDir = dataDir;
    this.shardingDir = shardingDir;
    numFiles = 1 << shardDims.length;
  }

  /**
   * To sort the data based on job's primary dimensions.
   * 
   * @param job
   * @throws FileNotFoundException
   * @throws ClassNotFoundException
   * @throws IOException
   * @throws InsufficientMemoryException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws JAXBException
   */
  public void doSortingJobWise(Job job)
      throws FileNotFoundException, ClassNotFoundException, IOException,
      InsufficientMemoryException, KeeperException, InterruptedException, JAXBException {
    int primDims = job.getPrimaryDimension();
    int keyIdBit = 1 << primDims;
    File inputFile;
    for (int fileId = 0; fileId < numFiles; fileId++) {
      if ((keyIdBit & fileId) > 0 && (fileId != (keyIdBit))) {
        inputFile = new File(dataDir + INPUT_DATAFILE_PRIFIX + fileId);
        doSorting(inputFile, primDims);
      }
    }
  }

  /**
   * To do the sorting once the data is ready.
   * 
   * @throws IOException
   * @throws InsufficientMemoryException
   * @throws ClassNotFoundException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws JAXBException
   */
  public void doSortingDefault() throws IOException, InsufficientMemoryException,
      ClassNotFoundException, KeeperException, InterruptedException, JAXBException {
    dataDir = validateDirectory(dataDir);
    dataFileSorted = new DataFileSorter(dataDir, shardingDir);
    File inputFile;
    for (int fileId = 0; fileId < dataFileSorted.shardDims.length; fileId++) {
      inputFile = new File(dataDir + INPUT_DATAFILE_PRIFIX + (1 << fileId));
      if (!inputFile.exists()) {
        break;
      }
      long dataSize = inputFile.length();
      if (dataSize <= 0) {
        continue;
      }
      doSorting(inputFile, fileId);
    }
  }

  private void doSorting(File inputFile, int key)
      throws FileNotFoundException, IOException, InsufficientMemoryException,
      ClassNotFoundException, KeeperException, InterruptedException, JAXBException {
    long startTIme = System.currentTimeMillis();
    DataInputStream in = null;
    File outputDir = new File(dataDir);
    dataFileSorted.comparator
        .setDimenstion(dataFileSorted.getSortingOrderDims(sortDims, key << 1));
    LOGGER.info("Sorting for file [{}] is started...", inputFile.getName());
    in = new DataInputStream(new FileInputStream(inputFile));
    List<File> files = dataFileSorted.sortInBatch(in, inputFile.length(), outputDir,
        Charset.defaultCharset(), inputFile);
    if (files.size() > 1) { // merge should happen for at least two files
      dataFileSorted.mergeSortedFiles(files, inputFile, Charset.defaultCharset(), true);
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
    int blocksize = getSizeOfBlocks(datalength, maxFreeMemory);
    int effectiveBlockSizeBytes =
        ((int) ((blocksize) / (noOfBytesInOneDataSet))) * noOfBytesInOneDataSet;
    byte[] chunk = null;
    LOGGER.info("Sorting in batch started...");
    int batchId = 0;
    try {
      long dataFileSize = datalength;
      while (dataFileSize > 0) {
        if (dataFileSize > effectiveBlockSizeBytes) {
          if (chunk == null) {
            chunk = new byte[effectiveBlockSizeBytes];
          }
          availableMemory();
        } else { // remaining chunk
          chunk = null;
          availableMemory();
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
    LOGGER.info("Sorting started for chunk size {}...", chunk.length);
    dataFileHeapSort.setChunk(chunk);
    dataFileHeapSort.heapSort();
    LOGGER.info("Sorting completed.");
    File file;
    if (isSingalBatch) {
      RandomAccessFile raf = new RandomAccessFile(output, "rw");
      try {
        raf.seek(0l);
        raf.write(chunk);
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
      OutputStream out = new FileOutputStream(file);
      try {
        out.write(chunk);
      } finally {
        if (out != null)
          out.close();
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

  private RandomAccessFile raf;

  private int mergeSortedFiles(File outputfile, List<BinaryFileBuffer> buffers, boolean append)
      throws IOException {
    int rowcounter = 0;
    OutputStream os = new FileOutputStream(outputfile, append);
    for (BinaryFileBuffer bfb : buffers) {
      if (!bfb.empty()) {
        pq.add(bfb);
      }
    }
    raf = new RandomAccessFile(outputfile, "rw");
    long position = 0l;
    try {
      while (pq.size() > 0) {
        BinaryFileBuffer bfb = pq.poll();
        ByteBuffer row = bfb.pop();
        raf.seek(position);
        raf.write(row.array());
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



  private int getSizeOfBlocks(final long fileSize, final long maxFreeMemory)
      throws InsufficientMemoryException {
    LOGGER.info("Input file size {} and maximum memory available {}", fileSize, maxFreeMemory);
    long blocksize = fileSize / DEFAULTMAXTEMPFILES + (fileSize % DEFAULTMAXTEMPFILES == 0 ? 0 : 1);
    blocksize = blocksize - DataSizeCalculator.getObjectOverhead();
    if (blocksize < maxFreeMemory) {
      blocksize = (2 * maxFreeMemory) / 3; // java retain
                                                                                    // 1/3 of the
                                                                                    // heap size.
    }
    if (blocksize > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) blocksize;
    }
  }

  public long availableMemory() {
    Runtime runtime = Runtime.getRuntime();
    long allocatedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();
    long currentFreeMemoryBefore = runtime.maxMemory() - allocatedMemoryBefore;
    LOGGER.info("Current memory before GC call {}", currentFreeMemoryBefore);
    System.gc();
    long allocatedMemoryAfter = runtime.totalMemory() - runtime.freeMemory();
    long currentFreeMemoryAfter = runtime.maxMemory() - allocatedMemoryAfter;
    LOGGER.info("Current free memory after GC call {}", currentFreeMemoryAfter);
    LOGGER.info("Total memory freed {}", (currentFreeMemoryAfter - currentFreeMemoryBefore));
    return currentFreeMemoryAfter;
  }

  private DynamicMarshal getDynamicMarshal() throws ClassNotFoundException, FileNotFoundException,
      KeeperException, InterruptedException, IOException, JAXBException {
    dataDescription = context.getConfiguredDataDescription();
    dataDescription.setKeyOrder(context.getShardingDimensions());
    DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
    return dynamicMarshal;
  }

  private int[] getSortingOrderDims(int[] sortOrderDims, int startPos) {
    for (int i = 0; i < shardDims.length; i++) {
      sortOrderDims[i] = shardDims[(i + startPos) % shardDims.length];
    }
    return sortOrderDims;
  }
}
