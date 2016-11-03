package com.talentica.hungryHippos.storage.sorting;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;
import com.talentica.hungryHippos.storage.DataFileAccess;


/**
 * To perform the sorting on file.
 * 
 * @author pooshans
 *
 */
public class DataFileSorter {

  public static final int DEFAULTMAXTEMPFILES = 1024;
  public static final Logger LOGGER = LoggerFactory.getLogger(DataFileSorter.class);
  private static final String INPUT_DATAFILE_PRIFIX = "data_";
  public static final String DATA_FILE_SORTED = "primdim.sorted";
  private final static String LOCK_FILE = "lock";
  private FieldTypeArrayDataDescription dataDescription;
  private DataFileHeapSort dataFileHeapSort;
  private int[] shardDims;
  private int[] sortDims;
  private String dataDir;
  private int numFiles;
  private DataFileComparator comparator;

  /**
   * instantiate the DataFileSorter class.
   * 
   * @param dataDir
   * @param context
   */
  public DataFileSorter(String dataDir, ShardingApplicationContext context) {
    this.dataDescription = context.getConfiguredDataDescription();
    this.dataDescription.setKeyOrder(context.getShardingDimensions());
    this.shardDims = context.getShardingIndexes();
    this.sortDimensions(this.shardDims);
    this.comparator = new DataFileComparator(dataDescription);
    this.dataFileHeapSort = new DataFileHeapSort(dataDescription.getSize(), comparator);
    this.sortDims = new int[shardDims.length];
    this.dataDir = dataDir;
    this.numFiles = 1 << shardDims.length;
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
  public synchronized void doSortingDefault() throws IOException {
    dataDir = validateDirectory(dataDir);
    File lockFile = new File(dataDir + LOCK_FILE);
    createLockFile(lockFile);
    for (int fileId = 0; fileId < this.shardDims.length; fileId++) {
      String childDirectory = dataDir + (INPUT_DATAFILE_PRIFIX + (1 << fileId));
      File inputDir = new File(childDirectory);
      if (!inputDir.exists()) {
        break;
      }
      if (inputDir.isDirectory()) {
        sortAllFilesInDirectory(fileId, inputDir, null);
      }
    }
    unlockFile(lockFile);
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
  public void doSortingPrimaryDimensionWise(int primaryDimensionIndex) throws FileNotFoundException,
      ClassNotFoundException, IOException, KeeperException, InterruptedException, JAXBException {
    int keyIdBit = 1 << primaryDimensionIndex;
    dataDir = validateDirectory(dataDir);
    for (int fileId = 0; fileId < numFiles; fileId++) {
      if ((keyIdBit & fileId) > 0 && (fileId != (keyIdBit))) {
        String absoluteDataFilePath = dataDir + (INPUT_DATAFILE_PRIFIX + fileId);
        File inputDir = new File(absoluteDataFilePath);
        File sortedFileFlag =
            new File(absoluteDataFilePath + File.separatorChar + DATA_FILE_SORTED);
        if (isFileSortedOnPrimDim(sortedFileFlag, primaryDimensionIndex)) {
          LOGGER.info("Data file {} is already sorted on primary dimension {}",
              absoluteDataFilePath, primaryDimensionIndex);
          continue;
        }
        if (inputDir.isDirectory()) {
          sortAllFilesInDirectory(primaryDimensionIndex, inputDir, DataFileAccess.fileNameFilter);
        } else {
          continue;
        }
        setPrimDimForSorting(sortedFileFlag, primaryDimensionIndex);
      }
    }
  }

  /**
   * To sort the all files in inputDir on particular primary dimension.
   * 
   * @param primaryDimensionIndex
   * @param inputDir
   * @param filter
   * @throws IOException
   */
  private void sortAllFilesInDirectory(int primaryDimensionIndex, File inputDir,
      FilenameFilter filter) throws IOException {
    File[] files = (filter == null) ? inputDir.listFiles() : inputDir.listFiles(filter);
    for (File inputFile : files) {
      long dataSize = inputFile.length();
      if (dataSize <= 0) {
        continue;
      }
      doSorting(inputFile, primaryDimensionIndex);
    }
  }

  /**
   * To create the lock file
   * 
   * @param lockFile
   * @throws IOException
   */
  private synchronized void createLockFile(File lockFile) throws IOException {
    LOGGER.info("Lock file path {}", lockFile.getAbsolutePath());
    if (lockFile.exists()) {
      lockFile.delete();
    }
    lockFile.createNewFile();
  }

  /**
   * To unlock the file
   * 
   * @param lockFile
   */
  private synchronized void unlockFile(File lockFile) {
    if (lockFile.exists()) {
      lockFile.delete();
    }
  }

  /**
   * Start sorting on file for particular key
   * 
   * @param inputFile
   * @param key
   * @throws IOException
   */
  private void doSorting(File inputFile, int key) throws IOException {
    long startTIme = System.currentTimeMillis();
    DataInputStream in = null;
    File outputDir = new File(dataDir);
    orderDimensions(key);
    LOGGER.info(
        "Sorting started for data directory {} on primary dimension {} and sorted by dimensions {}",
        inputFile.getAbsoluteFile(), key, Arrays.toString(sortDims));
    this.comparator.setDimensions(sortDims);
    LOGGER.info("Sorting for file [{}] is started...", inputFile.getName());
    in = new DataInputStream(new FileInputStream(inputFile));
    List<File> files = sortInBatch(in, inputFile.length(), outputDir, inputFile);
    if (files.size() > 1) { // merge should happen for at least two files
      mergeSortedFiles(files, inputFile, true);
    }
    LOGGER.info("Completed file sorting and total time taken in ms {} ",
        ((System.currentTimeMillis() - startTIme)));
  }

  byte[] chunk = null;

  /**
   * To sort the file in batch
   * 
   * @param dataInputStream
   * @param datalength
   * @param maxFreeMemory
   * @param outputdirectory
   * @param outputFile
   * @return list of sorted temporary files
   * @throws IOException
   */
  private List<File> sortInBatch(final DataInputStream dataInputStream, final long datalength,
      final File outputdirectory, final File outputFile) throws IOException {
    long startTime = System.currentTimeMillis();
    List<File> files = new ArrayList<>();
    allocateChunkSizeIfNot(datalength);
    LOGGER.info("Sorting in batch started...");
    int batchId = 0;
    long dataFileSize = datalength;
    int readBytesLength = 0;
    long startTimeChunkRead;
    try {
      while (dataFileSize > 0) {
        if (dataFileSize >= chunk.length) {
          startTimeChunkRead = System.currentTimeMillis();
          dataInputStream.readFully(chunk);
          LOGGER.info("Time taken to read the chunk in ms {}",
              (System.currentTimeMillis() - startTimeChunkRead));
          readBytesLength = chunk.length;
        } else { // remaining chunk or for singal block which totally fit in memory.
          startTimeChunkRead = System.currentTimeMillis();
          dataInputStream.readFully(chunk);
          LOGGER.info("Time taken to read the chunk in ms {}",
              (System.currentTimeMillis() - startTimeChunkRead));
          readBytesLength = (int) dataFileSize;
        }
        dataFileSize = dataFileSize - readBytesLength;

        if (dataFileSize == 0 && batchId == 0) {

     
          files.add(sortAndSave(chunk, outputFile, batchId, true, readBytesLength));
        } else {
          files.add(sortAndSave(chunk, outputdirectory, batchId, false, readBytesLength));
        }
        batchId++;
      }

    } catch (Exception e) {
      LOGGER.error("Unable to process due to {}", e.getMessage());
      throw e;
    } finally {
      dataInputStream.close();
    }
    LOGGER.info("Total sorting time taken in ms {} ", ((System.currentTimeMillis() - startTime)));
    return files;
  }


  private void allocateChunkSizeIfNot(final long datalength) {
    int blocksize;
    int effectiveBlockSizeBytes;
    int noOfBytesInOneDataSet;
    long maxFreeMemory = availableMemory();
    if (chunk == null) {
      noOfBytesInOneDataSet = dataDescription.getSize();
      blocksize = getSizeOfBlocks(datalength, maxFreeMemory);
      effectiveBlockSizeBytes = ((blocksize) / (noOfBytesInOneDataSet)) * noOfBytesInOneDataSet;
      if (blocksize > datalength) {
        chunk = new byte[(int) datalength];
      } else {
        chunk = new byte[effectiveBlockSizeBytes];
      }
    } else {
      Arrays.fill(chunk, (byte) 0);
    }
  }

  /**
   * To sort the file and save it.
   * 
   * @param chunk
   * @param output
   * @param batchId
   * @param isSingalBatch
   * @param readBytesLength
   * @return file sorted and saved
   * @throws IOException
   */
  private File sortAndSave(byte[] chunk, File output, int batchId, boolean isSingalBatch,
      int readBytesLength) throws IOException {
    LOGGER.info("Batch id {} is getting sorted and saved", (batchId));
    LOGGER.info("Sorting started for chunk size {}...", chunk.length);
    long sortStartTime = System.currentTimeMillis();
    dataFileHeapSort.setChunk(chunk);
    LOGGER.info("Sorting started...");
    dataFileHeapSort.heapSort();
    LOGGER.info("Sorting completed in time ms {}.", (System.currentTimeMillis() - sortStartTime));
    File file;
    if (isSingalBatch) {
      if (output.exists()) {
        output.delete();
        output.createNewFile();
      }
      FileOutputStream fos = new FileOutputStream(output);
      BufferedOutputStream bout = new BufferedOutputStream(fos);
      try {
        long startTime = System.currentTimeMillis();
        bout.write(chunk, 0, readBytesLength);
        LOGGER.info(
            "Total time taken (ms) to write data after sorting and saving batch id {} ,  {}",
            batchId, (System.currentTimeMillis() - startTime));
      } catch (IOException e) {
        bout.flush();
        LOGGER.info("Unable to write into file {}", e.getMessage());
        throw e;
      } finally {
        if (bout != null) {
          bout.flush();
          bout.close();
        }
      }
      file = output;
    } else {
      file = File.createTempFile("tmp_", "_sorted_file", output);
      LOGGER.info("Temporary directory {}", file.getAbsolutePath());
      file.deleteOnExit();
      OutputStream out = new FileOutputStream(file);
      try {
        long startTime = System.currentTimeMillis();

        out.write(chunk, 0, readBytesLength);

        LOGGER.info(
            "Total time taken in ms to write data after sorting and saving batch id {} ,  {}",
            batchId, (System.currentTimeMillis() - startTime));
      } finally {
        if (out != null) {
          out.flush();
          out.close();
        }
      }
    }
    return file;
  }


  /**
   * To merge all the temporary files into singal sorted file.
   * 
   * @param files
   * @param outputfile
   * @param append
   * @return number of lines in sorted file
   * @throws IOException
   */
  private int mergeSortedFiles(List<File> files, File outputfile, boolean append)
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
    LOGGER.info("Total merging time taken (ms) {} ", ((System.currentTimeMillis() - startTIme)));
    return rowcounter;
  }

  /**
   * To merge all the temporary files into singal sorted file.
   * 
   * @param outputfile
   * @param buffers
   * @param append
   * @return
   * @throws IOException
   */
  private int mergeSortedFiles(File outputfile, List<BinaryFileBuffer> buffers, boolean append)
      throws IOException {
    int rowcounter = 0;
    for (BinaryFileBuffer bfb : buffers) {
      if (!bfb.empty()) {
        pq.add(bfb);
      }
    }
    if (outputfile.exists()) {
      outputfile.delete();
      outputfile.createNewFile();
    }
    FileOutputStream fos = new FileOutputStream(outputfile);
    BufferedOutputStream bout = new BufferedOutputStream(fos);
    try {
      long startTime;
      long totalTime = 0l;
      while (pq.size() > 0) {
        BinaryFileBuffer bfb = pq.poll();
        ByteBuffer row = bfb.pop();
        startTime = System.currentTimeMillis();
        bout.write(row.array());
        totalTime = totalTime + (System.currentTimeMillis() - startTime);
        ++rowcounter;
        if (bfb.empty()) {
          bfb.getReader().close();
        } else {
          pq.add(bfb);
        }
      }
      LOGGER.info("Total time taken (ms) to write data during merging {}", totalTime);
    } finally {
      if (bout != null) {
        bout.flush();
        bout.close();
      }
      for (BinaryFileBuffer bfb : pq) {
        bfb.close();
      }
    }
    return rowcounter;
  }

  /**
   * To validate the directory and append file separator if not present.
   * 
   * @param dataDir
   * @return
   */
  private static String validateDirectory(String dataDir) {
    if (dataDir.charAt(dataDir.length() - 1) != File.separatorChar) {
      dataDir = dataDir.concat("" + File.separatorChar);
    }
    return dataDir;
  }

  /**
   * Instance of the priority queue to merge the temporary file.
   */
  private PriorityQueue<BinaryFileBuffer> pq =
      new PriorityQueue<>(11, new Comparator<BinaryFileBuffer>() {
        @Override
        public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
          return compareRow(i.peek().array(), j.peek().array());
        }
      });

  private int columnPos = 0;

  /**
   * Compare two row byte to byte.
   * 
   * @param row1
   * @param row2
   * @return
   */
  private int compareRow(byte[] row1, byte[] row2) {
    int res = 0;
    for (int dim = 0; dim < shardDims.length; dim++) {
      DataLocator locator = dataDescription.locateField(shardDims[dim]);
      columnPos = locator.getOffset();
      for (int pointer = 0; pointer < locator.getSize(); pointer++) {
        if (row1[columnPos] != row2[columnPos]) {
          return row1[columnPos] - row2[columnPos];
        }
        columnPos++;
      }
    }
    return res;
  }

  /**
   * To estimate the size of the block for sorting in chunck.
   * 
   * @param fileSize
   * @param maxFreeMemory
   * @return size of the block
   */
  private int getSizeOfBlocks(final long fileSize, final long maxFreeMemory) {
    LOGGER.info("Input file size {} and maximum memory available {}", fileSize, maxFreeMemory);
    long blocksize = fileSize / DEFAULTMAXTEMPFILES + (fileSize % DEFAULTMAXTEMPFILES == 0 ? 0 : 1);
    blocksize = blocksize - DataSizeCalculator.getObjectOverhead();
    if (blocksize < maxFreeMemory) {
      blocksize = (2 * maxFreeMemory) / 3;
    }
    if (blocksize > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) blocksize;
    }
  }

  /**
   * To get currently available memory
   * 
   * @return total available memory
   */
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

  /**
   * Sort the dimension in ascending order
   * 
   * @param dimes
   */
  private void sortDimensions(int[] dimes) {
    for (int i = 0; i < dimes.length - 1; i++) {
      for (int j = 1; j < dimes.length - i; j++) {
        if (dimes[j - 1] > dimes[j]) {
          int temp = dimes[j];
          dimes[j] = dimes[j - 1];
          dimes[j - 1] = temp;
        }
      }
    }
  }

  /**
   * To determine the order of the dimensions for sorting by primary dimension
   * 
   * @param primaryDimension
   */
  private void orderDimensions(int primaryDimension) {
    for (int i = 0; i < shardDims.length; i++) {
      sortDims[i] = shardDims[(i + primaryDimension) % shardDims.length];
    }
  }


  /**
   * To set the flag of the primary dimension in file.
   * 
   * @param primDimFile
   * @param primDim
   * @throws IOException
   */
  private synchronized void setPrimDimForSorting(File primDimFile, int primDim) throws IOException {
    FileOutputStream fos = new FileOutputStream(primDimFile, false);
    fos.write(primDim);
    fos.flush();
    fos.close();
  }

  /**
   * To check whether file is sorted on primDim.
   * 
   * @param primDimFile
   * @param primDim
   * @return boolean
   * @throws IOException
   */
  @SuppressWarnings("resource")
  private synchronized boolean isFileSortedOnPrimDim(File primDimFile, int primDim)
      throws IOException {
    if (!primDimFile.exists()) {
      primDimFile.createNewFile();
    }
    int primDimFlag = -1;
    FileInputStream fis = new FileInputStream(primDimFile);
    if (primDimFile.exists()) {
      primDimFlag = fis.read();
      if (primDimFlag == primDim) {
        LOGGER.info("File is already sorted on primary dimension {}", primDimFlag);
        return true;
      }
    }
    fis.close();
    LOGGER.info("File is being sorted on primary dimension {}", primDim);
    return false;
  }
}

