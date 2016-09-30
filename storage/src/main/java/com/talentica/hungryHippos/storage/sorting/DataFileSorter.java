package com.talentica.hungryHippos.storage.sorting;

import java.io.BufferedOutputStream;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import javax.xml.bind.JAXBException;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talentica.hungryHippos.client.domain.DataLocator;
import com.talentica.hungryHippos.client.domain.FieldTypeArrayDataDescription;
import com.talentica.hungryHippos.client.job.Job;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;


/**
 * @author pooshans
 *
 */
public class DataFileSorter {

  public static final int DEFAULTMAXTEMPFILES = 1024;
  public static final Logger LOGGER = LoggerFactory.getLogger(DataFileSorter.class);
  private final static String INPUT_DATAFILE_PRIFIX = "data_";
  private ShardingApplicationContext context;
  private FieldTypeArrayDataDescription dataDescription;
  private DataFileHeapSort dataFileHeapSort;
  private int[] shardDims;
  private int[] sortDims;
  private String dataDir;
  private int numFiles;
  private DataFileComparator comparator;

  public DataFileSorter(String dataDir, String shardingDir) throws ClassNotFoundException,
      FileNotFoundException, KeeperException, InterruptedException, IOException, JAXBException {
    this.context = new ShardingApplicationContext(shardingDir);
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
    File inputDir;
    List<String> filesPresentinFolder = new ArrayList<>();
    for (int fileId = 0; fileId < numFiles; fileId++) {
      if ((keyIdBit & fileId) > 0 && (fileId != (keyIdBit))) {
        inputDir = new File(dataDir + INPUT_DATAFILE_PRIFIX + fileId);
        if (inputDir.isDirectory()) {
          Files.walk(Paths.get(inputDir.getAbsolutePath())).forEach(filePath -> {
            if (Files.isRegularFile(filePath)) {
              filesPresentinFolder.add(filePath.toString());
            }
          });
        }
        for (String file : filesPresentinFolder) {
          File inputFile = new File(file);
          long dataSize = inputFile.length();
          if (dataSize <= 0) {
            continue;
          }
          doSorting(inputDir, primDims);
        }
        filesPresentinFolder.clear();
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
    File inputDir;
    List<String> filesPresentinFolder = new ArrayList<>();
    for (int fileId = 0; fileId < this.shardDims.length; fileId++) {
      inputDir = new File(dataDir + INPUT_DATAFILE_PRIFIX + (1 << fileId));
      if (!inputDir.exists()) {
        break;
      }
      if (inputDir.isDirectory()) {
        Files.walk(Paths.get(inputDir.getAbsolutePath())).forEach(filePath -> {
          if (Files.isRegularFile(filePath)) {
            filesPresentinFolder.add(filePath.toString());
          }
        });
      }

      for (String file : filesPresentinFolder) {
        File inputFile = new File(file);
        long dataSize = inputFile.length();
        if (dataSize <= 0) {
          continue;
        }
        doSorting(inputFile, fileId);
      }
      filesPresentinFolder.clear();
    }
  }

  private void doSorting(File inputFile, int key)
      throws FileNotFoundException, IOException, InsufficientMemoryException,
      ClassNotFoundException, KeeperException, InterruptedException, JAXBException {
    long startTIme = System.currentTimeMillis();
    DataInputStream in = null;
    File outputDir = new File(dataDir);
    sortDims = orderDimensions(key << 1);
    this.comparator.setDimensions(sortDims);
    LOGGER.info("Sorting for file [{}] is started...", inputFile.getName());
    in = new DataInputStream(new FileInputStream(inputFile));
    List<File> files = sortInBatch(in, inputFile.length(), outputDir, inputFile);
    if (files.size() > 1) { // merge should happen for at least two files
      mergeSortedFiles(files, inputFile, Charset.defaultCharset(), true);
    }
    LOGGER.info("Completed file sorting and total time taken in sec {} ",
        ((System.currentTimeMillis() - startTIme) / 1000));
  }

  private List<File> sortInBatch(DataInputStream file, final long datalength, File outputDirectory,
      final File outputFile) throws IOException, InsufficientMemoryException,
      ClassNotFoundException, KeeperException, InterruptedException, JAXBException {
    return sortInBatch(file, datalength, availableMemory(), outputDirectory, outputFile);
  }

  private List<File> sortInBatch(final DataInputStream dataInputStream, final long datalength,
      long maxFreeMemory, final File outputdirectory, final File outputFile)
      throws IOException, InsufficientMemoryException, ClassNotFoundException, KeeperException,
      InterruptedException, JAXBException {
    long startTime = System.currentTimeMillis();
    int noOfBytesInOneDataSet = dataDescription.getSize();
    List<File> files = new ArrayList<>();
    int blocksize = getSizeOfBlocks(datalength, maxFreeMemory);
    int effectiveBlockSizeBytes =
        ((int) ((blocksize) / (noOfBytesInOneDataSet))) * noOfBytesInOneDataSet;
    byte[] chunk;
    if (blocksize > datalength) {
      chunk = new byte[(int) datalength];
    } else {
      chunk = new byte[effectiveBlockSizeBytes];
    }
    LOGGER.info("Sorting in batch started...");
    int batchId = 0;
    long dataFileSize = datalength;
    int readBytesLength = 0;
    long startTimeChunkRead;
    try {
      while (dataFileSize > 0) {
        availableMemory();
        if (dataFileSize > effectiveBlockSizeBytes) {
          startTimeChunkRead = System.currentTimeMillis();
          dataInputStream.readFully(chunk);
          LOGGER.info("Time taken to read the chunk in ms {}",
              (System.currentTimeMillis() - startTimeChunkRead));
          readBytesLength = effectiveBlockSizeBytes;
        } else { // remaining chunk or for singal block which totally fit in memory.
          startTimeChunkRead = System.currentTimeMillis();
          dataInputStream.readFully(chunk, 0, (int) dataFileSize);
          LOGGER.info("Time taken to read the chunk in ms {}",
              (System.currentTimeMillis() - startTimeChunkRead));
          readBytesLength = (int) dataFileSize;
        }
        dataFileSize = dataFileSize - chunk.length;
        if (dataFileSize == 0 && batchId == 0) {
          files.add(sortAndSave(chunk, outputFile, batchId, true, readBytesLength));
        } else {
          files.add(sortAndSave(chunk, outputdirectory, batchId, false, readBytesLength));
        }
        batchId++;
      }
    } catch (EOFException eof) {
      dataFileSize = dataFileSize - chunk.length;
      files.add(sortAndSave(chunk, outputdirectory, batchId, (dataFileSize == 0 && batchId == 0),
          readBytesLength));
    } catch (Exception e) {
      LOGGER.error("Unable to process due to {}", e.getMessage());
      throw e;
    } finally {
      dataInputStream.close();
    }
    LOGGER.info("Total sorting time taken in sec {} ",
        ((System.currentTimeMillis() - startTime) / 1000));
    return files;
  }

  private File sortAndSave(byte[] chunk, File output, int batchId, boolean isSingalBatch,
      int lenght) throws IOException {
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
        bout.write(chunk);
        bout.flush();
        LOGGER.info(
            "Total time taken in ms to write data after sorting and saving batch id {} ,  {}",
            batchId, (System.currentTimeMillis() - startTime));
      } catch (IOException e) {
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
        out.write(chunk, 0, lenght);
        out.flush();
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
        bout.flush();
        totalTime = totalTime + (System.currentTimeMillis() - startTime);
        ++rowcounter;
        if (bfb.empty()) {
          bfb.getReader().close();
        } else {
          pq.add(bfb);
        }
      }
      LOGGER.info("Total time taken in ms to write data during merging {}", totalTime);
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
          return compareRow(i.peek().array(), j.peek().array());
        }
      });

  private int columnPos = 0;

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

  private int[] orderDimensions(int startPos) {
    for (int i = 0; i < shardDims.length; i++) {
      sortDims[i] = shardDims[(i + startPos) % shardDims.length];
    }
    return sortDims;
  }
}
