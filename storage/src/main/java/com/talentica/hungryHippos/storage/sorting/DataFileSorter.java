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
import com.talentica.hungryHippos.coordination.utility.marshaling.DynamicMarshal;
import com.talentica.hungryHippos.sharding.context.ShardingApplicationContext;


/**
 * @author pooshans
 *
 */
public class DataFileSorter {

  public static final int DEFAULTMAXTEMPFILES = 1024;
  public static final Logger LOGGER = LoggerFactory.getLogger(DataFileSorter.class);
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
    dynamicMarshal.setDimensions(shardDims);
    dataFileHeapSort = new DataFileHeapSort(dataDescription.getSize(), dynamicMarshal);
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
    dataFileSorted.orderDimensions(sortDims, key << 1);
    dynamicMarshal.setDimensions(sortDims);
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
    long startTime = System.currentTimeMillis();
    int noOfBytesInOneDataSet = dataDescription.getSize();
    List<File> files = new ArrayList<>();
    int blocksize = getSizeOfBlocks(datalength, maxFreeMemory);
    int effectiveBlockSizeBytes =
        ((int) ((blocksize) / (noOfBytesInOneDataSet))) * noOfBytesInOneDataSet;
    byte[] chunk ;
    if(blocksize > datalength){
      chunk = new byte[(int) datalength];
    }else{
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
          LOGGER.info("Time taken to read the chunk in ms {}",(System.currentTimeMillis() - startTimeChunkRead));
          readBytesLength = effectiveBlockSizeBytes;
        } else { // remaining chunk or for singal block which totally fit in memory.
          startTimeChunkRead = System.currentTimeMillis();
          dataInputStream.readFully(chunk,0,(int)dataFileSize);
          LOGGER.info("Time taken to read the chunk in ms {}",(System.currentTimeMillis() - startTimeChunkRead));
          readBytesLength = (int) dataFileSize;
        }
        dataFileSize = dataFileSize - chunk.length;
        if (dataFileSize == 0 && batchId == 0) {
          files.add(sortAndSave(chunk, cs, outputFile, batchId, true,readBytesLength));
        } else {
          files.add(sortAndSave(chunk, cs, outputdirectory, batchId, false,readBytesLength));
        }
        batchId++;
      }
    } catch(EOFException eof){
      dataFileSize = dataFileSize - chunk.length;
      files.add(sortAndSave(chunk, cs, outputdirectory, batchId, (dataFileSize == 0 && batchId == 0),readBytesLength));
    }catch (Exception e) {
      LOGGER.error("Unable to process due to {}", e.getMessage());
      throw e;
    } finally {
      dataInputStream.close();
    }
    LOGGER.info("Total sorting time taken in sec {} ",
        ((System.currentTimeMillis() - startTime) / 1000));
    return files;
  }

  private File sortAndSave(byte[] chunk, Charset cs, File output, int batchId,
      boolean isSingalBatch,int lenght) throws IOException {
    LOGGER.info("Batch id {} is getting sorted and saved", (batchId));
    LOGGER.info("Sorting started for chunk size {}...", chunk.length);
    dataFileHeapSort.setChunk(chunk);
    dataFileHeapSort.heapSort();
    LOGGER.info("Sorting completed.");
    File file;
    if (isSingalBatch) {
      if(output.exists()){
        output.delete();
        output.createNewFile();
      }
      FileOutputStream fos = new FileOutputStream(output);
      BufferedOutputStream bout=new BufferedOutputStream(fos);  
      try {
        long startTime = System.currentTimeMillis();
        bout.write(chunk);
        bout.flush();
        LOGGER.info("Total time taken in ms to write data after sorting and saving batch id {} ,  {}",batchId,(System.currentTimeMillis() - startTime));
      } catch (IOException e) {
        LOGGER.info("Unable to write into file {}", e.getMessage());
        throw e;
      } finally {
        if (bout != null)
          bout.close();
      }
      file = output;
    } else {
      file = File.createTempFile("tmp_", "_sorted_file", output);
      LOGGER.info("Temporary directory {}", file.getAbsolutePath());
      file.deleteOnExit();
      OutputStream out = new FileOutputStream(file);
      try {
        long startTime = System.currentTimeMillis();
        out.write(chunk,0,lenght);
        out.flush();
        LOGGER.info("Total time taken in ms to write data after sorting and saving batch id {} ,  {}",batchId,(System.currentTimeMillis() - startTime));
      } finally {
        out.flush();
        if (out != null)
          out.close();
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
    OutputStream os = new FileOutputStream(outputfile, append);
    for (BinaryFileBuffer bfb : buffers) {
      if (!bfb.empty()) {
        pq.add(bfb);
      }
    }
    if(outputfile.exists()) {
      outputfile.delete();
      outputfile.createNewFile();
      }
    FileOutputStream fos = new FileOutputStream(outputfile);
    BufferedOutputStream bout=new BufferedOutputStream(fos);  
    try {
      long startTime;
      long totalTime = 0l;
      while (pq.size() > 0) {
        BinaryFileBuffer bfb = pq.poll();
        ByteBuffer row = bfb.peek();
        startTime = System.currentTimeMillis();
        bout.write(row.array());
        bout.flush();
        totalTime = totalTime + (System.currentTimeMillis() -startTime);
        ++rowcounter;
        bfb.reload();
        if (bfb.empty()) {
          bfb.getReader().close();
        } else {
          pq.add(bfb);
        }
      }
      LOGGER.info("Total time taken in ms to write data during merging {}",totalTime);
    } finally {
      bout.flush();
      bout.close();
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
  
  private int compareRow(byte[] row1,byte[] row2){
    int res = 0;
    for (int dim = 0; dim < shardDims.length; dim++) {
      DataLocator locator = dataDescription.locateField(dim);
      for(int pointer = 0 ; pointer < locator.getSize() ; pointer++ ){
        if(row1[pointer] != row2[pointer]) {
          return row1[pointer] - row2[pointer];
        }
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

  private DynamicMarshal getDynamicMarshal() throws ClassNotFoundException, FileNotFoundException,
      KeeperException, InterruptedException, IOException, JAXBException {
    dataDescription = context.getConfiguredDataDescription();
    dataDescription.setKeyOrder(context.getShardingDimensions());
    DynamicMarshal dynamicMarshal = new DynamicMarshal(dataDescription);
    return dynamicMarshal;
  }

  private void orderDimensions(int[] sortOrderDims, int startPos) {
    for (int i = 0; i < shardDims.length; i++) {
      sortOrderDims[i] = shardDims[(i + startPos) % shardDims.length];
    }
  }
}
