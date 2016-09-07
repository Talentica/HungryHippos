package com.talentica.hungryHippos.storage.sorting;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExternalSort {

  private static int defaultCharBufferSize = 8192;
  public static long TEMPFILES;
  public static final Logger LOGGER = LoggerFactory.getLogger(ExternalSort.class);

  public static void main(String[] args) throws IOException, InsufficientMemoryException {
    long startTIme = System.currentTimeMillis();
    ExternalSort externalSort = new ExternalSort();
    externalSort.validateArguments(args);
    String tmpDirPath = (args.length == 2) ? args[1] : null;
    File inputFlatFile = new File(args[0]);
    File tmpDir = (tmpDirPath == null ? null : new File(tmpDirPath));
    File f2 = File.createTempFile("hh_", "_sorted_flat_file", tmpDir);
    externalSort.mergeSortedFiles(
        externalSort.sortInBatch(inputFlatFile, tmpDir, Charset.defaultCharset()), f2,
        Charset.defaultCharset(), true);
    LOGGER.info("Total time taken in sec {} ", ((System.currentTimeMillis() - startTIme) / 1000));

  }

  private int mergeSortedFiles(List<File> files, File outputfile, Charset cs, boolean append)
      throws IOException {
    long startTIme = System.currentTimeMillis();
    LOGGER.info("Now merging sorted files...");
    ArrayList<BinaryFileBuffer> bfbs = new ArrayList<>();
    for (File f : files) {
      InputStream in = new FileInputStream(f);
      BufferedReader br;
      br = new BufferedReader(new InputStreamReader(in, cs));
      BinaryFileBuffer bfb = new BinaryFileBuffer(br);
      bfbs.add(bfb);
    }
    BufferedWriter fbw =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputfile, append), cs));
    int rowcounter = mergeSortedFiles(fbw, bfbs);
    for (File f : files) {
      f.delete();
    }
    LOGGER.info("Sorted files are merged successfully.");
    LOGGER.info("Total merging time taken in sec {} ", ((System.currentTimeMillis() - startTIme) / 1000));
    return rowcounter;
  }

  private int mergeSortedFiles(BufferedWriter fbw, List<BinaryFileBuffer> buffers)
      throws IOException {
    for (BinaryFileBuffer bfb : buffers) {
      if (!bfb.empty()) {
        pq.add(bfb);
      }
    }
    int rowcounter = 0;
    try {
      String lastLine = null;
      if (pq.size() > 0) {
        BinaryFileBuffer bfb = pq.poll();
        lastLine = bfb.pop();
        fbw.write(lastLine);
        fbw.newLine();
        ++rowcounter;
        if (bfb.empty()) {
          bfb.getReader().close();
        } else {
          pq.add(bfb);
        }
      }
      while (pq.size() > 0) {
        BinaryFileBuffer bfb = pq.poll();
        String r = bfb.pop();
        if (defaultcomparator.compare(r, lastLine) != 0) {
          fbw.write(r);
          fbw.newLine();
          lastLine = r;
        }
        ++rowcounter;
        if (bfb.empty()) {
          bfb.getReader().close();
        } else {
          pq.add(bfb);
        }
      }
    } finally {
      fbw.close();
      for (BinaryFileBuffer bfb : pq) {
        bfb.close();
      }
    }
    return rowcounter;
  }

  private PriorityQueue<BinaryFileBuffer> pq =
      new PriorityQueue<>(11, new Comparator<BinaryFileBuffer>() {
        @Override
        public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
          return defaultcomparator.compare(i.peek(), j.peek());
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
    LOGGER.info("Number of blocks {} and each block size in bytes {}", TEMPFILES, blocksize);
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

  private Comparator<String> defaultcomparator = new Comparator<String>() {
    @Override
    public int compare(String r1, String r2) {
      return r1.compareTo(r2);
    }
  };

  private List<File> sortInBatch(File file, File tmpDirectory, Charset charset)
      throws IOException, InsufficientMemoryException {
    BufferedReader fbr =
        new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
    return sortInBatch(fbr, file.length(), availableMemory(), charset, tmpDirectory);
  }

  private List<File> sortInBatch(final BufferedReader fbr, final long datalength,
      long maxFreeMemory, final Charset cs, final File tmpdirectory)
      throws IOException, InsufficientMemoryException {
    long startTIme = System.currentTimeMillis();
    List<File> files = new ArrayList<>();
    long blocksize = getSizeOfBlocks(datalength, maxFreeMemory);
    try {
      List<String> tmplist = new ArrayList<>();
      String line = "";
      try {
        while (line != null) {
          long currentblocksize = 0;
          while ((currentblocksize < blocksize) && ((line = fbr.readLine()) != null)) {
            tmplist.add(line);
            currentblocksize += StringSize.estimatedSizeOfLine(line);
          }
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
      fbr.close();
    }
    LOGGER.info("Total sorting time taken in sec {} ", ((System.currentTimeMillis() - startTIme) / 1000));
    return files;
  }

  private int batchId = 0;
  private File sortAndSave(List<String> tmpList, Charset cs, File tmpDirectory)
      throws IOException {
    LOGGER.info("Batch id {} is getting sorted and saved",(batchId++));
    Collections.sort(tmpList, defaultcomparator);
    File newtmpfile = File.createTempFile("batch", "sorted_flat_file", tmpDirectory);
    LOGGER.info("Temporary directory {}", newtmpfile.getAbsolutePath());
    newtmpfile.deleteOnExit();
    OutputStream out = new FileOutputStream(newtmpfile);
    try (BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(out, cs))) {
      String lastLine = null;
      Iterator<String> i = tmpList.iterator();
      if (i.hasNext()) {
        lastLine = i.next();
        fbw.write(lastLine);
        fbw.newLine();
      }
      while (i.hasNext()) {
        String r = i.next();
        if (defaultcomparator.compare(r, lastLine) != 0) { // check duplicate line and skip if any.
          fbw.write(r);
          fbw.newLine();
          lastLine = r;
        }
      }
    }
    return newtmpfile;
  }

  private void validateArguments(String[] args) {
    if (args != null && args.length < 1) {
      throw new RuntimeException(
          "Invalid argument. Please provide 1st argument as input file and 2nd argument(optional) tmp directory.");
    }
  }
}
