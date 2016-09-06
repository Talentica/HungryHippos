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

public class ExternalSort {

  public static final int DEFAULTMAXTEMPFILES = 1024;

  public static void main(String[] args) throws IOException {
    long startTIme =  System.currentTimeMillis();
    File inputFlatFile = new File("/home/pooshans/HungryHippos/HungryHippos/utility/sampledata.txt");    
    File f2 = File.createTempFile("hh_", "_sorted");
    ExternalSort.mergeSortedFiles(ExternalSort.sortInBatch(inputFlatFile, null, Charset.defaultCharset()), f2,
        Charset.defaultCharset(), true);
    System.out.println("Total time taken in sec :: "+((System.currentTimeMillis()-startTIme)/1000));

  }

  public static int mergeSortedFiles(List<File> files, File outputfile, Charset cs, boolean append)
      throws IOException {
    System.out.println("Started merging sorted file...");
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
    System.out.println("Sorted files are merged successfully.");
    return rowcounter;
  }

  public static int mergeSortedFiles(BufferedWriter fbw, List<BinaryFileBuffer> buffers)
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

  static PriorityQueue<BinaryFileBuffer> pq =
      new PriorityQueue<>(11, new Comparator<BinaryFileBuffer>() {
        @Override
        public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
          return defaultcomparator.compare(i.peek(), j.peek());
        }
      });

  public static long sizeOfBlocks(final long fileSize, final int maxTempFiles,
      final long maxMemory) {
    long blocksize = fileSize / maxTempFiles + (fileSize % maxTempFiles == 0 ? 0 : 1);

    if (blocksize < maxMemory / 2) {
      blocksize = maxMemory / 2;
    }
    System.out.println("Block size in bytes :: ",);
    return blocksize;
  }

  public static long availableMemory() {
    System.gc();
    Runtime runtime = Runtime.getRuntime();
    long allocatedMemory = runtime.totalMemory() - runtime.freeMemory();
    long presFreeMemory = runtime.maxMemory() - allocatedMemory;
    return presFreeMemory;
  }

  public static Comparator<String> defaultcomparator = new Comparator<String>() {
    @Override
    public int compare(String r1, String r2) {
      return r1.compareTo(r2);
    }
  };

  public static List<File> sortInBatch(File file, File tmpdirectory, Charset charset)
      throws IOException {
    BufferedReader fbr =
        new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
    return sortInBatch(fbr, file.length(), DEFAULTMAXTEMPFILES, availableMemory(), charset, null);
  }

  public static List<File> sortInBatch(final BufferedReader fbr, final long datalength,
      final int maxtmpfiles, long maxMemory, final Charset cs, final File tmpdirectory)
      throws IOException {
    List<File> files = new ArrayList<>();
    long blocksize = sizeOfBlocks(datalength, maxtmpfiles, maxMemory);
    try {
      List<String> tmplist = new ArrayList<>();
      String line = "";
      try {
        while (line != null) {
          long currentblocksize = 0;
          while ((currentblocksize < blocksize) && ((line = fbr.readLine()) != null)) {
            tmplist.add(line);
            currentblocksize += StringSize.estimatedSize(line);
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
    return files;
  }

  public static File sortAndSave(List<String> tmpList, Charset cs, File tmpdirectory)
      throws IOException {
    Collections.sort(tmpList, defaultcomparator);
    File newtmpfile = File.createTempFile("sortInBatch", "flatfile", tmpdirectory);
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
        if (defaultcomparator.compare(r, lastLine) != 0) {
          fbw.write(r);
          fbw.newLine();
          lastLine = r;
        }
      }
    }
    return newtmpfile;
  }

}


final class BinaryFileBuffer {
  private BufferedReader fbr;
  private String cache;

  public BinaryFileBuffer(BufferedReader r) throws IOException {
    this.fbr = r;
    reload();
  }

  public void close() throws IOException {
    this.fbr.close();
  }

  public boolean empty() {
    return this.cache == null;
  }

  public String peek() {
    return this.cache;
  }

  public String pop() throws IOException {
    String answer = peek().toString();// make a copy
    reload();
    return answer;
  }

  private void reload() throws IOException {
    this.cache = this.fbr.readLine();
  }

  public BufferedReader getReader() {
    return fbr;
  }
}
