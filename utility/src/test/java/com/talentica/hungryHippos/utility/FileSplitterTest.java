package com.talentica.hungryHippos.utility;

import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Test;

public class FileSplitterTest {
  private String filepath = "/home/sudarshans/RD/HH_NEW1/HungryHippos/utility/sampledata.txt";
  private int numberOfChunks = 10;
  String eof = System.lineSeparator();


  @Test
  public void testStart() throws IOException {



    List<File> files = new ArrayList<>();

    files.add(new File("/home/sudarshans/RD/HH_NEW1/HungryHippos/utility/sampledata-1.txt"));
    files.add(new File("/home/sudarshans/RD/HH_NEW1/HungryHippos/utility/sampledata-2.txt"));
    files.add(new File("/home/sudarshans/RD/HH_NEW1/HungryHippos/utility/sampledata-3.txt"));
    files.add(new File("/home/sudarshans/RD/HH_NEW1/HungryHippos/utility/sampledata-4.txt"));
    files.add(new File("/home/sudarshans/RD/HH_NEW1/HungryHippos/utility/sampledata-5.txt"));
    files.add(new File("/home/sudarshans/RD/HH_NEW1/HungryHippos/utility/sampledata-6.txt"));
    files.add(new File("/home/sudarshans/RD/HH_NEW1/HungryHippos/utility/sampledata-7.txt"));
    files.add(new File("/home/sudarshans/RD/HH_NEW1/HungryHippos/utility/sampledata-8.txt"));
    files.add(new File("/home/sudarshans/RD/HH_NEW1/HungryHippos/utility/sampledata-9.txt"));
    files.add(new File("/home/sudarshans/RD/HH_NEW1/HungryHippos/utility/sampledata-10.txt"));

    FileSplitter fileSplitter = new FileSplitter(filepath, numberOfChunks);
    List<Chunk> chunks = fileSplitter.start();


    for (Chunk chunk : chunks) {
      System.out.println(chunk.toString());

    }

    List<File> chunkFiles = createFileFromChunk(chunks);
    // assertTrue(checkTwoFiles(files, chunkFiles));

    // assertTrue(verifyChunkContent(files, chunks));
    // assertTrue(verifyHHFStream(files, chunks));
  }

  private boolean checkTwoFiles(List<File> files, List<File> chunkFiles) throws IOException {
    boolean flag = true;
    Iterator<File> filesIterator = files.iterator();
    Iterator<File> chunkFilesIterator = chunkFiles.iterator();
    while (filesIterator.hasNext() && chunkFilesIterator.hasNext()) {
      File file = filesIterator.next();
      File chunkFile = chunkFilesIterator.next();
      Stream<String> fileLines = Files.lines(file.toPath());
      Stream<String> chunkFileLines = Files.lines(chunkFile.toPath());

      Iterator<String> fileLine = fileLines.iterator();
      Iterator<String> chunkFileLine = chunkFileLines.iterator();
      while (fileLine.hasNext() && chunkFileLine.hasNext()) {
        String line = fileLine.next();
        String line1 = chunkFileLine.next();

        if (!(line.equals(line1))) {
          flag = false;
          break;
        }

      }

      if (fileLine.hasNext() || chunkFileLine.hasNext()) {
        flag = false;
        break;
      }
      fileLines.close();
      chunkFileLines.close();
    }

    if (filesIterator.hasNext() || chunkFilesIterator.hasNext()) {
      flag = false;

    }


    return flag;
  }

  private List<File> createFileFromChunk(List<Chunk> chunks) throws IOException {
    List<File> files = new ArrayList<>();

    Iterator<Chunk> chunkIterator = chunks.iterator();
    while (chunkIterator.hasNext()) {
      Chunk chunk = chunkIterator.next();
      HHFStream hhfStream = chunk.getHHFStream();
      byte[] buffer = new byte[4 * 1024 * 1024];
      int read = 0;
      File chunkFile = new File(chunk.getFileName() + "-cp" + ".txt");
      try (OutputStream fos = new BufferedOutputStream(new FileOutputStream(chunkFile))) {
        while ((read = hhfStream.read(buffer)) != -1) {
          fos.write(buffer, 0, read);
        }
      }
      files.add(chunkFile);
    }
    return files;
  }

  private boolean verifyHHFStream(List<File> files, List<Chunk> chunks) throws IOException {
    boolean flag = true;
    Iterator<File> fileIterator = files.iterator();
    Iterator<Chunk> chunkIterator = chunks.iterator();
    byte[] buffer = new byte[1024];
    int next = 0;
    while (fileIterator.hasNext() && chunkIterator.hasNext()) {
      File file = fileIterator.next();
      Stream<String> fileLine = Files.lines(file.toPath());
      Iterator<String> iterator = fileLine.iterator();
      Chunk chunk = chunkIterator.next();
      HHFStream hhfStream = new HHFStream(chunk);

      Stream<byte[]> hhfsLines = null; // hhfStream.lines();
      Iterator<byte[]> hhfsIterator = hhfsLines.iterator();


      while (hhfsIterator.hasNext()) {
        byte[] line = hhfsIterator.next();
        String[] li = new String(line).split("\\n");
        int count = 0;
        int condition = 0;
        if (line[line.length - 1] == 0) {
          condition = li.length - 1;
        } else {
          condition = li.length;
        }

        while (iterator.hasNext() && count < condition) {
          String line1 = iterator.next();
          if (!(line1.equals(li[count]))) {

            System.out.println(line1);
            System.out.println(li[count]);
            System.out.println("failed here first");
            flag = false;
            break;
          }
          count++;
        }
      }

      if (hhfsIterator.hasNext() || iterator.hasNext()) {
        System.out.println("failed");
      }

      fileLine.close();
      hhfStream.close();
      System.out.println(++next);

    }


    return flag;
  }

  private boolean verifyChunkContent(List<File> files, List<Chunk> chunks) throws IOException {
    boolean flag = true;

    Iterator<File> fileIterator = files.iterator();
    Iterator<Chunk> chunkIterator = chunks.iterator();
    RandomAccessFile raf = new RandomAccessFile(new File(filepath), "rw");
    int count = 0;

    while (fileIterator.hasNext() && chunkIterator.hasNext()) {
      File file = fileIterator.next();
      Stream<String> fileLine = Files.lines(file.toPath());
      Iterator<String> iterator = fileLine.iterator();
      Chunk chunk = chunkIterator.next();
      long actualSizeTobeRead = chunk.getActualSizeOfChunk();

      try {

        raf.seek(chunk.getStart());

        while (actualSizeTobeRead != 0 && iterator.hasNext()) {
          String line = raf.readLine();
          String line1 = iterator.next();
          actualSizeTobeRead -= (line + eof).getBytes(StandardCharsets.UTF_8).length;
          if (!(line.equals(line1))) {
            System.out.println(line + " " + line1);
            System.out.println("failed here first");
            flag = false;
            break;
          }


        }
        count++;
        if (actualSizeTobeRead != 0 || iterator.hasNext() || !flag) {
          System.out.println(count);
          System.out.println(actualSizeTobeRead);
          System.out.println("failed here 2");
          break;
        }

      } catch (FileNotFoundException e) {
        flag = false;
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      fileLine.close();

    }
    raf.close();
    return flag;

  }

  private boolean checkLineByLine(String inputFile, List<File> splitFiles) {
    boolean flag = true;
    try {
      Stream<String> inputStream = Files.lines(Paths.get(inputFile));
      Iterator<String> inputStreamIterator = inputStream.iterator();
      for (File file : splitFiles) {
        Stream<String> splitStream = Files.lines(file.toPath());
        Iterator<String> splitStreamIterator = splitStream.iterator();
        while (inputStreamIterator.hasNext() && splitStreamIterator.hasNext()) {
          if (!(inputStreamIterator.next().equals(splitStreamIterator.next()))) {
            flag = false;
            break;
          }
        }
        splitStream.close();
      }
      inputStream.close();

    } catch (IOException e) {
      flag = false;
    }


    return flag;

  }
}
