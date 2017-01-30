package com.talentica.hungryHippos.utility;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.media.jfxmedia.events.NewFrameEvent;

/**
 * This class is used for splitting big input file. The first argument accepts the number of smaller
 * files to create. for example if inputfile is 50 gb and argument is 10, each file will contain
 * 5gb. also need to make sure while splitting the file, end line is not broken in half.
 * 
 * @author sudarshans
 *
 */
public class FileSplitter {

  private String filePath = null;
  private int numberOfChunks = 0;
  private long fileSizeInbytes = 0l;
  private long totalChunkSizeInbytes = 0l;
  private String dir = null;
  private Logger logger = LoggerFactory.getLogger(FileSplitter.class);
  private byte[] eof = System.lineSeparator().getBytes(StandardCharsets.UTF_8);
  private int idealBufferSize = 4 * 1024 * 1024; // 4mb

  public FileSplitter(String filePath, int numberOfChunks) {
    this.filePath = filePath;
    this.numberOfChunks = numberOfChunks;
  }


  /**
   * This method is used to start the file splitter, it takes two arguments and 1 optional argument.
   * 
   * 
   * @param args First argument :- user mention the filepath. Second argument :- number of chuncks.
   *        Third argument:- optional, directory location. if not mentioned the split files will be
   *        created in the parent directory of the input file.
   * 
   * @throws IOException
   */
  public List<Chunk> start() throws IOException {


    fileSizeInbytes = Files.size(Paths.get(filePath));
    System.out.println(fileSizeInbytes);
    totalChunkSizeInbytes = fileSizeInbytes / numberOfChunks;

    logger.info(
        "started splitting the input file of size  {}  in bytes into {} chunks of size approximately equivalent to {} in bytes ",
        fileSizeInbytes, numberOfChunks, totalChunkSizeInbytes);

    StopWatch stopWatch = new StopWatch();
    // List<Chunk> files = splitFile(totalChunkSizeInbytes);
    //List<Chunk> files = splitFileByte(totalChunkSizeInbytes);
     List<Chunk> files = splitFileByte_1(totalChunkSizeInbytes);
    logger.info("finished splitting file. It took {} seconds", stopWatch.elapsedTime());
    System.out.println("finished splitting file. It took " + stopWatch.elapsedTime() + " seconds");


    return files;
  }


  private List<Chunk> splitFile(long totalChunkSizeInbytes) throws IOException {
    int counter = 0;
    List<Chunk> chunks = new ArrayList<>();
    File file = new File(filePath);
    if (numberOfChunks == 1) {
      Chunk chunk = new Chunk(file.getParent(), file.getName(), counter, 0l, fileSizeInbytes,
          totalChunkSizeInbytes);
      chunks.add(chunk);
      return chunks;
    }


    String eof = System.lineSeparator();
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {

      String line = br.readLine();
      if (dir == null) {
        dir = file.getParent();
      }

      long startPos = 0l;
      long endPos = 0l;

      while (line != null) {

        // File chunkFile = new File(dir, "sampledata" + "-" + counter + ".txt");
        /*
         * try ( OutputStream outputStream = new BufferedOutputStream(new
         * FileOutputStream(chunkFile))) {
         */
        long fileSize = 0;

        while (line != null) {

          byte[] bytes = (line + eof).getBytes(StandardCharsets.UTF_8);

          if (fileSize + bytes.length > totalChunkSizeInbytes && counter < numberOfChunks - 2) {
            break;
          }
          // outputStream.write(bytes);
          fileSize += bytes.length;
          line = br.readLine();
        }
        endPos = endPos + fileSize;
        Chunk chunk = new Chunk(file.getParent(), file.getName(), counter++, startPos, endPos,
            totalChunkSizeInbytes);
        startPos = endPos;

        chunks.add(chunk);
      }
    }

    // }
    return Collections.unmodifiableList(chunks);

  }


  private List<Chunk> splitFileByte_1(long totalChunkSizeInbytes) throws IOException {
    int counter = 0;
    List<Chunk> chunks = new ArrayList<>();
    File file = new File(filePath);
    if (numberOfChunks == 1) {
      Chunk chunk = new Chunk(filePath, file.getName(), counter, 0l, fileSizeInbytes,
          totalChunkSizeInbytes);
      chunks.add(chunk);
      return chunks;
    }

    int idealBufferSize = 8192;

    InputStream inputStream = new FileInputStream(file);
    BufferedInputStream bis = new BufferedInputStream(inputStream);
    byte[] buffer = new byte[idealBufferSize];
    int read = 0;
    long bytesRead = 0;
    int carrier = 0;

    while (counter < numberOfChunks) {

      long startPos = bytesRead;
      long remainingBytesToRead = totalChunkSizeInbytes;
      long bytesToSkip = remainingBytesToRead - idealBufferSize;



      if (counter == numberOfChunks - 1) {

        bytesRead = fileSizeInbytes;

      } else {

        inputStream.skip(bytesToSkip);

        read = bis.read(buffer);

        int j = 0;

        for (int i = read - 1;; i--) {

          if (buffer[i] == eof[0]) {

            break;
          }
          j++;
        }

        bytesRead = bytesRead + totalChunkSizeInbytes - j;
        bytesRead += carrier;
        carrier = j;
      }
      Chunk chunk = new Chunk(filePath, file.getName(), counter++, startPos, bytesRead,
          totalChunkSizeInbytes);
      chunks.add(chunk);



      reset(buffer);

    }

    bis.close();

    return Collections.unmodifiableList(chunks);

  }

  private void reset(byte[] buffer) {

    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = 0;
    }

  }

  private List<Chunk> splitFileByte(long totalChunkSizeInbytes) throws IOException {
    int counter = 0;
    List<Chunk> chunks = new ArrayList<>();
    File file = new File(filePath);
    if (numberOfChunks == 1) {
      Chunk chunk = new Chunk(file.getParent(), file.getName(), counter, 0l, fileSizeInbytes,
          totalChunkSizeInbytes);
      chunks.add(chunk);
      return chunks;
    }

    InputStream inputStream = new FileInputStream(file);
    BufferedInputStream bis = new BufferedInputStream(inputStream);
    byte[] buffer = new byte[idealBufferSize];
    int read = 0;
    long bytesRead = 0;
    int carrier = 0;

    while (counter < numberOfChunks) {
      byte[] buffer1 = buffer;
      long startPos = bytesRead;
      long remainingBytesToRead = totalChunkSizeInbytes;

      while (remainingBytesToRead > 0) {

        if (remainingBytesToRead < idealBufferSize) {
          if (counter == numberOfChunks - 1) {
            read = bis.read(buffer1, 0, bis.available());
          } else {
            read = bis.read(buffer1, 0, (int) remainingBytesToRead);
          }
        } else {
          read = bis.read(buffer1);
        }
        remainingBytesToRead -= read;
        bytesRead += read;

      }

      int j = 0;

      for (int i = read - 1;; i--) {


        if (buffer1[i] == eof[0]) {

          break;
        }
        j++;
      }

      bytesRead -= j;
      bytesRead += carrier;
      Chunk chunk = new Chunk(filePath, file.getName(), counter++, startPos, bytesRead,
          totalChunkSizeInbytes);
      chunks.add(chunk);

      carrier = j;

    }

    bis.close();

    return Collections.unmodifiableList(chunks);

  }



}
