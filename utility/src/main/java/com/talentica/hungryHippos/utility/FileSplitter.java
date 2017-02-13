package com.talentica.hungryHippos.utility;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private long sizeOfChunk;


  private Logger logger = LoggerFactory.getLogger(FileSplitter.class);
  private byte[] eof = System.lineSeparator().getBytes(StandardCharsets.UTF_8);

  public FileSplitter(String filePath, long sizeOfChunk) throws IOException {
    this.sizeOfChunk = sizeOfChunk;
    this.filePath = filePath;
    this.fileSizeInbytes = Files.size(Paths.get(filePath));
    numberOfChunks = (int) Math.ceil((fileSizeInbytes * 1.0 / sizeOfChunk));
  }

  public int getNumberOfchunks() {
    return numberOfChunks;
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

    logger.info(
        "started splitting the input file of size  {}  in bytes into {} chunks of size approximately equivalent to {} in bytes ",
        fileSizeInbytes, numberOfChunks, sizeOfChunk);

    StopWatch stopWatch = new StopWatch();

    List<Chunk> files = splitFileByte();
    logger.info("finished splitting file. It took {} seconds", stopWatch.elapsedTime());
    System.out.println("finished splitting file. It took " + stopWatch.elapsedTime() + " seconds");

    return files;
  }



  private List<Chunk> splitFileByte() throws IOException {
    int counter = 0;
    List<Chunk> chunks = new ArrayList<>();
    File file = new File(filePath);
    if (numberOfChunks == 1) {
      Chunk chunk = new Chunk(filePath, file.getName(), counter, 0l, fileSizeInbytes, sizeOfChunk);
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
      long remainingBytesToRead = sizeOfChunk;
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

        bytesRead = bytesRead + sizeOfChunk - j;
        bytesRead += carrier;
        carrier = j;
      }
      Chunk chunk =
          new Chunk(filePath, file.getName(), counter++, startPos, bytesRead, sizeOfChunk);
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


}
