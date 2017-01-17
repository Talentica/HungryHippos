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

  private static String filePath = null;
  private static int numberOfChunks = 0;
  private static long fileSizeInbytes = 0l;
  private static long totalChunkSizeInbytes = 0l;
  private static String dir = null;
  private static Logger logger = LoggerFactory.getLogger(FileSplitter.class);
  private static byte[] eof = System.lineSeparator().getBytes(StandardCharsets.UTF_8);
  private static int idealBufferSize = 4 * 1024 * 1024; // 4mb



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
  public static List<Chunk> start(String[] args) throws IOException {

    validate(args);
    fileSizeInbytes = Files.size(Paths.get(filePath));
    System.out.println(fileSizeInbytes);
    totalChunkSizeInbytes = fileSizeInbytes / numberOfChunks;
    if (args.length == 3) {
      dir = args[2];
      if (!(Files.exists(Paths.get(dir)))) {
        Set<PosixFilePermission> posixFilePermission = new HashSet<>();
        posixFilePermission.add(PosixFilePermission.OWNER_WRITE);
        posixFilePermission.add(PosixFilePermission.OWNER_READ);
        posixFilePermission.add(PosixFilePermission.OWNER_EXECUTE);

        posixFilePermission.add(PosixFilePermission.GROUP_READ);
        posixFilePermission.add(PosixFilePermission.GROUP_EXECUTE);

        posixFilePermission.add(PosixFilePermission.OTHERS_READ);
        posixFilePermission.add(PosixFilePermission.OTHERS_EXECUTE);

        FileAttribute<?> attrs = PosixFilePermissions.asFileAttribute(posixFilePermission);
        Files.createDirectories(Paths.get(dir), attrs);
      }
    }

    logger.info(
        "started splitting the input file of size  {}  in bytes into {} chunks of size approximately equivalent to {} in bytes ",
        fileSizeInbytes, numberOfChunks, totalChunkSizeInbytes);
    StopWatch stopWatch = new StopWatch();
    List<Chunk> files = splitFile(filePath, totalChunkSizeInbytes);
    logger.info("finished splitting file. It took {} seconds", stopWatch.elapsedTime());

    System.out.println("finished splitting file. It took " + stopWatch.elapsedTime() + " seconds");
    return files;
  }


  private static List<Chunk> splitFile(String filePath, long totalChunkSizeInbytes)
      throws IOException {
    int counter = 0;
    File file = new File(filePath);
    List<Chunk> chunks = new ArrayList<>();
    String eof = System.lineSeparator();
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {

      String line = br.readLine();
      if (dir == null) {
        dir = file.getParent();
      }

      long startPos = 0l;
      long endPos = 0l;

      while (line != null) {
        counter++;
        File chunkFile = new File(dir, "sampledata" + "-" + counter + ".txt");
        try (
            OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(chunkFile))) {

          long fileSize = 0;

          while (line != null) {

            byte[] bytes = (line + eof).getBytes(StandardCharsets.UTF_8);

            if (fileSize + bytes.length > totalChunkSizeInbytes && counter < numberOfChunks) {
              break;
            }
            outputStream.write(bytes);
            fileSize += bytes.length;
            line = br.readLine();
          }
          endPos = endPos + fileSize;
          Chunk chunk = new Chunk(filePath, counter, startPos, endPos, totalChunkSizeInbytes);
          startPos = endPos;

          chunks.add(chunk);
        }
      }

    }
    return Collections.unmodifiableList(chunks);

  }


  private static List<Chunk> splitFileByte(String filePath, long totalChunkSizeInbytes)
      throws IOException {
    int counter = 0;
    File file = new File(filePath);
    List<Chunk> chunks = new ArrayList<>();
    InputStream inputStream = new FileInputStream(file);
    BufferedInputStream bis = new BufferedInputStream(inputStream);
    byte[] buffer = new byte[4 * 1024 * 1024];
    int read = 0;
    int bytesRead = 0;

    while (counter < numberOfChunks) {
      byte[] buffer1 = buffer;
      int startPos = bytesRead;
      long totalChunkSizeInbytesTemp = totalChunkSizeInbytes;

      File chunkFile = new File(dir, "sampledata" + "-" + ++counter + ".txt");

      try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(chunkFile))) {
        while (totalChunkSizeInbytesTemp > 0) {

          if (totalChunkSizeInbytesTemp < idealBufferSize) {
            // create new buffer
            buffer1 = new byte[(int) totalChunkSizeInbytesTemp];
          }

          if ((read = bis.read(buffer1)) != -1) {
            totalChunkSizeInbytesTemp -= read;
            bytesRead += read;
          } else if (read == -1) {

            break;
          }
          if (totalChunkSizeInbytesTemp == 0 || totalChunkSizeInbytesTemp < 0) {
            int j = 0;
            for (int i = buffer1.length - 1;; i--) {

              if (buffer1[i] == eof[0]) {
                break;
              }
              buffer1[i] = 0;
              j++;

            }

            bytesRead -= j;
            bis.mark(bytesRead);
            bis.reset();
          }
          outputStream.write(buffer1);
        }
        // counter++;
        Chunk chunk = new Chunk(filePath, counter, startPos, bytesRead, totalChunkSizeInbytes);
        System.out.println(chunk.toString());
        chunks.add(chunk);

      }

    }
    bis.close();
    return Collections.unmodifiableList(chunks);

  }



  private static void validate(String... args) {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "provide the path of the input file and number of file chunks to make.");
    }
    filePath = args[0];
    numberOfChunks = Integer.parseInt(args[1]);

  }


}
