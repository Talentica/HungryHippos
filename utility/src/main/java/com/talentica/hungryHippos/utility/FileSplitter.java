/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.utility;

import java.io.*;
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

    int idealBufferSize = 100;


    byte[] buffer = new byte[idealBufferSize];
    int read = 0;
    long endPos = 0;
    int extraRead = 0;
    int prevRem = 0;
    long startPos =0;
    boolean flag = true;
    logger.info("fileSizeInbytes : {}",fileSizeInbytes);
    while (flag) {

      if (sizeOfChunk>=fileSizeInbytes-startPos-prevRem) {
        endPos = fileSizeInbytes;
        flag = false;
      }else{
        FileInputStream inputStream = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(inputStream);
        bis.skip(startPos+sizeOfChunk);
        boolean prevCharCR = false;
        boolean foundLineSeparator = false;
        extraRead=0;
        while((read=bis.read(buffer))>-1&&!foundLineSeparator){
          for (int i = read - 1;i>=0; i--) {
            if (buffer[i] == '\n') {
              extraRead += (i+1);
              endPos = startPos+prevRem+sizeOfChunk+extraRead;
              foundLineSeparator = true;
              break;
            }
            if (prevCharCR) { //CR + notLF, we are at notLF
              extraRead += (i+1);
              endPos = startPos+prevRem+sizeOfChunk+extraRead;
              foundLineSeparator = true;
              break;
            }
            prevCharCR = (buffer[i] == '\r');
          }
          extraRead+=read;
        }
        bis.close();
        inputStream.close();
      }
      Chunk chunk =
          new Chunk(filePath, file.getName(), counter++, startPos, endPos, sizeOfChunk);
      chunks.add(chunk);
      reset(buffer);
      startPos = endPos;
    }

    return Collections.unmodifiableList(chunks);

  }

  private void reset(byte[] buffer) {

    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = 0;
    }

  }


}
