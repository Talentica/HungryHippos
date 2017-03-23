/*******************************************************************************
 * Copyright [2017] [Talentica Software Pvt. Ltd.]
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
package com.talentica.hungryHippos.client.data.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.talentica.hungryHippos.client.domain.DataDescription;
import com.talentica.hungryHippos.client.domain.DataTypes;
import com.talentica.hungryHippos.client.domain.InvalidRowException;
import com.talentica.hungryHippos.client.domain.MutableCharArrayString;

/**
 * Data parser implementation for line by line reading of data file.
 */
public abstract class LineByLineDataParser extends DataParser {

  public static final char[] WINDOWS_LINE_SEPARATOR_CHARS = {13, 10};

  private byte[] dataBytes = new byte[65536];
  private ByteBuffer buf = ByteBuffer.wrap(dataBytes);
  private int readCount = -1;
  private MutableCharArrayString buffer;

  /**
   * creates a new LineByLineDataParser which is used for parsing each line in the file proviuded.
   * @param dataDescription
   */
  public LineByLineDataParser(DataDescription dataDescription) {
    super(dataDescription);
    buf.clear();
  }

  @Override
  public Iterator<DataTypes[]> iterator(InputStream dataStream) {
    if (buffer == null) {
      buffer = new MutableCharArrayString(getDataDescription().getMaximumSizeOfSingleBlockOfData());
    }

    return new Iterator<DataTypes[]>() {

      @Override
      public boolean hasNext() {
        try {
          return dataStream.available() > 0 || readCount > 0;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public DataTypes[] next() {
        try {
          return read();
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (InvalidRowException e) {
          throw e;
        }
      }


      public DataTypes[] read() throws IOException, InvalidRowException {
        buffer.reset();
        while (true) {
          if (readCount <= 0) {
            buf.clear();
            readCount = dataStream.read(dataBytes);
            if (readCount != -1) {
              buf.limit(readCount);
            }
            if (readCount < 0 && buffer.length() > 0) {
              return processLine(buffer);
            } else if (readCount < 0 && buffer.length() <= 0) {
              return null;
            }
          }
          byte nextChar = readNextChar();
          if (isNewLine(nextChar)) {
            buffer.addCharacter((char) nextChar);
            break;
          }
          buffer.addCharacter((char) nextChar);
        }
        return processLine(buffer);
      }


      private byte readNextChar() {
        byte nextChar = buf.get();
        readCount--;
        return nextChar;
      }

      private boolean isNewLine(byte readByte) throws IOException {
        char[] windowsLineseparatorChars = WINDOWS_LINE_SEPARATOR_CHARS;
        if (windowsLineseparatorChars[1] == readByte) {
          return true;
        }
        boolean newLine = (windowsLineseparatorChars[0] == readByte);
        if (newLine) {
          for (int i = 1; i < windowsLineseparatorChars.length; i++) {
            if (readCount <= 0) {
              buf.clear();
              readCount = dataStream.read(dataBytes);
              if (readCount < 0) {
                return newLine;
              }
              buf.flip();
            }
            byte nextChar = readNextChar();
            newLine = newLine && (windowsLineseparatorChars[i] == nextChar);
          }
        }
        return newLine;
      }
    };
  }

  /**
   * client can implement their own parsing algorithm.
   * @param line
   * @return
   */
  protected abstract DataTypes[] processLine(MutableCharArrayString line);

}
