package com.talentica.hungryHippos.coordination.utility.marshaling;

import java.io.IOException;

import com.talentica.hungryHippos.client.data.parser.DataParser;
import com.talentica.hungryHippos.client.domain.DataTypes;

/**
 * 
 * {@code Reader} must be implemented by client so that they can read a file and using a {@link DataParser}
 *
 */
public interface Reader {

  /**
   * used for reading.
   * 
   * @return
   * @throws RuntimeException
   */
  DataTypes[] read() throws RuntimeException;

  /**
   * to close the stream.
   * 
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * to reset the stream.
   * 
   * @throws IOException
   */
  void reset() throws IOException;

}
