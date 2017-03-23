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
