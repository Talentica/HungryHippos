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
package com.talentica.hungryHippos.storage;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by debasishc on 27/8/15.
 */
public interface DataStore {
  boolean storeRow(String storeId, byte[] raw) throws IOException;

  void storeRow(int index, byte[] raw);

  public void sync();

  String getHungryHippoFilePath();

  void reset();
}
