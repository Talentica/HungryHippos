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
import java.util.ArrayList;
import java.util.List;

/**
 * Created by debasishc on 5/10/15.
 */
public class ConfigurableDataGenerator {

  public final static char[] allChars;
  public final static char[] allNumbers;

  static {
    allChars = new char[26];
    for (int i = 0; i < 26; i++) {
      allChars[i] = (char) ('a' + i);
    }

    allNumbers = new char[10];
    for (int i = 0; i < 10; i++) {
      allNumbers[i] = (char) ('0' + i);
    }
  }

  private static List<String> generateAllCombinations(int numChars, char[] sourceChars) {

    List<String> retList = new ArrayList<>();
    if (numChars <= 0) {

      retList.add("");
      return retList;
    }
    List<String> listForTheRest = generateAllCombinations(numChars - 1, sourceChars);
    for (char c : sourceChars) {
      for (String source : listForTheRest) {
        if("0".equals(source))
          retList.add(""+c);
        else{
          retList.add(source+c);
        }
      }
    }
    return retList;
  }

  private static double skewRandom() {
    double start = Math.random();
    return start * start;
  }

  private static class ColumnConfig {
    public final int count;
    public final String[] valueSet;

    public ColumnConfig(char[] sourceChars, int count) {
      this.count = count;
      this.valueSet = generateAllCombinations(count, sourceChars).toArray(new String[0]);
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      System.out.println("Usage java com.talentica.hungryHippos.utility.ConfigurableDataGenerator "
          + "[fileSizeInMbs] [filename] [column_desc]*");
      System.out.println("column_desc := (C|N)':'[number_of_characters]");
      System.exit(0);
    }
    long fileSizeInMbs = Long.parseLong(args[0]);

    long filesizeInBytes = fileSizeInMbs * 1024 * 1024;

    String filename = args[1];

    ColumnConfig[] configs = new ColumnConfig[args.length - 2];

    for (int i = 0; i < args.length - 2; i++) {
      String[] parts = args[i + 2].split(":");
      char[] sourceChars = null;
      switch (parts[0]) {
        case "C":
          sourceChars = allChars;
          break;
        case "N":
          sourceChars = allNumbers;
          break;
      }
      int count = Integer.parseInt(parts[1]);
      ColumnConfig config = new ColumnConfig(sourceChars, count);
      configs[i] = config;
    }
    FileOutputStream fos = new FileOutputStream(new File(filename), true);
    PrintWriter out = new PrintWriter(
        new OutputStreamWriter(fos), true);
    long start = System.currentTimeMillis();
    long sizeOfDataWrote = 0l;
    while (sizeOfDataWrote < filesizeInBytes) {
      for (int j = 0; j < configs.length; j++) {
        sizeOfDataWrote = sizeOfDataWrote + configs[j].count;
        int toSelct = (int) (skewRandom() * configs[j].valueSet.length);
        out.print(configs[j].valueSet[toSelct]);
        if (j < configs.length - 1) {
          out.print(",");
          sizeOfDataWrote = sizeOfDataWrote + 1;
        }
      }
      out.println();
    }
    long end = System.currentTimeMillis();
    out.flush();
    fos.flush();
    out.close();
    fos.close();
    System.out.println("Time taken in ms: " + (end - start));

  }
}
