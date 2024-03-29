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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by debasishc on 20/8/15.
 */
public class DataGenerator {


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
  public static String[] key1ValueSet = generateAllCombinations(2, allChars).toArray(new String[0]);

  public static String[] key2ValueSet = generateAllCombinations(1, allChars).toArray(new String[0]);

  public static String[] key3ValueSet = generateAllCombinations(2, allChars).toArray(new String[0]);

  public static String[] key4ValueSet = generateAllCombinations(2, allChars).toArray(new String[0]);

  public static String[] key5ValueSet = generateAllCombinations(2, allChars).toArray(new String[0]);

  /*
   * public static String[] key6ValueSet = generateAllCombinations(3, allNumbers).toArray(new
   * String[0]);
   * 
   * public static String[] key7ValueSet = generateAllCombinations(5, allNumbers).toArray(new
   * String[0]);
   */
  private static List<String> generateAllCombinations(int numChars, char[] sourceChars) {

    List<String> retList = new ArrayList<>();
    if (numChars <= 0) {

      retList.add("");
      return retList;
    }
    List<String> listForTheRest = generateAllCombinations(numChars - 1, sourceChars);
    for (char c : sourceChars) {
      for (String source : listForTheRest) {
        retList.add(c + source);
      }
    }
    return retList;
  }

  private static List<String> generateAllCombinations_1(int value, char[] sourceChars) {
    Random ran = new Random();
    int numChars = ran.nextInt(value) + 1;
    return generateAllCombinations(numChars, sourceChars);

  }

  private static double skewRandom() {
    double start = Math.random();
    return start * start;
  }

  public static void main(String[] args) throws FileNotFoundException {
    long entryCount = Long.parseLong(args[0]);
    PrintWriter out = new PrintWriter(new File("sampledata.txt"));
    long start = System.currentTimeMillis();
    System.out.println(generateAllCombinations(3, allNumbers));
    Random ran = new Random();
    for (int i = 0; i < entryCount; i++) {
      int i1 = (int) (key1ValueSet.length * skewRandom());
      int i2 = (int) (key2ValueSet.length * skewRandom());
      int i3 = (int) (key3ValueSet.length * skewRandom());
      int i4 = (int) (key4ValueSet.length * skewRandom());
      int i5 = (int) (key5ValueSet.length * skewRandom());
      // int i6 = (int) (key6ValueSet.length * skewRandom());

      String key1 = key1ValueSet[i1];
      int key2 = ran.nextInt(50) + 10;
      int key3 = ran.nextInt(60) + 10;
      String key4 = key4ValueSet[i4];
      int key5 = ran.nextInt(899) + 100;
      int key6 = ran.nextInt(89999) + 10000;
      String key7 = key3ValueSet[i3];
      String key8 = key5ValueSet[i5];

      out.println(key1 + "," + key2 + "," + key3 + "," + key4 + "," + key5 + "," + key6 + "," + key7
          + "," + key8 + ",xy");
    }
    long end = System.currentTimeMillis();
    out.flush();
    out.close();
    System.out.println("Time taken in ms: " + (end - start));

  }
}
