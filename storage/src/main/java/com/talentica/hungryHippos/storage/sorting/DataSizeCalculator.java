
package com.talentica.hungryHippos.storage.sorting;

/**
 * @author pooshans
 *
 */
public final class DataSizeCalculator {

  private static int OBJ_HEADER;
  private static int ARR_HEADER;
  private static int OBJ_REF;
  private static int INT_SIZE;
  private static boolean IS_64_BIT_JVM;

  private DataSizeCalculator() {

  }

  static {
    IS_64_BIT_JVM = true;
    String arch = System.getProperty("sun.arch.data.model");
    if (arch != null) {
      if (arch.contains("32")) {
        IS_64_BIT_JVM = false;
      }
    }
    OBJ_HEADER = IS_64_BIT_JVM ? 16 : 8;
    ARR_HEADER = IS_64_BIT_JVM ? 24 : 12;
    OBJ_REF = IS_64_BIT_JVM ? 8 : 4;
    INT_SIZE = 4;
  }

  public static long estimatedSizeOfRow(int totalByte) {
    return totalByte + getObjectOverhead();
  }

  public static long getObjectOverhead() {
    return OBJ_HEADER + OBJ_REF + ARR_HEADER;
  }

  public static long getArrayOverhead() {
    return ARR_HEADER;
  }

  public static int getIntSize() {
    return INT_SIZE;
  }

}
