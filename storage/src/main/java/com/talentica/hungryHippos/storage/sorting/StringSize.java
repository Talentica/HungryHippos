
package com.talentica.hungryHippos.storage.sorting;

public final class StringSize {

  private static int OBJ_HEADER;
  private static int ARR_HEADER;
  private static int INT_FIELDS;
  private static int OBJ_REF;
  private static int OBJ_OVERHEAD;
  private static boolean IS_64_BIT_JVM;

  private StringSize() {

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
    INT_FIELDS = 12;
    OBJ_OVERHEAD = OBJ_HEADER + INT_FIELDS + OBJ_REF + ARR_HEADER;

  }

  public static long estimatedSizeOfLine(int totalByte) {
    return  totalByte + OBJ_OVERHEAD;
  }

}
